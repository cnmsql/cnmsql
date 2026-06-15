/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func (r *ClusterReconciler) ensureCertificates(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	certs := cluster.Spec.Certificates
	needServerCertificates := certs == nil || certs.ServerTLSSecret == ""
	needClientCertificate := certs == nil || certs.ReplicationTLSSecret == ""
	needCAIssuer := needServerCertificates || needClientCertificate
	if needCAIssuer && (certs == nil || certs.ServerCASecret == "") {
		if err := r.ensureIssuer(ctx, cluster, plan.SelfSignedIssuer, map[string]any{
			"selfSigned": map[string]any{},
		}); err != nil {
			return err
		}
		if err := r.ensureCertificate(ctx, cluster, plan.CAIssuer, map[string]any{
			"secretName": plan.ServerCASecretName,
			"isCA":       true,
			"commonName": cluster.Name + ".ca.cloudnative-mysql",
			"issuerRef": map[string]any{
				"name": plan.SelfSignedIssuer,
				"kind": "Issuer",
			},
		}); err != nil {
			return err
		}
	}
	if needCAIssuer {
		if err := r.ensureIssuer(ctx, cluster, plan.CAIssuer, map[string]any{
			"ca": map[string]any{
				"secretName": plan.ServerCASecretName,
			},
		}); err != nil {
			return err
		}
	}

	// One server certificate per instance. Each cert carries both server- and
	// client-auth usages so a replica can reuse it to authenticate to the
	// primary's control API (backup stream) and to mysqld for replication.
	if needServerCertificates {
		for i := 1; i <= plan.Instances; i++ {
			inst := plan.instanceFor(cluster, i)
			if err := r.ensureCertificate(ctx, cluster, inst.ServerCertName, map[string]any{
				"secretName": inst.ServerTLSSecret,
				"commonName": inst.ServiceName + "." + cluster.Namespace + ".svc",
				"dnsNames":   serverDNSNames(cluster, plan, inst),
				"usages": []any{
					"server auth",
					"client auth",
				},
				"issuerRef": map[string]any{
					"name": plan.CAIssuer,
					"kind": "Issuer",
				},
			}); err != nil {
				return err
			}
		}
	}

	if !needClientCertificate {
		return nil
	}
	return r.ensureCertificate(ctx, cluster, cluster.Name+"-client", map[string]any{
		"secretName": plan.ClientTLSSecret,
		"commonName": "cloudnative-mysql-operator",
		"usages": []any{
			"client auth",
		},
		"issuerRef": map[string]any{
			"name": plan.CAIssuer,
			"kind": "Issuer",
		},
	})
}

// serverDNSNames are the SANs an instance certificate must carry: its own
// per-instance Service plus the shared rw/ro/r Services it can be reached
// through.
func serverDNSNames(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, inst instancePlan) []any {
	svcNames := []string{inst.ServiceName, plan.RWServiceName, plan.ROServiceName, plan.RServiceName}
	var altNames []string
	if cluster.Spec.Certificates != nil {
		altNames = cluster.Spec.Certificates.ServerAltDNSNames
	}
	names := make([]any, 0, len(svcNames)*4+len(altNames))
	for _, svc := range svcNames {
		names = append(names,
			svc,
			svc+"."+cluster.Namespace,
			svc+"."+cluster.Namespace+".svc",
			svc+"."+cluster.Namespace+".svc.cluster.local",
		)
	}
	for _, name := range altNames {
		names = append(names, name)
	}
	return names
}

func (r *ClusterReconciler) ensureIssuer(ctx context.Context, cluster *mysqlv1alpha1.Cluster, name string, spec map[string]any) error {
	issuer := &unstructured.Unstructured{}
	issuer.SetGroupVersionKind(issuerGVK)
	issuer.SetName(name)
	issuer.SetNamespace(cluster.Namespace)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, issuer, func() error {
		issuer.SetLabels(labelsFor(cluster, "", ""))
		if err := unstructured.SetNestedMap(issuer.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(cluster, issuer, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) ensureCertificate(ctx context.Context, cluster *mysqlv1alpha1.Cluster, name string, spec map[string]any) error {
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	cert.SetName(name)
	cert.SetNamespace(cluster.Namespace)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cert, func() error {
		cert.SetLabels(labelsFor(cluster, "", ""))
		if err := unstructured.SetNestedMap(cert.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(cluster, cert, r.Scheme)
	})
	return err
}

// certSecretsReady reports whether all TLS secrets the desired instances need
// (the CA, the operator client cert, and every instance server cert) exist.
func (r *ClusterReconciler) certSecretsReady(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (bool, error) {
	names := []string{plan.ServerCASecretName, plan.ClientCASecretName, plan.ClientTLSSecret}
	for i := 1; i <= plan.Instances; i++ {
		names = append(names, plan.instanceFor(cluster, i).ServerTLSSecret)
	}
	seen := map[string]struct{}{}
	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		secret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}
