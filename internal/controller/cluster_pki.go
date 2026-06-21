/*
Copyright 2026 The CloudNative MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
		if err := r.ensureCertManagerResource(ctx, cluster, plan.SelfSignedIssuer, map[string]any{
			"selfSigned": map[string]any{},
		}, issuerGVK); err != nil {
			return err
		}
		if err := r.ensureCertManagerResource(ctx, cluster, plan.CAIssuer, map[string]any{
			"secretName": plan.ServerCASecretName,
			"isCA":       true,
			"commonName": cluster.Name + ".ca.cloudnative-mysql",
			"issuerRef": map[string]any{
				"name": plan.SelfSignedIssuer,
				"kind": "Issuer",
			},
		}, certificateGVK); err != nil {
			return err
		}
	}
	if needCAIssuer {
		if err := r.ensureCertManagerResource(ctx, cluster, plan.CAIssuer, map[string]any{
			"ca": map[string]any{
				"secretName": plan.ServerCASecretName,
			},
		}, issuerGVK); err != nil {
			return err
		}
	}

	// One server certificate per instance. Each cert carries both server- and
	// client-auth usages so a replica can reuse it to authenticate to the
	// primary's control API (backup stream) and to mysqld for replication.
	if needServerCertificates {
		for i := 1; i <= plan.Instances; i++ {
			inst := plan.instanceFor(cluster, i)
			if err := r.ensureCertManagerResource(ctx, cluster, inst.ServerCertName, map[string]any{
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
			}, certificateGVK); err != nil {
				return err
			}
		}
	}

	if !needClientCertificate {
		return nil
	}
	return r.ensureCertManagerResource(ctx, cluster, cluster.Name+"-client", map[string]any{
		"secretName": plan.ClientTLSSecret,
		"commonName": "cloudnative-mysql-operator",
		"usages": []any{
			"client auth",
		},
		"issuerRef": map[string]any{
			"name": plan.CAIssuer,
			"kind": "Issuer",
		},
	}, certificateGVK)
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

func (r *ClusterReconciler) ensureCertManagerResource(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	name string,
	spec map[string]any,
	gvk schema.GroupVersionKind,
) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(cluster.Namespace)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		obj.SetLabels(labelsFor(cluster, "", ""))
		if err := unstructured.SetNestedMap(obj.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(cluster, obj, r.Scheme)
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
