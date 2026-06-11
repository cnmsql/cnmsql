/*
Copyright 2026 The CNMySQL Authors.

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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

func (r *ClusterReconciler) ensureCertificates(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	if hasUserCertificates(cluster) {
		return nil
	}
	if err := r.ensureIssuer(ctx, cluster, plan.SelfSignedIssuer, map[string]any{
		"selfSigned": map[string]any{},
	}); err != nil {
		return err
	}
	if err := r.ensureCertificate(ctx, cluster, plan.CAIssuer, map[string]any{
		"secretName": plan.CASecretName,
		"isCA":       true,
		"commonName": cluster.Name + ".ca.cnmysql",
		"issuerRef": map[string]any{
			"name": plan.SelfSignedIssuer,
			"kind": "Issuer",
		},
	}); err != nil {
		return err
	}
	if err := r.ensureIssuer(ctx, cluster, plan.CAIssuer, map[string]any{
		"ca": map[string]any{
			"secretName": plan.CASecretName,
		},
	}); err != nil {
		return err
	}
	if err := r.ensureCertificate(ctx, cluster, cluster.Name+"-server", map[string]any{
		"secretName": plan.ServerTLSSecret,
		"commonName": plan.ServiceName + "." + cluster.Namespace + ".svc",
		"dnsNames": []any{
			plan.InstanceName,
			plan.ServiceName,
			plan.ServiceName + "." + cluster.Namespace,
			plan.ServiceName + "." + cluster.Namespace + ".svc",
			plan.ServiceName + "." + cluster.Namespace + ".svc.cluster.local",
		},
		"issuerRef": map[string]any{
			"name": plan.CAIssuer,
			"kind": "Issuer",
		},
	}); err != nil {
		return err
	}
	return r.ensureCertificate(ctx, cluster, cluster.Name+"-client", map[string]any{
		"secretName": plan.ClientTLSSecret,
		"commonName": "cnmysql-operator",
		"usages": []any{
			"client auth",
		},
		"issuerRef": map[string]any{
			"name": plan.CAIssuer,
			"kind": "Issuer",
		},
	})
}

func hasUserCertificates(cluster *mysqlv1alpha1.Cluster) bool {
	certs := cluster.Spec.Certificates
	return certs != nil &&
		certs.ServerTLSSecret != "" &&
		certs.ClientCASecret != "" &&
		certs.ReplicationTLSSecret != ""
}

func (r *ClusterReconciler) ensureIssuer(ctx context.Context, cluster *mysqlv1alpha1.Cluster, name string, spec map[string]any) error {
	issuer := &unstructured.Unstructured{}
	issuer.SetGroupVersionKind(issuerGVK)
	issuer.SetName(name)
	issuer.SetNamespace(cluster.Namespace)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, issuer, func() error {
		issuer.SetLabels(labelsFor(cluster, ""))
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
		cert.SetLabels(labelsFor(cluster, ""))
		if err := unstructured.SetNestedMap(cert.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(cluster, cert, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) certSecretsReady(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (bool, error) {
	for _, name := range []string{plan.CASecretName, plan.ServerTLSSecret, plan.ClientTLSSecret} {
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
