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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

// ensureDefaultservices reconciles the rw/ro/r traffic-routing Services,
// honouring spec.managed.services.disabledDefaultServices.
func (r *ClusterReconciler) ensureDefaultServices(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	services := []struct {
		selector mysqlv1alpha1.ServiceSelectorType
		name     string
		labels   map[string]string
	}{
		// rw → the primary; ro → replicas; r → any instance.
		{mysqlv1alpha1.ServiceSelectorTypeRW, plan.RWServiceName, map[string]string{clusterLabel: cluster.Name, roleLabel: rolePrimary}},
		{mysqlv1alpha1.ServiceSelectorTypeRO, plan.ROServiceName, map[string]string{clusterLabel: cluster.Name, roleLabel: roleReplica}},
		{mysqlv1alpha1.ServiceSelectorTypeR, plan.RServiceName, map[string]string{clusterLabel: cluster.Name}},
	}
	for _, svc := range services {
		if plan.DisabledServices[svc.selector] {
			if err := r.deleteService(ctx, cluster.Namespace, svc.name); err != nil {
				return err
			}
			continue
		}
		if err := r.ensureRoutingService(ctx, cluster, svc.name, svc.selector, svc.selector == mysqlv1alpha1.ServiceSelectorTypeRW, svc.labels); err != nil {
			return err
		}
	}
	return nil
}

func (r *ClusterReconciler) ensureRoutingService(ctx context.Context, cluster *mysqlv1alpha1.Cluster, name string, role mysqlv1alpha1.ServiceSelectorType, primaryOnly bool, selector map[string]string) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		labels := labelsFor(cluster, "", "")
		labels[roleLabel] = string(role)
		service.Labels = labels
		service.Spec.Selector = selector
		service.Spec.Ports = []corev1.ServicePort{
			{Name: "mysql", Port: 3306, TargetPort: intstr.FromString("mysql")},
		}
		// The rw Service must never publish a not-ready primary; ro/r tolerate
		// in-progress members so clients can discover them as they catch up.
		service.Spec.PublishNotReadyAddresses = !primaryOnly
		return controllerutil.SetControllerReference(cluster, service, r.Scheme)
	})
	return err
}

func (r *ClusterReconciler) deleteService(ctx context.Context, namespace, name string) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if err := r.Delete(ctx, service); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
