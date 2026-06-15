/*
Copyright 2026 The cloudnative-mysql Authors.

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
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// ensureInstanceRBAC provisions the per-Cluster ServiceAccount, Role and
// RoleBinding that let each instance's in-Pod reconciler watch this Cluster and
// patch its status (to set currentPrimary on self-promotion). Scoped to this one
// Cluster for least privilege; owned by the Cluster so it is garbage-collected.
func (r *ClusterReconciler) ensureInstanceRBAC(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	name := plan.InstanceServiceAccount

	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		sa.Labels = labelsFor(cluster, "", "")
		return controllerutil.SetControllerReference(cluster, sa, r.Scheme)
	}); err != nil {
		return err
	}

	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Labels = labelsFor(cluster, "", "")
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups:     []string{mysqlv1alpha1.GroupVersion.Group},
				Resources:     []string{"clusters"},
				Verbs:         []string{"get", "list", "watch"},
				ResourceNames: []string{cluster.Name},
			},
			{
				APIGroups:     []string{mysqlv1alpha1.GroupVersion.Group},
				Resources:     []string{"clusters/status"},
				Verbs:         []string{"get", "update", "patch"},
				ResourceNames: []string{cluster.Name},
			},
			{
				APIGroups:     []string{"coordination.k8s.io"},
				Resources:     []string{"leases"},
				Verbs:         []string{"get", "create", "update", "patch", "delete", "watch"},
				ResourceNames: []string{primaryLeaseName(cluster)},
			},
		}
		return controllerutil.SetControllerReference(cluster, role, r.Scheme)
	}); err != nil {
		return err
	}

	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		binding.Labels = labelsFor(cluster, "", "")
		binding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		}
		binding.Subjects = []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      name,
			Namespace: cluster.Namespace,
		}}
		return controllerutil.SetControllerReference(cluster, binding, r.Scheme)
	})
	return err
}
