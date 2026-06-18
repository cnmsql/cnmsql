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
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// ensureInstanceRBAC provisions the per-Cluster Role and the per-instance
// ServiceAccounts that let each instance's in-Pod reconciler watch this Cluster
// and patch only the status fields it owns. Each Pod runs under its own
// ServiceAccount (<instance-name>-instance) so the admission webhook can
// identify the caller and authorise it by name. All resources are owned by the
// Cluster for garbage collection.
func (r *ClusterReconciler) ensureInstanceRBAC(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) error {
	name := cluster.Name + "-instance"

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

	desired := make(map[string]string, plan.Instances) // SA name -> instance name
	for _, inst := range plan.instanceNames(cluster) {
		saName := inst + "-instance"
		desired[saName] = inst
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: cluster.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
			sa.Labels = labelsFor(cluster, inst, "")
			return controllerutil.SetControllerReference(cluster, sa, r.Scheme)
		}); err != nil {
			return err
		}
	}

	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		binding.Labels = labelsFor(cluster, "", "")
		binding.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		}
		subjects := make([]rbacv1.Subject, 0, len(desired))
		for saName := range desired {
			subjects = append(subjects, rbacv1.Subject{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      saName,
				Namespace: cluster.Namespace,
			})
		}
		sort.Slice(subjects, func(i, j int) bool { return subjects[i].Name < subjects[j].Name })
		binding.Subjects = subjects
		return controllerutil.SetControllerReference(cluster, binding, r.Scheme)
	}); err != nil {
		return err
	}

	// Remove ServiceAccounts for instances that are no longer desired (scale-down)
	// so their identities cannot be reused to patch status later.
	saList := &corev1.ServiceAccountList{}
	if err := r.List(ctx, saList, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{clusterLabel: cluster.Name}); err != nil {
		return err
	}
	for i := range saList.Items {
		sa := &saList.Items[i]
		if _, ok := desired[sa.Name]; ok {
			continue
		}
		if strings.HasSuffix(sa.Name, "-instance") && strings.HasPrefix(sa.Name, cluster.Name+"-") {
			if err := r.Delete(ctx, sa); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}
