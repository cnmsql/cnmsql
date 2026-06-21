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

// Package groupreplication reconciles Group Replication topology details.
package groupreplication

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

// InstancePolicyRules intentionally grants no Cluster status or Lease access.
// GR instances report observations through mTLS and use group quorum for safety.
func (r *Reconciler) InstancePolicyRules(*mysqlv1alpha1.Cluster) []rbacv1.PolicyRule {
	return nil
}

// ReconcileInstanceRBAC lets one GR instance ring the observation doorbell on
// its own Pod and no other. resourceNames requires one Role per instance.
func (r *Reconciler) ReconcileInstanceRBAC(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	identity topology.InstanceIdentity,
) error {
	name := identity.InstanceName + "-gr-doorbell"
	role := &rbacv1.Role{ObjectMeta: v1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.client, role, func() error {
		role.Labels = identity.Labels
		role.Rules = []rbacv1.PolicyRule{{
			APIGroups:     []string{""},
			Resources:     []string{"pods"},
			Verbs:         []string{"get", "patch"},
			ResourceNames: []string{identity.InstanceName},
		}}
		return controllerutil.SetControllerReference(cluster, role, r.scheme)
	}); err != nil {
		return err
	}

	binding := &rbacv1.RoleBinding{ObjectMeta: v1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.client, binding, func() error {
		binding.Labels = identity.Labels
		binding.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}
		binding.Subjects = []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      identity.ServiceAccountName,
			Namespace: cluster.Namespace,
		}}
		return controllerutil.SetControllerReference(cluster, binding, r.scheme)
	})
	return err
}
