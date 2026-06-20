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

// Package async reconciles async and semi-sync replication topology details.
package async

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

// InstancePolicyRules grants the in-Pod async reconciler permission to report
// status and participate in the split-brain guard Lease.
func (r *Reconciler) InstancePolicyRules(cluster *mysqlv1alpha1.Cluster) []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		{
			APIGroups:     []string{mysqlv1alpha1.GroupVersion.Group},
			Resources:     []string{"clusters/status"},
			Verbs:         []string{"get", "update", "patch"},
			ResourceNames: []string{cluster.Name},
		},
		{
			APIGroups:     []string{"coordination.k8s.io"},
			Resources:     []string{"leases"},
			Verbs:         []string{"get", "create", "update", "patch", "delete", "watch", "list"},
			ResourceNames: []string{cluster.Name + "-primary"},
		},
	}
}

// ReconcileInstanceRBAC has no per-instance resources for async replication.
func (r *Reconciler) ReconcileInstanceRBAC(
	context.Context,
	*mysqlv1alpha1.Cluster,
	topology.InstanceIdentity,
) error {
	return nil
}
