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

// Package topology defines the boundary between the common Cluster reconciler
// and replication-topology-specific reconciliation.
package topology

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// InstanceIdentity describes one instance's topology-specific Kubernetes
// identity. Common ServiceAccount and RoleBinding lifecycle stays in the
// Cluster reconciler.
type InstanceIdentity struct {
	InstanceName       string
	ServiceAccountName string
	Labels             map[string]string
}

// Reconciler owns behavior that differs between replication topologies. The
// interface starts with RBAC and will grow as failover, switchover, status, and
// topology configuration move out of the common Cluster reconciler.
type Reconciler interface {
	InstancePolicyRules(cluster *mysqlv1alpha1.Cluster) []rbacv1.PolicyRule
	ReconcileInstanceRBAC(
		ctx context.Context,
		cluster *mysqlv1alpha1.Cluster,
		identity InstanceIdentity,
	) error
}
