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

package async

import (
	"context"
	"fmt"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlconfig "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
)

// Name is the user-facing topology name used in reconciliation logs.
func (r *Reconciler) Name() string { return "async" }

// EnsureConfigured has no async topology preflight.
func (r *Reconciler) EnsureConfigured(context.Context, *mysqlv1alpha1.Cluster) error { return nil }

// ConfigureServer applies async semi-sync server settings.
func (r *Reconciler) ConfigureServer(
	cluster *mysqlv1alpha1.Cluster,
	_ topology.ServerConfigInput,
	config *mysqlconfig.ServerConfig,
) {
	if cluster.Spec.MySQL.SemiSync == nil {
		return
	}
	config.SemiSync.Enabled = cluster.Spec.MySQL.SemiSync.Enabled
	config.SemiSync.WaitForReplicaCount = initialSemiSyncWaitForReplicaCount(cluster)
	if cluster.Spec.MySQL.SemiSync.TimeoutMillis != nil {
		config.SemiSync.TimeoutMillis = int(*cluster.Spec.MySQL.SemiSync.TimeoutMillis)
	}
}

// DonorAvailable requires a healthy async primary for physical cloning.
func (r *Reconciler) DonorAvailable(_ topology.Observation, observed topology.FailoverState) bool {
	return PrimaryHealthy(observed)
}

// PodPolicy uses physical clone for replicas and the async instance strategy.
func (r *Reconciler) PodPolicy(cluster *mysqlv1alpha1.Cluster) topology.PodPolicy {
	policy := topology.PodPolicy{}
	if cluster.Spec.MySQL.SemiSync == nil || !cluster.Spec.MySQL.SemiSync.Enabled {
		return policy
	}
	policy.RunArgs = append(policy.RunArgs,
		"--semi-sync",
		fmt.Sprintf("--semi-sync-wait-for-replica-count=%d", initialSemiSyncWaitForReplicaCount(cluster)),
	)
	if cluster.Spec.MySQL.SemiSync.TimeoutMillis != nil {
		policy.RunArgs = append(policy.RunArgs,
			fmt.Sprintf("--semi-sync-timeout-millis=%d", *cluster.Spec.MySQL.SemiSync.TimeoutMillis))
	}
	return policy
}

// PublishNotReadyAddresses lets async read Services discover catching-up replicas.
func (r *Reconciler) PublishNotReadyAddresses(role mysqlv1alpha1.ServiceSelectorType) bool {
	return role != mysqlv1alpha1.ServiceSelectorTypeRW
}

func initialSemiSyncWaitForReplicaCount(cluster *mysqlv1alpha1.Cluster) int {
	count := cluster.Spec.MinSyncReplicas
	if count <= 0 {
		return 0
	}
	if cluster.SemiSyncDurabilityPreferred() {
		return 1
	}
	return count
}
