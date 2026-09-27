/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

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

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	mysqlconfig "github.com/cnmsql/cnmsql/pkg/management/mysql/config"
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

// PublishNotReadyAddresses decides whether the routing Services publish pods
// that are not Ready. The rw Service never does. The async read Services
// tolerate in-progress members by default, so clients can discover replicas as
// they catch up. Once the cluster configures a readiness lag bound
// (spec.replication.maxReadyLag), readiness is what says a replica is fit to
// serve reads — an unbound lag gate is the one thing holding a catching-up
// replica out of -ro/-r — so the read Services stop publishing not-ready
// addresses and Kubernetes endpoint readiness governs membership.
func (r *Reconciler) PublishNotReadyAddresses(cluster *mysqlv1alpha1.Cluster, role mysqlv1alpha1.ServiceSelectorType) bool {
	if role == mysqlv1alpha1.ServiceSelectorTypeRW {
		return false
	}
	return cluster.MaxReadyLag() == nil
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
