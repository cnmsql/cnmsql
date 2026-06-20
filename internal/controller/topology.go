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

	ctrl "sigs.k8s.io/controller-runtime"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// topologyReconciler abstracts the topology-specific parts of Cluster
// reconciliation behind a single strategy selected by spec.replication.mode.
// The main reconciler loop keeps the overall ordering (credentials, RBAC,
// certificates, instances, labels, fencing, status) and calls through the
// strategy for failover, switchover, availability self-healing, observation and
// topology-specific infrastructure decisions such as the primary lease.
type topologyReconciler interface {
	// Name identifies the topology for logging.
	Name() string

	// DonorAvailable reports whether a new instance may be provisioned now.
	// Async needs a healthy primary to clone; Group Replication needs a quorate
	// group with at least one ONLINE donor for distributed recovery.
	DonorAvailable(observed observedCluster) bool

	// NeedsPrimaryLease reports whether the topology requires the async primary
	// Lease object. Group Replication uses quorum instead.
	NeedsPrimaryLease() bool

	// IsSemiSyncRelevant reports whether semi-sync self-healing applies. It is
	// only meaningful for the asynchronous topology; the spec webhook already
	// rejects semi-sync with group replication.
	IsSemiSyncRelevant() bool

	// ReconcileFailover handles an observed unhealthy primary. Under async the
	// operator elects and fences; under GR the operator only observes and never
	// drives failover. Returns (handled, result, error).
	ReconcileFailover(
		ctx context.Context,
		r *ClusterReconciler,
		cluster *mysqlv1alpha1.Cluster,
		plan clusterPlan,
		observed observedCluster,
	) (bool, ctrl.Result, error)

	// ReconcileSwitchover drives a planned primary change. Async delegates to the
	// in-Pod reconcilers; GR invokes the group's set_as_primary UDF. Returns
	// (handled, error): handled=true keeps the reconcile in a switchover phase and
	// requeues.
	ReconcileSwitchover(
		ctx context.Context,
		r *ClusterReconciler,
		cluster *mysqlv1alpha1.Cluster,
		plan clusterPlan,
		observed observedCluster,
	) (bool, error)

	// ReconcileAvailability runs best-effort availability adjustments while the
	// cluster is degraded, such as async semi-sync self-healing. Failures are
	// logged and retried on the next resync rather than failing the reconcile.
	ReconcileAvailability(
		ctx context.Context,
		r *ClusterReconciler,
		cluster *mysqlv1alpha1.Cluster,
		observed observedCluster,
	) error

	// ObserveTopology performs topology-specific observation and mutates the
	// observed cluster (primary name, divergence, group view, etc.).
	ObserveTopology(
		ctx context.Context,
		cluster *mysqlv1alpha1.Cluster,
		observed *observedCluster,
	)
}

// topologyFor returns the strategy implementation for a Cluster's replication
// mode. The default (no spec.replication or mode==async) uses asyncTopology.
func topologyFor(cluster *mysqlv1alpha1.Cluster) topologyReconciler {
	if cluster.IsGroupReplication() {
		return &groupReplicationTopology{}
	}
	return &asyncTopology{}
}
