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
	"slices"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// reconcileSemiSync self-heals semi-synchronous replication. With semi-sync on,
// the primary blocks commits until minSyncReplicas replicas acknowledge. If a
// sync replica becomes unhealthy and data durability is "preferred" (the
// default), the operator lowers the primary's acknowledgement count to the
// number of healthy replicas so writes keep flowing; it restores the configured
// count as replicas recover. Under "required" the count stays fixed and writes
// block until enough replicas acknowledge.
//
// It is best-effort and can run while the cluster is degraded, as long as the
// primary is reachable. Failures are logged by the caller and retried on the
// next resync rather than failing the reconcile.
func (r *ClusterReconciler) reconcileSemiSync(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) error {
	minSync := cluster.Spec.MinSyncReplicas
	if !semiSyncEnabled(cluster) || minSync <= 0 {
		// Nothing to enforce: semi-sync off or no synchronous floor configured.
		return nil
	}

	primary := observed.PrimaryName
	if primary == "" {
		return nil
	}
	// Only act when the primary is reachable; otherwise there is nothing to drive
	// and a failover/observe pass will revisit this.
	if _, ok := observed.StatusByInstance[primary]; !ok {
		return nil
	}

	desired := minSync
	if semiSyncDurabilityPreferred(cluster) {
		healthy := healthyReplicaCount(observed)
		if healthy < minSync {
			// Never drop below one acknowledgement: semi-sync still prefers a
			// durable replica when any healthy one exists, and the source timeout
			// falls back to async if none can ack.
			desired = max(1, healthy)
		}
	}

	if err := r.instanceControlClient().SetSemiSyncWaitForReplicaCount(ctx, cluster, primary, desired); err != nil {
		return err
	}
	if desired != minSync {
		logf.FromContext(ctx).Info("Self-healed semi-sync acknowledgement count",
			"primary", primary, "configured", minSync, "effective", desired)
	}
	return nil
}

// healthyReplicaCount counts ready, non-diverged replicas (excluding the
// primary): the replicas that can currently acknowledge a semi-sync commit.
func healthyReplicaCount(observed observedCluster) int {
	healthy := 0
	for name, status := range observed.StatusByInstance {
		if name == observed.PrimaryName || status == nil {
			continue
		}
		if status.IsReady &&
			!slices.Contains(observed.DivergedInstances, name) &&
			!slices.Contains(observed.FencedInstances, name) {
			healthy++
		}
	}
	return healthy
}

// semiSyncEnabled reports whether semi-synchronous replication is configured.
func semiSyncEnabled(cluster *mysqlv1alpha1.Cluster) bool {
	return cluster.Spec.MySQL.SemiSync != nil && cluster.Spec.MySQL.SemiSync.Enabled
}

// semiSyncDurabilityPreferred reports whether data durability is "preferred"
// (the default when unset), under which the operator self-heals the
// acknowledgement count instead of letting writes block.
func semiSyncDurabilityPreferred(cluster *mysqlv1alpha1.Cluster) bool {
	if cluster.Spec.MySQL.SemiSync == nil {
		return true
	}
	return cluster.Spec.MySQL.SemiSync.DataDurability != mysqlv1alpha1.DataDurabilityRequired
}
