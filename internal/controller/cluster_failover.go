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

package controller

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
)

func (r *ClusterReconciler) reconcileFailover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed *observedCluster,
) (bool, ctrl.Result, error) {
	result, err := r.topologyReconciler(cluster).ReconcileFailover(ctx, cluster, topology.FailoverRequest{
		Instances:         plan.Instances,
		Observed:          topologyFailoverState(*observed),
		RetryInterval:     readyResync,
		ProvisioningRetry: provisioningRequeue,
	})
	if err != nil {
		return result.Handled, ctrl.Result{}, err
	}
	if result.Phase != nil {
		// A failover that refuses to promote reports Handled=false on purpose, so
		// the rest of the pass still runs and can recreate the failed primary's Pod.
		// That means this pass ends in patchStatus, which writes the phase the
		// observation computed: Degraded, because a replica whose primary is gone has
		// a broken replication thread. Written on its own, the refusal would be
		// overwritten within the same reconcile by the very state that caused it, and
		// the operator would be left reading "replication broken" with no hint that a
		// promotion was available and deliberately declined.
		//
		// So the decision is folded back into the observation and carried to the end
		// of the pass. Phases the failover path handles itself (it returns early) do
		// not reach patchStatus and are unaffected.
		observed.Phase = result.Phase.Phase
		observed.PhaseReason = result.Phase.Reason
		observed.Ready = result.Phase.Ready
		observed.Progressing = result.Phase.Progressing
		err = r.patchOperationPhase(ctx, cluster, *observed, *result.Phase)
	}
	return result.Handled, ctrl.Result{RequeueAfter: result.RequeueAfter}, err
}

// patchOperationPhase records an in-flight operation phase (e.g. Degraded,
// Blocked) while keeping the observed instance topology fields fresh.
func (r *ClusterReconciler) patchOperationPhase(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
	phase topology.OperationPhase,
) error {
	op := observed
	op.Phase = phase.Phase
	op.PhaseReason = phase.Reason
	op.Ready = phase.Ready
	op.Progressing = phase.Progressing
	return r.patchStatus(ctx, cluster, op)
}

// updateStatus applies mutate to the latest Cluster status and patches it.
func (r *ClusterReconciler) updateStatus(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	mutate func(*mysqlv1alpha1.ClusterStatus),
) error {
	return topology.PatchClusterStatus(ctx, r.Client, cluster, mutate)
}
