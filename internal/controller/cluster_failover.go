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
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

func (r *ClusterReconciler) reconcileFailover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, ctrl.Result, error) {
	result, err := r.topologyReconciler(cluster).ReconcileFailover(ctx, cluster, topology.FailoverRequest{
		Instances:         plan.Instances,
		Observed:          topologyFailoverState(observed),
		RetryInterval:     readyResync,
		ProvisioningRetry: provisioningRequeue,
	})
	if err != nil {
		return result.Handled, ctrl.Result{}, err
	}
	if result.Phase != nil {
		err = r.patchOperationPhase(ctx, cluster, observed, *result.Phase)
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
