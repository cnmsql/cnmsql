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
)

func (r *ClusterReconciler) reconcileSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) (bool, error) {
	result, err := r.topologyReconciler(cluster).ReconcileSwitchover(ctx, cluster, topologyFailoverState(observed))
	if err != nil {
		return result.Handled, err
	}
	if result.Phase != nil {
		err = r.patchOperationPhase(ctx, cluster, observed, *result.Phase)
	}
	return result.Handled, err
}

// reconcileDrainSwitchover initiates a planned switchover when the primary Pod is
// gracefully terminating (e.g. a node drain). It returns handled=true (with a
// requeue) once it has committed a new TargetPrimary or hit an error, so the
// caller can short-circuit and let the switchover path drive the promotion.
func (r *ClusterReconciler) reconcileDrainSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) (ctrl.Result, error, bool) {
	result, err := r.topologyReconciler(cluster).ReconcileDrainSwitchover(ctx, cluster, topologyFailoverState(observed))
	if err != nil {
		return ctrl.Result{}, err, true
	}
	if result.Phase != nil {
		if perr := r.patchOperationPhase(ctx, cluster, observed, *result.Phase); perr != nil {
			return ctrl.Result{}, perr, true
		}
	}
	if result.Handled {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, nil, true
	}
	return ctrl.Result{}, nil, false
}
