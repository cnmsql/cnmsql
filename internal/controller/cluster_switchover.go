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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
)

func (r *ClusterReconciler) reconcileSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) (bool, error) {
	// Bring the primary home first. When the cluster names a preferred primary and
	// the role has ended up elsewhere — a failover moved it, and the preferred
	// instance has since come back healthy — this requests a switchover back to it
	// by setting targetPrimary, which the switchover below drives on the next pass.
	if requested, err := r.reconcilePreferredPrimary(ctx, cluster, observed); requested || err != nil {
		return requested, err
	}
	result, err := r.topologyReconciler(cluster).ReconcileSwitchover(ctx, cluster, topologyFailoverState(observed))
	if err != nil {
		return result.Handled, err
	}
	if result.Phase != nil {
		err = r.patchOperationPhase(ctx, cluster, observed, *result.Phase)
	}
	return result.Handled, err
}

// reconcilePreferredPrimary moves the primary back onto the most preferred
// instance the cluster names in spec.failoverPolicy.preferredPrimary, by
// requesting an ordinary planned switchover to it: the switchover path on the
// next pass performs the handoff, with all the safety it normally applies.
//
// It only ever fires on a healthy cluster whose primary is somewhere the
// preference did not ask for, which is what a cluster looks like after a failover
// moved the primary off the node it was meant to run on and the preferred
// instance has since come back. A cluster whose primary is already the most
// preferred available instance is left alone.
func (r *ClusterReconciler) reconcilePreferredPrimary(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) (bool, error) {
	failover := topologyFailoverState(observed)
	reconciler := r.topologyReconciler(cluster)
	target := topology.PreferredFailbackTarget(cluster, failover, func(instanceName string) bool {
		return reconciler.SwitchoverTargetReady(cluster, failover, instanceName)
	})
	if target == "" {
		return false, nil
	}

	current := cluster.Status.CurrentPrimary
	message := fmt.Sprintf("Switching over to %s, the preferred primary", target)
	logf.FromContext(ctx).Info("Switching over to the preferred primary", "from", current, "to", target)
	now := metav1.Now()
	if err := topology.PatchClusterStatus(ctx, r.Client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
		status.TargetPrimary = target
		status.TargetPrimaryTimestamp = &now
		status.Phase = topology.PhaseSwitchover
		status.PhaseReason = message
	}); err != nil {
		return true, err
	}
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeNormal, topology.PhaseSwitchover, message)
	}
	return true, nil
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
