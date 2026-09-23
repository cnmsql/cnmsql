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
	// A primary left above the desired count (a failover elected it just as a
	// scale-down lowered spec.instances) goes back in range first, so the
	// scale-down can finish. It outranks the preference: an instance the cluster
	// was asked to drop cannot be where the primary belongs.
	if requested, err := r.reconcileInRangePrimary(ctx, cluster, observed); requested || err != nil {
		return requested, err
	}
	// Bring the primary home next. When the cluster names a preferred primary and
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
		// A preferred instance above the desired count is on its way out; handing
		// it the role would only strand the scale-down.
		return instanceInRange(cluster, observed.Plan, instanceName) &&
			reconciler.SwitchoverTargetReady(cluster, failover, instanceName)
	})
	if target == "" {
		return false, nil
	}
	return true, r.requestSwitchover(ctx, cluster, target,
		"Switching over to the preferred primary",
		fmt.Sprintf("Switching over to %s, the preferred primary", target))
}

// reconcileInRangePrimary hands the primary role to an instance within the
// desired count when the current primary sits above it. Scale-down never removes
// a primary, so without this a primary stranded there by a failover keeps the
// cluster one instance over what it was asked for, indefinitely. The handoff is an
// ordinary planned switchover; once it lands, scale-down removes the old primary.
//
// Group Replication is left alone: the group elects its primary, so a
// targetPrimary there would move nothing.
func (r *ClusterReconciler) reconcileInRangePrimary(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) (bool, error) {
	if cluster.IsGroupReplication() {
		return false, nil
	}
	failover := topologyFailoverState(observed)
	reconciler := r.topologyReconciler(cluster)
	target := topology.InRangeSwitchoverTarget(cluster, failover,
		func(instanceName string) bool { return instanceInRange(cluster, observed.Plan, instanceName) },
		func(instanceName string) bool {
			return reconciler.SwitchoverTargetReady(cluster, failover, instanceName)
		},
	)
	if target == "" {
		return false, nil
	}
	return true, r.requestSwitchover(ctx, cluster, target,
		"Switching over to scale down",
		fmt.Sprintf("Switching over to %s so the cluster can scale down to %d instances",
			target, observed.Plan.Instances))
}

// requestSwitchover records target as the primary to switch over to, which the
// switchover path drives from the next pass on.
func (r *ClusterReconciler) requestSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	target, logMessage, message string,
) error {
	logf.FromContext(ctx).Info(logMessage, "from", cluster.Status.CurrentPrimary, "to", target)
	now := metav1.Now()
	if err := topology.PatchClusterStatus(ctx, r.Client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
		status.TargetPrimary = target
		status.TargetPrimaryTimestamp = &now
		status.Phase = topology.PhaseSwitchover
		status.PhaseReason = message
	}); err != nil {
		return err
	}
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeNormal, topology.PhaseSwitchover, message)
	}
	return nil
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
