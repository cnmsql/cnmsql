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

package rolereconciler

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// reconcileGroupRole is the Group Replication topology role strategy. It embodies
// "the group decides, the operator reflects": this in-Pod side only ensures the
// local member is a running group member. It never self-promotes, never demotes,
// never touches the primary lease, and never writes Cluster status — the operator
// is the sole writer of currentPrimary and status.groupReplication under GR.
//
// Bootstrap is exactly-once and signal-derived (no dedicated API field): the
// member bootstraps the group only when it is the operator-designated bootstrap
// member (status.targetPrimary) AND the group has never been bootstrapped
// (status.groupReplication.bootstrapped == false). Otherwise it joins an existing
// group with START GROUP_REPLICATION. The operator flips bootstrapped to true once
// it observes the member ONLINE, closing the gate forever.
func (r *Reconciler) reconcileGroupRole(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	status *webserver.Status,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	me := r.InstanceName

	// status.GroupReplication is non-nil once the plugin is running and the member
	// appears in replication_group_members (any state). Its absence means GR has
	// not been started on this member yet.
	gr := status.GroupReplication
	if gr != nil {
		if gr.State == groupreplication.MemberStateOnline {
			// Steady state: a running, online member. Nothing to do; the operator
			// observes and reflects. Re-check periodically without a Cluster event.
			return ctrl.Result{RequeueAfter: steadyRequeue}, nil
		}
		// Started but not yet ONLINE (RECOVERING/OFFLINE/ERROR). Let GR converge or
		// auto-rejoin; just re-check shortly. Guarded recovery for stuck members is
		// a later phase.
		log.Info("Group member not yet ONLINE; waiting", "instance", me, "state", gr.State)
		return ctrl.Result{RequeueAfter: waitRequeue}, nil
	}

	// GR is not running locally (fresh start, or restarted with start_on_boot=OFF).
	if r.shouldBootstrap(cluster) {
		log.Info("Bootstrapping the group as the designated bootstrap member", "instance", me)
		if err := r.Local.BootstrapGroup(ctx); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: waitRequeue}, nil
	}

	// Join the existing group. For a single-member group whose only member has
	// fully restarted (total outage), the group view is gone and this start cannot
	// re-form the group; that re-bootstrap is a guarded, opt-in recovery handled in
	// a later phase. Until then the member stays OFFLINE and the operator surfaces
	// the degradation.
	log.Info("Starting group replication to join the group", "instance", me)
	if err := r.Local.StartGroupReplication(ctx); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: waitRequeue}, nil
}

// shouldBootstrap reports whether this member is the one-and-only member that may
// create the group: it is the operator-designated bootstrap member and the group
// has never been bootstrapped. The sticky status.groupReplication.bootstrapped
// flag (monotonic and webhook-enforced) makes this exactly-once across restarts.
func (r *Reconciler) shouldBootstrap(cluster *mysqlv1alpha1.Cluster) bool {
	if cluster.Status.TargetPrimary != r.InstanceName {
		return false
	}
	grStatus := cluster.Status.GroupReplication
	return grStatus == nil || !grStatus.Bootstrapped
}
