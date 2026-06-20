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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// grObservedAnnotation is the advisory doorbell the in-Pod manager bumps on its
// own Pod whenever its observed Group Replication snapshot changes (primary
// member, view id, own state, quorum). The operator watches Pods, so the bump
// turns a clean set_as_primary handover — where both members stay ONLINE/Ready
// and no readiness condition changes — into an immediate reconcile. The value is
// a hint; the operator still cross-validates the group view before routing.
const grObservedAnnotation = "mysql.cloudnative-mysql.io/gr-observed"

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
	// appears in replication_group_members. A member that is in the group (ONLINE
	// or RECOVERING) is left to the group; only a member that is not in the group
	// (no GR row, or an OFFLINE row left behind after a failed/aborted start) needs
	// to (re)join.
	gr := status.GroupReplication
	if gr != nil {
		switch gr.State {
		case groupreplication.MemberStateOnline:
			// Steady state: a running, online member. Nothing to do; the operator
			// observes and reflects. Re-check periodically without a Cluster event.
			if err := r.bumpGRObservedDoorbell(ctx, cluster, status); err != nil {
				log.Error(err, "Could not bump GR doorbell; will retry", "instance", me)
				return ctrl.Result{RequeueAfter: waitRequeue}, nil
			}
			return ctrl.Result{RequeueAfter: steadyRequeue}, nil
		case groupreplication.MemberStateRecovering:
			// Distributed recovery in progress (binlog catch-up or a clone). Wait.
			log.Info("Group member is recovering; waiting", "instance", me, "state", gr.State)
			return ctrl.Result{RequeueAfter: waitRequeue}, nil
		case groupreplication.MemberStateError:
			// The member could not join (e.g. errant transactions). Guarded recovery
			// (re-clone via the reinit annotation) is a later phase; surface and wait.
			log.Info("Group member is in ERROR; manual recovery required", "instance", me)
			return ctrl.Result{RequeueAfter: waitRequeue}, nil
		}
		// OFFLINE: GR is not running on this member (a stale row from a previous
		// start that left the group). Fall through and (re)join below.
		log.Info("Group member is OFFLINE; (re)joining the group", "instance", me)
	}

	// GR is not running locally (fresh start, restarted with start_on_boot=OFF, or
	// OFFLINE after leaving). Bootstrap the group only on the designated member.
	if r.shouldBootstrap(cluster) {
		log.Info("Bootstrapping the group as the designated bootstrap member", "instance", me)
		if err := r.Local.BootstrapGroup(ctx); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: waitRequeue}, nil
	}

	// Prepare for distributed recovery, then join. PrepareGroupJoin clears the
	// initdb-authored GTIDs and forces a clone for a fresh member (so it is
	// provisioned wholesale from a donor), and sets the recovery-channel account;
	// it authenticates with the replication account (X509, no password) over the
	// rendered recovery SSL material. It is idempotent and safe on every attempt.
	if err := r.Local.PrepareGroupJoin(ctx, r.SourceTemplate.User, r.SourceTemplate.Password); err != nil {
		return ctrl.Result{}, err
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

// bumpGRObservedDoorbell writes/updates the gr-observed annotation on this
// instance's Pod whenever the locally observed GR snapshot changes. The
// annotation is purely a wake-up: its value encodes (primaryMemberID, viewID,
// ownState, hasQuorum) so a failed bump is harmless aside from a delayed
// reconcile. Instance ServiceAccounts are scoped to patch only their own Pod
// resourceNames, so a compromised instance can only ring its own doorbell.
func (r *Reconciler) bumpGRObservedDoorbell(
	ctx context.Context,
	_ *mysqlv1alpha1.Cluster,
	status *webserver.Status,
) error {
	gr := status.GroupReplication
	if gr == nil {
		return nil
	}
	primaryMemberID := gr.PrimaryMemberID
	viewID := gr.ViewID
	hasQuorum := false
	for _, m := range gr.Members {
		if m.State == groupreplication.MemberStateOnline {
			hasQuorum = true
			break
		}
	}
	fingerprint := fmt.Sprintf("%s:%s:%s:%t",
		primaryMemberID, viewID, gr.State, hasQuorum,
	)

	pod := &corev1.Pod{}
	key := types.NamespacedName{Namespace: r.ClusterKey.Namespace, Name: r.InstanceName}
	if err := r.Get(ctx, key, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	if pod.DeletionTimestamp != nil {
		return nil
	}
	if pod.Annotations[grObservedAnnotation] == fingerprint {
		return nil
	}
	before := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[grObservedAnnotation] = fingerprint
	return r.Patch(ctx, pod, client.MergeFrom(before))
}

// SetAsPrimary is a command-path hook exposed by the instance manager so an
// external caller (e.g. the kubectl plugin or the operator directly) can invoke
// group_replication_set_as_primary on this member. Under normal operation the
// operator triggers the UDF from any ONLINE member via the control API; this
// method makes the same primitive available locally on an instance.
func (r *Reconciler) SetAsPrimary(ctx context.Context, memberUUID string) error {
	return r.Local.SetAsPrimary(ctx, memberUUID)
}
