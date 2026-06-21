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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

const groupObservationAnnotation = "mysql.cloudnative-mysql.io/gr-observed"
const forceQuorumMembersAnnotation = "cloudnative-mysql.cloudnative-mysql.io/force-quorum-members"
const forceGroupRebootstrapAnnotation = "cloudnative-mysql.cloudnative-mysql.io/force-group-rebootstrap"

// clusterPhaseBlocked mirrors topology.PhaseBlocked. The in-Pod agent must not
// pull in the controller's internal packages, so the literal is duplicated here.
const clusterPhaseBlocked = "Blocked"

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

	// Guarded quorum recovery: the operator stamps this Pod with the force_members
	// addresses when the group has lost quorum and this member is the designated
	// survivor. Execute group_replication_force_members and clear the annotation.
	// Use the doorbell client to avoid the informer cache, which requires
	// cluster-scoped list/watch on Pods that the instance SA does not hold.
	doorbellClient := r.DoorbellClient
	if doorbellClient == nil {
		doorbellClient = r.Client
	}
	pod := &corev1.Pod{}
	key := types.NamespacedName{Namespace: r.ClusterKey.Namespace, Name: me}
	if err := doorbellClient.Get(ctx, key, pod); err != nil {
		// Pod not found or transient error; skip and try normal GR reconciliation.
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Could not read own Pod for force_members check", "instance", me)
		}
	} else if addrs, ok := pod.Annotations[forceQuorumMembersAnnotation]; ok && addrs != "" {
		log.Info("Executing guarded quorum recovery via force_members",
			"instance", me, "addresses", addrs)
		if err := r.Local.ForceGroupMembers(ctx, []string{addrs}); err != nil {
			log.Error(err, "force_members failed; will retry", "instance", me)
			return ctrl.Result{RequeueAfter: waitRequeue}, nil
		}
		log.Info("force_members succeeded; group re-formed", "instance", me)
		before := pod.DeepCopy()
		delete(pod.Annotations, forceQuorumMembersAnnotation)
		if err := doorbellClient.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			log.Error(err, "Could not clear force-quorum-members annotation", "instance", me)
		}
		return ctrl.Result{RequeueAfter: steadyRequeue}, nil
	} else if pod.Annotations[forceGroupRebootstrapAnnotation] == "yes" {
		// Total-outage re-bootstrap: the operator has proven this member holds
		// every committed transaction and there is no surviving group view to
		// reset. Re-create the group from scratch by bootstrapping here; the other
		// members rejoin via normal distributed recovery once this group exists.
		log.Info("Executing guarded total-outage re-bootstrap", "instance", me)
		// Clear any half-started or OFFLINE-leftover member state first. A bootstrap
		// fails with "Another instance of START/STOP GROUP_REPLICATION command is
		// executing" if a previous join is still in flight, so stop before creating
		// the group. STOP on an already-stopped member is a no-op.
		if err := r.Local.StopGroupReplication(ctx); err != nil {
			log.Error(err, "Could not stop Group Replication before re-bootstrap; proceeding", "instance", me)
		}
		if err := r.Local.BootstrapGroup(ctx); err != nil {
			log.Error(err, "re-bootstrap failed; will retry", "instance", me)
			return ctrl.Result{RequeueAfter: waitRequeue}, nil
		}
		log.Info("re-bootstrap succeeded; group re-formed", "instance", me)
		before := pod.DeepCopy()
		delete(pod.Annotations, forceGroupRebootstrapAnnotation)
		if err := doorbellClient.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			log.Error(err, "Could not clear force-group-rebootstrap annotation", "instance", me)
		}
		return ctrl.Result{RequeueAfter: steadyRequeue}, nil
	}

	// status.GroupReplication is non-nil once the plugin is running and the member
	// appears in replication_group_members. A member that is in the group (ONLINE
	// or RECOVERING) is left to the group; only a member that is not in the group
	// (no GR row, or an OFFLINE row left behind after a failed/aborted start) needs
	// to (re)join.
	gr := status.GroupReplication
	if err := r.ringGroupObservationDoorbell(ctx, gr); err != nil {
		// The annotation only wakes the operator; it never carries authority. Do
		// not hold up group bootstrap/recovery on a transient API-server failure.
		log.Error(err, "Could not publish Group Replication observation", "instance", me)
	}
	if gr != nil {
		switch gr.State {
		case groupreplication.MemberStateOnline:
			// Steady state: a running, online member. Nothing to do; the operator
			// observes and reflects. Watch the local snapshot at a short cadence so
			// an election rings the doorbell without waiting for operator polling.
			return ctrl.Result{RequeueAfter: groupObservationRequeue}, nil
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

	// When the operator has both blocked an established group and reported lost
	// quorum, it has declared a crisis that only guided recovery can resolve. A
	// plain join cannot re-form a dead group — START GROUP_REPLICATION blocks for
	// the full member-join timeout trying to reach unreachable seeds. That holds
	// MySQL's START/STOP lock and starves the operator's recovery command, which then
	// collides with Error 3663. Stand down and wait; the operator designates a safe
	// survivor via the Pod annotations handled above. Requiring both signals avoids
	// the transient false quorum observation seen during restarts and other Blocked
	// states such as a denied fence operation, where an offline member should still join.
	groupStatus := cluster.Status.GroupReplication
	if cluster.Status.Phase == clusterPhaseBlocked &&
		groupStatus != nil && groupStatus.Bootstrapped && !groupStatus.HasQuorum {
		log.Info("Cluster is Blocked; awaiting operator-guided recovery instead of joining", "instance", me)
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

	// Join the existing group. For a total outage where the group view is gone,
	// this start cannot re-form the group; the member stays OFFLINE and the
	// operator surfaces the degradation. Re-forming is the guarded, opt-in
	// re-bootstrap handled above (forceGroupRebootstrapAnnotation), driven by the
	// operator once it has proven a safe survivor.
	log.Info("Starting group replication to join the group", "instance", me)
	if err := r.Local.StartGroupReplication(ctx); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: waitRequeue}, nil
}

// ringGroupObservationDoorbell publishes a short fingerprint of the locally
// observed GR snapshot on this instance's own Pod. The operator watches owned
// Pods and treats the annotation only as a wake-up before cross-validating the
// group over mTLS.
func (r *Reconciler) ringGroupObservationDoorbell(
	ctx context.Context,
	gr *webserver.GroupReplicationMemberStatus,
) error {
	doorbellClient := r.DoorbellClient
	if doorbellClient == nil {
		doorbellClient = r.Client
	}
	pod := &corev1.Pod{}
	key := types.NamespacedName{Namespace: r.ClusterKey.Namespace, Name: r.InstanceName}
	if err := doorbellClient.Get(ctx, key, pod); err != nil {
		return err
	}
	fingerprint := groupObservationFingerprint(gr)
	if pod.Annotations[groupObservationAnnotation] == fingerprint {
		return nil
	}
	before := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[groupObservationAnnotation] = fingerprint
	return doorbellClient.Patch(ctx, pod, client.MergeFrom(before))
}

// groupObservationFingerprint covers every GR transition that needs to wake the
// operator: primary, membership view, local state and quorum. It intentionally
// excludes continuously changing data such as GTID execution.
func groupObservationFingerprint(gr *webserver.GroupReplicationMemberStatus) string {
	primary, viewID, state := "", "", ""
	hasQuorum := false
	if gr != nil {
		primary = gr.PrimaryMemberID
		viewID = gr.ViewID
		state = gr.State
		online := 0
		for _, member := range gr.Members {
			if member.State == groupreplication.MemberStateOnline {
				online++
			}
		}
		hasQuorum = online*2 > len(gr.Members)
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s\x00%t", primary, viewID, state, hasQuorum))
	return hex.EncodeToString(sum[:8])
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
