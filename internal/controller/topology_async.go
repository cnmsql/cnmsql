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
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// asyncTopology implements topologyReconciler for the asynchronous /
// semi-synchronous GTID primary-replica topology. This is the original operator
// behaviour, moved largely unchanged behind the strategy interface.
type asyncTopology struct{}

func (asyncTopology) Name() string { return "async" }

func (asyncTopology) DonorAvailable(observed observedCluster) bool {
	status, ok := observed.StatusByInstance[observed.PrimaryName]
	if !ok {
		return false
	}
	return status.IsReady && status.Role == webserver.RolePrimary
}

func (asyncTopology) NeedsPrimaryLease() bool { return true }

func (asyncTopology) IsSemiSyncRelevant() bool { return true }

// ReconcileFailover promotes a safe replica when the current primary is
// unreachable for longer than spec.failoverDelay. It returns handled=true when
// it took ownership of this reconcile.
func (asyncTopology) ReconcileFailover(
	ctx context.Context,
	r *ClusterReconciler,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, ctrl.Result, error) {
	// Only an already-established primary can be failed over; never during the
	// initial bootstrap, and never for a single-instance cluster (no candidate).
	if cluster.Status.CurrentPrimary == "" || observed.PrimaryName == "" || plan.Instances < 2 {
		return false, ctrl.Result{}, nil
	}
	if primaryHealthy(observed) {
		if cluster.Status.PrimaryFailingSince != "" {
			return false, ctrl.Result{}, r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
				s.PrimaryFailingSince = ""
			})
		}
		return false, ctrl.Result{}, nil
	}

	// The primary is unreachable: remember when it first failed so failoverDelay
	// can be honoured across reconciles.
	failingSince, err := r.recordPrimaryFailing(ctx, cluster)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	delay := time.Duration(cluster.Spec.FailoverDelay) * time.Second

	// During an in-place operator upgrade the manager re-execs itself, causing a
	// brief (~1-2s) control-API outage while the process image is replaced. An
	// unreachable primary whose persisted instance-manager hash is stale (not yet
	// matching the operator's) is likely mid-upgrade rather than genuinely dead.
	// Give it a minimum grace window before triggering failover so a transient
	// outage during the swap does not fence a working primary.
	if cluster.Spec.InPlaceInstanceManagerUpdates && r.OperatorExecutableHash != "" {
		if isStale, ok := isInstanceHashStale(cluster, observed.PrimaryName, r.OperatorExecutableHash); ok && isStale {
			logf.FromContext(ctx).Info("Primary is unreachable but its manager hash is stale; extending failover grace for in-place upgrade",
				"instance", observed.PrimaryName)
			if minDelay := 30 * time.Second; delay < minDelay {
				delay = minDelay
			}
		}
	}

	if remaining := delay - time.Since(failingSince); remaining > 0 {
		reason := fmt.Sprintf("Primary %s unreachable; waiting %s before failover", observed.PrimaryName, remaining.Round(time.Second))
		return true, ctrl.Result{RequeueAfter: remaining}, r.patchOperationPhase(ctx, cluster, observed, phaseDegraded, reason, false)
	}

	// Exclude any replica known to have diverged. observed.DivergedInstances is
	// recomputed each reconcile, but during a primary outage the primary's GTID is
	// unavailable so divergence cannot be recomputed and that set is empty — hence
	// we also carry the last-known set persisted in status, recorded while the
	// primary was still reachable, so a diverged replica stays ineligible across
	// the outage.
	knownDiverged := append(slices.Clone(observed.DivergedInstances), cluster.Status.DivergedInstances...)
	candidate, reason := selectFailoverCandidate(observed, knownDiverged)
	if candidate == "" {
		// No safe candidate to promote. If no replica is even observed yet, the
		// cluster is still bootstrapping its replica set: the replicas that would
		// become candidates are created by reconcileInstances, which runs after this
		// function and only when we do not take over the reconcile. Blocking here
		// would therefore deadlock provisioning at "1/3 ready". Yield so the
		// reconcile proceeds to create them. Once a replica is observed (an
		// established cluster), keep blocking rather than risk promoting an unsafe
		// or diverged replica. Failover ordering is otherwise preserved: when a
		// candidate exists this branch is never reached.
		if !hasObservedReplica(observed) {
			return false, ctrl.Result{}, nil
		}
		blockReason := fmt.Sprintf("Cannot fail over from %s: %s", observed.PrimaryName, reason)
		return true, ctrl.Result{RequeueAfter: readyResync}, r.patchOperationPhase(ctx, cluster, observed, phaseBlocked, blockReason, false)
	}

	held, err := r.isPrimaryLeaseHeld(ctx, cluster, observed.PrimaryName)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if held {
		reason := fmt.Sprintf("Primary lease still held by %s; waiting for expiry", observed.PrimaryName)
		return true, ctrl.Result{RequeueAfter: primaryLeaseDuration}, r.patchOperationPhase(ctx, cluster, observed, phaseDegraded, reason, false)
	}

	// Fence the old primary before moving the primary role so a recovered node
	// cannot accept writes (split brain). The PVC is retained for rejoin.
	if err := r.fenceInstancePod(ctx, cluster, observed.PrimaryName); err != nil {
		return true, ctrl.Result{}, fmt.Errorf("fence old primary %s: %w", observed.PrimaryName, err)
	}
	// Point targetPrimary at the chosen candidate. Its in-Pod reconciler promotes
	// it and sets currentPrimary; the surviving replicas re-point themselves.
	logf.FromContext(ctx).Info("Failing over primary", "from", observed.PrimaryName, "to", candidate)
	if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		s.TargetPrimary = candidate
		s.TargetPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
		s.PrimaryFailingSince = ""
		s.Phase = phaseFailingOver
		s.PhaseReason = fmt.Sprintf("Failing over from %s to %s", observed.PrimaryName, candidate)
	}); err != nil {
		return true, ctrl.Result{}, err
	}
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, phaseFailingOver,
			fmt.Sprintf("Failing over from %s to %s", observed.PrimaryName, candidate))
	}
	return true, ctrl.Result{RequeueAfter: provisioningRequeue}, nil
}

// ReconcileSwitchover drives a planned switchover requested by setting
// status.targetPrimary to a replica. In the CNPG pull-model the operator only
// validates the request and bounds it by spec.maxSwitchoverDelay; the actual
// promotion/demotion is performed by the instances' in-Pod reconcilers. It
// returns handled=true while the switchover is in flight.
func (asyncTopology) ReconcileSwitchover(
	ctx context.Context,
	r *ClusterReconciler,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, error) {
	target := cluster.Status.TargetPrimary
	current := cluster.Status.CurrentPrimary
	if target == "" || target == current {
		return false, nil
	}
	if current == "" {
		// Initial bootstrap has a target primary before any instance has promoted
		// itself and recorded currentPrimary. Let normal observation surface the
		// Pending/Provisioning phase while the target's in-Pod reconciler starts.
		return false, nil
	}

	if err := validateSwitchoverTarget(observed, target); err != nil {
		return true, r.patchStatus(ctx, cluster, observedCluster{
			Phase:          phaseBlocked,
			PhaseReason:    err.Error(),
			Ready:          false,
			Progressing:    false,
			Plan:           plan,
			PrimaryName:    observed.PrimaryName,
			InstanceNames:  observed.InstanceNames,
			ReadyInstances: observed.ReadyInstances,
			GTIDByInstance: observed.GTIDByInstance,
		})
	}

	// Bound the switchover by spec.maxSwitchoverDelay (RTO): if the target's
	// in-Pod reconciler has not promoted it (currentPrimary still != target)
	// within the budget, abort and restore the original primary.
	startedAt, err := r.ensureSwitchoverStarted(ctx, cluster)
	if err != nil {
		return false, err
	}
	maxDelay := time.Duration(cluster.Spec.MaxSwitchoverDelay) * time.Second
	if maxDelay > 0 && time.Since(startedAt) > maxDelay {
		return r.abortSwitchover(ctx, cluster, current, target)
	}

	// Switchover in flight: the instances do the work. Surface the phase and wait
	// for currentPrimary to flip to the target.
	return true, r.patchStatus(ctx, cluster, observedCluster{
		Phase:          phaseSwitchover,
		PhaseReason:    fmt.Sprintf("Switching over to %s", target),
		Ready:          false,
		Progressing:    true,
		Plan:           plan,
		PrimaryName:    observed.PrimaryName,
		InstanceNames:  observed.InstanceNames,
		ReadyInstances: observed.ReadyInstances,
		GTIDByInstance: observed.GTIDByInstance,
	})
}

func (asyncTopology) ReconcileAvailability(
	ctx context.Context,
	r *ClusterReconciler,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) error {
	return r.reconcileSemiSync(ctx, cluster, observed)
}

func (asyncTopology) ObserveTopology(
	_ context.Context,
	_ *mysqlv1alpha1.Cluster,
	observed *observedCluster,
) {
	observed.DivergedInstances = detectDivergedReplicas(*observed)
	for _, name := range observed.DivergedInstances {
		// A diverged replica may keep its threads running while silently
		// diverging, so do not count it as a healthy ready instance.
		if status, ok := observed.StatusByInstance[name]; ok && status.IsReady {
			observed.ReadyInstances--
		}
	}
	observed.ReplicationBrokenInstances = detectReplicationBroken(*observed)
}

// primaryHealthy reports whether the expected primary is reachable, ready and
// still acting as the primary.
func primaryHealthy(observed observedCluster) bool {
	status, ok := observed.StatusByInstance[observed.PrimaryName]
	if !ok {
		return false
	}
	return status.IsReady && status.Role == webserver.RolePrimary
}

// hasObservedReplica reports whether any non-primary instance is currently
// observed with a control status. It distinguishes an established cluster (whose
// replicas exist and were reachable) from one still bootstrapping its replica
// set, so failover does not block initial provisioning when there is nothing yet
// to fail over to.
func hasObservedReplica(observed observedCluster) bool {
	for name := range observed.StatusByInstance {
		if name != observed.PrimaryName {
			return true
		}
	}
	return false
}

// selectFailoverCandidate picks the safest reachable replica to promote: it
// must still have SQL apply running, and its executed GTID set must contain
// every other candidate's. During a real primary outage the replica IO thread
// is expected to stop because the source vanished, so failover must not require
// IORunning or instance readiness. Ties (equal GTID) break to the lowest ordinal.
// When no replica dominates all others the sets are incomparable and failover is
// blocked rather than risking data loss.
//
// knownDiverged lists replicas known to carry errant transactions the old
// primary never had. They must never be promoted: a diverged replica's GTID set
// is a *superset*, so it would dominate the comparison below and be chosen,
// making those errant transactions canonical and stranding the clean replicas.
// The dominance check alone does not catch this — a superset legitimately
// "contains" the others. Divergence can only be computed while the primary is
// reachable (it is the comparison baseline), so the caller passes the last-known
// set persisted in status, which survives the very outage that triggers failover.
func selectFailoverCandidate(observed observedCluster, knownDiverged []string) (string, string) {
	var candidates []string
	divergedSkipped := 0
	for _, name := range observed.InstanceNames {
		if name == observed.PrimaryName {
			continue
		}
		// A fenced instance is deliberately held out of service; never promote it.
		if slices.Contains(observed.FencedInstances, name) {
			continue
		}
		if slices.Contains(knownDiverged, name) {
			divergedSkipped++
			continue
		}
		status, ok := observed.StatusByInstance[name]
		if !ok || status.Role != webserver.RoleReplica {
			continue
		}
		if status.Replication == nil || !status.Replication.SQLRunning {
			continue
		}
		if observed.GTIDByInstance[name] == "" {
			continue
		}
		candidates = append(candidates, name)
	}
	if len(candidates) == 0 {
		if divergedSkipped > 0 {
			return "", "every replica candidate has diverged from the failed primary (errant transactions); manual recovery required"
		}
		return "", "no healthy replica candidate available"
	}
	// InstanceNames is ordinal-ordered, so the first dominating candidate is the
	// lowest ordinal among equally complete replicas.
	for _, c := range candidates {
		dominatesAll := true
		for _, other := range candidates {
			if c == other {
				continue
			}
			contains, err := replication.GTIDContains(observed.GTIDByInstance[c], observed.GTIDByInstance[other])
			if err != nil {
				return "", fmt.Sprintf("comparing gtid sets: %v", err)
			}
			if !contains {
				dominatesAll = false
				break
			}
		}
		if dominatesAll {
			return c, ""
		}
	}
	return "", "candidate replicas have diverged GTID sets that cannot be proven safe"
}

// validateSwitchoverTarget checks that the requested target exists, is ready, is
// a replica, and has healthy replication threads.
func validateSwitchoverTarget(observed observedCluster, target string) error {
	status, ok := observed.StatusByInstance[target]
	if !ok {
		return fmt.Errorf("target primary %s is not reporting status", target)
	}
	if !status.IsReady {
		return fmt.Errorf("target primary %s is not ready", target)
	}
	if status.Role != webserver.RoleReplica {
		return fmt.Errorf("target primary %s has role %s, want replica", target, status.Role)
	}
	if status.Replication == nil || !status.Replication.IORunning || !status.Replication.SQLRunning {
		return fmt.Errorf("target primary %s replication is not healthy", target)
	}
	return nil
}
