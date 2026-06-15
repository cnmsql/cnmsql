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
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// primaryHealthy reports whether the expected primary is reachable, ready and
// still acting as the primary.
func primaryHealthy(observed observedCluster) bool {
	status, ok := observed.StatusByInstance[observed.PrimaryName]
	if !ok {
		return false
	}
	return status.IsReady && status.Role == webserver.RolePrimary
}

// reconcileFailover promotes a safe replica when the current primary is
// unreachable for longer than spec.failoverDelay. It returns handled=true when
// it took ownership of this reconcile (caller should return the given Result).
func (r *ClusterReconciler) reconcileFailover(
	ctx context.Context,
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
	if remaining := delay - time.Since(failingSince); remaining > 0 {
		reason := fmt.Sprintf("Primary %s unreachable; waiting %s before failover", observed.PrimaryName, remaining.Round(time.Second))
		return true, ctrl.Result{RequeueAfter: remaining}, r.patchOperationPhase(ctx, cluster, observed, phaseDegraded, reason, false)
	}

	candidate, reason := selectFailoverCandidate(observed)
	if candidate == "" {
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

// selectFailoverCandidate picks the safest reachable replica to promote: it
// must still have SQL apply running, and its executed GTID set must contain
// every other candidate's. During a real primary outage the replica IO thread
// is expected to stop because the source vanished, so failover must not require
// IORunning or instance readiness. Ties (equal GTID) break to the lowest ordinal.
// When no replica dominates all others the sets are incomparable and failover is
// blocked rather than risking data loss.
func selectFailoverCandidate(observed observedCluster) (string, string) {
	var candidates []string
	for _, name := range observed.InstanceNames {
		if name == observed.PrimaryName {
			continue
		}
		// A fenced instance is deliberately held out of service; never promote it.
		if slices.Contains(observed.FencedInstances, name) {
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

// fenceInstancePod deletes the instance Pod (retaining its PVC) so a recovered
// old primary cannot serve writes. A missing Pod is already fenced.
func (r *ClusterReconciler) fenceInstancePod(ctx context.Context, cluster *mysqlv1alpha1.Cluster, name string) error {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	if pod.DeletionTimestamp != nil {
		return nil
	}
	if err := r.Delete(ctx, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

// recordPrimaryFailing stamps status.primaryFailingSince on the first reconcile
// that observes the primary unreachable and returns the effective failing time.
func (r *ClusterReconciler) recordPrimaryFailing(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (time.Time, error) {
	if existing := cluster.Status.PrimaryFailingSince; existing != "" {
		if ts, err := time.Parse(time.RFC3339, existing); err == nil {
			return ts, nil
		}
	}
	now := time.Now().Truncate(time.Second)
	if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		s.PrimaryFailingSince = now.Format(time.RFC3339)
	}); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

// patchOperationPhase records an in-flight operation phase (e.g. Degraded,
// Blocked) while keeping the observed instance topology fields fresh.
func (r *ClusterReconciler) patchOperationPhase(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
	phase, reason string,
	ready bool,
) error {
	op := observed
	op.Phase = phase
	op.PhaseReason = reason
	op.Ready = ready
	op.Progressing = !ready
	return r.patchStatus(ctx, cluster, op)
}

// updateStatus applies mutate to the latest Cluster status and patches it.
func (r *ClusterReconciler) updateStatus(ctx context.Context, cluster *mysqlv1alpha1.Cluster, mutate func(*mysqlv1alpha1.ClusterStatus)) error {
	latest := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := r.Get(ctx, key, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	mutate(&latest.Status)
	if err := r.Status().Patch(ctx, latest, client.MergeFrom(before)); err != nil {
		return err
	}
	latest.Status.DeepCopyInto(&cluster.Status)
	return nil
}
