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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	controllerasync "github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/async"
)

// reconcileFailover promotes a safe replica when the current primary is
// unreachable for longer than spec.failoverDelay. It returns handled=true when
// it took ownership of this reconcile (caller should return the given Result).
func (r *ClusterReconciler) reconcileFailover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, ctrl.Result, error) {
	failoverState := topologyFailoverState(observed)
	// Group Replication elects and fails over within the group; the operator must
	// never drive an async failover (that is the "operator does not promote"
	// guarantee). It observes the new primary instead.
	if cluster.IsGroupReplication() {
		return false, ctrl.Result{}, nil
	}
	// Only an already-established primary can be failed over; never during the
	// initial bootstrap, and never for a single-instance cluster (no candidate).
	if cluster.Status.CurrentPrimary == "" || observed.PrimaryName == "" || plan.Instances < 2 {
		return false, ctrl.Result{}, nil
	}
	if controllerasync.PrimaryHealthy(failoverState) {
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
	candidate, reason := controllerasync.SelectFailoverCandidate(failoverState, knownDiverged)
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
		if !controllerasync.HasObservedReplica(failoverState) {
			return false, ctrl.Result{}, nil
		}
		blockReason := fmt.Sprintf("Cannot fail over from %s: %s", observed.PrimaryName, reason)
		return true, ctrl.Result{RequeueAfter: readyResync}, r.patchOperationPhase(ctx, cluster, observed, phaseBlocked, blockReason, false)
	}

	lease, err := r.topologyReconciler(cluster).PrimaryLeaseStatus(ctx, cluster, observed.PrimaryName)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if lease.Held {
		reason := fmt.Sprintf("Primary lease still held by %s; waiting for expiry", observed.PrimaryName)
		return true, ctrl.Result{RequeueAfter: lease.RetryAfter}, r.patchOperationPhase(ctx, cluster, observed, phaseDegraded, reason, false)
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

// isInstanceHashStale reports whether the named instance's instance-manager
// binary hash, as persisted in the Cluster status from the last successful
// observation, is non-empty and does not yet match the operator's hash. It
// returns (false, false) when the instance has never been observed, so the
// caller treats a brand-new cluster normally.
func isInstanceHashStale(cluster *mysqlv1alpha1.Cluster, name, operatorHash string) (bool, bool) {
	hash, ok := cluster.Status.ExecutableHashByInstance[name]
	if !ok {
		return false, false
	}
	return hash != "" && hash != operatorHash, true
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
