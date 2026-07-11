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

package async

import (
	"context"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/engine"
)

// ReconcileFailover fences an unreachable async primary and selects the safest
// replica after the configured delay and primary Lease have expired.
func (r *Reconciler) ReconcileFailover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	request topology.FailoverRequest,
) (topology.FailoverResult, error) {
	observed := request.Observed
	if cluster.Status.CurrentPrimary == "" || observed.PrimaryName == "" || request.Instances < 2 {
		return topology.FailoverResult{}, nil
	}
	if PrimaryHealthy(observed) {
		if cluster.Status.PrimaryFailingSince != "" {
			return topology.FailoverResult{}, topology.PatchClusterStatus(ctx, r.client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
				status.PrimaryFailingSince = ""
			})
		}
		return topology.FailoverResult{}, nil
	}

	failingSince, err := r.recordPrimaryFailing(ctx, cluster)
	if err != nil {
		return topology.FailoverResult{Handled: true}, err
	}
	delay := time.Duration(cluster.Spec.FailoverDelay) * time.Second
	if cluster.Spec.InPlaceInstanceManagerUpdates && r.operatorHash != "" {
		if stale, ok := isInstanceHashStale(cluster, observed.PrimaryName, r.operatorHash); ok && stale {
			logf.FromContext(ctx).Info("Primary is unreachable but its manager hash is stale; extending failover grace for in-place upgrade",
				"instance", observed.PrimaryName)
			delay = max(delay, 30*time.Second)
		}
	}
	if primary, ok := observed.Instances[observed.PrimaryName]; ok && primary.InPlaceUpgrading {
		logf.FromContext(ctx).Info("Primary is undergoing an in-place manager upgrade; extending failover grace",
			"instance", observed.PrimaryName)
		delay = max(delay, 30*time.Second)
	}
	if remaining := delay - time.Since(failingSince); remaining > 0 {
		reason := fmt.Sprintf("Primary %s unreachable; waiting %s before failover", observed.PrimaryName, remaining.Round(time.Second))
		return phaseResult(remaining, topology.PhaseDegraded, reason), nil
	}

	knownDiverged := append(slices.Clone(observed.Diverged), cluster.Status.DivergedInstances...)
	eng, err := engine.ForFlavor(engine.Flavor(cluster.ResolvedFlavor()))
	if err != nil {
		return topology.FailoverResult{}, fmt.Errorf("unknown engine flavor %q", cluster.ResolvedFlavor())
	}
	elected := SelectFailoverCandidate(Election{
		Observed:              observed,
		KnownDiverged:         knownDiverged,
		GTID:                  eng.GTID(),
		MaxTransactionsBehind: maxTransactionsBehind(cluster),
		// The primary is unreachable by now, so its live position is unreadable and
		// the last persisted snapshot is all there is to measure the gap against.
		ReferenceGTID: cluster.Status.GTIDExecutedByInstance[observed.PrimaryName],
	})
	candidate := elected.Name
	if candidate == "" {
		if !hasObservedReplica(observed) {
			return topology.FailoverResult{}, nil
		}
		blockReason := fmt.Sprintf("Cannot fail over from %s: %s", observed.PrimaryName, elected.Reason)
		// Failover is blocked, but must not prevent the reconciler from
		// recreating the former primary Pod. Setting phase to Blocked
		// surfaces the reason while returning Handled=false lets the
		// reconciler fall through to instance provisioning.
		return topology.FailoverResult{
			Handled: false,
			Phase: &topology.OperationPhase{
				Phase:       topology.PhaseBlocked,
				Reason:      blockReason,
				Progressing: true,
			},
		}, nil
	}

	lease, err := r.PrimaryLeaseStatus(ctx, cluster, observed.PrimaryName)
	if err != nil {
		return topology.FailoverResult{Handled: true}, err
	}
	if lease.Held {
		reason := fmt.Sprintf("Primary lease still held by %s; waiting for expiry", observed.PrimaryName)
		return phaseResult(lease.RetryAfter, topology.PhaseDegraded, reason), nil
	}
	if err := r.fenceInstancePod(ctx, cluster, observed.PrimaryName); err != nil {
		return topology.FailoverResult{Handled: true}, fmt.Errorf("fence old primary %s: %w", observed.PrimaryName, err)
	}

	logf.FromContext(ctx).Info("Failing over primary",
		"from", observed.PrimaryName, "to", candidate, "transactionsBehind", elected.TransactionsBehind)
	message := fmt.Sprintf("Failing over from %s to %s", observed.PrimaryName, candidate)
	if elected.TransactionsBehind > 0 {
		message += fmt.Sprintf(", which is %d transactions behind it", elected.TransactionsBehind)
	}
	if err := topology.PatchClusterStatus(ctx, r.client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
		status.TargetPrimary = candidate
		status.TargetPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
		status.PrimaryFailingSince = ""
		status.Phase = topology.PhaseFailingOver
		status.PhaseReason = message
	}); err != nil {
		return topology.FailoverResult{Handled: true}, err
	}
	if r.recorder != nil {
		r.recorder.Event(cluster, corev1.EventTypeWarning, topology.PhaseFailingOver, message)
	}
	return topology.FailoverResult{Handled: true, RequeueAfter: request.ProvisioningRetry}, nil
}

// maxTransactionsBehind returns the configured promotion bound, or nil when the
// cluster sets no failover policy.
func maxTransactionsBehind(cluster *mysqlv1alpha1.Cluster) *int64 {
	if cluster.Spec.FailoverPolicy == nil {
		return nil
	}
	return cluster.Spec.FailoverPolicy.MaxTransactionsBehind
}

func phaseResult(requeueAfter time.Duration, phase, reason string) topology.FailoverResult {
	return topology.FailoverResult{
		Handled:      true,
		RequeueAfter: requeueAfter,
		Phase: &topology.OperationPhase{
			Phase:       phase,
			Reason:      reason,
			Progressing: true,
		},
	}
}

func (r *Reconciler) fenceInstancePod(ctx context.Context, cluster *mysqlv1alpha1.Cluster, name string) error {
	pod := &corev1.Pod{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	if pod.DeletionTimestamp != nil {
		return nil
	}
	return client.IgnoreNotFound(r.client.Delete(ctx, pod))
}

func (r *Reconciler) recordPrimaryFailing(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (time.Time, error) {
	if existing := cluster.Status.PrimaryFailingSince; existing != "" {
		if timestamp, err := time.Parse(time.RFC3339, existing); err == nil {
			return timestamp, nil
		}
	}
	now := time.Now().Truncate(time.Second)
	if err := topology.PatchClusterStatus(ctx, r.client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
		status.PrimaryFailingSince = now.Format(time.RFC3339)
	}); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

func isInstanceHashStale(cluster *mysqlv1alpha1.Cluster, name, operatorHash string) (bool, bool) {
	hash, ok := cluster.Status.ExecutableHashByInstance[name]
	if !ok {
		return false, false
	}
	return hash != "" && hash != operatorHash, true
}

// PrimaryHealthy reports whether the expected primary is reachable, ready, and
// still acting as primary.
func PrimaryHealthy(observed topology.FailoverState) bool {
	status, ok := observed.Instances[observed.PrimaryName]
	return ok && status.Ready && status.Primary
}

// hasObservedReplica distinguishes an established replica set from one that is
// still being provisioned.
func hasObservedReplica(observed topology.FailoverState) bool {
	for name := range observed.Instances {
		if name != observed.PrimaryName {
			return true
		}
	}
	return false
}

// FailoverCandidate is the outcome of an election.
type FailoverCandidate struct {
	// Name is the elected replica, empty when none could be chosen.
	Name string
	// TransactionsBehind is how many transactions Name is missing relative to the
	// most advanced reachable replica. Zero when it holds everything the surviving
	// replicas still have between them, which is the best that can be known once
	// the primary is gone.
	TransactionsBehind int64
	// Reason explains why Name is empty.
	Reason string
}

// Election is the input to SelectFailoverCandidate.
type Election struct {
	Observed      topology.FailoverState
	KnownDiverged []string
	GTID          engine.GTIDModel
	// MaxTransactionsBehind bounds how far behind an elected replica may be. Nil
	// means unbounded, the behaviour of a cluster with no failoverPolicy.
	MaxTransactionsBehind *int64
	// ReferenceGTID is the position the lag bound is measured against: the last
	// position known for the primary being replaced. During failover the primary
	// is unreachable, so this comes from the persisted status snapshot, which can
	// lag the primary's true final position. The gap it yields is therefore a
	// lower bound on the transactions a promotion would lose. It is empty when no
	// snapshot exists, which degrades the measurement to peer-relative.
	ReferenceGTID string
}

// SelectFailoverCandidate chooses the safest reachable async replica. The SQL
// applier must be running and its GTID set must contain every other candidate's.
// Equal GTID sets resolve to the first instance, preserving ordinal order.
//
// When MaxTransactionsBehind is set, replicas missing more than that many
// transactions are excluded first. If that excludes every replica the election
// is refused: promoting one would silently discard the transactions it never
// received, and the caller blocks the failover instead. Leaving the bound unset
// keeps the historical behaviour of promoting the best replica however far
// behind it is.
func SelectFailoverCandidate(e Election) FailoverCandidate {
	observed, knownDiverged, gtidModel := e.Observed, e.KnownDiverged, e.GTID
	var candidates []string
	divergedSkipped := 0
	for _, name := range observed.InstanceNames {
		if name == observed.PrimaryName || slices.Contains(observed.Fenced, name) {
			continue
		}
		if slices.Contains(knownDiverged, name) {
			divergedSkipped++
			continue
		}
		status, ok := observed.Instances[name]
		if !ok || !status.Replica || !status.SQLRunning || status.GTID == "" {
			continue
		}
		candidates = append(candidates, name)
	}
	if len(candidates) == 0 {
		if divergedSkipped > 0 {
			return FailoverCandidate{Reason: "every replica candidate has diverged from the failed primary (errant transactions); manual recovery required"}
		}
		return FailoverCandidate{Reason: "no healthy replica candidate available"}
	}

	behind, err := transactionsBehind(observed, candidates, gtidModel, e.ReferenceGTID)
	if err != nil {
		return FailoverCandidate{Reason: err.Error()}
	}
	eligible := withinBound(candidates, behind, e.MaxTransactionsBehind)
	if len(eligible) == 0 {
		closest := slices.MinFunc(candidates, func(a, b string) int {
			return int(behind[a] - behind[b])
		})
		return FailoverCandidate{
			TransactionsBehind: behind[closest],
			Reason: fmt.Sprintf(
				"the closest replica (%s) is %d transactions behind the failed primary, more than maxTransactionsBehind (%d); promoting it would lose those transactions",
				closest, behind[closest], *e.MaxTransactionsBehind),
		}
	}

	for _, candidate := range eligible {
		dominatesAll := true
		for _, other := range eligible {
			if candidate == other {
				continue
			}
			contains, err := gtidModel.Contains(
				observed.Instances[candidate].GTID,
				observed.Instances[other].GTID,
			)
			if err != nil {
				return FailoverCandidate{Reason: fmt.Sprintf("comparing gtid sets: %v", err)}
			}
			if !contains {
				dominatesAll = false
				break
			}
		}
		if dominatesAll {
			return FailoverCandidate{
				Name:               candidate,
				TransactionsBehind: behind[candidate],
			}
		}
	}
	return FailoverCandidate{Reason: "candidate replicas have diverged GTID sets that cannot be proven safe"}
}

// transactionsBehind measures, for each candidate, how many transactions it is
// missing relative to the most complete position anyone is known to have held.
//
// A replica's holdings are its executed set unioned with its retrieved set: relay
// log it has not applied yet is still data it holds, and the applier drains before
// promotion, so only transactions it never received are truly lost.
//
// The reference is referenceGTID, the departing primary's last known position,
// unioned with what the surviving replicas hold. Measuring against the survivors
// alone would be circular, since the candidate that wins the dominance check is
// by construction the most advanced survivor and would always score zero.
func transactionsBehind(
	observed topology.FailoverState,
	candidates []string,
	gtidModel engine.GTIDModel,
	referenceGTID string,
) (map[string]int64, error) {
	held := make(map[string]string, len(candidates))
	positions := make([]string, 0, len(candidates)+1)
	positions = append(positions, referenceGTID)
	for _, name := range candidates {
		status := observed.Instances[name]
		union, err := gtidModel.Union(status.GTID, status.RetrievedGTID)
		if err != nil {
			return nil, fmt.Errorf("merging gtid sets of %s: %w", name, err)
		}
		held[name] = union
		positions = append(positions, union)
	}
	reference, err := gtidModel.Union(positions...)
	if err != nil {
		return nil, fmt.Errorf("merging candidate gtid sets: %w", err)
	}
	behind := make(map[string]int64, len(candidates))
	for _, name := range candidates {
		missing, err := gtidModel.MissingCount(held[name], reference)
		if err != nil {
			return nil, fmt.Errorf("measuring lag of %s: %w", name, err)
		}
		behind[name] = missing
	}
	return behind, nil
}

// withinBound drops candidates lagging by more than maxBehind, returning nothing
// when none qualify. A nil bound admits everyone.
func withinBound(candidates []string, behind map[string]int64, maxBehind *int64) []string {
	if maxBehind == nil {
		return candidates
	}
	var eligible []string
	for _, name := range candidates {
		if behind[name] <= *maxBehind {
			eligible = append(eligible, name)
		}
	}
	return eligible
}
