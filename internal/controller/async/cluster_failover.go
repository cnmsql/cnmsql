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

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
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
	candidate, reason := SelectFailoverCandidate(observed, knownDiverged)
	if candidate == "" {
		if !hasObservedReplica(observed) {
			return topology.FailoverResult{}, nil
		}
		blockReason := fmt.Sprintf("Cannot fail over from %s: %s", observed.PrimaryName, reason)
		return phaseResult(request.RetryInterval, topology.PhaseBlocked, blockReason), nil
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

	logf.FromContext(ctx).Info("Failing over primary", "from", observed.PrimaryName, "to", candidate)
	message := fmt.Sprintf("Failing over from %s to %s", observed.PrimaryName, candidate)
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

// SelectFailoverCandidate chooses the safest reachable async replica. The SQL
// applier must be running and its GTID set must contain every other candidate's.
// Equal GTID sets resolve to the first instance, preserving ordinal order.
func SelectFailoverCandidate(observed topology.FailoverState, knownDiverged []string) (string, string) {
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
			return "", "every replica candidate has diverged from the failed primary (errant transactions); manual recovery required"
		}
		return "", "no healthy replica candidate available"
	}
	for _, candidate := range candidates {
		dominatesAll := true
		for _, other := range candidates {
			if candidate == other {
				continue
			}
			contains, err := replication.GTIDContains(
				observed.Instances[candidate].GTID,
				observed.Instances[other].GTID,
			)
			if err != nil {
				return "", fmt.Sprintf("comparing gtid sets: %v", err)
			}
			if !contains {
				dominatesAll = false
				break
			}
		}
		if dominatesAll {
			return candidate, ""
		}
	}
	return "", "candidate replicas have diverged GTID sets that cannot be proven safe"
}
