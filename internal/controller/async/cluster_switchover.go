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
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
)

// ReconcileSwitchover validates and bounds a planned async primary change. The
// in-Pod reconcilers perform the actual promotion and replication reconfiguration.
func (r *Reconciler) ReconcileSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed topology.FailoverState,
) (topology.FailoverResult, error) {
	target := cluster.Status.TargetPrimary
	current := cluster.Status.CurrentPrimary
	if target == "" || target == current || current == "" {
		return topology.FailoverResult{}, nil
	}
	if err := validateSwitchoverTarget(observed, target); err != nil {
		return topology.FailoverResult{
			Handled: true,
			Phase: &topology.OperationPhase{
				Phase:  topology.PhaseBlocked,
				Reason: err.Error(),
			},
		}, nil
	}

	startedAt, err := r.ensureSwitchoverStarted(ctx, cluster)
	if err != nil {
		return topology.FailoverResult{}, err
	}
	maxDelay := time.Duration(cluster.Spec.MaxSwitchoverDelay) * time.Second
	if maxDelay > 0 && time.Since(startedAt) > maxDelay {
		return r.abortSwitchover(ctx, cluster, current, target)
	}

	return topology.FailoverResult{
		Handled: true,
		Phase: &topology.OperationPhase{
			Phase:       topology.PhaseSwitchover,
			Reason:      fmt.Sprintf("Switching over to %s", target),
			Progressing: true,
		},
	}, nil
}

// ReconcileDrainSwitchover promotes a GTID-safe replica via a planned switchover
// when the established primary's Pod is gracefully terminating (e.g. a node
// drain) while the primary is still healthy. It records TargetPrimary and the
// Switchover phase; the next pass drives the promotion through the normal
// switchover path. It is a no-op (deferring to reactive failover) when the
// feature is disabled, the primary is not the terminating instance, a switchover
// is already in flight, the primary is already unreachable, or no safe candidate
// exists.
func (r *Reconciler) ReconcileDrainSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed topology.FailoverState,
) (topology.FailoverResult, error) {
	if !cluster.IsSwitchoverOnDrainEnabled() {
		return topology.FailoverResult{}, nil
	}
	current := cluster.Status.CurrentPrimary
	if current == "" || observed.PrimaryName == "" {
		return topology.FailoverResult{}, nil
	}
	// Only act when the established primary is the instance being drained.
	if !slices.Contains(observed.Terminating, current) {
		return topology.FailoverResult{}, nil
	}
	// A switchover is already in flight toward another instance: let it finish.
	if cluster.Status.TargetPrimary != "" && cluster.Status.TargetPrimary != current {
		return topology.FailoverResult{}, nil
	}
	// The primary must still be reachable and acting as primary. If it has already
	// gone, this is a failure, not a planned drain, and the failover path owns it.
	if !PrimaryHealthy(observed) {
		return topology.FailoverResult{}, nil
	}
	knownDiverged := append(slices.Clone(observed.Diverged), cluster.Status.DivergedInstances...)
	candidate, _ := SelectFailoverCandidate(observed, knownDiverged)
	if candidate == "" {
		// No provably-safe replica: do nothing and let failover handle the primary
		// once it becomes unreachable.
		return topology.FailoverResult{}, nil
	}

	message := fmt.Sprintf("Primary %s is draining; switching over to %s", current, candidate)
	logf.FromContext(ctx).Info("Switching over a draining primary", "from", current, "to", candidate)
	if err := topology.PatchClusterStatus(ctx, r.client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
		status.TargetPrimary = candidate
		status.TargetPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
		status.Phase = topology.PhaseSwitchover
		status.PhaseReason = message
	}); err != nil {
		return topology.FailoverResult{Handled: true}, err
	}
	if r.recorder != nil {
		r.recorder.Event(cluster, corev1.EventTypeNormal, topology.PhaseSwitchover, message)
	}
	return topology.FailoverResult{Handled: true}, nil
}

func validateSwitchoverTarget(observed topology.FailoverState, target string) error {
	status, ok := observed.Instances[target]
	if !ok {
		return fmt.Errorf("target primary %s is not reporting status", target)
	}
	if !status.Ready {
		return fmt.Errorf("target primary %s is not ready", target)
	}
	if !status.Replica {
		return fmt.Errorf("target primary %s has role %s, want replica", target, status.Role)
	}
	if !status.IORunning || !status.SQLRunning {
		return fmt.Errorf("target primary %s replication is not healthy", target)
	}
	return nil
}

func (r *Reconciler) ensureSwitchoverStarted(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) (time.Time, error) {
	if timestamp := cluster.Status.TargetPrimaryTimestamp; timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339, timestamp); err == nil {
			return parsed, nil
		}
	}
	now := time.Now().Truncate(time.Second)
	if err := topology.PatchClusterStatus(ctx, r.client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
		status.TargetPrimaryTimestamp = now.Format(time.RFC3339)
	}); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

func (r *Reconciler) abortSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	current, target string,
) (topology.FailoverResult, error) {
	reason := fmt.Sprintf("switchover to %s aborted: not promoted within maxSwitchoverDelay (%ds)",
		target, cluster.Spec.MaxSwitchoverDelay)
	logf.FromContext(ctx).Info("Aborting switchover", "target", target, "restoredPrimary", current, "reason", reason)
	if err := topology.PatchClusterStatus(ctx, r.client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
		status.TargetPrimary = current
		status.TargetPrimaryTimestamp = ""
		status.Phase = topology.PhaseBlocked
		status.PhaseReason = reason
	}); err != nil {
		return topology.FailoverResult{}, err
	}
	if r.recorder != nil {
		r.recorder.Event(cluster, corev1.EventTypeWarning, topology.PhaseBlocked, reason)
	}
	return topology.FailoverResult{Handled: true}, nil
}
