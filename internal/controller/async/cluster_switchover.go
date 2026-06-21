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
	"time"

	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
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
