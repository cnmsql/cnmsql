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

package groupreplication

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlgr "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
)

// ReconcileSwitchover validates the target SECONDARY and invokes
// group_replication_set_as_primary on the current primary to initiate a
// planned handoff. The operator observes the resulting election via
// MergeStatus and the in-Pod reconcilers never self-promote under GR.
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

	targetUUID, err := r.memberUUIDForInstance(ctx, cluster, current, target)
	if err != nil {
		return topology.FailoverResult{
			Handled: true,
			Phase: &topology.OperationPhase{
				Phase:  topology.PhaseBlocked,
				Reason: fmt.Sprintf("cannot resolve target primary %s: %v", target, err),
			},
		}, nil
	}

	grStatus := cluster.Status.GroupReplication
	if grStatus == nil {
		return topology.FailoverResult{}, nil
	}
	var targetMember *mysqlv1alpha1.GroupMember
	for i := range grStatus.Members {
		if grStatus.Members[i].Instance == target {
			targetMember = &grStatus.Members[i]
			break
		}
	}
	if targetMember == nil {
		return topology.FailoverResult{
			Handled: true,
			Phase: &topology.OperationPhase{
				Phase:  topology.PhaseBlocked,
				Reason: fmt.Sprintf("target primary %s is not in the group", target),
			},
		}, nil
	}
	if targetMember.State != mysqlgr.MemberStateOnline {
		return topology.FailoverResult{
			Handled: true,
			Phase: &topology.OperationPhase{
				Phase:  topology.PhaseBlocked,
				Reason: fmt.Sprintf("target primary %s is not ONLINE (state=%s)", target, targetMember.State),
			},
		}, nil
	}
	if targetMember.Role != mysqlgr.MemberRoleSecondary {
		return topology.FailoverResult{
			Handled: true,
			Phase: &topology.OperationPhase{
				Phase:  topology.PhaseBlocked,
				Reason: fmt.Sprintf("target primary %s has role %s, want SECONDARY", target, targetMember.Role),
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

	if err := r.switchoverControl.SetAsPrimary(ctx, cluster, current, targetUUID); err != nil {
		logf.FromContext(ctx).Error(err, "SetAsPrimary failed", "target", target, "uuid", targetUUID)
		return topology.FailoverResult{
			Handled: true,
			Phase: &topology.OperationPhase{
				Phase:  topology.PhaseBlocked,
				Reason: fmt.Sprintf("switchover to %s failed: %v", target, err),
			},
		}, nil
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

// memberUUIDForInstance queries the group view from queryInstance and returns the
// server_uuid of targetInstance.
func (r *Reconciler) memberUUIDForInstance(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	queryInstance, targetInstance string,
) (string, error) {
	status, err := r.switchoverControl.Status(ctx, cluster, queryInstance)
	if err != nil {
		return "", fmt.Errorf("querying status of %s: %w", queryInstance, err)
	}
	if status.GroupReplication == nil {
		return "", fmt.Errorf("instance %s has no Group Replication status", queryInstance)
	}
	for _, member := range status.GroupReplication.Members {
		if member.Host != "" {
			host := member.Host
			if dot := strings.IndexByte(host, '.'); dot > 0 {
				host = host[:dot]
			}
			if host == targetInstance {
				return member.MemberID, nil
			}
		}
	}
	return "", fmt.Errorf("instance %s is not in the group view of %s", targetInstance, queryInstance)
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
