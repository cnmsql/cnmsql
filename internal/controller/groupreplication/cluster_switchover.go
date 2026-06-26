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

package groupreplication

import (
	"context"
	"fmt"
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	mysqlgr "github.com/cnmsql/cnmsql/pkg/management/mysql/groupreplication"
)

// ReconcileSwitchover is an optimistic best-effort attempt to hand off the
// primary to the target instance via group_replication_set_as_primary. If the
// target is not yet resolvable, valid, or reachable the call is silently
// retried on the next reconcile; it never blocks the Cluster phase. Only a
// maxSwitchoverDelay expiry clears the request by resetting targetPrimary.
func (r *Reconciler) ReconcileSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed topology.FailoverState,
) (topology.FailoverResult, error) {
	log := logf.FromContext(ctx)
	target := cluster.Status.TargetPrimary
	current := cluster.Status.CurrentPrimary
	if target == "" || target == current || current == "" {
		return topology.FailoverResult{}, nil
	}

	targetUUID, err := r.memberUUIDForInstance(ctx, cluster, current, target)
	if err != nil {
		log.V(1).Info("Skipping switchover: cannot resolve target UUID yet", "target", target, "error", err)
		return topology.FailoverResult{}, nil
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
		log.V(1).Info("Skipping switchover: target not in the group view yet", "target", target)
		return topology.FailoverResult{}, nil
	}
	if targetMember.State != mysqlgr.MemberStateOnline || targetMember.Role != mysqlgr.MemberRoleSecondary {
		log.V(1).Info("Skipping switchover: target not ready yet",
			"target", target, "state", targetMember.State, "role", targetMember.Role)
		return topology.FailoverResult{}, nil
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
		log.Info("SetAsPrimary failed, will retry", "target", target, "uuid", targetUUID, "error", err)
		return topology.FailoverResult{}, nil
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

// ReconcileDrainSwitchover is a no-op under Group Replication: when a primary
// member's Pod is drained, the group itself elects a new primary via its
// membership protocol, so the operator does not drive a planned switchover.
func (r *Reconciler) ReconcileDrainSwitchover(
	_ context.Context,
	_ *mysqlv1alpha1.Cluster,
	_ topology.FailoverState,
) (topology.FailoverResult, error) {
	return topology.FailoverResult{}, nil
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
	logf.FromContext(ctx).Info("Aborting switchover: not promoted within maxSwitchoverDelay",
		"target", target, "delay", cluster.Spec.MaxSwitchoverDelay)
	if err := topology.PatchClusterStatus(ctx, r.client, cluster, func(status *mysqlv1alpha1.ClusterStatus) {
		status.TargetPrimary = current
		status.TargetPrimaryTimestamp = ""
	}); err != nil {
		return topology.FailoverResult{}, err
	}
	return topology.FailoverResult{Handled: true}, nil
}
