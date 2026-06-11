/*
Copyright 2026 The CNMySQL Authors.

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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

// reconcileSwitchover drives a planned switchover requested by setting
// status.targetPrimary to a replica. In the CNPG pull-model the operator only
// validates the request and bounds it by spec.maxSwitchoverDelay; the actual
// promotion/demotion is performed by the instances' in-Pod reconcilers (the
// target promotes itself and sets currentPrimary, the old primary and other
// replicas re-point themselves). It returns handled=true while the switchover is
// in flight so the caller requeues and does not run steady-state status logic.
func (r *ClusterReconciler) reconcileSwitchover(
	ctx context.Context,
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

// ensureSwitchoverStarted stamps status.targetPrimaryTimestamp on the first
// reconcile of a switchover request and returns the effective start time, so the
// wait for the target to promote can be bounded by spec.maxSwitchoverDelay.
func (r *ClusterReconciler) ensureSwitchoverStarted(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (time.Time, error) {
	if ts := cluster.Status.TargetPrimaryTimestamp; ts != "" {
		if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
			return parsed, nil
		}
	}
	now := time.Now().Truncate(time.Second)
	if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		s.TargetPrimaryTimestamp = now.Format(time.RFC3339)
	}); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

// abortSwitchover cancels a planned switchover whose target failed to be
// promoted within spec.maxSwitchoverDelay, restoring the original primary as the
// target.
func (r *ClusterReconciler) abortSwitchover(ctx context.Context, cluster *mysqlv1alpha1.Cluster, current, target string) (bool, error) {
	reason := fmt.Sprintf("switchover to %s aborted: not promoted within maxSwitchoverDelay (%ds)",
		target, cluster.Spec.MaxSwitchoverDelay)
	if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		s.TargetPrimary = current
		s.TargetPrimaryTimestamp = ""
		s.Phase = phaseBlocked
		s.PhaseReason = reason
	}); err != nil {
		return false, err
	}
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, phaseBlocked, reason)
	}
	return true, nil
}

// reconcileRoleLabels keeps Pod role labels in step with the current primary so
// the rw Service points only at it and ro/r point at the replicas. The current
// primary is the authoritative value written by whichever instance promoted
// itself; it falls back to the observed reporting primary before it is set.
func (r *ClusterReconciler) reconcileRoleLabels(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) error {
	primary := cluster.Status.CurrentPrimary
	if primary == "" {
		primary = observed.PrimaryName
	}
	if primary == "" {
		return nil
	}
	return r.patchRoleLabels(ctx, cluster, observed.InstanceNames, primary)
}

func (r *ClusterReconciler) patchRoleLabels(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceNames []string, primaryName string) error {
	for _, name := range instanceNames {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			return client.IgnoreNotFound(err)
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		desired := roleReplica
		if name == primaryName {
			desired = rolePrimary
		}
		if pod.Labels[roleLabel] == desired {
			continue
		}
		before := pod.DeepCopy()
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[roleLabel] = desired
		if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			return err
		}
	}
	return nil
}
