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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// reconcileSwitchover delegates to the selected topology strategy. Async relies
// on the in-Pod reconcilers; Group Replication invokes set_as_primary on the
// group.
func (r *ClusterReconciler) reconcileSwitchover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, error) {
	return topologyFor(cluster).ReconcileSwitchover(ctx, r, cluster, plan, observed)
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
	logf.FromContext(ctx).Info("Aborting switchover", "target", target, "restoredPrimary", current, "reason", reason)
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
// itself; under GR it is mirrored from the group's elected PRIMARY.
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
