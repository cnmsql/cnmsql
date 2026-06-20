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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

// reconcileFailover delegates to the selected topology strategy. The async
// strategy elects and fences a safe candidate; Group Replication elects inside
// the group and is observed, so that strategy is a no-op here.
func (r *ClusterReconciler) reconcileFailover(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (bool, ctrl.Result, error) {
	return topologyFor(cluster).ReconcileFailover(ctx, r, cluster, plan, observed)
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
