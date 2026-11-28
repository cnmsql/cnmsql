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

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// clusterBackupFinalizer reclaims the cluster's entire object-store archive and
// its volume-snapshot Backups when the Cluster is deleted. It is added only
// when spec.backup.reclaimPolicy is Delete and an object store or volume
// snapshots are configured.
const clusterBackupFinalizer = mysqlv1alpha1.ClusterBackupCleanupFinalizer

// wantsObjectStoreCleanup reports whether the Cluster opted into removing its
// backups on deletion: the object-store archive, the volume snapshots, or both.
func (r *ClusterReconciler) wantsObjectStoreCleanup(cluster *mysqlv1alpha1.Cluster) bool {
	backup := cluster.Spec.Backup
	return backup != nil &&
		(backup.ObjectStore != nil || backup.VolumeSnapshot != nil) &&
		backup.ReclaimPolicy == mysqlv1alpha1.BackupReclaimDelete
}

// handleBackupReclaimLifecycle manages the object-store reclaim finalizer across
// the Cluster lifecycle. While the Cluster is being deleted it wipes the archive
// (when opted in) and releases the finalizer; otherwise it keeps the finalizer in
// sync with the reclaim policy. It returns handled=true when the caller should
// stop and return immediately (with the given result and error).
func (r *ClusterReconciler) handleBackupReclaimLifecycle(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) (bool, ctrl.Result, error) {
	if !cluster.DeletionTimestamp.IsZero() {
		result, err := r.reconcileClusterDelete(ctx, cluster)
		return true, result, err
	}
	changed, err := r.reconcileBackupReclaimFinalizer(ctx, cluster)
	if err != nil || changed {
		return true, ctrl.Result{}, err
	}
	return false, ctrl.Result{}, nil
}

// reconcileBackupReclaimFinalizer keeps the teardown finalizer in sync with the
// reclaim policy. It reports whether it changed the object, in which case the
// caller stops this pass and lets the update re-trigger reconcile.
func (r *ClusterReconciler) reconcileBackupReclaimFinalizer(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) (bool, error) {
	has := controllerutil.ContainsFinalizer(cluster, clusterBackupFinalizer)
	switch {
	case r.wantsObjectStoreCleanup(cluster) && !has:
		controllerutil.AddFinalizer(cluster, clusterBackupFinalizer)
		return true, r.Update(ctx, cluster)
	case !r.wantsObjectStoreCleanup(cluster) && has:
		controllerutil.RemoveFinalizer(cluster, clusterBackupFinalizer)
		return true, r.Update(ctx, cluster)
	}
	return false, nil
}

// reconcileClusterDelete runs while the Cluster is being deleted. Owned Pods,
// PVCs and Secrets are garbage-collected by owner references; the only thing the
// operator has to reclaim explicitly is the remote archive, and only when the
// teardown finalizer is present. A cleanup failure requeues (via the returned
// error) so deletion never silently leaves half-removed remote state; an
// operator can remove the finalizer by hand to force teardown if the object
// store is permanently unreachable.
//
// The archive is removed only once nothing can write to it any more. With the
// default background cascade the garbage collector deletes the instance Pods
// only after the Cluster itself is gone, which this finalizer delays: wiping
// first let the still-running instances archive again, so objects reappeared
// under the removed prefix. A primary with a replica streaming from it keeps
// archiving for up to 25s more once its Pod is deleted, while its preStop hook
// waits for a demotion that a deleted Cluster never sends. The operator
// therefore stops its instance Pods itself and waits for
// every Pod of the cluster (instances and backup workers) to stop.
func (r *ClusterReconciler) reconcileClusterDelete(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cluster, clusterBackupFinalizer) {
		return ctrl.Result{}, nil
	}
	// Volume snapshots need no wait: the Backup controller deletes them, and
	// releases a source a Backup still holds first.
	if err := r.reclaimSnapshotBackups(ctx, cluster); err != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "CleanupFailed", err.Error())
		return ctrl.Result{}, err
	}
	if cluster.Spec.Backup != nil && cluster.Spec.Backup.ObjectStore != nil {
		if wait, err := r.stopArchiveWriters(ctx, cluster); err != nil || wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, err
		}
	}
	if err := r.cleanupClusterObjectStore(ctx, cluster); err != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "CleanupFailed", err.Error())
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(cluster, clusterBackupFinalizer)
	return ctrl.Result{}, r.Update(ctx, cluster)
}

const (
	// archiveWriterPoll is how often a deletion re-checks the cluster's Pods.
	// Instance Pods are owned and re-trigger reconcile as they go; backup
	// worker Pods are not, so the deletion also polls.
	archiveWriterPoll = 5 * time.Second
	// stuckPodMargin is how long past the end of its grace period a
	// terminating Pod is still waited for. A Pod left Terminating beyond that
	// sits on a node that no longer answers, and waiting for it would block
	// the deletion for as long as the node is lost.
	stuckPodMargin = 30 * time.Second
)

// stopArchiveWriters deletes the cluster's instance Pods and reports how long to
// wait before the archive can be removed: zero once no Pod of the cluster is
// running or still within its termination grace period. Pods the deletion
// orphans are left alone, and not waited for: the user asked to keep them.
func (r *ClusterReconciler) stopArchiveWriters(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) (time.Duration, error) {
	if controllerutil.ContainsFinalizer(cluster, metav1.FinalizerOrphanDependents) {
		return 0, nil
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{clusterLabel: cluster.Name}); err != nil {
		return 0, err
	}
	now := time.Now()
	var writers []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp == nil && metav1.IsControlledBy(pod, cluster) {
			if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
				return 0, fmt.Errorf("stopping instance Pod %s before removing the archive: %w", pod.Name, err)
			}
			writers = append(writers, pod.Name)
			continue
		}
		if mayWriteArchive(pod, now) {
			writers = append(writers, pod.Name)
		}
	}
	if len(writers) == 0 {
		return 0, nil
	}
	logf.FromContext(ctx).Info("Waiting for Pods to stop before removing the object-store archive",
		"cluster", cluster.Name, "pods", writers)
	r.Recorder.Event(cluster, corev1.EventTypeNormal, "CleanupWaiting",
		fmt.Sprintf("Waiting for %s to stop before removing the object-store archive", strings.Join(writers, ", ")))
	return archiveWriterPoll, nil
}

// mayWriteArchive reports whether pod can still upload to the archive: it has
// not finished, and it is not deleted or still within its grace period (plus
// stuckPodMargin). A Pod that never started (Pending) is counted too, since it
// would archive once scheduled.
func mayWriteArchive(pod *corev1.Pod, now time.Time) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	if pod.DeletionTimestamp == nil {
		return true
	}
	return now.Before(pod.DeletionTimestamp.Add(stuckPodMargin))
}

// reclaimSnapshotBackups deletes every volume-snapshot Backup of the cluster
// with its VolumeSnapshots, whatever the Backup's reclaimPolicy. A
// VolumeSnapshot whose Backup is already gone was kept on purpose and is left
// alone.
func (r *ClusterReconciler) reclaimSnapshotBackups(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	var backups mysqlv1alpha1.BackupList
	if err := r.List(ctx, &backups, client.InNamespace(cluster.Namespace)); err != nil {
		return err
	}
	deleted := 0
	for i := range backups.Items {
		b := &backups.Items[i]
		if b.Spec.Cluster.Name != cluster.Name || !b.IsVolumeSnapshot() || !b.DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.deleteSnapshotBackup(ctx, cluster.Namespace, b.Name); err != nil {
			return fmt.Errorf("deleting volume snapshot Backup %s: %w", b.Name, err)
		}
		deleted++
	}
	if deleted > 0 {
		logf.FromContext(ctx).Info("Deleted the cluster's volume snapshot Backups", "cluster", cluster.Name, "backups", deleted)
		r.Recorder.Event(cluster, corev1.EventTypeNormal, "Cleanup",
			fmt.Sprintf("Deleted %d volume snapshot Backup(s) with their VolumeSnapshots", deleted))
	}
	return nil
}

// cleanupClusterObjectStore removes the cluster's whole archive prefix (every
// base backup, the archived binlogs and the archive index) from the base-backup
// store and, when it is kept apart, from the binary-log archive store. It is a
// no-op when no object store is configured.
func (r *ClusterReconciler) cleanupClusterObjectStore(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) error {
	backup := cluster.Spec.Backup
	if backup == nil || backup.ObjectStore == nil {
		return nil
	}
	stores := []*mysqlv1alpha1.S3ObjectStore{backup.ObjectStore}
	if archive := cluster.BinlogObjectStore(); !archive.SameLocation(backup.ObjectStore) {
		stores = append(stores, archive)
	}
	for _, store := range stores {
		if err := r.removeClusterPrefix(ctx, cluster, store); err != nil {
			return err
		}
	}
	return nil
}

// removeClusterPrefix removes the cluster's prefix from store.
func (r *ClusterReconciler) removeClusterPrefix(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	store *mysqlv1alpha1.S3ObjectStore,
) error {
	log := logf.FromContext(ctx)

	osClient, err := r.objectStoreClient(ctx, cluster.Namespace, store)
	if err != nil {
		return err
	}

	prefix := objectstore.ClusterPrefix(*store, cluster.Name)
	if err := osClient.RemovePrefix(ctx, store.Bucket, prefix); err != nil {
		return err
	}
	log.Info("Removed cluster object-store archive",
		"cluster", cluster.Name, "bucket", store.Bucket, "prefix", prefix)
	r.Recorder.Event(cluster, corev1.EventTypeNormal, "Cleanup",
		fmt.Sprintf("Removed object-store archive under s3://%s/%s", store.Bucket, prefix))
	return nil
}
