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

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// clusterBackupFinalizer reclaims the cluster's entire object-store archive when
// the Cluster is deleted. It is added only when spec.backup.reclaimPolicy is
// Delete and an object store is configured.
const clusterBackupFinalizer = mysqlv1alpha1.ClusterBackupCleanupFinalizer

// wantsObjectStoreCleanup reports whether the Cluster opted into wiping its
// object-store archive on deletion.
func (r *ClusterReconciler) wantsObjectStoreCleanup(cluster *mysqlv1alpha1.Cluster) bool {
	backup := cluster.Spec.Backup
	return backup != nil &&
		backup.ObjectStore != nil &&
		backup.ReclaimPolicy == mysqlv1alpha1.BackupReclaimDelete
}

// handleBackupReclaimLifecycle manages the object-store reclaim finalizer across
// the Cluster lifecycle. While the Cluster is being deleted it wipes the archive
// (when opted in) and releases the finalizer; otherwise it keeps the finalizer in
// sync with the reclaim policy. It returns handled=true when the caller should
// stop and return immediately (with the given error).
func (r *ClusterReconciler) handleBackupReclaimLifecycle(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) (bool, error) {
	if !cluster.DeletionTimestamp.IsZero() {
		return true, r.reconcileClusterDelete(ctx, cluster)
	}
	changed, err := r.reconcileBackupReclaimFinalizer(ctx, cluster)
	if err != nil || changed {
		return true, err
	}
	return false, nil
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
func (r *ClusterReconciler) reconcileClusterDelete(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) error {
	if !controllerutil.ContainsFinalizer(cluster, clusterBackupFinalizer) {
		return nil
	}
	if err := r.cleanupClusterObjectStore(ctx, cluster); err != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "CleanupFailed", err.Error())
		return err
	}
	controllerutil.RemoveFinalizer(cluster, clusterBackupFinalizer)
	return r.Update(ctx, cluster)
}

// cleanupClusterObjectStore removes the cluster's whole archive prefix (every
// base backup, the archived binlogs and the archive index) from the object
// store. It is a no-op when no object store is configured.
func (r *ClusterReconciler) cleanupClusterObjectStore(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
) error {
	log := logf.FromContext(ctx)

	backup := cluster.Spec.Backup
	if backup == nil || backup.ObjectStore == nil {
		return nil
	}
	store := backup.ObjectStore

	cfg, err := r.objectStoreConfig(ctx, cluster.Namespace, store)
	if err != nil {
		return err
	}
	osClient, err := objectstore.NewClient(cfg)
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
