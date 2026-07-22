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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// retentionInterval throttles how often the operator runs a backup-retention GC
// pass against the object store. Listing/reading manifests is comparatively
// expensive, so it runs at most this often rather than every readyResync tick.
const retentionInterval = time.Hour

// reconcileRetention expires base backups (and the now-uncoverable binlog
// segments) past spec.backup.retentionPolicy. It is best-effort and throttled:
// it only runs on an established, archiving cluster, at most once per
// retentionInterval, and a transient object-store error is returned for a retry
// without failing the wider reconcile.
func (r *ClusterReconciler) reconcileRetention(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	log := logf.FromContext(ctx)

	backup := cluster.Spec.Backup
	if backup == nil || backup.RetentionPolicy == "" || backup.ObjectStore == nil {
		return nil
	}
	// Only an established cluster owns its archive; a fresh/recovering one must
	// not prune.
	if cluster.Status.CurrentPrimary == "" {
		return nil
	}
	window, err := mysqlv1alpha1.ParseRetentionPolicy(backup.RetentionPolicy)
	if err != nil {
		// Validation already rejects this; guard anyway.
		return nil
	}
	if last := cluster.Status.LastRetentionRunTime; last != nil && time.Since(last.Time) < retentionInterval {
		return nil
	}

	store := backup.ObjectStore
	cfg, err := r.objectStoreConfig(ctx, cluster.Namespace, store)
	if err != nil {
		return err
	}
	client, err := objectstore.NewClient(cfg)
	if err != nil {
		return err
	}

	backups, err := objectstore.ListBaseBackups(ctx, client, *store, cluster.Name)
	if err != nil {
		return err
	}
	binlogs, err := objectstore.ListArchivedBinlogs(ctx, client, *store, cluster.Name)
	if err != nil {
		return err
	}

	var index *objectstore.ArchiveIndex
	indexKey := objectstore.ArchiveIndexKey(*store, cluster.Name)
	if exists, err := client.Exists(ctx, store.Bucket, indexKey); err != nil {
		return err
	} else if exists {
		index = &objectstore.ArchiveIndex{}
		if err := client.GetJSON(ctx, store.Bucket, indexKey, index); err != nil {
			return err
		}
	}

	if index == nil && len(binlogs) > 0 {
		// The archiver writes the index in the same pass as every file it ships, so
		// a non-empty archive with no index means the index was lost. PlanRetention
		// expires base backups but parks binlog GC until it comes back.
		log.Info("Archive index missing; retaining all archived binlogs this pass",
			"archivedBinlogs", len(binlogs))
	}

	cutoff := time.Now().Add(-window)
	plan := objectstore.PlanRetention(backups, binlogs, index, cutoff)

	if !plan.Empty() {
		if err := objectstore.ApplyRetention(ctx, client, *store, cluster.Name, plan); err != nil {
			return err
		}
		msg := fmt.Sprintf(
			"Retention (%s) removed %d base backup(s) and %d archived binlog(s); horizon %s",
			backup.RetentionPolicy, len(plan.DeleteBackupPrefixes),
			len(plan.DeleteBinlogKeys)/2, plan.Horizon.UTC().Format(time.RFC3339))
		log.Info("Applied backup retention", "deletedBackups", len(plan.DeleteBackupPrefixes),
			"deletedBinlogs", len(plan.DeleteBinlogKeys)/2)
		r.Recorder.Event(cluster, corev1.EventTypeNormal, "BackupRetention", msg)
	}

	// Stamp the run time even on a no-op pass so the throttle holds.
	return r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		now := metav1.Now()
		s.LastRetentionRunTime = &now
	})
}
