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
	"errors"
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

// eventRetentionNotOwned is the Warning event reason for a retention pass
// skipped because nothing at the store proves it is this cluster's.
const eventRetentionNotOwned = "RetentionNotOwned"

// reconcileRetention expires base backups (and the now-uncoverable binlog
// segments) and logical backups past spec.backup.retentionPolicy. It is
// best-effort and throttled: it only runs on an established, archiving cluster,
// at most once per retentionInterval, and a transient object-store error is
// returned for a retry without failing the wider reconcile.
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
	if reason := retentionCredentialsRefused(err); reason != "" {
		return r.skipRetention(ctx, cluster, reason, fmt.Sprintf("Skipped backup retention: %v", err))
	}
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
	// Logical backups expire on their own (below), but are listed up front:
	// their manifests help prove the store is this cluster's.
	logical, err := objectstore.ListLogicalBackups(ctx, client, *store, cluster.Name)
	if err != nil {
		return err
	}
	// The archive may live in its own store; base and logical backups stay in
	// the backup store.
	binlogStore := cluster.BinlogObjectStore()
	binlogClient := client
	if !store.SameLocation(binlogStore) {
		binlogClient, err = r.objectStoreClient(ctx, cluster.Namespace, binlogStore)
		if reason := retentionCredentialsRefused(err); reason != "" {
			return r.skipRetention(ctx, cluster, reason, fmt.Sprintf("Skipped backup retention: %v", err))
		}
		if err != nil {
			return err
		}
	}
	binlogKeys, index, err := listArchive(ctx, binlogClient, *binlogStore, cluster.Name)
	if err != nil {
		return err
	}

	if reason := retentionOwnership(string(cluster.UID), store.SameLocation(binlogStore),
		backups, logical, len(binlogKeys), index); reason != "" {
		return r.skipRetention(ctx, cluster, eventRetentionNotOwned, "Skipped backup retention: "+reason)
	}

	if index == nil && len(binlogKeys) > 0 {
		// The archiver writes the index in the same pass as every file it ships, so
		// a non-empty archive with no index means the index was lost. PlanRetention
		// expires base backups but parks binlog GC until it comes back.
		log.Info("Archive index missing; retaining all archived binlogs this pass",
			"archivedBinlogs", len(binlogKeys))
	}

	cutoff := time.Now().Add(-window)
	plan, err := planRetention(ctx, binlogClient, *binlogStore, backups, binlogKeys, index, cutoff)
	if err != nil {
		return err
	}

	// Base backups and binlogs anchor recovery, so their expiry is applied
	// before the logical pass below: a failure on the logical side must not
	// keep expired recovery points alive.
	if !plan.Empty() {
		if err := objectstore.ApplyBackupExpiry(ctx, client, *store, plan); err != nil {
			return err
		}
		if err := objectstore.ApplyBinlogExpiry(ctx, binlogClient, *binlogStore, cluster.Name, plan); err != nil {
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

	// An index write that started from a copy older than a retention pass
	// brings back the entries that pass removed; their files stay deleted.
	if err := dropDanglingIndexEntries(ctx, binlogClient, *binlogStore, cluster.Name, binlogKeys, index); err != nil {
		return err
	}

	// Logical backups expire on the same window but on their own: they never
	// anchor recovery, so they neither move the horizon nor keep binlogs.
	expiredDumps := objectstore.PlanLogicalRetention(logical, cutoff)
	for _, prefix := range expiredDumps {
		if err := client.RemovePrefix(ctx, store.Bucket, prefix); err != nil {
			return err
		}
	}
	if len(expiredDumps) > 0 {
		log.Info("Applied logical backup retention", "deletedLogicalBackups", len(expiredDumps))
		r.Recorder.Event(cluster, corev1.EventTypeNormal, "BackupRetention", fmt.Sprintf(
			"Retention (%s) removed %d logical backup(s)", backup.RetentionPolicy, len(expiredDumps)))
	}

	// Stamp the run time even on a no-op pass so the throttle holds.
	return r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		now := metav1.Now()
		s.LastRetentionRunTime = &now
	})
}

// listArchive lists the archived files and reads the archive index, nil when
// there is none. One listing gives the files; their manifests are read later
// (planRetention), only once a base backup expired and there may be binlogs to
// expire.
func listArchive(
	ctx context.Context, client *objectstore.Client, store mysqlv1alpha1.S3ObjectStore, clusterName string,
) ([]objectstore.BinlogKeys, *objectstore.ArchiveIndex, error) {
	keys, err := objectstore.ListArchivedBinlogKeys(ctx, client, store, clusterName)
	if err != nil {
		return nil, nil, err
	}
	indexKey := objectstore.ArchiveIndexKey(store, clusterName)
	exists, err := client.Exists(ctx, store.Bucket, indexKey)
	if err != nil || !exists {
		return keys, nil, err
	}
	index := &objectstore.ArchiveIndex{}
	if err := client.GetJSON(ctx, store.Bucket, indexKey, index); err != nil {
		return nil, nil, err
	}
	return keys, index, nil
}

// planRetention plans a pass. Binlogs only expire behind an expired base
// backup, so the per-file manifests, one read each, are only read once a
// backup did expire and there is an index to reflect the deletions in. Most
// passes expire nothing and read none.
func planRetention(
	ctx context.Context, client *objectstore.Client, store mysqlv1alpha1.S3ObjectStore,
	backups []objectstore.BackupEntry, binlogKeys []objectstore.BinlogKeys, index *objectstore.ArchiveIndex,
	cutoff time.Time,
) (objectstore.RetentionPlan, error) {
	plan := objectstore.PlanRetention(backups, nil, nil, cutoff)
	if len(plan.DeleteBackupPrefixes) == 0 || index == nil {
		return plan, nil
	}
	binlogs, err := objectstore.ReadArchivedBinlogs(ctx, client, store, binlogKeys)
	if err != nil {
		return objectstore.RetentionPlan{}, err
	}
	return objectstore.PlanRetention(backups, binlogs, index, cutoff), nil
}

// retentionCredentialsRefused returns the event reason for an object-store
// error that no retry fixes, or "" for any other error.
func retentionCredentialsRefused(err error) string {
	switch {
	case errors.Is(err, objectstore.ErrAmbientCredentialsDisabled):
		return eventObjectStoreCredentialsRequired
	case errors.Is(err, objectstore.ErrCredentialsSecretRefused):
		return reasonObjectStoreCredentialsRefused
	}
	return ""
}

// skipRetention reports a retention pass that will not run and stamps the run
// time anyway, so the throttle holds and the warning repeats once an interval.
func (r *ClusterReconciler) skipRetention(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster, reason, message string,
) error {
	logf.FromContext(ctx).Info("Skipped backup retention", "reason", reason, "message", message)
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeWarning, reason, message)
	}
	return r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		now := metav1.Now()
		s.LastRetentionRunTime = &now
	})
}

// retentionOwnership returns why retention must not delete anything, or "" when
// every location it would delete from is proven to be this cluster's.
//
// Cluster names are only unique per namespace, so the key layout alone cannot
// tell this cluster's archive from another namespace's same-named one at the
// same bucket and path. Proof is an object at the location that carries uid:
// the archive index, or a base or logical backup manifest. Only this
// cluster's workers stamp it, and they write under the cluster's own
// credentials, so a Cluster pointed at a store it cannot write to never
// proves ownership of it. A location with nothing to delete needs no proof.
func retentionOwnership(
	uid string,
	sameLocation bool,
	backups []objectstore.BackupEntry,
	logical []objectstore.LogicalBackupEntry,
	archivedBinlogs int,
	index *objectstore.ArchiveIndex,
) string {
	if uid == "" {
		return "the Cluster has no UID"
	}
	backupsOwned := false
	for _, b := range backups {
		backupsOwned = backupsOwned || b.Meta.ClusterUID == uid
	}
	for _, l := range logical {
		backupsOwned = backupsOwned || l.Meta.ClusterUID == uid
	}
	archiveOwned := index != nil && index.ClusterUID == uid
	if sameLocation {
		backupsOwned = backupsOwned || archiveOwned
		archiveOwned = backupsOwned
	}

	if !backupsOwned && len(backups)+len(logical) > 0 {
		return "no backup manifest or archive index at the backup store carries this Cluster's UID, " +
			"so its backups may belong to another cluster with the same name. " +
			"The next backup or archived binary log taken by this Cluster proves the store is its own"
	}
	if !archiveOwned && (archivedBinlogs > 0 || index != nil) {
		return "the binary-log archive index does not carry this Cluster's UID, " +
			"so the archive may belong to another cluster with the same name. " +
			"The next binary log this Cluster archives proves the archive is its own"
	}
	return ""
}

// dropDanglingIndexEntries removes the archive index entries whose binlogs
// are gone (see objectstore.DropDanglingIndexEntries). Without an index there
// is nothing to drop.
func dropDanglingIndexEntries(
	ctx context.Context, client *objectstore.Client, store mysqlv1alpha1.S3ObjectStore, clusterName string,
	keys []objectstore.BinlogKeys, index *objectstore.ArchiveIndex,
) error {
	if index == nil {
		return nil
	}
	dropped, err := objectstore.DropDanglingIndexEntries(ctx, client, store, clusterName, keys)
	if err != nil {
		return err
	}
	if dropped > 0 {
		logf.FromContext(ctx).Info("Dropped archive index entries whose binlogs are gone", "entries", dropped)
	}
	return nil
}
