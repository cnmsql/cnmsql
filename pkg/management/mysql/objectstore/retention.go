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

package objectstore

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// SelectLatestBackup returns the backup entry with the most recent CompletedAt.
// It errors when the slice is empty.
func SelectLatestBackup(entries []BackupEntry) (BackupEntry, error) {
	if len(entries) == 0 {
		return BackupEntry{}, fmt.Errorf("no base backups found in object store")
	}
	latest := entries[0]
	for _, entry := range entries[1:] {
		if entry.Meta.CompletedAt.After(latest.Meta.CompletedAt) {
			latest = entry
		}
	}
	return latest, nil
}

// FindBackupByID returns the backup entry whose manifest BackupID matches id. It
// errors when no entry matches.
func FindBackupByID(entries []BackupEntry, id string) (BackupEntry, error) {
	for _, entry := range entries {
		if entry.Meta.BackupID == id {
			return entry, nil
		}
	}
	return BackupEntry{}, fmt.Errorf("no base backup with backupID %q found in object store", id)
}

// BackupEntry pairs a base backup's directory prefix with its manifest.
type BackupEntry struct {
	// Prefix is the object-store key prefix of the backup directory (trailing
	// slash), holding the archive and its metadata.json.
	Prefix string
	// Meta is the backup's manifest.
	Meta BackupMetadata
}

// RetentionPlan is the pure decision of a retention pass: which base backups and
// binlog files to delete, and the rewritten archive index.
type RetentionPlan struct {
	// DeleteBackupPrefixes are the backup directory prefixes to remove wholesale.
	DeleteBackupPrefixes []string
	// DeleteBinlogKeys are the object keys to remove: each file's key followed
	// by its manifest's.
	DeleteBinlogKeys []string
	// NewIndex is the rewritten archive index, or nil when no binlog was removed
	// (so the existing index is left untouched).
	NewIndex *ArchiveIndex
	// DeletedBinlogs maps a segment's server UUID to the binlog basenames the
	// plan deletes from it. ApplyBinlogExpiry re-applies it to the index as it
	// is at write time, not to the copy the plan was computed from, so a write
	// another index writer made in between is kept.
	DeletedBinlogs map[string]map[string]struct{}
	// Horizon is the recovery horizon: the oldest retained base backup's start
	// time. Binlogs ending before it are uncoverable. Zero when nothing expired.
	Horizon time.Time
}

// Empty reports whether the plan would delete nothing.
func (p RetentionPlan) Empty() bool {
	return len(p.DeleteBackupPrefixes) == 0 && len(p.DeleteBinlogKeys) == 0
}

// PlanRetention plans which base backups a retention pass expires, and the
// recovery horizon that leaves. It is pure so the keep/expire policy is
// exhaustively unit-testable.
//
// Policy (CloudNativePG-parity, GTID-adapted, anchor-horizon binlog GC):
//   - A base backup expires when CompletedAt < cutoff.
//   - The most recent base backup is always retained (the floor), even if
//     expired — a cluster must always have something to recover from.
//   - The recovery horizon is the oldest retained base backup's StartedAt. A
//     binlog file whose last event predates the horizon is uncoverable (no
//     retained base to apply it onto): ExpiredBinlogs finds those files and
//     ExpireBinlogs adds them to the plan. The horizon is zero, and no binlog
//     expires, unless a base backup did.
func PlanRetention(backups []BackupEntry, cutoff time.Time) RetentionPlan {
	var plan RetentionPlan
	if len(backups) == 0 {
		// No base backups to anchor a horizon: never strand the binlog archive.
		return plan
	}

	sorted := make([]BackupEntry, len(backups))
	copy(sorted, backups)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Meta.CompletedAt.Before(sorted[j].Meta.CompletedAt)
	})

	newestIdx := len(sorted) - 1
	var retained []BackupEntry
	for i, entry := range sorted {
		if i != newestIdx && entry.Meta.CompletedAt.Before(cutoff) {
			plan.DeleteBackupPrefixes = append(plan.DeleteBackupPrefixes, entry.Prefix)
			continue
		}
		retained = append(retained, entry)
	}

	if len(plan.DeleteBackupPrefixes) == 0 {
		// Nothing expired: leave binlogs alone (the oldest backup still covers
		// the whole archive).
		return plan
	}

	plan.Horizon = retained[0].Meta.StartedAt
	return plan
}

// ExpireBinlogs adds to the plan the archived files found behind its horizon
// (ExpiredBinlogs) and rewrites the index to drop them.
//
// A nil index parks the whole pass. The index is the only record tying archived
// files to timeline segments, and rewriting it is what keeps the two in step.
// Deleting without one leaves the files gone but unrecorded, so the index the
// archiver writes next can claim GTID coverage no surviving file carries — the
// same silent gap the archiver's lockstep write exists to prevent. Keeping
// binlogs the horizon calls uncoverable only costs storage, and the archiver
// restores the index on its next pass, so the stall clears itself.
func (plan *RetentionPlan) ExpireBinlogs(expired []ArchivedBinlog, index *ArchiveIndex) {
	if plan.Horizon.IsZero() || index == nil {
		return
	}

	deleted := map[string]map[string]struct{}{} // serverUUID -> set of binlog basenames
	for _, file := range expired {
		plan.DeleteBinlogKeys = append(plan.DeleteBinlogKeys, file.Keys.BinlogKey, file.Keys.ManifestKey)
		set := deleted[file.ServerUUID]
		if set == nil {
			set = map[string]struct{}{}
			deleted[file.ServerUUID] = set
		}
		set[file.Name] = struct{}{}
	}

	if len(deleted) == 0 {
		return
	}
	plan.NewIndex = rewriteIndex(index, deleted)
	plan.DeletedBinlogs = deleted
}

// rewriteIndex returns a copy of index with the deleted binlog basenames removed
// from each segment; segments left with no binlogs are dropped, taking their
// fork records with them.
func rewriteIndex(index *ArchiveIndex, deleted map[string]map[string]struct{}) *ArchiveIndex {
	// Copy every index-level field (fork-check stamp, generation, disowned
	// set, ...) as is; only the segments are rebuilt. Segments are copied whole
	// below, so their fork records survive.
	copied := *index
	out := &copied
	out.Segments = nil
	out.UpdatedAt = time.Now().UTC()
	for _, seg := range index.Segments {
		set := deleted[seg.ServerUUID]
		if set == nil {
			out.Segments = append(out.Segments, seg)
			continue
		}
		kept := make([]string, 0, len(seg.Binlogs))
		for _, name := range seg.Binlogs {
			if _, gone := set[name]; gone {
				continue
			}
			kept = append(kept, name)
		}
		if len(kept) == 0 {
			continue
		}
		seg.Binlogs = kept
		out.Segments = append(out.Segments, seg)
	}
	return out
}

// ListBaseBackups walks a cluster's backup prefix and reads every base backup
// manifest. Binlog manifests (which live under the same cluster prefix but a
// different sub-path) are excluded by matching only metadata.json objects.
func ListBaseBackups(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	clusterName string,
) ([]BackupEntry, error) {
	prefix := ClusterPrefix(store, clusterName)
	objects, err := client.ListObjects(ctx, store.Bucket, prefix, true)
	if err != nil {
		return nil, err
	}
	var entries []BackupEntry
	for _, object := range objects {
		if !strings.HasSuffix(object.Key, "/"+BackupMetadataName) {
			continue
		}
		var meta BackupMetadata
		if err := client.GetJSON(ctx, store.Bucket, object.Key, &meta); err != nil {
			return nil, err
		}
		entries = append(entries, BackupEntry{
			Prefix: strings.TrimSuffix(object.Key, BackupMetadataName),
			Meta:   meta,
		})
	}
	return entries, nil
}

// ListArchivedBinlogs walks a cluster's binlog archive and returns every
// archived file, skipping the per-segment status and cluster index objects. It
// is one listing: no manifest is read.
func ListArchivedBinlogs(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	clusterName string,
) ([]ArchivedBinlog, error) {
	prefix := BinlogPrefix(store, clusterName)
	objects, err := client.ListObjects(ctx, store.Bucket, prefix, true)
	if err != nil {
		return nil, err
	}
	var files []ArchivedBinlog
	for _, object := range objects {
		// A file's manifest is at <server uuid>/<name><suffix>.
		serverUUID, manifest, ok := strings.Cut(strings.TrimPrefix(object.Key, prefix), "/")
		name, isManifest := strings.CutSuffix(manifest, BinlogManifestSuffix)
		if !ok || !isManifest || strings.Contains(manifest, "/") || manifest == ArchiveStatusName {
			continue
		}
		files = append(files, ArchivedBinlog{
			Keys: BinlogKeys{
				BinlogKey:   strings.TrimSuffix(object.Key, BinlogManifestSuffix),
				ManifestKey: object.Key,
			},
			ServerUUID: serverUUID,
			Name:       name,
			UploadedAt: object.LastModified,
		})
	}
	return files, nil
}

// ReadBinlogManifests returns the ManifestReader of the archive in bucket.
func ReadBinlogManifests(client *Client, bucket string) ManifestReader {
	return func(ctx context.Context, keys BinlogKeys) (BinlogMetadata, error) {
		var meta BinlogMetadata
		err := client.GetJSON(ctx, bucket, keys.ManifestKey, &meta)
		return meta, err
	}
}

// binlogDeleteConcurrency is how many archived binlogs a retention pass deletes
// at once. Deletes stay one object per request (see Client.Remove).
const binlogDeleteConcurrency = 8

// ApplyBackupExpiry deletes the expired base backups from the store holding
// them. It runs before ApplyBinlogExpiry so a failure on the binlog side never
// keeps an expired recovery point alive.
func ApplyBackupExpiry(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	plan RetentionPlan,
) error {
	for _, prefix := range plan.DeleteBackupPrefixes {
		if err := client.RemovePrefix(ctx, store.Bucket, prefix); err != nil {
			return err
		}
	}
	return nil
}

// ApplyBinlogExpiry deletes the uncoverable binlogs from the archive store,
// then rewrites the archive index. The index rewrite is done last so a mid-run
// failure leaves a still-valid index; any objects deleted but not yet
// de-indexed are cleaned up on the next pass.
//
// Files are deleted binlogDeleteConcurrency at a time, each one's manifest
// after the file: the listing finds a file by its manifest, so a manifest
// deleted first would leave a file no later pass sees.
func ApplyBinlogExpiry(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	clusterName string,
	plan RetentionPlan,
) error {
	deletes, deleteCtx := errgroup.WithContext(ctx)
	deletes.SetLimit(binlogDeleteConcurrency)
	for file := range slices.Chunk(plan.DeleteBinlogKeys, 2) {
		deletes.Go(func() error {
			for _, key := range file {
				if err := client.Remove(deleteCtx, store.Bucket, key); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := deletes.Wait(); err != nil {
		return err
	}
	return rewriteArchiveIndex(ctx, client, store.Bucket, ArchiveIndexKey(store, clusterName), plan)
}

// rewriteArchiveIndex drops the plan's deleted binlogs from the index as it is
// now. The archiver, a drain and retention all write the index; rewriting the
// copy the plan was computed from would discard whatever another writer added
// in between, and the archiver writing a copy it read before this rewrite
// would bring dropped segments, fork records included, back. The
// compare-and-swap in UpdateArchiveIndex closes both.
func rewriteArchiveIndex(ctx context.Context, store VersionedStore, bucket, key string, plan RetentionPlan) error {
	if len(plan.DeletedBinlogs) == 0 {
		return nil
	}
	return UpdateArchiveIndex(ctx, store, bucket, key, func(idx *ArchiveIndex, exists bool) (bool, error) {
		if !exists {
			return false, nil
		}
		*idx = *rewriteIndex(idx, plan.DeletedBinlogs)
		return true, nil
	})
}

// DanglingStore is what DropDanglingIndexEntries needs of the archive store.
type DanglingStore interface {
	VersionedStore
	Exists(ctx context.Context, bucket, key string) (bool, error)
}

// DropDanglingIndexEntries removes from the archive index the binlogs it
// lists whose files are gone, and returns how many it removed.
//
// Retention deletes a file and then drops it from the index. On a store
// without conditional writes that can serve a stale read, another writer's
// index write can start from a copy older than that and bring the entries
// back. The files stay deleted, so a recovery that planned them would fail on
// the download. keys is the listing of archived files a retention pass already
// made; an entry it lacks is dropped only once a direct look confirms the
// manifest is gone, since a file archived after the listing is in the index
// and not in the listing.
func DropDanglingIndexEntries(
	ctx context.Context, store DanglingStore, objectStore mysqlv1alpha1.S3ObjectStore, clusterName string,
	keys []BinlogKeys,
) (int, error) {
	listed := make(map[BinlogKeys]struct{}, len(keys))
	for _, k := range keys {
		listed[k] = struct{}{}
	}
	key := ArchiveIndexKey(objectStore, clusterName)
	var index ArchiveIndex
	if _, exists, err := store.GetJSONVersion(ctx, objectStore.Bucket, key, &index); err != nil || !exists {
		return 0, err
	}
	dangling := map[string]map[string]struct{}{}
	count := 0
	for _, seg := range index.Segments {
		for _, name := range seg.Binlogs {
			keys, err := BuildBinlogKeys(objectStore, clusterName, seg.ServerUUID, name)
			if err != nil {
				return 0, err
			}
			if _, ok := listed[keys]; ok {
				continue
			}
			exists, err := store.Exists(ctx, objectStore.Bucket, keys.ManifestKey)
			if err != nil {
				return 0, err
			}
			if exists {
				continue
			}
			if dangling[seg.ServerUUID] == nil {
				dangling[seg.ServerUUID] = map[string]struct{}{}
			}
			dangling[seg.ServerUUID][name] = struct{}{}
			count++
		}
	}
	if count == 0 {
		return 0, nil
	}
	return count, rewriteArchiveIndex(ctx, store, objectStore.Bucket, key, RetentionPlan{DeletedBinlogs: dangling})
}
