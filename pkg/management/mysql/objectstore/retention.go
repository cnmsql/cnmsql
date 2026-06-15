/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package objectstore

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
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

// BinlogEntry pairs an archived binlog file's keys with its manifest.
type BinlogEntry struct {
	// Keys are the binlog file and manifest object keys.
	Keys BinlogKeys
	// Meta is the per-file manifest.
	Meta BinlogMetadata
}

// RetentionPlan is the pure decision of a retention pass: which base backups and
// binlog files to delete, and the rewritten archive index.
type RetentionPlan struct {
	// DeleteBackupPrefixes are the backup directory prefixes to remove wholesale.
	DeleteBackupPrefixes []string
	// DeleteBinlogKeys are the object keys (file + manifest) to remove.
	DeleteBinlogKeys []string
	// NewIndex is the rewritten archive index, or nil when no binlog was removed
	// (so the existing index is left untouched).
	NewIndex *ArchiveIndex
	// Horizon is the recovery horizon: the oldest retained base backup's start
	// time. Binlogs ending before it are uncoverable. Zero when nothing expired.
	Horizon time.Time
}

// Empty reports whether the plan would delete nothing.
func (p RetentionPlan) Empty() bool {
	return len(p.DeleteBackupPrefixes) == 0 && len(p.DeleteBinlogKeys) == 0
}

// PlanRetention computes a retention plan over already-loaded archive metadata.
// It is pure so the keep/expire policy is exhaustively unit-testable.
//
// Policy (CloudNativePG-parity, GTID-adapted, anchor-horizon binlog GC):
//   - A base backup expires when CompletedAt < cutoff.
//   - The most recent base backup is always retained (the floor), even if
//     expired — a cluster must always have something to recover from.
//   - The recovery horizon is the oldest retained base backup's StartedAt. A
//     binlog file whose last event predates the horizon is uncoverable (no
//     retained base to apply it onto) and is deleted; the index is rewritten to
//     drop it. Binlogs with an unknown (zero) last-event time are kept.
func PlanRetention(
	backups []BackupEntry,
	binlogs []BinlogEntry,
	index *ArchiveIndex,
	cutoff time.Time,
) RetentionPlan {
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
	plan.applyBinlogGC(binlogs, index)
	return plan
}

// applyBinlogGC selects binlog files older than the horizon and rewrites the
// index to drop them.
func (plan *RetentionPlan) applyBinlogGC(binlogs []BinlogEntry, index *ArchiveIndex) {
	if plan.Horizon.IsZero() {
		return
	}

	deleted := map[string]map[string]struct{}{} // serverUUID -> set of binlog basenames
	for _, entry := range binlogs {
		last := entry.Meta.LastEventTime
		if last.IsZero() || !last.Before(plan.Horizon) {
			continue
		}
		plan.DeleteBinlogKeys = append(plan.DeleteBinlogKeys, entry.Keys.BinlogKey, entry.Keys.ManifestKey)
		set := deleted[entry.Meta.ServerUUID]
		if set == nil {
			set = map[string]struct{}{}
			deleted[entry.Meta.ServerUUID] = set
		}
		set[entry.Meta.BinlogName] = struct{}{}
	}

	if len(deleted) == 0 || index == nil {
		return
	}
	plan.NewIndex = rewriteIndex(index, deleted)
}

// rewriteIndex returns a copy of index with the deleted binlog basenames removed
// from each segment; segments left with no binlogs are dropped.
func rewriteIndex(index *ArchiveIndex, deleted map[string]map[string]struct{}) *ArchiveIndex {
	out := &ArchiveIndex{
		ClusterName:    index.ClusterName,
		CoveredGTIDSet: index.CoveredGTIDSet,
		UpdatedAt:      time.Now().UTC(),
	}
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

// ListArchivedBinlogs walks a cluster's binlog archive and reads every per-file
// manifest, skipping the per-segment status and cluster index objects.
func ListArchivedBinlogs(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	clusterName string,
) ([]BinlogEntry, error) {
	prefix := BinlogPrefix(store, clusterName)
	objects, err := client.ListObjects(ctx, store.Bucket, prefix, true)
	if err != nil {
		return nil, err
	}
	var entries []BinlogEntry
	for _, object := range objects {
		if !strings.HasSuffix(object.Key, BinlogManifestSuffix) {
			continue
		}
		base := path.Base(object.Key)
		if base == ArchiveStatusName || base == ArchiveIndexName {
			continue
		}
		var meta BinlogMetadata
		if err := client.GetJSON(ctx, store.Bucket, object.Key, &meta); err != nil {
			return nil, err
		}
		entries = append(entries, BinlogEntry{
			Keys: BinlogKeys{
				BinlogKey:   strings.TrimSuffix(object.Key, BinlogManifestSuffix),
				ManifestKey: object.Key,
			},
			Meta: meta,
		})
	}
	return entries, nil
}

// ApplyRetention executes a retention plan: it deletes the expired base backups
// and uncoverable binlogs, then rewrites the archive index. The index rewrite is
// done last so a mid-run failure leaves a still-valid index; any objects deleted
// but not yet de-indexed are cleaned up on the next pass.
func ApplyRetention(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	clusterName string,
	plan RetentionPlan,
) error {
	for _, prefix := range plan.DeleteBackupPrefixes {
		if err := client.RemovePrefix(ctx, store.Bucket, prefix); err != nil {
			return err
		}
	}
	for _, key := range plan.DeleteBinlogKeys {
		if err := client.Remove(ctx, store.Bucket, key); err != nil {
			return err
		}
	}
	if plan.NewIndex != nil {
		indexKey := ArchiveIndexKey(store, clusterName)
		if err := client.PutJSON(ctx, store.Bucket, indexKey, plan.NewIndex); err != nil {
			return err
		}
	}
	return nil
}
