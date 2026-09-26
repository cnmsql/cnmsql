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
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// LogicalFormatVersion is the dump layout written today: one zstd-compressed
// SQL file per backup. A future per-table layout would bump it and keep the
// same manifest name.
const LogicalFormatVersion = 1

// LogicalCompressionZstd is the only compression logical backups use.
const LogicalCompressionZstd = "zstd"

// LogicalBackupMetadata is the logical.json manifest written next to a dump.
type LogicalBackupMetadata struct {
	// FormatVersion is the dump layout (see LogicalFormatVersion).
	FormatVersion int `json:"formatVersion"`
	// BackupID uniquely identifies the backup within the object store.
	BackupID string `json:"backupID"`
	// ClusterName is the cluster the dump was taken from.
	ClusterName string `json:"clusterName"`
	// BackupName is the Backup object that produced this dump.
	BackupName string `json:"backupName"`
	// InstanceName is the instance the dump was taken on.
	InstanceName string `json:"instanceName,omitempty"`
	// Method is always "logical".
	Method string `json:"method"`
	// Tool is the dump client that produced the dump (mysqldump, mariadb-dump).
	Tool string `json:"tool,omitempty"`
	// Flavor is the source engine flavor (mysql, mariadb).
	Flavor string `json:"flavor,omitempty"`
	// ServerVersion is the source server's version string.
	ServerVersion string `json:"serverVersion,omitempty"`
	// Compression is the archive's compression (zstd).
	Compression string `json:"compression"`
	// ArchiveKey is the object key of the compressed dump.
	ArchiveKey string `json:"archiveKey"`
	// SizeBytes is the compressed archive size.
	SizeBytes int64 `json:"sizeBytes"`
	// UncompressedBytes is the size of the SQL stream before compression.
	UncompressedBytes int64 `json:"uncompressedBytes"`
	// SHA256 is the hex-encoded checksum of the compressed archive.
	SHA256 string `json:"sha256,omitempty"`
	// Databases are the schemas in the dump.
	Databases []string `json:"databases"`
	// SnapshotGTID is the GTID position of the dump's snapshot, for reference
	// only. MariaDB records it; MySQL dumps are taken GTID-neutral and leave it
	// empty.
	SnapshotGTID string `json:"snapshotGTID,omitempty"`
	// SnapshotBinlog is the snapshot's binlog coordinates as file:position.
	SnapshotBinlog string `json:"snapshotBinlog,omitempty"`
	// StartedAt and CompletedAt bound the dump transfer.
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
}

// CheckImportable reports why a cluster of the given flavor cannot load this
// dump, or loading only the selected databases from it: a layout or
// compression this operator does not read, another flavor, or a selected
// database that is not in the dump. It returns nil when the import can go
// ahead.
func (m LogicalBackupMetadata) CheckImportable(flavor string, selected []string) error {
	if m.FormatVersion != LogicalFormatVersion {
		return fmt.Errorf("the dump uses format version %d and this operator reads version %d",
			m.FormatVersion, LogicalFormatVersion)
	}
	if m.Compression != LogicalCompressionZstd {
		return fmt.Errorf("the dump uses compression %q and this operator reads %q",
			m.Compression, LogicalCompressionZstd)
	}
	// The worker always records the flavor. An empty one leaves nothing to
	// compare, so it is not refused.
	if m.Flavor != "" && m.Flavor != flavor {
		return fmt.Errorf("the dump was taken on a %s server and this cluster runs %s; "+
			"a dump only loads into the flavor it was taken on", m.Flavor, flavor)
	}
	var missing []string
	for _, db := range selected {
		if !slices.Contains(m.Databases, db) {
			missing = append(missing, db)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("databases %s are not in the dump, which holds %s",
			strings.Join(missing, ", "), strings.Join(m.Databases, ", "))
	}
	return nil
}

// LogicalBackupEntry pairs a logical backup's directory prefix with its
// manifest.
type LogicalBackupEntry struct {
	// Prefix is the object-store key prefix of the backup directory (trailing
	// slash), holding the dump and its logical.json.
	Prefix string
	// Meta is the backup's manifest.
	Meta LogicalBackupMetadata
}

// ListLogicalBackups walks a cluster's backup prefix and reads every logical
// backup manifest. It is the counterpart of ListBaseBackups: each only matches
// its own manifest name, so neither ever sees the other kind.
func ListLogicalBackups(
	ctx context.Context,
	client *Client,
	store mysqlv1alpha1.S3ObjectStore,
	clusterName string,
) ([]LogicalBackupEntry, error) {
	prefix := ClusterPrefix(store, clusterName)
	objects, err := client.ListObjects(ctx, store.Bucket, prefix, true)
	if err != nil {
		return nil, err
	}
	var entries []LogicalBackupEntry
	for _, object := range objects {
		if !strings.HasSuffix(object.Key, "/"+LogicalMetadataName) {
			continue
		}
		var meta LogicalBackupMetadata
		if err := client.GetJSON(ctx, store.Bucket, object.Key, &meta); err != nil {
			return nil, err
		}
		entries = append(entries, LogicalBackupEntry{
			Prefix: strings.TrimSuffix(object.Key, LogicalMetadataName),
			Meta:   meta,
		})
	}
	return entries, nil
}

// SelectLatestLogicalBackup returns the logical backup that completed last.
func SelectLatestLogicalBackup(entries []LogicalBackupEntry) (LogicalBackupEntry, error) {
	if len(entries) == 0 {
		return LogicalBackupEntry{}, fmt.Errorf("no logical backups found in object store")
	}
	latest := entries[0]
	for _, entry := range entries[1:] {
		if entry.Meta.CompletedAt.After(latest.Meta.CompletedAt) {
			latest = entry
		}
	}
	return latest, nil
}

// FindLogicalBackupByID returns the logical backup whose manifest BackupID is
// id.
func FindLogicalBackupByID(entries []LogicalBackupEntry, id string) (LogicalBackupEntry, error) {
	for _, entry := range entries {
		if entry.Meta.BackupID == id {
			return entry, nil
		}
	}
	return LogicalBackupEntry{}, fmt.Errorf("no logical backup with backupID %q found in object store", id)
}

// PlanLogicalRetention returns the directory prefixes of the logical backups
// that expired: those completed before cutoff, except the newest one, which is
// always kept. A manifest without a completion time cannot be dated, so it is
// kept no matter the cutoff. It is pure, and it is separate from PlanRetention
// on purpose: dumps never anchor recovery, so they never move the recovery
// horizon or keep a binlog alive.
func PlanLogicalRetention(entries []LogicalBackupEntry, cutoff time.Time) []string {
	if len(entries) == 0 {
		return nil
	}
	sorted := make([]LogicalBackupEntry, len(entries))
	copy(sorted, entries)
	// Oldest first, with the prefix breaking ties so equal completion times
	// always keep the same backup.
	sort.SliceStable(sorted, func(i, j int) bool {
		ti, tj := sorted[i].Meta.CompletedAt, sorted[j].Meta.CompletedAt
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return sorted[i].Prefix < sorted[j].Prefix
	})
	var expired []string
	for _, entry := range sorted[:len(sorted)-1] {
		// Malformed (undated) manifests are never expired: they cannot be
		// compared to the cutoff, so retention fails open.
		if entry.Meta.CompletedAt.IsZero() {
			continue
		}
		if entry.Meta.CompletedAt.Before(cutoff) {
			expired = append(expired, entry.Prefix)
		}
	}
	return expired
}

// NewZstdWriter compresses everything written to it into w. Close flushes the
// final frame; it does not close w.
func NewZstdWriter(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w, zstd.WithEncoderConcurrency(1))
}

// NewZstdReader decompresses r. Close releases the decoder; it does not close
// r.
func NewZstdReader(r io.Reader) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	return dec.IOReadCloser(), nil
}
