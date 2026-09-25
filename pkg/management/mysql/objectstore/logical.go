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
	"io"
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

// PlanLogicalRetention returns the directory prefixes of the logical backups
// that expired: those completed before cutoff, except the newest one, which is
// always kept. It is pure, and it is separate from PlanRetention on purpose:
// dumps never anchor recovery, so they never move the recovery horizon or keep
// a binlog alive.
func PlanLogicalRetention(entries []LogicalBackupEntry, cutoff time.Time) []string {
	if len(entries) == 0 {
		return nil
	}
	sorted := make([]LogicalBackupEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Meta.CompletedAt.Before(sorted[j].Meta.CompletedAt)
	})
	var expired []string
	for _, entry := range sorted[:len(sorted)-1] {
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
