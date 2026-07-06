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

import "time"

// BackupMetadata is the inspectable manifest written next to a backup archive.
// It is the source of truth a recovery uses to locate and verify the archive.
type BackupMetadata struct {
	// BackupID uniquely identifies the backup within the object store.
	BackupID string `json:"backupID"`
	// ClusterName is the cluster the backup was taken from.
	ClusterName string `json:"clusterName"`
	// BackupName is the Backup object that produced this archive.
	BackupName string `json:"backupName"`
	// InstanceName is the instance the backup was streamed from.
	InstanceName string `json:"instanceName,omitempty"`
	// Method is the backup method (e.g. "xtrabackup").
	Method string `json:"method"`
	// ArchiveKey is the object key of the xbstream archive.
	ArchiveKey string `json:"archiveKey"`
	// Compressed indicates the archive needs decompression before prepare.
	Compressed bool `json:"compressed"`
	// SizeBytes is the uploaded archive size.
	SizeBytes int64 `json:"sizeBytes"`
	// SHA256 is the hex-encoded checksum of the uploaded archive.
	SHA256 string `json:"sha256,omitempty"`
	// AnchorGTID is the GTID position of the base backup's consistent point,
	// resolved on the source at backup time (MariaDB: via BINLOG_GTID_POS over the
	// exact binlog coordinates mariabackup recorded). It gives point-in-time
	// recovery a fully-specified anchor even when the in-archive binlog-info file
	// carries only file+position (MariaDB 10.11). Empty for MySQL (whose
	// binlog-info already carries the GTID) and for legacy backups.
	AnchorGTID string `json:"anchorGTID,omitempty"`
	// AnchorServerUUID is the archive-partition identity of the incarnation the base
	// backup was taken from (MariaDB's persisted per-incarnation token). It lets a
	// GTID-less point-in-time recovery disambiguate the anchor binlog when a
	// re-clone/failover left several incarnations numbering their binlogs from
	// 000001, so the anchor's bare filename would otherwise match more than one
	// server. Empty for MySQL and for legacy backups.
	AnchorServerUUID string `json:"anchorServerUUID,omitempty"`
	// StartedAt and CompletedAt bound the backup transfer.
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
}
