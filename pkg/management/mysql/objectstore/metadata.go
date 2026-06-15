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
	// StartedAt and CompletedAt bound the backup transfer.
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
}
