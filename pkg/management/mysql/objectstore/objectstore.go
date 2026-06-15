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

// Package objectstore contains object-store helpers shared by backup and
// recovery code.
package objectstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

const (
	// BackupArchiveName is the object name used for the xbstream archive.
	BackupArchiveName = "backup.xbstream"
	// BackupMetadataName is the object name used for inspectable metadata.
	BackupMetadataName = "metadata.json"
)

// BackupKeys contains the object keys and URI for a physical backup.
type BackupKeys struct {
	ArchiveKey  string
	MetadataKey string
	ArchiveURI  string
}

// BuildBackupKeys returns deterministic object-store keys for a backup.
func BuildBackupKeys(store mysqlv1alpha1.S3ObjectStore, clusterName, backupName, backupID string) (BackupKeys, error) {
	if store.Bucket == "" {
		return BackupKeys{}, fmt.Errorf("object store bucket is required")
	}
	if clusterName == "" || backupName == "" || backupID == "" {
		return BackupKeys{}, fmt.Errorf("cluster name, backup name and backup id are required")
	}
	prefix := cleanPath(store.Path)
	base := strings.Join([]string{clusterName, backupName, backupID}, "/")
	if prefix != "" {
		base = prefix + "/" + base
	}
	archiveKey := base + "/" + BackupArchiveName
	return BackupKeys{
		ArchiveKey:  archiveKey,
		MetadataKey: base + "/" + BackupMetadataName,
		ArchiveURI:  "s3://" + store.Bucket + "/" + archiveKey,
	}, nil
}

// ClusterPrefix returns the object-store key prefix under which a cluster's
// backups live. The trailing slash keeps it from matching sibling clusters
// whose names share this one as a prefix (e.g. "demo" vs "demo-staging").
func ClusterPrefix(store mysqlv1alpha1.S3ObjectStore, clusterName string) string {
	prefix := cleanPath(store.Path)
	if prefix != "" {
		return prefix + "/" + clusterName + "/"
	}
	return clusterName + "/"
}

func cleanPath(path string) string {
	parts := strings.FieldsFunc(path, func(r rune) bool {
		return r == '/'
	})
	return strings.Join(parts, "/")
}

// SHA256Writer computes a SHA256 checksum while forwarding writes to the
// wrapped writer.
type SHA256Writer struct {
	writer io.Writer
	hash   hash.Hash
}

// NewSHA256Writer wraps writer and tracks the SHA256 checksum of all bytes
// written through it.
func NewSHA256Writer(writer io.Writer) *SHA256Writer {
	return &SHA256Writer{
		writer: writer,
		hash:   sha256.New(),
	}
}

// Write records p in the checksum and forwards it to the wrapped writer.
func (w *SHA256Writer) Write(p []byte) (int, error) {
	if _, err := w.hash.Write(p); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}

// SumHex returns the hex-encoded SHA256 checksum of bytes written so far.
func (w *SHA256Writer) SumHex() string {
	return hex.EncodeToString(w.hash.Sum(nil))
}

// SHA256Reader wraps a reader and tracks the SHA256 checksum and byte count of
// everything read through it. It lets the upload path checksum a streamed
// archive without buffering it.
type SHA256Reader struct {
	reader io.Reader
	hash   hash.Hash
	count  int64
}

// NewSHA256Reader wraps reader and tracks the SHA256 checksum of all bytes read.
func NewSHA256Reader(reader io.Reader) *SHA256Reader {
	return &SHA256Reader{
		reader: reader,
		hash:   sha256.New(),
	}
}

// Read forwards from the wrapped reader, updating the checksum and byte count.
func (r *SHA256Reader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		_, _ = r.hash.Write(p[:n])
		r.count += int64(n)
	}
	return n, err
}

// SumHex returns the hex-encoded SHA256 checksum of bytes read so far.
func (r *SHA256Reader) SumHex() string {
	return hex.EncodeToString(r.hash.Sum(nil))
}

// Count returns the number of bytes read so far.
func (r *SHA256Reader) Count() int64 {
	return r.count
}
