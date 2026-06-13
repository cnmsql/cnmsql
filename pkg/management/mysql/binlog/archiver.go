/*
Copyright 2026 The CNMySQL Authors.

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

package binlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
)

// ErrCollision is returned when a binlog about to be uploaded would clobber an
// existing, byte-different object at the same key. It means a server_uuid
// uniqueness invariant broke (a cloned auto.cnf or a RESET MASTER reusing a
// name) and must surface loudly rather than silently overwrite the archive.
var ErrCollision = errors.New("binlog: archive key already holds a different object")

// Store is the subset of objectstore.Client the archiver needs. It is an
// interface so the archiver is unit-testable with an in-memory fake.
type Store interface {
	Upload(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) error
	PutJSON(ctx context.Context, bucket, key string, v any) error
	GetJSON(ctx context.Context, bucket, key string, v any) error
	Exists(ctx context.Context, bucket, key string) (bool, error)
}

// Scanner extracts a file's GTID/timestamp summary. The real implementation
// runs mysqlbinlog; tests inject a fake.
type Scanner func(ctx context.Context, path string) (ScanResult, error)

// Archiver ships rotated binary-log files from the local datadir to the object
// store, keeping a gapless, GTID-addressable archive. It is the in-Pod engine
// the run loop drives while the instance is the current primary.
type Archiver struct {
	store        Store
	objectStore  mysqlv1alpha1.S3ObjectStore
	clusterName  string
	instanceName string
	serverUUID   string
	binlogDir    string
	scan         Scanner
	now          func() time.Time
}

// ArchiverOptions configures an Archiver.
type ArchiverOptions struct {
	Store        Store
	ObjectStore  mysqlv1alpha1.S3ObjectStore
	ClusterName  string
	InstanceName string
	// ServerUUID partitions this instance's segment of the archive.
	ServerUUID string
	// BinlogDir is the directory holding the local binary-log files.
	BinlogDir string
	// Scan extracts GTID/timestamps from a file; defaults to nil and must be set.
	Scan Scanner
	// Now is the clock; defaults to time.Now.
	Now func() time.Time
}

// NewArchiver builds an Archiver from validated options.
func NewArchiver(opts ArchiverOptions) (*Archiver, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("binlog: archiver store is required")
	}
	if opts.ClusterName == "" || opts.ServerUUID == "" {
		return nil, fmt.Errorf("binlog: cluster name and server uuid are required")
	}
	if opts.BinlogDir == "" {
		return nil, fmt.Errorf("binlog: binlog dir is required")
	}
	if opts.Scan == nil {
		return nil, fmt.Errorf("binlog: scanner is required")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Archiver{
		store:        opts.Store,
		objectStore:  opts.ObjectStore,
		clusterName:  opts.ClusterName,
		instanceName: opts.InstanceName,
		serverUUID:   opts.ServerUUID,
		binlogDir:    opts.BinlogDir,
		scan:         opts.Scan,
		now:          now,
	}, nil
}

// ArchiveResult summarizes one ArchivePending pass.
type ArchiveResult struct {
	// Archived lists the binlog basenames newly shipped this pass.
	Archived []string
	// LastArchivedBinlog and LastArchivedGTID reflect the advanced frontier.
	LastArchivedBinlog string
	LastArchivedGTID   string
	// CoveredGTIDSet is this segment's cumulative covered GTID set.
	CoveredGTIDSet string
	// LastArchivedTime is when the most recent file finished archiving.
	LastArchivedTime time.Time
}

// ArchivePending ships every rotated, not-yet-archived binlog in the provided
// list (which must already be MarkActive'd) in sequence order, advancing the
// per-segment status as it goes. The active log is never touched. It returns
// the resulting frontier or the first error; on error the frontier is not
// advanced past the file that failed.
func (a *Archiver) ArchivePending(ctx context.Context, logs []BinaryLog) (ArchiveResult, error) {
	bucket := a.objectStore.Bucket

	status, err := a.loadStatus(ctx, bucket)
	if err != nil {
		return ArchiveResult{}, err
	}
	covered, err := replication.ParseGTIDSet(status.CoveredGTIDSet)
	if err != nil {
		return ArchiveResult{}, fmt.Errorf("binlog: parsing covered gtid set: %w", err)
	}

	result := ArchiveResult{
		LastArchivedBinlog: status.LastArchivedBinlog,
		LastArchivedGTID:   status.LastArchivedGTID,
		CoveredGTIDSet:     covered.String(),
	}

	for _, l := range Archivable(logs) {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		meta, archived, err := a.archiveFile(ctx, bucket, l)
		if err != nil {
			return result, err
		}
		if archived {
			result.Archived = append(result.Archived, l.Name)
		}

		// Whether freshly archived or already present, fold its coverage into the
		// segment frontier so a resumed pass converges.
		fileSet, err := replication.ParseGTIDSet(meta.GTIDSet)
		if err != nil {
			return result, fmt.Errorf("binlog: parsing file gtid set for %q: %w", l.Name, err)
		}
		covered.Union(fileSet)
		result.LastArchivedBinlog = l.Name
		if meta.LastGTID != "" {
			result.LastArchivedGTID = meta.LastGTID
		}
		result.CoveredGTIDSet = covered.String()
		result.LastArchivedTime = meta.ArchivedAt

		status.LastArchivedBinlog = result.LastArchivedBinlog
		status.LastArchivedGTID = result.LastArchivedGTID
		status.CoveredGTIDSet = result.CoveredGTIDSet
		status.UpdatedAt = a.now()
		statusKey := objectstore.ArchiveStatusKey(a.objectStore, a.clusterName, a.serverUUID)
		if err := a.store.PutJSON(ctx, bucket, statusKey, status); err != nil {
			return result, fmt.Errorf("binlog: writing archive status: %w", err)
		}
		if err := a.updateIndex(ctx, bucket, status); err != nil {
			return result, err
		}
	}

	return result, nil
}

// archiveFile archives a single rotated file. It returns the file's manifest,
// whether it was freshly uploaded (false ⇒ already archived), and any error.
// Commit order is bytes → manifest, so a present manifest means a complete
// archive; a present body without manifest is a partial upload that is retried.
func (a *Archiver) archiveFile(
	ctx context.Context, bucket string, l BinaryLog,
) (objectstore.BinlogMetadata, bool, error) {
	keys, err := objectstore.BuildBinlogKeys(a.objectStore, a.clusterName, a.serverUUID, l.Name)
	if err != nil {
		return objectstore.BinlogMetadata{}, false, err
	}
	path := filepath.Join(a.binlogDir, l.Name)

	// Scan first: we need the GTID range/timestamps for the manifest and the
	// collision check, and it is cheap relative to the upload.
	scanRes, err := a.scan(ctx, path)
	if err != nil {
		return objectstore.BinlogMetadata{}, false, fmt.Errorf("binlog: scanning %q: %w", l.Name, err)
	}
	sum, size, err := hashFile(path)
	if err != nil {
		return objectstore.BinlogMetadata{}, false, err
	}

	seq, _ := ParseSequence(l.Name)
	meta := objectstore.BinlogMetadata{
		ClusterName:    a.clusterName,
		ServerUUID:     a.serverUUID,
		InstanceName:   a.instanceName,
		BinlogName:     l.Name,
		Sequence:       seq,
		FirstGTID:      scanRes.FirstGTID,
		LastGTID:       scanRes.LastGTID,
		GTIDSet:        scanRes.GTIDSet,
		FirstEventTime: scanRes.FirstEventTime,
		LastEventTime:  scanRes.LastEventTime,
		SizeBytes:      size,
		SHA256:         sum,
		ArchivedAt:     a.now(),
	}

	// If a manifest already exists, this file landed on a prior pass.
	existsManifest, err := a.store.Exists(ctx, bucket, keys.ManifestKey)
	if err != nil {
		return objectstore.BinlogMetadata{}, false, err
	}
	if existsManifest {
		var prior objectstore.BinlogMetadata
		if err := a.store.GetJSON(ctx, bucket, keys.ManifestKey, &prior); err != nil {
			return objectstore.BinlogMetadata{}, false,
				fmt.Errorf("binlog: reading existing manifest %q: %w", keys.ManifestKey, err)
		}
		if prior.SHA256 != "" && prior.SHA256 != sum {
			return objectstore.BinlogMetadata{}, false, fmt.Errorf("%w: %s (uuid %s): stored sha %s != local %s",
				ErrCollision, l.Name, a.serverUUID, prior.SHA256, sum)
		}
		// Byte-identical: already archived, nothing to do.
		return prior, false, nil
	}

	// Upload the raw bytes, then the manifest. A crash between the two leaves a
	// body with no manifest, which the next pass retries (idempotent overwrite).
	f, err := os.Open(path)
	if err != nil {
		return objectstore.BinlogMetadata{}, false, fmt.Errorf("binlog: opening %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if err := a.store.Upload(ctx, bucket, keys.BinlogKey, f, size, "application/octet-stream"); err != nil {
		return objectstore.BinlogMetadata{}, false, fmt.Errorf("binlog: uploading %q: %w", l.Name, err)
	}
	if err := a.store.PutJSON(ctx, bucket, keys.ManifestKey, meta); err != nil {
		return objectstore.BinlogMetadata{}, false, fmt.Errorf("binlog: writing manifest for %q: %w", l.Name, err)
	}
	return meta, true, nil
}

// loadStatus reads this segment's archive status, returning a fresh zero status
// when none exists yet.
func (a *Archiver) loadStatus(ctx context.Context, bucket string) (objectstore.ArchiveStatus, error) {
	key := objectstore.ArchiveStatusKey(a.objectStore, a.clusterName, a.serverUUID)
	exists, err := a.store.Exists(ctx, bucket, key)
	if err != nil {
		return objectstore.ArchiveStatus{}, err
	}
	status := objectstore.ArchiveStatus{
		ClusterName:  a.clusterName,
		ServerUUID:   a.serverUUID,
		InstanceName: a.instanceName,
	}
	if !exists {
		return status, nil
	}
	if err := a.store.GetJSON(ctx, bucket, key, &status); err != nil {
		return objectstore.ArchiveStatus{}, fmt.Errorf("binlog: reading archive status: %w", err)
	}
	return status, nil
}

// updateIndex folds this segment's current coverage into the cluster-level
// archive index, the discovery/ordering record recovery walks across all server
// UUIDs. It is best-effort idempotent: the active segment is upserted with its
// latest covered set, and the cumulative set is recomputed.
func (a *Archiver) updateIndex(ctx context.Context, bucket string, status objectstore.ArchiveStatus) error {
	key := objectstore.ArchiveIndexKey(a.objectStore, a.clusterName)
	var index objectstore.ArchiveIndex
	exists, err := a.store.Exists(ctx, bucket, key)
	if err != nil {
		return err
	}
	if exists {
		if err := a.store.GetJSON(ctx, bucket, key, &index); err != nil {
			return fmt.Errorf("binlog: reading archive index: %w", err)
		}
	}
	index.ClusterName = a.clusterName

	seg, ok := index.Segment(a.serverUUID)
	if !ok {
		index.Segments = append(index.Segments, objectstore.ArchiveSegment{
			ServerUUID:   a.serverUUID,
			InstanceName: a.instanceName,
			StartedAt:    a.now(),
		})
		seg = &index.Segments[len(index.Segments)-1]
	}
	seg.GTIDSet = status.CoveredGTIDSet
	seg.EndedAt = a.now()

	// Accumulate the binlog file names in this segment so that recovery's
	// PlanReplay can discover which files to download. The status always carries
	// the most recently shipped file; add it to the segment's list when it is
	// new (deduplicated so retries and idempotent updates are safe).
	if status.LastArchivedBinlog != "" {
		if !slices.Contains(seg.Binlogs, status.LastArchivedBinlog) {
			seg.Binlogs = append(seg.Binlogs, status.LastArchivedBinlog)
		}
	}

	// Recompute the cumulative covered set across every segment.
	cumulative := replication.GTIDSet{}
	for i := range index.Segments {
		parsed, err := replication.ParseGTIDSet(index.Segments[i].GTIDSet)
		if err != nil {
			return fmt.Errorf("binlog: parsing segment gtid set: %w", err)
		}
		cumulative.Union(parsed)
	}
	index.CoveredGTIDSet = cumulative.String()
	index.UpdatedAt = a.now()

	if err := a.store.PutJSON(ctx, bucket, key, &index); err != nil {
		return fmt.Errorf("binlog: writing archive index: %w", err)
	}
	return nil
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, fmt.Errorf("binlog: opening %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	r := objectstore.NewSHA256Reader(f)
	if _, err := io.Copy(io.Discard, r); err != nil {
		return "", 0, fmt.Errorf("binlog: hashing %q: %w", path, err)
	}
	return r.SumHex(), r.Count(), nil
}
