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

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// ErrCollision is returned when a binlog about to be uploaded would clobber an
// existing, byte-different object at the same key. It means a server_uuid
// uniqueness invariant broke (a cloned auto.cnf or a RESET MASTER reusing a
// name) and must surface loudly rather than silently overwrite the archive.
var ErrCollision = errors.New("binlog: archive key already holds a different object")

// ErrNotCurrentPrimary is returned by a primary archive pass while the
// instance's Cluster view does not name it status.currentPrimary yet: until
// then it does not know the generation that fences its index writes.
var ErrNotCurrentPrimary = errors.New("binlog: the cluster does not name this instance its current primary yet")

// AuthoritySource reports the status.currentPrimaryGeneration of this instance
// while its Cluster view names it status.currentPrimary; ok is false otherwise.
type AuthoritySource func() (generation int64, ok bool)

// Store is the subset of objectstore.Client the archiver needs. It is an
// interface so the archiver is unit-testable with an in-memory fake. The index
// is written through its versioned methods (objectstore.UpdateArchiveIndex),
// since the primary, a draining former primary and retention all write it.
type Store interface {
	Upload(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) error
	PutJSON(ctx context.Context, bucket, key string, v any) error
	GetJSON(ctx context.Context, bucket, key string, v any) error
	Exists(ctx context.Context, bucket, key string) (bool, error)
	objectstore.VersionedStore
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
	newSet       func() gtidOps
	// forks snapshots the authority the fork check compares segments against;
	// nil disables the check.
	forks ForkSource
	// authority fences the primary's index writes (see AuthoritySource); nil
	// treats the archiver as an unfenced primary of generation zero.
	authority AuthoritySource
	// verified memoizes files this process has already proven byte-identical to
	// their archived copy, so a steady-state pass costs one stat per file instead
	// of re-decoding and re-hashing the whole retained set on every tick.
	verified map[string]archivedStamp
	// deferredScans memoizes the scan of files a drain filter deferred. A
	// disowned tail is deferred forever, and decoding it with mysqlbinlog on
	// every tick would cost a full read of the file every poll interval.
	deferredScans map[string]scannedStamp
	// indexed memoizes the files known to be listed in this instance's index
	// segment. A file whose manifest landed but whose index write failed is
	// archived yet unindexed, and recovery reads the index: such a file has to
	// count as advanced so the pass folds it in. indexSeeded is false until the
	// memo is seeded from one index read, and again after any failed index
	// write, since the index may then lack files the memo cannot name.
	indexed     map[string]bool
	indexSeeded bool
}

// scannedStamp is a scan result together with the identity of the file it was
// taken from.
type scannedStamp struct {
	info os.FileInfo
	scan ScanResult
}

// archivedStamp records the identity a file had when it was proven archived,
// together with the manifest that describes it. A rotated binlog is immutable,
// so an unchanged identity means the proof still holds; anything else re-runs
// the full verification, including the collision check.
type archivedStamp struct {
	info os.FileInfo
	meta objectstore.BinlogMetadata
}

// matches reports whether a file is still the one a scan was taken from.
func (s scannedStamp) matches(fi os.FileInfo) bool {
	return archivedStamp{info: s.info}.matches(fi)
}

// matches reports whether a file is still the one this stamp was taken from.
// os.SameFile compares device and inode, so a RESET MASTER that recreates a
// binlog under a reused name fails the check even if the replacement happens to
// have the same length and timestamp, and the collision check runs.
func (s archivedStamp) matches(fi os.FileInfo) bool {
	return os.SameFile(s.info, fi) &&
		s.info.Size() == fi.Size() &&
		s.info.ModTime().Equal(fi.ModTime())
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
	// NewSet returns a fresh GTIDOps accumulator for this archiver's engine.
	// Defaults to a MySQL (replication.GTIDSet-backed) set.
	NewSet func() GTIDOps
	// Forks snapshots what the surviving timeline holds for the fork check run
	// on every index write and by CheckForks. Nil disables fork checks.
	Forks ForkSource
	// Authority fences the primary's index writes with its primary generation.
	Authority AuthoritySource
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
	newSet := opts.NewSet
	if newSet == nil {
		newSet = newMysqlGTIDSet
	}
	return &Archiver{
		store:         opts.Store,
		objectStore:   opts.ObjectStore,
		clusterName:   opts.ClusterName,
		instanceName:  opts.InstanceName,
		serverUUID:    opts.ServerUUID,
		binlogDir:     opts.BinlogDir,
		scan:          opts.Scan,
		now:           now,
		newSet:        newSet,
		forks:         opts.Forks,
		authority:     opts.Authority,
		verified:      make(map[string]archivedStamp),
		deferredScans: make(map[string]scannedStamp),
		indexed:       make(map[string]bool),
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
	// Files lists every rotated file the pass proved archived, in sequence
	// order, with the GTIDs it holds. The purge gate reads it to decide how far
	// the replicas let it go.
	Files []ArchivedFile
	// ForkCheck reports the fork check run with the pass's last index write;
	// nil when the pass wrote no index.
	ForkCheck *ForkReport
	// Deferred names the file a drain filter refused, which stopped the pass;
	// empty when nothing was deferred.
	Deferred string
}

// FileFilter decides whether a file about to be uploaded may enter the archive,
// given its scan. Refusing defers the file and stops the pass there, so a later
// file never moves the frontier past it.
type FileFilter func(name string, scan ScanResult) (bool, error)

// ForkReport is the outcome of one fork check over the archive index.
type ForkReport struct {
	// Checked is true when the check compared every foreign segment against an
	// authority; CheckedAt is when.
	Checked   bool
	CheckedAt time.Time
	// Superseded is true when a primary with a newer generation already wrote
	// the index, so this one did not judge it.
	Superseded bool
	// Read is true when the report comes from a read of the index, so Forks and
	// OldestSegmentPosition describe it; false when nothing was read.
	Read bool
	// Forks are the fork records the index carries after the check (or as read,
	// when no check ran), not only those this check added.
	Forks []SegmentFork
	// OldestSegmentPosition is the lowest MariaDB position any segment reached
	// (see OldestSegmentPosition); empty on MySQL.
	OldestSegmentPosition string
	// Err is why the check could not run, if it failed.
	Err error
}

// ArchivedFile is one binlog known to be in the object store.
type ArchivedFile struct {
	Name    string
	GTIDSet string
}

// ArchivePending ships every rotated, not-yet-archived binlog in the provided
// list (which must already be MarkActive'd) in sequence order, advancing the
// per-segment status as it goes. The active log is never touched. It returns
// the resulting frontier or the first error; on error the frontier is not
// advanced past the file that failed.
func (a *Archiver) ArchivePending(ctx context.Context, logs []BinaryLog) (ArchiveResult, error) {
	generation, ok := a.Authority()
	if !ok {
		return ArchiveResult{}, ErrNotCurrentPrimary
	}
	return a.archivePending(ctx, logs, nil, &forkPass{source: a.forks, primary: true, generation: generation})
}

// Authority reports the generation this archiver writes the index under as the
// primary, and whether its Cluster view names it the current primary.
func (a *Archiver) Authority() (int64, bool) {
	if a.authority == nil {
		return 0, true
	}
	return a.authority()
}

// DrainPending is ArchivePending for a former primary draining the tail it
// stranded: every file to upload must pass allow first, and the index writes
// run no fork check. A demoted instance is not the authority on what the
// surviving timeline holds; judging the live primary's segment against its own
// executed set would record transactions it simply has not replicated yet.
func (a *Archiver) DrainPending(ctx context.Context, logs []BinaryLog, allow FileFilter) (ArchiveResult, error) {
	return a.archivePending(ctx, logs, allow, &forkPass{})
}

// ChecksForks reports whether this archiver runs fork checks at all.
func (a *Archiver) ChecksForks() bool { return a.forks != nil }

func (a *Archiver) archivePending(
	ctx context.Context, logs []BinaryLog, allow FileFilter, pass *forkPass,
) (result ArchiveResult, err error) {
	bucket := a.objectStore.Bucket

	status, err := a.loadStatus(ctx, bucket)
	if err != nil {
		return ArchiveResult{}, err
	}
	covered := a.newSet()
	if err := covered.Parse(status.CoveredGTIDSet); err != nil {
		return ArchiveResult{}, fmt.Errorf("binlog: parsing covered gtid set: %w", err)
	}

	result = ArchiveResult{
		LastArchivedBinlog: status.LastArchivedBinlog,
		LastArchivedGTID:   status.LastArchivedGTID,
		CoveredGTIDSet:     status.CoveredGTIDSet,
	}

	defer func() { result.ForkCheck = pass.report }()
	for _, l := range Archivable(logs) {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		meta, archived, deferred, err := a.archiveFile(ctx, bucket, l, allow)
		if err != nil {
			return result, err
		}
		if deferred {
			result.Deferred = l.Name
			return result, nil
		}
		if archived {
			result.Archived = append(result.Archived, l.Name)
		}
		result.Files = append(result.Files, ArchivedFile{Name: l.Name, GTIDSet: meta.GTIDSet})

		// A file that was already archived is only known to be indexed once the
		// memo is seeded; seeding costs one index read, so it waits for the first
		// file that needs it.
		if !archived && !a.indexed[l.Name] {
			if err := a.seedIndexed(ctx, bucket); err != nil {
				return result, err
			}
		}

		// Whether freshly archived or already present, fold its coverage into the
		// segment frontier so a resumed pass converges.
		fileSet := a.newSet()
		if err := fileSet.Parse(meta.GTIDSet); err != nil {
			return result, fmt.Errorf("binlog: parsing file gtid set for %q: %w", l.Name, err)
		}
		priorCovered := covered.String()
		covered.Union(fileSet)
		// A file already present whose coverage the persisted status already
		// records leaves nothing to commit. Recomputing the same status and index
		// objects on every tick would be two object-store writes per retained file
		// forever, so only write when this pass actually moved something. A crash
		// between the manifest and the status write lands here with coverage still
		// missing, which makes the union change and the writes happen. One between
		// the status and the index write leaves the coverage recorded but the file
		// unindexed, which the indexed memo catches.
		advanced := archived || covered.String() != priorCovered ||
			(status.FirstGTID == "" && meta.FirstGTID != "") || !a.indexed[l.Name]

		result.LastArchivedBinlog = l.Name
		if meta.LastGTID != "" {
			result.LastArchivedGTID = meta.LastGTID
		}
		result.CoveredGTIDSet = covered.String()
		result.LastArchivedTime = meta.ArchivedAt

		// Record the segment's first GTID once: it anchors the segment's per-domain
		// range start for recovery's gap-stitching.
		if status.FirstGTID == "" && meta.FirstGTID != "" {
			status.FirstGTID = meta.FirstGTID
		}
		status.LastArchivedBinlog = result.LastArchivedBinlog
		status.LastArchivedGTID = result.LastArchivedGTID
		status.CoveredGTIDSet = result.CoveredGTIDSet
		if !advanced {
			continue
		}
		status.UpdatedAt = a.now()
		statusKey := objectstore.ArchiveStatusKey(a.objectStore, a.clusterName, a.serverUUID)
		if err := a.store.PutJSON(ctx, bucket, statusKey, status); err != nil {
			return result, fmt.Errorf("binlog: writing archive status: %w", err)
		}
		if err := a.updateIndex(ctx, bucket, status, l.Name, meta.GTIDSet, pass); err != nil {
			a.indexSeeded = false
			return result, err
		}
		a.indexed[l.Name] = true
	}

	return result, nil
}

// seedIndexed seeds the indexed memo from the archive index, once per process
// and again after a failed index write.
func (a *Archiver) seedIndexed(ctx context.Context, bucket string) error {
	if a.indexSeeded {
		return nil
	}
	var index objectstore.ArchiveIndex
	key := objectstore.ArchiveIndexKey(a.objectStore, a.clusterName)
	_, exists, err := a.store.GetJSONVersion(ctx, bucket, key, &index)
	if err != nil {
		return fmt.Errorf("binlog: reading archive index: %w", err)
	}
	a.indexed = make(map[string]bool)
	if seg, ok := index.Segment(a.serverUUID); exists && ok {
		for _, name := range seg.Binlogs {
			a.indexed[name] = true
		}
	}
	a.indexSeeded = true
	return nil
}

// archiveFile archives a single rotated file. It returns the file's manifest,
// whether it was freshly uploaded (false ⇒ already archived), whether allow
// deferred it (nothing uploaded), and any error.
// Commit order is bytes → manifest, so a present manifest means a complete
// archive; a present body without manifest is a partial upload that is retried.
func (a *Archiver) archiveFile(
	ctx context.Context, bucket string, l BinaryLog, allow FileFilter,
) (objectstore.BinlogMetadata, bool, bool, error) {
	meta, archived, err := a.archiveFileAllowed(ctx, bucket, l, allow)
	if errors.Is(err, errDeferred) {
		return objectstore.BinlogMetadata{}, false, true, nil
	}
	return meta, archived, false, err
}

// errDeferred is archiveFileAllowed's internal signal that allow refused the
// file.
var errDeferred = errors.New("binlog: file deferred")

func (a *Archiver) archiveFileAllowed(
	ctx context.Context, bucket string, l BinaryLog, allow FileFilter,
) (objectstore.BinlogMetadata, bool, error) {
	keys, err := objectstore.BuildBinlogKeys(a.objectStore, a.clusterName, a.serverUUID, l.Name)
	if err != nil {
		return objectstore.BinlogMetadata{}, false, err
	}
	path := filepath.Join(a.binlogDir, l.Name)

	// A rotated log we already proved archived in this process cannot have
	// changed, so re-reading it would buy nothing. Confirm its identity with a
	// stat and reuse the manifest we recorded. Retention is unbounded when the
	// purge gate is off, so this is what keeps a pass O(new files) rather than
	// O(everything still on disk).
	st, err := os.Stat(path)
	if err != nil {
		return objectstore.BinlogMetadata{}, false, fmt.Errorf("binlog: stat %q: %w", l.Name, err)
	}
	if stamp, ok := a.verified[l.Name]; ok && stamp.matches(st) {
		return stamp.meta, false, nil
	}

	// Check the archive before reading the file. A manifest means this file
	// landed on a prior pass, and then only its hash is needed — to prove the
	// bytes still match what was shipped. Decoding it with mysqlbinlog is only
	// worthwhile for a file we are about to upload, which is the one case that
	// needs a fresh GTID range and timestamps for the manifest.
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
		sum, _, err := hashFile(path)
		if err != nil {
			return objectstore.BinlogMetadata{}, false, err
		}
		if prior.SHA256 != "" && prior.SHA256 != sum {
			return objectstore.BinlogMetadata{}, false, fmt.Errorf("%w: %s (uuid %s): stored sha %s != local %s",
				ErrCollision, l.Name, a.serverUUID, prior.SHA256, sum)
		}
		// Byte-identical: already archived, nothing to do.
		a.verified[l.Name] = archivedStamp{info: st, meta: prior}
		return prior, false, nil
	}

	var scanRes ScanResult
	if cached, ok := a.deferredScans[l.Name]; ok && cached.matches(st) {
		scanRes = cached.scan
	} else {
		if scanRes, err = a.scan(ctx, path); err != nil {
			return objectstore.BinlogMetadata{}, false, fmt.Errorf("binlog: scanning %q: %w", l.Name, err)
		}
	}
	if allow != nil {
		ok, err := allow(l.Name, scanRes)
		if err != nil {
			return objectstore.BinlogMetadata{}, false, fmt.Errorf("binlog: evaluating %q for the archive: %w", l.Name, err)
		}
		if !ok {
			a.deferredScans[l.Name] = scannedStamp{info: st, scan: scanRes}
			return objectstore.BinlogMetadata{}, false, errDeferred
		}
	}
	delete(a.deferredScans, l.Name)
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
	a.verified[l.Name] = archivedStamp{info: st, meta: meta}
	return meta, true, nil
}

// HasSegment reports whether this instance has already archived under its
// current identity, i.e. it owns a segment in the archive.
//
// It is what distinguishes a former primary holding its own un-shipped tail from
// a replica that merely re-logs someone else's history: only an instance that
// archived while it was primary has a segment keyed by its server identity. The
// drain path (see Loop.tick) uses it to stay silent on every never-promoted
// replica, so no replica's redundant copy of the timeline is ever uploaded.
func (a *Archiver) HasSegment(ctx context.Context) (bool, error) {
	status, err := a.loadStatus(ctx, a.objectStore.Bucket)
	if err != nil {
		return false, err
	}
	return status.LastArchivedBinlog != "", nil
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

// updateIndex folds one just-archived file into the cluster-level archive index,
// the discovery/ordering record recovery walks across all server UUIDs. The
// file's name and its GTID coverage advance together in this single write, so the
// segment's file list and its covered set can never disagree: a GTID counts as
// covered only once the file that carries it is in seg.Binlogs.
//
// It must not copy the per-segment status's cumulative covered set into the
// segment. Status is committed in a separate object one step earlier, so a crash
// between the two (followed by the file leaving the local listing, e.g. mysqld
// expiring it) would leave the segment claiming coverage for a file its list no
// longer names, and recovery would silently skip a GTID range it believes it has.
// Folding only fileGTIDSet — the coverage of the file being added — keeps the two
// in lockstep. Union is idempotent, so retries and resumes are safe.
func (a *Archiver) updateIndex(
	ctx context.Context, bucket string, status objectstore.ArchiveStatus, fileName, fileGTIDSet string, pass *forkPass,
) error {
	key := objectstore.ArchiveIndexKey(a.objectStore, a.clusterName)
	// The fold re-runs on a fresh copy whenever another writer got in first,
	// so it is a pure function of the index it is handed.
	err := objectstore.UpdateArchiveIndex(ctx, a.store, bucket, key,
		func(index *objectstore.ArchiveIndex, _ bool) (bool, error) {
			if err := a.foldFile(index, status, fileName, fileGTIDSet); err != nil {
				return false, err
			}
			if pass.primary && pass.generation > index.Generation {
				index.Generation = pass.generation
			}
			// This write carries a fresh copy of the index anyway, so check the
			// seams on it: a fork that landed late, or a record lost to a racing
			// writer, is caught on the primary's next write.
			_, report := a.checkForks(ctx, index, pass)
			if report.Checked {
				index.ForkCheck = &objectstore.ArchiveForkCheck{CheckedAt: report.CheckedAt, CheckedBy: a.forkIdentity()}
			}
			pass.report = &report
			return true, nil
		})
	if err != nil {
		return fmt.Errorf("binlog: updating archive index: %w", err)
	}
	return nil
}

// foldFile adds one archived file and its coverage to the index.
func (a *Archiver) foldFile(
	index *objectstore.ArchiveIndex, status objectstore.ArchiveStatus, fileName, fileGTIDSet string,
) error {
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
	if seg.StartGTIDSet == "" {
		seg.StartGTIDSet = status.FirstGTID
	}
	seg.EndedAt = a.now()

	// Add the file's name and fold its coverage in the same write, so recovery's
	// PlanReplay never sees a covered GTID it has no file to replay. Deduplicated
	// so retries and idempotent resumes are safe.
	if fileName != "" && !slices.Contains(seg.Binlogs, fileName) {
		seg.Binlogs = append(seg.Binlogs, fileName)
	}
	segSet := a.newSet()
	if err := segSet.Parse(seg.GTIDSet); err != nil {
		return fmt.Errorf("binlog: parsing segment gtid set: %w", err)
	}
	fileSet := a.newSet()
	if err := fileSet.Parse(fileGTIDSet); err != nil {
		return fmt.Errorf("binlog: parsing file gtid set for %q: %w", fileName, err)
	}
	segSet.Union(fileSet)
	seg.GTIDSet = segSet.String()

	// Recompute the cumulative covered set across every segment.
	cumulative := a.newSet()
	for i := range index.Segments {
		parsed := a.newSet()
		if err := parsed.Parse(index.Segments[i].GTIDSet); err != nil {
			return fmt.Errorf("binlog: parsing segment gtid set: %w", err)
		}
		cumulative.Union(parsed)
	}
	index.CoveredGTIDSet = cumulative.String()
	index.UpdatedAt = a.now()
	return nil
}

// forkPass carries what one archive pass writes the index as: the primary of
// a generation (whose writes stamp it and may run the fork check), or a
// draining former primary (neither).
type forkPass struct {
	source     ForkSource
	primary    bool
	generation int64
	report     *ForkReport
	// preloaded is an authority read just before the index, used by the first
	// write attempt instead of reading it again.
	preloaded ForkJudge
}

// load reads the fork-check authority. It runs for every index write, after
// the index was read, so the authority is never older than the segments it
// judges.
func (p *forkPass) load(ctx context.Context) (ForkJudge, error) {
	if p.preloaded != nil {
		judge := p.preloaded
		p.preloaded = nil
		return judge, nil
	}
	if p.source == nil {
		return nil, nil
	}
	return p.source(ctx)
}

func (a *Archiver) forkIdentity() string {
	return forkCheckIdentity(a.instanceName, a.serverUUID)
}

// checkForks folds into every foreign segment's fork record the transactions
// the surviving timeline does not hold, and reports whether any record grew.
// Its own segment is skipped: it holds only this instance's binlog, which its
// authority contains. The index is only modified when every segment could be
// judged.
func (a *Archiver) checkForks(
	ctx context.Context, index *objectstore.ArchiveIndex, pass *forkPass,
) (bool, ForkReport) {
	report := ForkReport{}
	finish := func() ForkReport {
		report.Read = true
		report.Forks = SegmentForks(index)
		report.OldestSegmentPosition = OldestSegmentPosition(index.Segments)
		return report
	}
	if pass.source == nil {
		return false, finish()
	}
	if index.Generation > pass.generation {
		// A newer primary already wrote the index: this one was demoted and is
		// finishing a pass. Its authority no longer speaks for the timeline.
		report.Superseded = true
		return false, finish()
	}
	judge, err := pass.load(ctx)
	if err != nil {
		report.Err = fmt.Errorf("binlog: reading fork check authority: %w", err)
		return false, finish()
	}
	if judge == nil {
		return false, finish()
	}

	now := a.now()
	merged := make([]*objectstore.ArchiveFork, len(index.Segments))
	grew := false
	for i := range index.Segments {
		seg := &index.Segments[i]
		merged[i] = seg.Fork
		if seg.ServerUUID == a.serverUUID {
			continue
		}
		delta, err := judge.Judge(*seg)
		if err != nil {
			report.Err = err
			return false, finish()
		}
		fork, g, err := mergeFork(seg.Fork, delta, judge.Authority(), now, a.forkIdentity())
		if err != nil {
			report.Err = err
			return false, finish()
		}
		merged[i] = fork
		grew = grew || g
	}
	for i := range index.Segments {
		index.Segments[i].Fork = merged[i]
	}
	report.Checked = true
	report.CheckedAt = now
	return grew, finish()
}

// CheckForks runs the fork check over the archive index outside an archive
// pass. The loop calls it on the first writable pass of a process, so a
// promotion, failback or restart checks the archive without waiting for the
// next rotation. It writes the index only when a record grew or the index has
// never been checked. With no index yet there is nothing to check.
//
// The authority is taken before the index is read: without one (a MariaDB
// primary that has no timeline) there is nothing to judge, and reading the
// index on every pass to find that out would cost a round trip each time.
func (a *Archiver) CheckForks(ctx context.Context) (ForkReport, error) {
	if a.forks == nil {
		return ForkReport{}, nil
	}
	generation, ok := a.Authority()
	if !ok {
		return ForkReport{}, ErrNotCurrentPrimary
	}
	pass := &forkPass{source: a.forks, primary: true, generation: generation}
	judge, err := pass.load(ctx)
	if err != nil {
		err = fmt.Errorf("binlog: reading fork check authority: %w", err)
		return ForkReport{Err: err}, err
	}
	if judge == nil {
		return ForkReport{}, nil
	}
	pass.preloaded = judge
	var report ForkReport
	key := objectstore.ArchiveIndexKey(a.objectStore, a.clusterName)
	err = objectstore.UpdateArchiveIndex(ctx, a.store, a.objectStore.Bucket, key,
		func(index *objectstore.ArchiveIndex, exists bool) (bool, error) {
			if !exists {
				report = ForkReport{}
				return false, nil
			}
			var grew bool
			grew, report = a.checkForks(ctx, index, pass)
			if report.Err != nil {
				return false, report.Err
			}
			if !report.Checked || (!grew && index.ForkCheck != nil && index.Generation >= generation) {
				return false, nil
			}
			index.Generation = generation
			index.ForkCheck = &objectstore.ArchiveForkCheck{CheckedAt: report.CheckedAt, CheckedBy: a.forkIdentity()}
			index.UpdatedAt = a.now()
			return true, nil
		})
	if err != nil {
		if report.Err == nil {
			report.Err = err
		}
		return report, err
	}
	return report, nil
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
