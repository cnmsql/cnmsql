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
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

// Default loop cadences. The flush interval bounds time-based RPO; mysqld's
// max_binlog_size handles the size trigger by rotating on its own.
const (
	DefaultPollInterval  = 10 * time.Second
	DefaultFlushInterval = 5 * time.Minute
)

// Loop drives continuous archiving in-Pod: while the instance is the writable
// primary it forces rotation on the RPO cadence, ships rotated files, advances
// the archive frontier, and purges shipped logs. It only ever archives from the
// primary, so on failover the new primary's Loop takes over (its archiver keys
// under a different server_uuid and GTID stitches the streams).
// ReplicationProbe reports whether this instance is replicating from its source.
// The Loop uses it as the authorisation signal for draining a stranded tail (see
// Loop.drain); it is nil for engines with no such path, which disables the drain.
type ReplicationProbe interface {
	// Streaming is true only when replication is configured and both threads are
	// running — i.e. the source accepted this instance's GTID position rather than
	// rejecting it as diverged.
	Streaming(ctx context.Context) (bool, error)
}

// ReplicaFloor reports how far the other instances of the cluster have applied
// the primary's history. The purge gate uses it to keep every binlog some
// expected instance still needs.
type ReplicaFloor interface {
	// Positions returns the applied GTID set of every instance the purge must
	// wait for, and the names of those whose position is not known yet. ok is
	// false when nothing about the cluster has been observed, which forbids any
	// purge.
	Positions() (positions map[string]string, unknown []string, ok bool)
}

type Loop struct {
	reader   *Reader
	archiver *Archiver
	logger   logr.Logger
	// replication authorises draining a demoted primary's un-shipped binlogs.
	replication ReplicationProbe

	pollInterval  time.Duration
	flushInterval time.Duration
	// purge, when true, lets the loop issue PURGE BINARY LOGS up to the archived
	// frontier (the purge gate: mysqld can never recycle an un-shipped log).
	purge bool
	// floor further bounds the purge to what every expected instance has
	// applied. When nil, nothing is purged.
	floor ReplicaFloor

	mu    sync.Mutex
	state State
}

// State is a snapshot of archiving health, surfaced into Cluster.status.
type State struct {
	// Active is true while this instance is the writable primary and archiving.
	Active bool
	// LastArchivedBinlog/GTID/Time reflect the archive frontier.
	LastArchivedBinlog string
	LastArchivedGTID   string
	LastArchivedTime   time.Time
	// PendingFiles is the count of rotated files not yet shipped (archive lag).
	PendingFiles int
	// LastError and LastErrorTime record the most recent failure, if any.
	LastError     string
	LastErrorTime time.Time
	// PurgeHeldBy lists the instances that have not applied the oldest archived
	// file yet (or whose position is unknown), so the purge gate keeps it.
	// PurgeHeldSince is when that file started being held. Both are empty
	// while nothing archived is held back.
	PurgeHeldBy    []string
	PurgeHeldSince time.Time
	// purgeHeldFile is the file PurgeHeldSince refers to.
	purgeHeldFile string
}

// LoopOptions configures a Loop.
type LoopOptions struct {
	Reader        *Reader
	Archiver      *Archiver
	Logger        logr.Logger
	PollInterval  time.Duration
	FlushInterval time.Duration
	// Purge enables the active purge gate (PURGE BINARY LOGS to the frontier).
	Purge bool
	// Floor bounds the purge to what every expected instance has applied. The
	// gate purges nothing without it.
	Floor ReplicaFloor
	// Replication authorises the drain of binlogs stranded by a demotion. When
	// nil, a non-writable instance never archives.
	Replication ReplicationProbe
}

// NewLoop builds a Loop from options, applying cadence defaults.
func NewLoop(opts LoopOptions) *Loop {
	poll := opts.PollInterval
	if poll <= 0 {
		poll = DefaultPollInterval
	}
	flush := opts.FlushInterval
	if flush <= 0 {
		flush = DefaultFlushInterval
	}
	return &Loop{
		reader:        opts.Reader,
		archiver:      opts.Archiver,
		logger:        opts.Logger,
		pollInterval:  poll,
		flushInterval: flush,
		purge:         opts.Purge,
		floor:         opts.Floor,
		replication:   opts.Replication,
	}
}

// State returns a copy of the current archiving state.
func (l *Loop) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.state
	s.PurgeHeldBy = slices.Clone(s.PurgeHeldBy)
	return s
}

// Run blocks driving the archive until ctx is cancelled.
func (l *Loop) Run(ctx context.Context) error {
	ticker := time.NewTicker(l.pollInterval)
	defer ticker.Stop()

	var lastFlush time.Time
	var lastFlushSize int64
	for {
		l.tick(ctx, &lastFlush, &lastFlushSize)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// tick runs one archive pass. It gates on writability so only the primary
// rotates and ships on the RPO cadence; a non-primary clears its Active flag and
// at most drains a tail it stranded while it was primary (see drain).
func (l *Loop) tick(ctx context.Context, lastFlush *time.Time, lastFlushSize *int64) {
	writable, err := l.reader.Writable(ctx)
	if err != nil {
		l.fail("checking writability", err)
		return
	}
	if !writable {
		l.mu.Lock()
		l.state.Active = false
		l.mu.Unlock()
		// Reset the flush schedule so a freshly-promoted primary flushes promptly.
		*lastFlush = time.Time{}
		l.drain(ctx)
		return
	}

	logs, err := l.reader.ListBinaryLogs(ctx)
	if err != nil {
		l.fail("listing binary logs", err)
		return
	}

	// Time-based RPO trigger: if data has accumulated in the active log since the
	// last flush and the interval elapsed, force a rotation so it becomes
	// archivable. Avoid churning empty files on an idle cluster.
	active := activeLog(logs)
	if lastFlush.IsZero() {
		*lastFlush = time.Now()
		*lastFlushSize = active.SizeBytes
	} else if time.Since(*lastFlush) >= l.flushInterval && active.SizeBytes > *lastFlushSize {
		if err := l.reader.FlushLogs(ctx); err != nil {
			l.fail("flushing binary logs", err)
			return
		}
		*lastFlush = time.Now()
		if logs, err = l.reader.ListBinaryLogs(ctx); err != nil {
			l.fail("re-listing binary logs", err)
			return
		}
		*lastFlushSize = activeLog(logs).SizeBytes
	}

	res, err := l.archiver.ArchivePending(ctx, logs)
	if err != nil {
		l.fail("archiving binary logs", err)
		return
	}

	var held purgeHold
	if l.purge {
		plan, err := l.planPurge(logs, res)
		if err != nil {
			l.fail("evaluating the replica floor", err)
			return
		}
		held = plan.held
		if plan.to != "" {
			if err := l.reader.PurgeLogsTo(ctx, plan.to); err != nil {
				l.fail("purging archived logs", err)
				return
			}
		}
	}

	l.mu.Lock()
	heldSince := time.Time{}
	if held.file != "" {
		heldSince = time.Now()
		if held.file == l.state.purgeHeldFile && !l.state.PurgeHeldSince.IsZero() {
			heldSince = l.state.PurgeHeldSince
		}
	}
	l.state = State{
		PurgeHeldBy:        held.by,
		PurgeHeldSince:     heldSince,
		purgeHeldFile:      held.file,
		Active:             true,
		LastArchivedBinlog: res.LastArchivedBinlog,
		LastArchivedGTID:   res.LastArchivedGTID,
		LastArchivedTime:   res.LastArchivedTime,
		PendingFiles:       pendingAfter(logs, res.LastArchivedBinlog),
	}
	l.mu.Unlock()
	if len(res.Archived) > 0 {
		l.logger.Info("Archived binary logs",
			"files", res.Archived,
			"lastArchivedGTID", res.LastArchivedGTID)
	}
}

// purgePlan is where the purge gate may go this pass.
type purgePlan struct {
	// to is the file to PURGE BINARY LOGS TO (everything before it goes), or ""
	// to purge nothing.
	to string
	// held describes the archived file the replicas keep, if any.
	held purgeHold
}

// purgeHold names the oldest archived file the replica floor keeps and the
// instances that still need it.
type purgeHold struct {
	file string
	by   []string
}

// planPurge bounds the purge by two conditions, both required for a file to go:
// it is archived, and every expected instance has applied every GTID in it.
//
// The archive bound is unchanged: everything strictly before the file preceding
// the frontier. The replica floor then lowers it to the first file some instance
// has not applied, so a replica that is disconnected, restarting or rejoining
// can still catch up from the primary instead of being re-cloned. mysqld only
// protects the files a connected replica is reading, which is not enough.
//
// Every unknown fails closed: no observation of the cluster, or an instance with
// no known position, keeps every file.
func (l *Loop) planPurge(logs []BinaryLog, res ArchiveResult) (purgePlan, error) {
	limit := fileBefore(logs, res.LastArchivedBinlog)
	if limit == "" || l.floor == nil {
		return purgePlan{}, nil
	}
	// Only files strictly before limit are candidates; limit itself is kept.
	var candidates []ArchivedFile
	for _, f := range res.Files {
		if f.Name == limit {
			break
		}
		candidates = append(candidates, f)
	}
	if len(candidates) == 0 {
		return purgePlan{}, nil
	}

	positions, unknown, ok := l.floor.Positions()
	if !ok {
		return purgePlan{}, nil
	}
	if len(unknown) > 0 {
		by := slices.Clone(unknown)
		slices.Sort(by)
		return purgePlan{held: purgeHold{file: candidates[0].Name, by: by}}, nil
	}

	names := make([]string, 0, len(positions))
	applied := make(map[string]gtidOps, len(positions))
	for name, raw := range positions {
		set := l.archiver.newSet()
		if err := set.Parse(raw); err != nil {
			return purgePlan{}, fmt.Errorf("parsing the gtid position of %s: %w", name, err)
		}
		names = append(names, name)
		applied[name] = set
	}
	slices.Sort(names)

	for i, f := range candidates {
		fileSet := l.archiver.newSet()
		if err := fileSet.Parse(f.GTIDSet); err != nil {
			return purgePlan{}, fmt.Errorf("parsing the gtid set of %s: %w", f.Name, err)
		}
		var by []string
		for _, name := range names {
			if !applied[name].Contains(fileSet) {
				by = append(by, name)
			}
		}
		if len(by) == 0 {
			continue
		}
		plan := purgePlan{held: purgeHold{file: f.Name, by: by}}
		if i > 0 {
			plan.to = f.Name
		}
		return plan, nil
	}
	return purgePlan{to: limit}, nil
}

// drain ships the closed binlogs a former primary stranded when it stopped being
// writable, and does nothing at all on any other instance.
//
// A primary that dies holds every transaction it committed since its last
// rotation in its still-open binlog, which the archiver cannot ship. Its
// successor normally re-logs that history (log_slave_updates) and the hole
// closes itself — but a re-cloned successor starts a virgin binlog and re-logs
// nothing, so those transactions survive only in the dead primary's data
// directory. Once its Pod restarts, mysqld closes that file and it becomes
// archivable; without this, it is stranded for good and recovery across the
// re-clone fails with ErrForkedTimeline against a hole no segment can bridge.
//
// Two conditions authorise the upload, and both are needed:
//
//   - The instance owns a segment (HasSegment): it archived while it was
//     primary, so the files it holds are its own history and not a replica's
//     redundant re-log of someone else's.
//   - Replication is streaming: the source accepted this instance's GTID
//     position, which under MASTER_USE_GTID=current_pos is its true frontier —
//     including everything it authored as primary. Acceptance means the source's
//     binlog contains that exact GTID (domain-server-sequence), so this
//     instance's history is an ancestor of the surviving timeline.
//
// The second is the safety property. A former primary whose final transactions
// never reached its successor is diverged: they sit on a dead branch, and the
// promoted server has since reused those sequence numbers for different
// transactions under its own server id. Archiving them would put two different
// transactions at the same sequence into the archive, and the MariaDB planner
// stitches segments by sequence — it could replay the dead branch and silently
// produce a database state that never existed. So authorisation comes only from
// a source that accepted us: MariaDB refuses a diverged replica with error 1236,
// and a diverged instance therefore never reaches a streaming state. No error is
// ever read as permission — a failure to connect leaves the tail unshipped, and
// recovery keeps failing closed, which is the correct outcome for a hole that
// genuinely cannot be filled.
//
// ArchivePending never touches the active log and is idempotent, so a drain
// ships exactly the closed, un-shipped files and converges. No flush (a
// non-writable server must not rotate) and no purge (the purge gate stays with
// the primary).
func (l *Loop) drain(ctx context.Context) {
	if l.replication == nil {
		return
	}
	mine, err := l.archiver.HasSegment(ctx)
	if err != nil {
		l.fail("checking archive segment", err)
		return
	}
	if !mine {
		return
	}

	streaming, err := l.replication.Streaming(ctx)
	if err != nil {
		l.fail("checking replication state", err)
		return
	}
	if !streaming {
		// Either still connecting, or the source rejected us as diverged. Both mean
		// our history is unproven, so the tail stays on disk.
		return
	}

	logs, err := l.reader.ListBinaryLogs(ctx)
	if err != nil {
		l.fail("listing binary logs", err)
		return
	}
	res, err := l.archiver.ArchivePending(ctx, logs)
	if err != nil {
		l.fail("draining stranded binary logs", err)
		return
	}
	if len(res.Archived) == 0 {
		return
	}

	l.mu.Lock()
	l.state.LastArchivedBinlog = res.LastArchivedBinlog
	l.state.LastArchivedGTID = res.LastArchivedGTID
	l.state.LastArchivedTime = res.LastArchivedTime
	l.state.PendingFiles = pendingAfter(logs, res.LastArchivedBinlog)
	l.mu.Unlock()
	l.logger.Info("Drained binary logs stranded by a demotion",
		"files", res.Archived,
		"lastArchivedGTID", res.LastArchivedGTID)
}

func (l *Loop) fail(action string, err error) {
	l.logger.Error(err, "Continuous archiving error", "action", action)
	l.mu.Lock()
	l.state.LastError = action + ": " + err.Error()
	l.state.LastErrorTime = time.Now()
	l.mu.Unlock()
}

// activeLog returns the active (currently-written) log, or a zero value.
func activeLog(logs []BinaryLog) BinaryLog {
	for _, l := range logs {
		if l.Active {
			return l
		}
	}
	return BinaryLog{}
}

// fileBefore returns the basename of the archivable log immediately preceding
// the named file, or "" if it is the earliest. Used to bound a safe purge.
func fileBefore(logs []BinaryLog, name string) string {
	prev := ""
	for _, l := range logs {
		if l.Name == name {
			return prev
		}
		prev = l.Name
	}
	return ""
}

// pendingAfter counts rotated logs not yet covered by the frontier.
func pendingAfter(logs []BinaryLog, frontier string) int {
	pending := 0
	seenFrontier := frontier == ""
	for _, l := range Archivable(logs) {
		if !seenFrontier {
			if l.Name == frontier {
				seenFrontier = true
			}
			continue
		}
		if l.Name != frontier {
			pending++
		}
	}
	return pending
}
