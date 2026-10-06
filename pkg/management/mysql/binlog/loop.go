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

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
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

// ClusterView is the part of the Cluster the drain gate reads: what the
// surviving timeline provably holds.
type ClusterView interface {
	// Primary returns status.currentPrimary and its recorded position
	// (status.gtidExecutedByInstance). ok is false when either is unknown.
	Primary() (name, position string, ok bool)
	// Diverged reports whether name is listed in status.divergedInstances.
	Diverged(name string) bool
	// Timeline returns status.mariadbTimeline; ok is false when there is none
	// (MySQL, replica clusters, or not observed yet).
	Timeline() (engine.MariaDBTimeline, bool)
}

type Loop struct {
	reader   *Reader
	archiver *Archiver
	logger   logr.Logger
	// replication authorises draining a demoted primary's un-shipped binlogs.
	replication ReplicationProbe
	// cluster is what the drain gate proves a stranded file canonical against.
	cluster ClusterView
	// instance is this instance's name, as the Cluster status lists it.
	instance string
	// mariadb selects the MariaDB drain gate (timeline verdict on top of
	// position containment).
	mariadb bool
	// forkChecked is set once a fork check ran in the current writable stretch
	// of this process; losing writability clears it, so every promotion checks.
	// Only the Run goroutine touches it.
	forkChecked bool

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
	// Forks are the fork records the archive index carried at this primary's
	// last read of it, ForkCheckedAt when it last ran a fork check (zero until
	// it has), and OldestSegmentPosition the lowest MariaDB segment position.
	Forks                 []SegmentFork
	ForkCheckedAt         time.Time
	OldestSegmentPosition string
	// DeferredFile is the stranded file the drain gate keeps deferring because
	// the surviving timeline does not provably hold it.
	DeferredFile string
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
	// Cluster is what the drain gate proves a stranded file canonical against;
	// without it the drain defers every file.
	Cluster ClusterView
	// Instance is this instance's name in the Cluster status.
	Instance string
	// MariaDB selects the MariaDB drain gate.
	MariaDB bool
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
		cluster:       opts.Cluster,
		instance:      opts.Instance,
		mariadb:       opts.MariaDB,
	}
}

// State returns a copy of the current archiving state.
func (l *Loop) State() State {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.state
	s.PurgeHeldBy = slices.Clone(s.PurgeHeldBy)
	s.Forks = slices.Clone(s.Forks)
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
		// Only the writable primary speaks for the archive's fork records.
		l.state.Forks = nil
		l.state.ForkCheckedAt = time.Time{}
		l.state.OldestSegmentPosition = ""
		l.mu.Unlock()
		l.forkChecked = false
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
	forks, forkErr := l.checkForks(ctx, res.ForkCheck)

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

		Forks:                 forks.Forks,
		ForkCheckedAt:         forks.ForkCheckedAt,
		OldestSegmentPosition: forks.OldestSegmentPosition,
	}
	if forkErr != nil {
		l.state.LastError = "checking archive forks: " + forkErr.Error()
		l.state.LastErrorTime = time.Now()
	}
	l.mu.Unlock()
	if len(res.Archived) > 0 {
		l.logger.Info("Archived binary logs",
			"files", res.Archived,
			"lastArchivedGTID", res.LastArchivedGTID)
	}
}

// checkForks completes this pass's fork check and returns the fork fields the
// state should carry. The pass's own index write already checked when it
// archived something; otherwise the first writable pass of a stretch runs the
// check on its own, so a promotion, failback or restart does not wait for the
// next rotation. Fields the pass did not refresh carry over from the last one.
func (l *Loop) checkForks(ctx context.Context, report *ForkReport) (State, error) {
	l.mu.Lock()
	out := State{
		Forks:                 l.state.Forks,
		ForkCheckedAt:         l.state.ForkCheckedAt,
		OldestSegmentPosition: l.state.OldestSegmentPosition,
	}
	l.mu.Unlock()
	if !l.archiver.ChecksForks() {
		return out, nil
	}
	if !l.forkChecked && (report == nil || !report.Checked) {
		r, err := l.archiver.CheckForks(ctx)
		if err != nil {
			l.logger.Error(err, "Could not check the archive for forks")
			return out, err
		}
		report = &r
	}
	if report == nil {
		return out, nil
	}
	if report.Err != nil {
		l.logger.Error(report.Err, "Could not check the archive for forks")
		return out, report.Err
	}
	out.Forks = report.Forks
	out.OldestSegmentPosition = report.OldestSegmentPosition
	if report.Checked {
		l.forkChecked = true
		out.ForkCheckedAt = report.CheckedAt
		if len(report.Forks) > 0 {
			l.logger.V(1).Info("Checked the archive for forks", "forks", len(report.Forks))
		}
	}
	return out, nil
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
// Its purpose is filling canonical holes: transactions the surviving timeline
// executed but never logged. A successor provisioned by clone holds the clone
// point's history in its executed set but not in its binlog. If the old primary
// dies before rotating, the transactions between its last rotation and the
// clone point survive only in its still-open binlog, which the archiver cannot
// ship. Once its Pod restarts, mysqld closes that file and it becomes
// archivable; without the drain, a recovery across the re-clone fails with
// ErrForkedTimeline against a hole no segment can bridge.
//
// Four gates authorise an upload, all required:
//
//  1. The instance owns a segment (HasSegment): it archived while it was
//     primary, so the files it holds are its own history and not a replica's
//     redundant re-log of someone else's.
//  2. Replication is streaming: the source accepted this instance's GTID
//     position. On MariaDB the MASTER_USE_GTID=current_pos handshake refuses a
//     diverged instance (1236), so this is a strong signal; MySQL's
//     AUTO_POSITION accepts errant transactions, so there it is a weak one.
//  3. The instance is not listed in status.divergedInstances.
//  4. Each file is on the surviving timeline: its GTIDs are in the current
//     primary's recorded position, and on MariaDB (whose positions compare by
//     sequence alone, so a dead 0-1-219 looks contained in 0-2-300) every
//     domain's last GTID is on the primary timeline too.
//
// Gate 4 is the safety property. A former primary whose final transactions
// never reached its successor is diverged: they sit on a dead branch the
// surviving timeline disowned, and archiving them would let recovery resurrect
// a state the cluster never served. The current primary's position is the
// surviving timeline, and a stale record only understates it, so a file that
// passes is canonical; a disowned file never passes, even in the window before
// the operator marks the instance diverged. A canonical tail passes within one
// position refresh. A file that fails is deferred, not shipped and not
// recorded, and the pass stops there so the frontier never moves past it. No
// error is read as permission.
//
// DrainPending never touches the active log and is idempotent, so a drain
// ships exactly the closed, un-shipped files and converges. No flush (a
// non-writable server must not rotate), no purge (the purge gate stays with the
// primary) and no fork check (only the writable primary is an authority).
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
	if l.cluster != nil && l.cluster.Diverged(l.instance) {
		return
	}

	logs, err := l.reader.ListBinaryLogs(ctx)
	if err != nil {
		l.fail("listing binary logs", err)
		return
	}
	res, err := l.archiver.DrainPending(ctx, logs, l.onSurvivingTimeline)
	if err != nil {
		l.fail("draining stranded binary logs", err)
		return
	}

	l.mu.Lock()
	l.state.LastArchivedBinlog = res.LastArchivedBinlog
	l.state.LastArchivedGTID = res.LastArchivedGTID
	if !res.LastArchivedTime.IsZero() {
		l.state.LastArchivedTime = res.LastArchivedTime
	}
	l.state.PendingFiles = pendingAfter(logs, res.LastArchivedBinlog)
	l.state.DeferredFile = res.Deferred
	l.mu.Unlock()
	if len(res.Archived) > 0 {
		l.logger.Info("Drained binary logs stranded by a demotion",
			"files", res.Archived,
			"lastArchivedGTID", res.LastArchivedGTID)
	}
}

// onSurvivingTimeline is drain gate 4: a stranded file may enter the archive
// only when the surviving timeline provably holds it. Anything unknown defers.
func (l *Loop) onSurvivingTimeline(_ string, scan ScanResult) (bool, error) {
	if l.cluster == nil {
		return false, nil
	}
	primary, position, ok := l.cluster.Primary()
	if !ok || primary == "" || primary == l.instance || position == "" {
		return false, nil
	}
	if scan.GTIDSet == "" {
		return true, nil
	}
	if !l.mariadb {
		return replication.GTIDContains(position, scan.GTIDSet)
	}
	timeline, ok := l.cluster.Timeline()
	if !ok {
		return false, nil
	}
	file, err := engine.ParseMariaDBPosition(scan.GTIDSet)
	if err != nil {
		return false, err
	}
	held, err := engine.ParseMariaDBPosition(position)
	if err != nil {
		return false, err
	}
	reached := make(map[uint32]uint64, len(held))
	for _, g := range held {
		reached[g.Domain] = g.Seq
	}
	for _, g := range file {
		if on, known := timeline.Verdict(g); !on || !known {
			return false, nil
		}
		if reached[g.Domain] < g.Seq {
			return false, nil
		}
	}
	return true, nil
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
