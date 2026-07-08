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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// stopDatetimeLayout is the timestamp format mysqlbinlog --stop-datetime expects
// ("YYYY-MM-DD HH:MM:SS", server-local). Recovery targets are RFC3339 UTC; the
// caller is responsible for the instant, we only format it.
const stopDatetimeLayout = "2006-01-02 15:04:05"

var (
	// ErrTargetBeforeBackup is returned when the requested GTID target is older
	// than the base backup's anchor GTID: the base backup already contains
	// transactions the target excludes, so the point is unreachable by replay.
	ErrTargetBeforeBackup = errors.New("binlog: recovery target is older than the base backup")
	// ErrTargetBeyondArchive is returned when the requested GTID target is not
	// fully covered by the archive: there is no binlog to replay it from.
	ErrTargetBeyondArchive = errors.New("binlog: recovery target is beyond archive coverage")
	// ErrForkedTimeline is returned when the archive index is incoherent: its
	// declared cumulative coverage is not reconstructable from the per-segment
	// GTID sets plus the anchor, i.e. a segment is missing or the timeline forked
	// rather than nesting. Recovery refuses to straddle it.
	ErrForkedTimeline = errors.New("binlog: archive timeline is forked or has a gap")
)

// gtidOps abstracts GTID-set operations needed for replay planning. MySQL uses
// replication.GTIDSet (UUID-keyed intervals); MariaDB uses string-based
// domain-server-seq operations backed by the engine's GTIDModel.
type gtidOps interface {
	Parse(raw string) error
	Contains(other gtidOps) bool
	Union(other gtidOps)
	IsEmpty() bool
	String() string
	Clone() gtidOps
}

// mysqlGTIDSet wraps replication.GTIDSet to satisfy gtidOps.
type mysqlGTIDSet struct {
	s replication.GTIDSet
}

func (m *mysqlGTIDSet) Parse(raw string) error {
	if raw == "" {
		m.s = replication.GTIDSet{}
		return nil
	}
	parsed, err := replication.ParseGTIDSet(raw)
	if err != nil {
		return err
	}
	m.s = parsed
	return nil
}

func (m *mysqlGTIDSet) Contains(other gtidOps) bool {
	o, ok := other.(*mysqlGTIDSet)
	if !ok {
		return false
	}
	if o.s.IsEmpty() {
		return true
	}
	return m.s.Contains(o.s)
}

func (m *mysqlGTIDSet) Union(other gtidOps) {
	o, ok := other.(*mysqlGTIDSet)
	if !ok {
		return
	}
	m.s = m.s.Clone()
	m.s.Union(o.s)
}

func (m *mysqlGTIDSet) IsEmpty() bool { return m.s.IsEmpty() }

func (m *mysqlGTIDSet) String() string { return m.s.String() }

func (m *mysqlGTIDSet) Clone() gtidOps {
	return &mysqlGTIDSet{s: m.s.Clone()}
}

// mariadbGTIDSet implements gtidOps via string-based GTIDModel operations.
// MariaDB GTID format is "domain-server-seq,..." triples; Contains/Union use the
// engine's GTID model, and the ordering check is approximated via Canonical+Compare
// (domain reordering, seq comparison).
type mariadbGTIDSet struct {
	raw   string
	model MariaDBGTIDModel
}

// MariaDBGTIDModel is the subset of engine.GTIDModel the replay planner needs.
// It is declared here to avoid importing engine into the binlog package.
type MariaDBGTIDModel interface {
	Contains(superset, subset string) (bool, error)
	IsEmpty(raw string) (bool, error)
	Canonical(raw string) (string, error)
}

func (m *mariadbGTIDSet) Parse(raw string) error {
	// Validate eagerly so malformed anchor/segment/target positions fail loudly
	// at plan time, matching the MySQL path (Contains/IsEmpty swallow errors).
	if raw != "" {
		if _, err := m.model.Canonical(raw); err != nil {
			return err
		}
	}
	m.raw = raw
	return nil
}

func (m *mariadbGTIDSet) Contains(other gtidOps) bool {
	o, ok := other.(*mariadbGTIDSet)
	if !ok {
		return false
	}
	if o.raw == "" {
		return true
	}
	if m.raw == "" {
		return false
	}
	ok, _ = m.model.Contains(m.raw, o.raw)
	return ok
}

func (m *mariadbGTIDSet) Union(other gtidOps) {
	o, ok := other.(*mariadbGTIDSet)
	if !ok {
		return
	}
	if m.raw == "" {
		m.raw = o.raw
		return
	}
	if o.raw == "" {
		return
	}
	m.raw = m.unionStrings(m.raw, o.raw)
}

func (m *mariadbGTIDSet) unionStrings(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	return strings.TrimSpace(a + "," + b)
}

func (m *mariadbGTIDSet) IsEmpty() bool {
	isEmpty, _ := m.model.IsEmpty(m.raw)
	return isEmpty
}

func (m *mariadbGTIDSet) String() string {
	if m.raw == "" {
		return ""
	}
	canonical, err := m.model.Canonical(m.raw)
	if err != nil {
		return m.raw
	}
	return canonical
}

func (m *mariadbGTIDSet) Clone() gtidOps {
	return &mariadbGTIDSet{raw: m.raw, model: m.model}
}

func newMariadbGTIDSet(model MariaDBGTIDModel) gtidOps {
	return &mariadbGTIDSet{model: model}
}

// NewMariadbGTIDSet returns a gtidOps backed by a MariaDBGTIDModel. It is
// exported for use by the instance package when wiring the archiver.
func NewMariadbGTIDSet(model MariaDBGTIDModel) gtidOps {
	return newMariadbGTIDSet(model)
}

// GTIDOps is the exported gtidOps interface alias for external constructors.
type GTIDOps = gtidOps

func newMysqlGTIDSet() gtidOps {
	return &mysqlGTIDSet{}
}

// RecoveryTarget is the resolved point-in-time recovery bound. At most one of
// Time/GTID is set; an empty target (or Immediate) means replay to the latest
// archived point.
type RecoveryTarget struct {
	// Time, when non-nil, stops replay at the first event at or after it.
	Time *time.Time
	// GTID, when set, is the exact GTID set to recover up to (inclusive).
	GTID string
	// Immediate stops as soon as a consistent state is reached; equivalent to an
	// empty target for our purposes (replay nothing past the base backup unless a
	// later bound is needed). It is accepted for API symmetry.
	Immediate bool
}

// ReplaySegment is one server-UUID segment's ordered file list to download and
// replay, in timeline order.
type ReplaySegment struct {
	// ServerUUID partitions the segment's object keys.
	ServerUUID string
	// Files are the binlog basenames to replay, in sequence order.
	Files []string
}

// ReplayPlan is the ordered set of segments to download and the GTID/time bounds
// to hand mysqlbinlog. It de-duplicates against the base backup via ExcludeGTIDs
// and bounds the upper end via IncludeGTIDs (targetGTID) or StopDatetime
// (targetTime); both empty means replay everything from the anchor to latest.
//
// For MariaDB, ExcludeGTIDs and IncludeGTIDs are empty and replay instead uses
// AnchorFile/StartPosition (the binlog file and byte offset from the backup's
// binlog-info file) to skip already-applied transactions.
type ReplayPlan struct {
	// Segments are the timeline segments to replay, oldest first.
	Segments []ReplaySegment
	// ExcludeGTIDs drops transactions already present in the base backup (and the
	// overlap successors re-emit). Passed as mysqlbinlog --exclude-gtids.
	ExcludeGTIDs string
	// IncludeGTIDs bounds a targetGTID recovery to exactly that set.
	IncludeGTIDs string
	// StopDatetime bounds a targetTime recovery ("YYYY-MM-DD HH:MM:SS").
	StopDatetime string
	// AnchorFile is the binlog basename from the backup's binlog-info file; used
	// for MariaDB positional replay to identify the starting file in the first
	// segment.
	AnchorFile string
	// StartPosition is the byte offset to start replaying from in AnchorFile.
	StartPosition int64
	// AnchorServerUUID is the archive identity of the incarnation the base backup was
	// taken from, recorded in the backup metadata. It disambiguates AnchorFile when a
	// re-clone/failover left several incarnations numbering their binlogs from 000001,
	// so the anchor's bare filename matches more than one downloaded segment. Empty
	// when unknown (legacy backups), which falls back to fail-closed detection.
	AnchorServerUUID string

	// MariaDBPositional selects the byte-offset-bounded MariaDB replay: because
	// mariadb-binlog cannot filter by GTID, a targetGTID recovery is executed as
	// positionally-bounded chunks computed from the domain's anchor/target
	// sequence numbers (see PlanMariadbPositional). The fields below are only
	// meaningful when it is true.
	MariaDBPositional bool
	// MariaDBDomain is the single replication domain the target/anchor live in.
	MariaDBDomain uint32
	// MariaDBAnchorSeq is the sequence the base backup already contains (replay
	// starts just after it); zero when the backup predates the domain.
	MariaDBAnchorSeq uint64
	// MariaDBTargetSeq is the sequence to recover up to, inclusive.
	MariaDBTargetSeq uint64
}

// PlanReplay turns the cluster archive index, the base backup's anchor GTID, and
// a recovery target into an ordered download+replay plan. It is pure and
// unit-testable; the actual download/replay is the caller's job.
//
// The anchor is the GTID set the restored base backup already contains
// (xtrabackup_binlog_info). Segments fully covered by the running frontier are
// skipped (nothing new to apply). The plan never advances past a target the
// archive cannot satisfy: it returns ErrTargetBeforeBackup / ErrTargetBeyondArchive
// / ErrForkedTimeline instead.
//
// PlanReplay uses MySQL GTID semantics; for MariaDB use PlanReplayWithModel.
func PlanReplay(idx *objectstore.ArchiveIndex, anchorGTID string, target RecoveryTarget) (ReplayPlan, error) {
	return planReplayWithOps(idx, anchorGTID, target, newMysqlGTIDSet)
}

// PlanReplayWithModel is PlanReplay using the given MariaDB GTID model for domain-
// server-seq set operations instead of MySQL UUID-keyed interval arithmetic.
// The MariaDB scanner (binlog.scanMariaDB) populates segment GTID sets, so the
// strict frontier validation normally applies. It falls back to a positional
// plan only when the segment GTID sets are empty — an archive written before
// GTID extraction existed, or files that carry no GTID events — in which case
// the replay bounds are positional and stop at the anchor/target anyway.
func PlanReplayWithModel(
	idx *objectstore.ArchiveIndex, anchorGTID string, target RecoveryTarget, model MariaDBGTIDModel,
) (ReplayPlan, error) {
	newSet := func() gtidOps { return newMariadbGTIDSet(model) }
	plan, err := planReplayWithOps(idx, anchorGTID, target, newSet)
	if err != nil && errors.Is(err, ErrTargetBeyondArchive) {
		// Segment GTID sets are empty (pre-extraction archive or GTID-less
		// files), so the frontier never advances beyond the anchor. Build a
		// best-effort plan that replays every available file; the positional
		// replay will naturally stop at the anchor/target bound.
		plan, err = planReplayWithoutFrontier(idx, anchorGTID, target, newSet)
	}
	return plan, err
}

type newGTIDSetFunc func() gtidOps

func planReplayWithOps(
	idx *objectstore.ArchiveIndex, anchorGTID string, target RecoveryTarget, newSet newGTIDSetFunc,
) (ReplayPlan, error) {
	if idx == nil {
		return ReplayPlan{}, fmt.Errorf("binlog: archive index is required")
	}

	anchor := newSet()
	if err := anchor.Parse(anchorGTID); err != nil {
		return ReplayPlan{}, fmt.Errorf("binlog: parsing anchor GTID: %w", err)
	}

	frontier := anchor.Clone()

	plan := ReplayPlan{ExcludeGTIDs: anchor.String()}
	for i := range idx.Segments {
		seg := &idx.Segments[i]
		segSet := newSet()
		if err := segSet.Parse(seg.GTIDSet); err != nil {
			return ReplayPlan{}, fmt.Errorf("binlog: parsing segment %q GTID set: %w", seg.ServerUUID, err)
		}
		if !segSet.IsEmpty() && frontier.Contains(segSet) {
			continue
		}
		plan.Segments = append(plan.Segments, ReplaySegment{
			ServerUUID: seg.ServerUUID,
			Files:      append([]string(nil), seg.Binlogs...),
		})
		frontier.Union(segSet)
	}

	if idx.CoveredGTIDSet != "" {
		covered := newSet()
		if err := covered.Parse(idx.CoveredGTIDSet); err != nil {
			return ReplayPlan{}, fmt.Errorf("binlog: parsing index coverage: %w", err)
		}
		if !frontier.Contains(covered) {
			return ReplayPlan{}, ErrForkedTimeline
		}
	}

	if err := applyTargetWithOps(&plan, frontier, anchor, target, newSet); err != nil {
		return ReplayPlan{}, err
	}
	return plan, nil
}

// planReplayWithoutFrontier builds a replay plan without the strict GTID
// frontier validation, used when the archiver cannot extract GTIDs (e.g.
// MariaDB). Every segment with files is included; the replay bounds are
// positional and the actual stop is enforced by StartPosition/StopDatetime.
func planReplayWithoutFrontier(
	idx *objectstore.ArchiveIndex, anchorGTID string, target RecoveryTarget, newSet newGTIDSetFunc,
) (ReplayPlan, error) {
	anchor := newSet()
	if err := anchor.Parse(anchorGTID); err != nil {
		return ReplayPlan{}, fmt.Errorf("binlog: parsing anchor GTID: %w", err)
	}

	plan := ReplayPlan{ExcludeGTIDs: anchor.String()}
	for i := range idx.Segments {
		seg := &idx.Segments[i]
		if len(seg.Binlogs) == 0 {
			continue
		}
		plan.Segments = append(plan.Segments, ReplaySegment{
			ServerUUID: seg.ServerUUID,
			Files:      append([]string(nil), seg.Binlogs...),
		})
	}

	switch {
	case target.GTID != "":
		want := newSet()
		if err := want.Parse(target.GTID); err != nil {
			return ReplayPlan{}, fmt.Errorf("binlog: parsing target GTID: %w", err)
		}
		if !want.Contains(anchor) {
			return ReplayPlan{}, ErrTargetBeforeBackup
		}
		plan.IncludeGTIDs = want.String()
	case target.Time != nil:
		plan.StopDatetime = target.Time.UTC().Format(stopDatetimeLayout)
	}
	return plan, nil
}

// SelectMariadbSegments chooses the minimal set of index segments whose per-domain
// sequence ranges cover (anchorSeq, targetSeq], so recovery downloads only the
// binlogs it needs instead of every segment. It replaces the MySQL frontier-
// containment check on the MariaDB positional path, where a single-point GTID set
// cannot express the gaps a re-init clone leaves in one server's history.
//
// Each segment's range in the target domain is [start, end], where
// start = MariaSeqForDomain(seg.StartGTIDSet) and end = MariaSeqForDomain(seg.GTIDSet).
// A single incarnation is contiguous per domain, so one interval per segment suffices.
// The cover is greedy: from the highest sequence covered so far, extend with the
// segment that reaches furthest among those that start no later than one past it
// (contiguity, so the union stays gap-free). The first pick is lenient — if no
// segment starts at or below anchorSeq+1, the archive simply begins later than the
// anchor, so the earliest-starting segment is taken (mirrors PlanMariadbPositional's
// leading-edge leniency).
//
// A hole the union cannot bridge is fatal: ErrForkedTimeline if some segment reaches
// the target but a gap precedes it, ErrTargetBeyondArchive if no segment reaches the
// target at all. Segments with no transactions in the domain (or missing StartGTIDSet,
// treated as genesis for back-compat with pre-range archives) are handled leniently.
func SelectMariadbSegments(
	segments []objectstore.ArchiveSegment, domain uint32, anchorSeq, targetSeq uint64,
) ([]ReplaySegment, error) {
	if targetSeq <= anchorSeq {
		return nil, nil
	}

	type interval struct {
		seg        *objectstore.ArchiveSegment
		start, end uint64
	}
	var ivs []interval
	var maxEnd uint64
	for i := range segments {
		end := MariaSeqForDomain(segments[i].GTIDSet, domain)
		if end == 0 {
			continue // nothing archived in this domain
		}
		start := MariaSeqForDomain(segments[i].StartGTIDSet, domain)
		if start == 0 {
			start = 1 // unknown start (pre-range archive): treat as from genesis
		}
		ivs = append(ivs, interval{seg: &segments[i], start: start, end: end})
		if end > maxEnd {
			maxEnd = end
		}
	}
	if maxEnd < targetSeq {
		return nil, ErrTargetBeyondArchive
	}

	used := make([]bool, len(ivs))
	current := anchorSeq
	var out []ReplaySegment
	for current < targetSeq {
		best := -1
		for i, v := range ivs {
			if used[i] || v.end <= current || v.start > current+1 {
				continue
			}
			if best < 0 || v.end > ivs[best].end {
				best = i
			}
		}
		if best < 0 && len(out) == 0 {
			// Leading edge: no segment starts at or below the anchor. The archive
			// begins later than the backup; take the earliest-starting segment.
			for i, v := range ivs {
				if used[i] || v.end <= current {
					continue
				}
				if best < 0 || v.start < ivs[best].start {
					best = i
				}
			}
		}
		if best < 0 {
			return nil, ErrForkedTimeline
		}
		used[best] = true
		current = ivs[best].end
		out = append(out, ReplaySegment{
			ServerUUID: ivs[best].seg.ServerUUID,
			Files:      append([]string(nil), ivs[best].seg.Binlogs...),
		})
	}
	return out, nil
}

// ReplayChunk is one bounded mariadb-binlog invocation: an ordered file list with
// optional positional bounds. StartPosition applies to the first file only;
// StopPosition (when set) requires a single file (mysqlbinlog's constraint).
type ReplayChunk struct {
	Files         []string
	StartPosition int64
	StopPosition  int64
}

// SingleDomainMariaGTID parses a MariaDB GTID position that references exactly one
// replication domain, returning its domain id and sequence number. Positional PITR
// bounds a single linear domain timeline at one byte offset, so a multi-domain
// target — which would need per-domain offsets in one interleaved stream — is
// rejected. An empty position returns ok=false with no error (no bound to apply).
func SingleDomainMariaGTID(pos string) (domain uint32, seq uint64, ok bool, err error) {
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\n', '\r', '\t':
			return -1
		default:
			return r
		}
	}, pos)
	if cleaned == "" {
		return 0, 0, false, nil
	}
	triples := strings.Split(cleaned, ",")
	if len(triples) != 1 {
		return 0, 0, false, fmt.Errorf("binlog: multi-domain MariaDB GTID %q is not supported for positional recovery", pos)
	}
	parts := strings.Split(triples[0], "-")
	if len(parts) != 3 {
		return 0, 0, false, fmt.Errorf("binlog: invalid MariaDB GTID %q: want domain-server-seq", pos)
	}
	d, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, false, fmt.Errorf("binlog: invalid MariaDB GTID domain in %q: %w", pos, err)
	}
	s, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("binlog: invalid MariaDB GTID sequence in %q: %w", pos, err)
	}
	return uint32(d), s, true, nil
}

// MariaSeqForDomain returns the sequence number a MariaDB GTID position has
// reached in the given domain, or 0 if the domain is absent or the position is
// empty/unparseable. It is lenient by design: the anchor may be empty (backup at
// genesis) or, in principle, multi-domain, and a missing domain simply means the
// backup contains nothing in it yet.
func MariaSeqForDomain(pos string, domain uint32) uint64 {
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\n', '\r', '\t':
			return -1
		default:
			return r
		}
	}, pos)
	if cleaned == "" {
		return 0
	}
	for triple := range strings.SplitSeq(cleaned, ",") {
		parts := strings.Split(triple, "-")
		if len(parts) != 3 {
			continue
		}
		d, err := strconv.ParseUint(parts[0], 10, 32)
		if err != nil || uint32(d) != domain {
			continue
		}
		if seq, err := strconv.ParseUint(parts[2], 10, 64); err == nil {
			return seq
		}
	}
	return 0
}

// AnchorSeqFromBoundaries derives the domain sequence a base backup already
// contains from a byte-position anchor: the highest sequence whose transaction
// begins before pos in the anchor file's boundaries. A transaction whose GTID
// event starts before the backup's recorded binlog position committed before the
// backup, so it is captured by the physical copy; pos itself is a transaction
// boundary (the offset after the last committed transaction), so StartPos == pos
// is the first transaction past the backup and must be excluded.
//
// This is the fallback for MariaDB backups whose mariadb_backup_binlog_info
// carries only file+position and no GTID column (as 10.11 mariabackup writes):
// without it the positional planner treats the empty GTID anchor as sequence 0
// and rewinds replay to genesis, re-applying transactions already in the backup.
func AnchorSeqFromBoundaries(boundaries []TxnBoundary, domain uint32, pos int64) uint64 {
	var seq uint64
	for _, b := range boundaries {
		if b.Domain == domain && b.StartPos < pos && b.Seq > seq {
			seq = b.Seq
		}
	}
	return seq
}

// PlanMariadbPositional turns per-file transaction boundaries into the ordered,
// positionally-bounded chunks that replay the domain's transactions with sequence
// in (anchorSeq, targetSeq] exactly once. files and boundaries are parallel, in
// timeline order.
//
// mariadb-binlog cannot filter by GTID, so replay is bounded by byte offsets: a
// chunk's first file starts at the offset of the first transaction with seq past
// the highest sequence already applied, and the target file stops at the offset of
// the first transaction with seq > targetSeq (so the target is included and nothing
// after it is).
//
// The wrinkle is failover. With log_slave_updates the promoted server re-logs the
// transactions it replicated under their original server_id before appending its
// own, so across a failover two segments carry overlapping domain sequence ranges
// (e.g. server 1 archives 0-1-15..0-1-26 and server 2's binlog re-logs 0-1-15..26
// then continues 0-2-27..). Concatenating whole segments would feed mariadb-binlog
// a non-monotonic stream ("Found out of order GTID"). To avoid that we walk the
// files tracking the highest sequence applied so far and, for each file:
//   - skip it entirely when all its transactions are already applied (the re-logged
//     overlap prefix, or files at/below the anchor);
//   - start a fresh start-position-bounded chunk when it overlaps (its first new
//     transaction is mid-file), because --start-position only applies to a chunk's
//     first file;
//   - otherwise append it to the current chunk (replayed whole, in one invocation).
//
// The file carrying the target is always its own chunk bounded by --stop-position
// (mysqlbinlog requires a single file for a stop offset), unless the target is the
// last archived transaction, in which case it is replayed to EOF with no stop bound.
func PlanMariadbPositional(
	files []string, boundaries [][]TxnBoundary, domain uint32, anchorSeq, targetSeq uint64,
) ([]ReplayChunk, error) {
	if len(files) != len(boundaries) {
		return nil, fmt.Errorf("binlog: files/boundaries length mismatch (%d vs %d)", len(files), len(boundaries))
	}

	maxSeq, hasAny := maxSeqInDomain(boundaries, domain)
	if !hasAny || maxSeq < targetSeq {
		return nil, ErrTargetBeyondArchive
	}
	if targetSeq < anchorSeq {
		// The target predates the base backup: unreachable by forward replay.
		return nil, ErrTargetBeforeBackup
	}

	// Replay must proceed in ascending sequence order, but the caller passes files
	// grouped by segment (server). Across a failover — and especially a re-init clone,
	// where one server's segment covers a gap in another's — segment order is not
	// sequence order. Sort a working copy by each file's first domain sequence so
	// contiguous runs from different segments stitch together; files with no
	// transactions in this domain sort last (the loop skips them anyway). For inputs
	// already in sequence order (the single-server case) this is a no-op.
	files, boundaries = sortByDomainSeq(files, boundaries, domain)

	applied := anchorSeq
	firstReplayed := true
	var chunks []ReplayChunk
	var cur *ReplayChunk
	flush := func() {
		if cur != nil {
			chunks = append(chunks, *cur)
			cur = nil
		}
	}

	for i := range files {
		minSeq, fileMax, found := domainFileStats(boundaries[i], domain)
		if !found || fileMax <= applied {
			// No transactions this replay hasn't already applied: the anchor covers
			// them, or an earlier segment re-logs the same sequences. Skipping keeps
			// the stream monotonic without breaking coalescing of the files around it.
			continue
		}

		overlap := minSeq <= applied // re-logs sequences already applied (failover re-log)

		// A hole between runs: this file's first new sequence is more than one past
		// what we've applied, and it is not the leading edge (nothing replayed yet,
		// where starting above the anchor just means the archive begins later). No
		// downloaded file supplies the missing sequences, so replay cannot proceed
		// monotonically. B2 (segment selection) should have caught this before
		// download; fail closed here as a backstop.
		if !firstReplayed && !overlap && minSeq > applied+1 {
			return nil, ErrForkedTimeline
		}

		if fileMax >= targetSeq {
			// This file carries the target. It is its own chunk: a stop offset needs a
			// single file, and even without a stop bound an overlapping target file
			// needs its own start offset.
			stopPos, stopHere := firstAfterInFile(boundaries[i], domain, targetSeq)
			if !stopHere && cur != nil && !overlap {
				// Target is this file's last transaction and it continues the current
				// coalescing chunk with no overlap: replay it whole in the same invocation.
				cur.Files = append(cur.Files, files[i])
				flush()
				return chunks, nil
			}
			flush()
			c := ReplayChunk{Files: []string{files[i]}}
			if overlap || firstReplayed {
				c.StartPosition, _ = firstAfterInFile(boundaries[i], domain, applied)
			}
			if stopHere {
				c.StopPosition = stopPos
			}
			chunks = append(chunks, c)
			return chunks, nil
		}

		// Whole file is new transactions below the target: replay it fully.
		if cur != nil && !overlap {
			cur.Files = append(cur.Files, files[i])
		} else {
			flush()
			startPos, _ := firstAfterInFile(boundaries[i], domain, applied)
			cur = &ReplayChunk{Files: []string{files[i]}, StartPosition: startPos}
		}
		applied = fileMax
		firstReplayed = false
	}

	// The target's file was never reached even though maxSeq >= targetSeq: the only
	// way here is that every file was skipped as already-applied, i.e. the base
	// backup already covers the target.
	flush()
	return chunks, nil
}

// sortByDomainSeq returns copies of the parallel files/boundaries slices reordered
// by each file's first (minimum) sequence in the domain, stably. Files carrying no
// transaction in the domain sort last. The inputs are left unmodified.
func sortByDomainSeq(files []string, boundaries [][]TxnBoundary, domain uint32) ([]string, [][]TxnBoundary) {
	order := make([]int, len(files))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		minA, _, okA := domainFileStats(boundaries[order[a]], domain)
		minB, _, okB := domainFileStats(boundaries[order[b]], domain)
		if okA != okB {
			return okA // files with domain transactions before those without
		}
		if !okA {
			return false
		}
		return minA < minB
	})
	sf := make([]string, len(files))
	sb := make([][]TxnBoundary, len(files))
	for newIdx, oldIdx := range order {
		sf[newIdx] = files[oldIdx]
		sb[newIdx] = boundaries[oldIdx]
	}
	return sf, sb
}

// domainFileStats returns the minimum and maximum sequence a single file's
// boundaries carry for domain, and whether it carries any.
func domainFileStats(list []TxnBoundary, domain uint32) (minSeq, maxSeq uint64, found bool) {
	for _, b := range list {
		if b.Domain != domain {
			continue
		}
		if !found || b.Seq < minSeq {
			minSeq = b.Seq
		}
		if !found || b.Seq > maxSeq {
			maxSeq = b.Seq
		}
		found = true
	}
	return minSeq, maxSeq, found
}

// firstAfterInFile returns the start offset of the first transaction in one file
// (in file order) whose domain sequence exceeds seq.
func firstAfterInFile(list []TxnBoundary, domain uint32, seq uint64) (int64, bool) {
	for _, b := range list {
		if b.Domain == domain && b.Seq > seq {
			return b.StartPos, true
		}
	}
	return 0, false
}

// maxSeqInDomain returns the highest sequence archived for the domain.
func maxSeqInDomain(boundaries [][]TxnBoundary, domain uint32) (uint64, bool) {
	var max uint64
	var found bool
	for _, list := range boundaries {
		for _, b := range list {
			if b.Domain == domain && (!found || b.Seq > max) {
				max = b.Seq
				found = true
			}
		}
	}
	return max, found
}

func applyTargetWithOps(
	plan *ReplayPlan, frontier, anchor gtidOps, target RecoveryTarget, newSet newGTIDSetFunc,
) error {
	switch {
	case target.GTID != "":
		want := newSet()
		if err := want.Parse(target.GTID); err != nil {
			return fmt.Errorf("binlog: parsing target GTID: %w", err)
		}
		if !want.Contains(anchor) {
			return ErrTargetBeforeBackup
		}
		if !frontier.Contains(want) {
			return ErrTargetBeyondArchive
		}
		plan.IncludeGTIDs = want.String()
	case target.Time != nil:
		plan.StopDatetime = target.Time.UTC().Format(stopDatetimeLayout)
	}
	return nil
}
