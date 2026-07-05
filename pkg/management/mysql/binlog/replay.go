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
	for _, triple := range strings.Split(cleaned, ",") {
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

// PlanMariadbPositional turns per-file transaction boundaries into the ordered,
// positionally-bounded chunks that replay the domain's transactions with sequence
// in (anchorSeq, targetSeq]. files and boundaries are parallel, in timeline order.
//
// mariadb-binlog cannot filter by GTID, so replay is bounded by byte offsets:
//   - start = the offset of the first transaction with seq > anchorSeq (skips what
//     the base backup already contains);
//   - stop  = the offset of the first transaction with seq > targetSeq (so replay
//     includes the target transaction and nothing after it).
//
// Because a stop offset requires a single file, the plan is at most two chunks: the
// files up to the target file (replayed fully from the start offset), then the
// target file bounded by the stop offset. When the target is the last archived
// transaction there is no stop bound and a single start-bounded chunk is returned.
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

	startFile, startPos, startFound := locateFirstAfter(boundaries, domain, anchorSeq)
	if !startFound {
		// Nothing past the anchor is archived: the base backup already covers it.
		return nil, nil
	}

	stopFile, stopPos, stopFound := locateFirstAfter(boundaries, domain, targetSeq)
	if !stopFound {
		// Target is the last archived transaction: replay from the start to the end
		// with no upper bound.
		return []ReplayChunk{{
			Files:         append([]string(nil), files[startFile:]...),
			StartPosition: startPos,
		}}, nil
	}

	if stopFile < startFile || (stopFile == startFile && stopPos <= startPos) {
		return nil, ErrTargetBeforeBackup
	}

	if stopFile == startFile {
		return []ReplayChunk{{
			Files:         []string{files[startFile]},
			StartPosition: startPos,
			StopPosition:  stopPos,
		}}, nil
	}

	return []ReplayChunk{
		{Files: append([]string(nil), files[startFile:stopFile]...), StartPosition: startPos},
		{Files: []string{files[stopFile]}, StopPosition: stopPos},
	}, nil
}

// locateFirstAfter returns the file index and start offset of the first
// transaction (in domain, timeline order) whose sequence exceeds seq.
func locateFirstAfter(boundaries [][]TxnBoundary, domain uint32, seq uint64) (int, int64, bool) {
	for i, list := range boundaries {
		for _, b := range list {
			if b.Domain == domain && b.Seq > seq {
				return i, b.StartPos, true
			}
		}
	}
	return 0, 0, false
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
