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
func PlanReplayWithModel(
	idx *objectstore.ArchiveIndex, anchorGTID string, target RecoveryTarget, model MariaDBGTIDModel,
) (ReplayPlan, error) {
	newSet := func() gtidOps { return newMariadbGTIDSet(model) }
	return planReplayWithOps(idx, anchorGTID, target, newSet)
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
