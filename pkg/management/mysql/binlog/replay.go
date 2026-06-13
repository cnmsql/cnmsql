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
	"errors"
	"fmt"
	"time"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
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
func PlanReplay(idx *objectstore.ArchiveIndex, anchorGTID string, target RecoveryTarget) (ReplayPlan, error) {
	if idx == nil {
		return ReplayPlan{}, fmt.Errorf("binlog: archive index is required")
	}

	anchor, err := parseGTIDSetOrEmpty(anchorGTID)
	if err != nil {
		return ReplayPlan{}, fmt.Errorf("binlog: parsing anchor GTID: %w", err)
	}

	// frontier accumulates everything we will have applied (anchor + replayed
	// segments); it both drives the skip optimization and reconstructs coverage.
	frontier := anchor.Clone()

	plan := ReplayPlan{ExcludeGTIDs: anchor.String()}
	for i := range idx.Segments {
		seg := &idx.Segments[i]
		segSet, err := parseGTIDSetOrEmpty(seg.GTIDSet)
		if err != nil {
			return ReplayPlan{}, fmt.Errorf("binlog: parsing segment %q GTID set: %w", seg.ServerUUID, err)
		}
		// Skip segments whose every transaction is already on the frontier.
		if !segSet.IsEmpty() && frontier.Contains(segSet) {
			continue
		}
		plan.Segments = append(plan.Segments, ReplaySegment{
			ServerUUID: seg.ServerUUID,
			Files:      append([]string(nil), seg.Binlogs...),
		})
		frontier.Union(segSet)
	}

	// Coherence guard: the index's declared coverage must be reconstructable from
	// the anchor plus the segments. If it isn't, a segment is missing or the
	// timeline forked — refuse rather than silently recover a partial history.
	if idx.CoveredGTIDSet != "" {
		covered, err := parseGTIDSetOrEmpty(idx.CoveredGTIDSet)
		if err != nil {
			return ReplayPlan{}, fmt.Errorf("binlog: parsing index coverage: %w", err)
		}
		if !frontier.Contains(covered) {
			return ReplayPlan{}, ErrForkedTimeline
		}
	}

	if err := applyTarget(&plan, frontier, anchor, target); err != nil {
		return ReplayPlan{}, err
	}
	return plan, nil
}

// applyTarget bounds the plan's upper end and validates the target is reachable.
func applyTarget(plan *ReplayPlan, frontier, anchor replication.GTIDSet, target RecoveryTarget) error {
	switch {
	case target.GTID != "":
		want, err := replication.ParseGTIDSet(target.GTID)
		if err != nil {
			return fmt.Errorf("binlog: parsing target GTID: %w", err)
		}
		// The target must include everything the base backup already has, else
		// it is a point before the backup and unreachable by forward replay.
		if !want.Contains(anchor) {
			return ErrTargetBeforeBackup
		}
		// The archive (anchor + segments) must cover the whole target set.
		if !frontier.Contains(want) {
			return ErrTargetBeyondArchive
		}
		plan.IncludeGTIDs = want.String()
	case target.Time != nil:
		plan.StopDatetime = target.Time.UTC().Format(stopDatetimeLayout)
	default:
		// Immediate / latest: replay everything from the anchor to the frontier.
	}
	return nil
}

// parseGTIDSetOrEmpty parses a GTID set string, treating "" as the empty set.
func parseGTIDSetOrEmpty(raw string) (replication.GTIDSet, error) {
	if raw == "" {
		return replication.GTIDSet{}, nil
	}
	return replication.ParseGTIDSet(raw)
}
