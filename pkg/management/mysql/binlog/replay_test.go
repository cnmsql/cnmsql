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
	"reflect"
	"testing"
	"time"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
)

const (
	uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuidB = "5f22fb58-82db-22f2-af44-d91bb953063a"
)

func segment(uuid, gtidSet string, files ...string) objectstore.ArchiveSegment {
	return objectstore.ArchiveSegment{ServerUUID: uuid, GTIDSet: gtidSet, Binlogs: files}
}

func TestPlanReplayLatestSingleSegment(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{
		Segments:       []objectstore.ArchiveSegment{segment(uuidA, uuidA+":1-10", "binlog.000002", "binlog.000003")},
		CoveredGTIDSet: uuidA + ":1-10",
	}
	// Base backup already holds 1-5.
	plan, err := PlanReplay(idx, uuidA+":1-5", RecoveryTarget{})
	if err != nil {
		t.Fatalf("PlanReplay: %v", err)
	}
	if plan.ExcludeGTIDs != uuidA+":1-5" {
		t.Errorf("ExcludeGTIDs = %q, want anchor", plan.ExcludeGTIDs)
	}
	if plan.IncludeGTIDs != "" || plan.StopDatetime != "" {
		t.Errorf("latest target should have no upper bound, got include=%q stop=%q", plan.IncludeGTIDs, plan.StopDatetime)
	}
	if len(plan.Segments) != 1 || !reflect.DeepEqual(plan.Segments[0].Files, []string{"binlog.000002", "binlog.000003"}) {
		t.Fatalf("segments = %+v", plan.Segments)
	}
}

func TestPlanReplaySkipsCoveredSegment(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{
		Segments: []objectstore.ArchiveSegment{
			segment(uuidA, uuidA+":1-5", "binlog.000002"),
			segment(uuidB, uuidB+":1-3", "binlog.000002"),
		},
		CoveredGTIDSet: uuidA + ":1-5," + uuidB + ":1-3",
	}
	// Anchor already covers the entire first segment; it must be skipped.
	plan, err := PlanReplay(idx, uuidA+":1-5", RecoveryTarget{})
	if err != nil {
		t.Fatalf("PlanReplay: %v", err)
	}
	if len(plan.Segments) != 1 || plan.Segments[0].ServerUUID != uuidB {
		t.Fatalf("expected only segment B, got %+v", plan.Segments)
	}
}

func TestPlanReplayTargetGTID(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{
		Segments:       []objectstore.ArchiveSegment{segment(uuidA, uuidA+":1-10", "binlog.000002")},
		CoveredGTIDSet: uuidA + ":1-10",
	}
	plan, err := PlanReplay(idx, uuidA+":1-3", RecoveryTarget{GTID: uuidA + ":1-7"})
	if err != nil {
		t.Fatalf("PlanReplay: %v", err)
	}
	if plan.IncludeGTIDs != uuidA+":1-7" {
		t.Errorf("IncludeGTIDs = %q, want %q", plan.IncludeGTIDs, uuidA+":1-7")
	}
	if plan.ExcludeGTIDs != uuidA+":1-3" {
		t.Errorf("ExcludeGTIDs = %q, want anchor", plan.ExcludeGTIDs)
	}
}

func TestPlanReplayTargetGTIDBeforeBackup(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{
		Segments:       []objectstore.ArchiveSegment{segment(uuidA, uuidA+":1-10", "binlog.000002")},
		CoveredGTIDSet: uuidA + ":1-10",
	}
	// Target 1-3 is older than the base backup's 1-5.
	_, err := PlanReplay(idx, uuidA+":1-5", RecoveryTarget{GTID: uuidA + ":1-3"})
	if !errors.Is(err, ErrTargetBeforeBackup) {
		t.Fatalf("err = %v, want ErrTargetBeforeBackup", err)
	}
}

func TestPlanReplayTargetGTIDBeyondArchive(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{
		Segments:       []objectstore.ArchiveSegment{segment(uuidA, uuidA+":1-10", "binlog.000002")},
		CoveredGTIDSet: uuidA + ":1-10",
	}
	_, err := PlanReplay(idx, uuidA+":1-5", RecoveryTarget{GTID: uuidA + ":1-20"})
	if !errors.Is(err, ErrTargetBeyondArchive) {
		t.Fatalf("err = %v, want ErrTargetBeyondArchive", err)
	}
}

func TestPlanReplayTargetTime(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{
		Segments:       []objectstore.ArchiveSegment{segment(uuidA, uuidA+":1-10", "binlog.000002")},
		CoveredGTIDSet: uuidA + ":1-10",
	}
	ts := time.Date(2026, 6, 12, 10, 30, 0, 0, time.UTC)
	plan, err := PlanReplay(idx, uuidA+":1-5", RecoveryTarget{Time: &ts})
	if err != nil {
		t.Fatalf("PlanReplay: %v", err)
	}
	if plan.StopDatetime != "2026-06-12 10:30:00" {
		t.Errorf("StopDatetime = %q", plan.StopDatetime)
	}
	if plan.IncludeGTIDs != "" {
		t.Errorf("targetTime must not set IncludeGTIDs, got %q", plan.IncludeGTIDs)
	}
}

func TestPlanReplayMultiSegmentOrder(t *testing.T) {
	t.Parallel()
	// Two failovers: A → B → A again (overlap from log_replica_updates).
	idx := &objectstore.ArchiveIndex{
		Segments: []objectstore.ArchiveSegment{
			segment(uuidA, uuidA+":1-5", "binlog.000002"),
			segment(uuidB, uuidA+":1-5,"+uuidB+":1-4", "binlog.000002"),
		},
		CoveredGTIDSet: uuidA + ":1-5," + uuidB + ":1-4",
	}
	plan, err := PlanReplay(idx, uuidA+":1-2", RecoveryTarget{})
	if err != nil {
		t.Fatalf("PlanReplay: %v", err)
	}
	if len(plan.Segments) != 2 {
		t.Fatalf("want both segments, got %+v", plan.Segments)
	}
	if plan.Segments[0].ServerUUID != uuidA || plan.Segments[1].ServerUUID != uuidB {
		t.Errorf("segment order = %s,%s", plan.Segments[0].ServerUUID, plan.Segments[1].ServerUUID)
	}
}

func TestPlanReplayForkedTimeline(t *testing.T) {
	t.Parallel()
	// Index claims coverage 1-10 but the only segment provides 1-5: a gap.
	idx := &objectstore.ArchiveIndex{
		Segments:       []objectstore.ArchiveSegment{segment(uuidA, uuidA+":1-5", "binlog.000002")},
		CoveredGTIDSet: uuidA + ":1-10",
	}
	_, err := PlanReplay(idx, "", RecoveryTarget{})
	if !errors.Is(err, ErrForkedTimeline) {
		t.Fatalf("err = %v, want ErrForkedTimeline", err)
	}
}

func TestPlanReplayNilIndex(t *testing.T) {
	t.Parallel()
	if _, err := PlanReplay(nil, "", RecoveryTarget{}); err == nil {
		t.Fatal("expected error on nil index")
	}
}
