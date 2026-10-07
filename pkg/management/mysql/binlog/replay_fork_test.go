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
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// forkedArchive is the motivating MySQL scenario: the old primary (otherUUID)
// uploaded 1:219 before crashing; the lagged successor (testUUID) promoted at
// 1:218 and wrote 2:1-300. The successor's check recorded {1:219}.
func forkedArchive() *objectstore.ArchiveIndex {
	return &objectstore.ArchiveIndex{
		ClusterName: "demo",
		Segments: []objectstore.ArchiveSegment{
			{ServerUUID: "old", Binlogs: []string{"binlog.000001", "binlog.000002"}, GTIDSet: otherUUID + ":1-219",
				Fork: &objectstore.ArchiveFork{GTIDSet: otherUUID + ":219", DetectedAt: time.Unix(1700000000, 0).UTC()}},
			{ServerUUID: "new", Binlogs: []string{"binlog.000001"}, GTIDSet: otherUUID + ":1-218," + testUUID + ":1-300"},
		},
		CoveredGTIDSet: otherUUID + ":1-219," + testUUID + ":1-300",
		ForkCheck:      &objectstore.ArchiveForkCheck{CheckedAt: time.Unix(1700000000, 0).UTC(), CheckedBy: "demo-2/new"},
	}
}

// replayed computes what a replay plan applies on top of the anchor: every
// archived GTID the include set admits, minus the exclude set.
func replayed(t *testing.T, idx *objectstore.ArchiveIndex, plan ReplayPlan, anchor string) string {
	t.Helper()
	all := replication.GTIDSet{}
	for _, seg := range plan.Segments {
		for _, s := range idx.Segments {
			if s.ServerUUID == seg.ServerUUID {
				set, err := replication.ParseGTIDSet(s.GTIDSet)
				if err != nil {
					t.Fatal(err)
				}
				all.Union(set)
			}
		}
	}
	if plan.IncludeGTIDs != "" {
		include, _ := replication.ParseGTIDSet(plan.IncludeGTIDs)
		all = all.Difference(all.Difference(include))
	}
	exclude, _ := replication.ParseGTIDSet(plan.ExcludeGTIDs)
	out := all.Difference(exclude)
	anchorSet, _ := replication.ParseGTIDSet(anchor)
	out.Union(anchorSet)
	return out.String()
}

func TestPlanReplayExcludesForksForTimeAndLatest(t *testing.T) {
	t.Parallel()
	anchor := otherUUID + ":1-100"
	when := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	for name, target := range map[string]RecoveryTarget{
		"latest":    {},
		"immediate": {Immediate: true},
		"time":      {Time: &when},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			idx := forkedArchive()
			plan, err := PlanReplay(idx, anchor, target)
			if err != nil {
				t.Fatal(err)
			}
			if plan.ExcludeGTIDs != otherUUID+":1-100:219" {
				t.Fatalf("exclude = %q, want the anchor plus the fork", plan.ExcludeGTIDs)
			}
			if got, want := replayed(t, idx, plan, anchor), testUUID+":1-300,"+otherUUID+":1-218"; got != want {
				t.Fatalf("recovered %q, want the surviving timeline %q", got, want)
			}
			if len(plan.Forks) != 1 || plan.Forks[0].GTIDs != otherUUID+":219" || !plan.ForkChecked {
				t.Fatalf("plan forks = %+v checked=%v", plan.Forks, plan.ForkChecked)
			}
			if len(plan.Warnings) != 0 {
				t.Fatalf("warnings = %v", plan.Warnings)
			}
		})
	}
}

// targetGTID is the operator's explicit choice and is never rewritten. A target
// on the surviving branch recovers it; a target naming the dead transaction
// recovers that branch, with a warning.
func TestPlanReplayTargetGTIDIgnoresForks(t *testing.T) {
	t.Parallel()
	anchor := otherUUID + ":1-100"

	survivor := testUUID + ":1-50," + otherUUID + ":1-218"
	plan, err := PlanReplay(forkedArchive(), anchor, RecoveryTarget{GTID: survivor})
	if err != nil {
		t.Fatal(err)
	}
	if plan.IncludeGTIDs != survivor || plan.ExcludeGTIDs != anchor || len(plan.Warnings) != 0 {
		t.Fatalf("plan = include %q exclude %q warnings %v", plan.IncludeGTIDs, plan.ExcludeGTIDs, plan.Warnings)
	}

	dead := otherUUID + ":1-219"
	plan, err = PlanReplay(forkedArchive(), anchor, RecoveryTarget{GTID: dead})
	if err != nil {
		t.Fatal(err)
	}
	if plan.IncludeGTIDs != dead || plan.ExcludeGTIDs != anchor {
		t.Fatalf("plan = include %q exclude %q", plan.IncludeGTIDs, plan.ExcludeGTIDs)
	}
	if got := replayed(t, forkedArchive(), plan, anchor); got != dead {
		t.Fatalf("recovered %q, want the dead branch %q", got, dead)
	}
	if len(plan.Warnings) != 1 {
		t.Fatalf("warnings = %v, want one about the disowned transactions", plan.Warnings)
	}
}

// A base backup holding a disowned transaction was taken on the dead branch:
// replay cannot remove what the backup already contains, so time and latest
// recovery fail closed. A targetGTID that contains the anchor proceeds.
func TestPlanReplayRefusesABackupOnTheDeadBranch(t *testing.T) {
	t.Parallel()
	anchor := otherUUID + ":1-219"
	if _, err := PlanReplay(forkedArchive(), anchor, RecoveryTarget{}); !errors.Is(err, ErrBackupOnDeadBranch) {
		t.Fatalf("latest from a dead-branch backup: err = %v, want ErrBackupOnDeadBranch", err)
	}
	when := time.Now()
	if _, err := PlanReplay(forkedArchive(), anchor, RecoveryTarget{Time: &when}); !errors.Is(err, ErrBackupOnDeadBranch) {
		t.Fatalf("time from a dead-branch backup: err = %v", err)
	}
	if _, err := PlanReplay(forkedArchive(), anchor, RecoveryTarget{GTID: otherUUID + ":1-219"}); err != nil {
		t.Fatalf("targetGTID containing the anchor must proceed: %v", err)
	}
}

// A dead-branch backup is refused even when the forked segment is not planned
// (the anchor already covers all of it).
func TestPlanReplayDeadBranchCheckCoversUnplannedSegments(t *testing.T) {
	t.Parallel()
	idx := forkedArchive()
	anchor := otherUUID + ":1-219," + testUUID + ":1-10"
	if _, err := PlanReplay(idx, anchor, RecoveryTarget{}); !errors.Is(err, ErrBackupOnDeadBranch) {
		t.Fatalf("err = %v, want ErrBackupOnDeadBranch", err)
	}
}

func TestPlanReplayReportsUncheckedArchive(t *testing.T) {
	t.Parallel()
	idx := forkedArchive()
	idx.ForkCheck = nil
	idx.Segments[0].Fork = nil
	plan, err := PlanReplay(idx, otherUUID+":1-100", RecoveryTarget{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ForkChecked || len(plan.Forks) != 0 || plan.ExcludeGTIDs != otherUUID+":1-100" {
		t.Fatalf("plan = %+v", plan)
	}
}

// Planned segments carry their position and fork record, which the MariaDB
// positional executor cuts the replay with.
func TestPlanReplayCarriesSegmentForks(t *testing.T) {
	t.Parallel()
	plan, err := PlanReplay(forkedArchive(), otherUUID+":1-100", RecoveryTarget{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Segments) != 2 || plan.Segments[0].Fork == nil || plan.Segments[0].GTIDSet != otherUUID+":1-219" {
		t.Fatalf("segments = %+v", plan.Segments)
	}
}
