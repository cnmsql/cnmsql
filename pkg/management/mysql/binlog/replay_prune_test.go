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
	"slices"
	"testing"
	"time"
)

// prunedPlan is a single-segment MySQL plan whose base backup holds 1-30: the
// first three files are already applied and only the last two are needed.
func prunedPlan() (ReplayPlan, map[ReplayFile]ReplayFileSpan) {
	plan := ReplayPlan{
		AnchorGTID:   orderPrimary + ":1-30",
		ExcludeGTIDs: orderPrimary + ":1-30",
		Segments: []ReplaySegment{{ServerUUID: orderPrimary, Files: []string{
			"binlog.000001", "binlog.000002", "binlog.000003", "binlog.000004", "binlog.000005",
		}}},
	}
	return plan, map[ReplayFile]ReplayFileSpan{
		{orderPrimary, "binlog.000001"}: span(1, 10, 0),
		{orderPrimary, "binlog.000002"}: span(11, 20, 5),
		{orderPrimary, "binlog.000003"}: span(21, 30, 10),
		{orderPrimary, "binlog.000004"}: span(31, 40, 15),
		{orderPrimary, "binlog.000005"}: span(41, 50, 20),
	}
}

// A file whose every transaction the replay excludes (the base backup holds
// it, or the archive disowned it) is never downloaded: mysqlbinlog would drop
// all of it anyway.
func TestPruneReplayFilesDropsFilesTheExcludeSetHolds(t *testing.T) {
	t.Parallel()
	plan, spans := prunedPlan()
	got := PruneReplayFiles(plan, spans)
	want := []string{"a/binlog.000004", "a/binlog.000005"}
	if names := orderNames(got.Files()); !slices.Equal(names, want) {
		t.Fatalf("pruned files = %v, want %v", names, want)
	}
	if got.Segments[0].GTIDSet != plan.Segments[0].GTIDSet || len(plan.Segments[0].Files) != 5 {
		t.Fatal("pruning must not touch the segment's coverage or the caller's plan")
	}
}

// A file the exclude set holds only in part carries needed transactions and
// is kept whole.
func TestPruneReplayFilesKeepsAPartlyExcludedFile(t *testing.T) {
	t.Parallel()
	plan, spans := prunedPlan()
	plan.ExcludeGTIDs = orderPrimary + ":1-25"
	got := PruneReplayFiles(plan, spans)
	want := []string{"a/binlog.000003", "a/binlog.000004", "a/binlog.000005"}
	if names := orderNames(got.Files()); !slices.Equal(names, want) {
		t.Fatalf("pruned files = %v, want %v", names, want)
	}
}

// With a time target, a file whose first event is at or past the target is
// never reached: mysqlbinlog stops at the first event stamped past the target.
func TestPruneReplayFilesDropsFilesPastATimeTarget(t *testing.T) {
	t.Parallel()
	plan, spans := prunedPlan()
	plan.TargetTime = time.Date(2026, 10, 20, 6, 20, 0, 0, time.UTC) // binlog.000005's first event
	got := PruneReplayFiles(plan, spans)
	want := []string{"a/binlog.000004"}
	if names := orderNames(got.Files()); !slices.Equal(names, want) {
		t.Fatalf("pruned files = %v, want %v", names, want)
	}
}

// A file with no manifest, no transactions or no time stamp gives nothing to
// judge it by, so it is kept.
func TestPruneReplayFilesKeepsFilesItCannotJudge(t *testing.T) {
	t.Parallel()
	plan, spans := prunedPlan()
	plan.TargetTime = time.Date(2026, 10, 20, 6, 20, 0, 0, time.UTC)
	delete(spans, ReplayFile{orderPrimary, "binlog.000001"})
	spans[ReplayFile{orderPrimary, "binlog.000002"}] = ReplayFileSpan{}
	spans[ReplayFile{orderPrimary, "binlog.000005"}] = ReplayFileSpan{GTIDSet: orderPrimary + ":41-50"}
	got := PruneReplayFiles(plan, spans)
	want := []string{"a/binlog.000001", "a/binlog.000002", "a/binlog.000004", "a/binlog.000005"}
	if names := orderNames(got.Files()); !slices.Equal(names, want) {
		t.Fatalf("pruned files = %v, want %v", names, want)
	}
}

// A plan whose exclude set does not parse is replayed as planned rather than
// pruned on a guess.
func TestPruneReplayFilesLeavesAnUnparsablePlanAlone(t *testing.T) {
	t.Parallel()
	plan, spans := prunedPlan()
	plan.ExcludeGTIDs = "not-a-gtid-set"
	got := PruneReplayFiles(plan, spans)
	if len(got.Files()) != 5 {
		t.Fatalf("pruned files = %v, want all five", orderNames(got.Files()))
	}
}
