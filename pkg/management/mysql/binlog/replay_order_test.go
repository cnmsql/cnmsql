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
	"fmt"
	"slices"
	"testing"
	"time"
)

const (
	orderPrimary = "aaaaaaaa-0000-0000-0000-000000000001"
	orderReplica = "bbbbbbbb-0000-0000-0000-000000000002"
)

// span is the manifest of a file holding orderPrimary:first-last.
func span(first, last int, minute int) ReplayFileSpan {
	return ReplayFileSpan{
		FirstGTID:      fmt.Sprintf("%s:%d", orderPrimary, first),
		GTIDSet:        fmt.Sprintf("%s:%d-%d", orderPrimary, first, last),
		FirstEventTime: time.Date(2026, 10, 20, 6, minute, 0, 0, time.UTC),
	}
}

func orderNames(files []ReplayFile) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, f.ServerUUID[:1]+"/"+f.Name)
	}
	return out
}

// holedPrimary is a primary segment whose index lost binlog.000003 (31-40),
// and a replica segment whose one file, spanning 15-55, holds it.
func holedPrimary(replicaFirst bool) (ReplayPlan, map[ReplayFile]ReplayFileSpan) {
	primary := ReplaySegment{ServerUUID: orderPrimary, Files: []string{
		"binlog.000001", "binlog.000002", "binlog.000004", "binlog.000005",
	}}
	replica := ReplaySegment{ServerUUID: orderReplica, Files: []string{"binlog.000900"}}
	plan := ReplayPlan{AnchorGTID: orderPrimary + ":1-10", Segments: []ReplaySegment{primary, replica}}
	if replicaFirst {
		plan.Segments = []ReplaySegment{replica, primary}
	}
	return plan, map[ReplayFile]ReplayFileSpan{
		{orderPrimary, "binlog.000001"}: span(11, 20, 0),
		{orderPrimary, "binlog.000002"}: span(21, 30, 5),
		{orderPrimary, "binlog.000004"}: span(41, 50, 15),
		{orderPrimary, "binlog.000005"}: span(51, 60, 20),
		{orderReplica, "binlog.000900"}: span(15, 55, 2),
	}
}

// assertInTimelineOrder replays an order on paper: every file must start right
// after a transaction already applied.
func assertInTimelineOrder(t *testing.T, plan ReplayPlan, spans map[ReplayFile]ReplayFileSpan, order []ReplayFile) {
	t.Helper()
	applied := newMysqlGTIDSet()
	if err := applied.Parse(plan.AnchorGTID); err != nil {
		t.Fatal(err)
	}
	for _, f := range order {
		var first int
		if _, err := fmt.Sscanf(spans[f].FirstGTID, orderPrimary+":%d", &first); err != nil {
			t.Fatal(err)
		}
		before := newMysqlGTIDSet()
		if err := before.Parse(fmt.Sprintf("%s:%d", orderPrimary, first-1)); err != nil {
			t.Fatal(err)
		}
		if !applied.Contains(before) {
			t.Errorf("%s/%s starts at %d before %d is applied (order %v)",
				f.ServerUUID[:1], f.Name, first, first-1, orderNames(order))
		}
		set := newMysqlGTIDSet()
		if err := set.Parse(spans[f].GTIDSet); err != nil {
			t.Fatal(err)
		}
		applied.Union(set)
	}
}

// A file from another segment that fills a hole is replayed where it belongs
// on the timeline: once its first transaction's predecessor is applied, and
// before any file that starts after the hole. Segment-by-segment replay would
// apply transactions out of order either way round.
func TestOrderReplayFilesPlacesAHoleFillerOnTheTimeline(t *testing.T) {
	t.Parallel()
	for _, replicaFirst := range []bool{true, false} {
		plan, spans := holedPrimary(replicaFirst)
		got := OrderReplayFiles(plan, spans)
		if len(got) != 5 {
			t.Fatalf("replicaFirst=%v: order = %v, want all five files", replicaFirst, orderNames(got))
		}
		assertInTimelineOrder(t, plan, spans, got)
	}
}

// Segments that follow one another (a failover, the successor re-logging the
// end of its predecessor) keep their segment order.
func TestOrderReplayFilesKeepsSuccessiveSegments(t *testing.T) {
	t.Parallel()
	plan := ReplayPlan{AnchorGTID: orderPrimary + ":1-10", Segments: []ReplaySegment{
		{ServerUUID: orderPrimary, Files: []string{"binlog.000001", "binlog.000002"}},
		{ServerUUID: orderReplica, Files: []string{"binlog.000001", "binlog.000002"}},
	}}
	spans := map[ReplayFile]ReplayFileSpan{
		{orderPrimary, "binlog.000001"}: span(11, 20, 0),
		{orderPrimary, "binlog.000002"}: span(21, 30, 5),
		{orderReplica, "binlog.000001"}: span(18, 30, 3),
		{orderReplica, "binlog.000002"}: {
			FirstGTID: orderReplica + ":1", GTIDSet: orderReplica + ":1-9",
			FirstEventTime: time.Date(2026, 10, 20, 6, 10, 0, 0, time.UTC),
		},
	}
	got := orderNames(OrderReplayFiles(plan, spans))
	want := []string{"a/binlog.000001", "a/binlog.000002", "b/binlog.000001", "b/binlog.000002"}
	if !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}

// A hole nothing fills still yields every file, each segment in its own
// order: the replay runs and the recovered set's check reports the gap.
func TestOrderReplayFilesKeepsEveryFileAcrossAnUnfilledHole(t *testing.T) {
	t.Parallel()
	plan, spans := holedPrimary(false)
	plan.Segments = plan.Segments[:1]
	got := orderNames(OrderReplayFiles(plan, spans))
	want := []string{"a/binlog.000001", "a/binlog.000002", "a/binlog.000004", "a/binlog.000005"}
	if !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}

// Without an explicit order a plan's files are its segments', one after the
// other; with one, that order.
func TestReplayPlanFiles(t *testing.T) {
	t.Parallel()
	plan, spans := holedPrimary(false)
	if got := orderNames(plan.Files()); !slices.Equal(got, []string{
		"a/binlog.000001", "a/binlog.000002", "a/binlog.000004", "a/binlog.000005", "b/binlog.000900",
	}) {
		t.Errorf("segment order = %v", got)
	}
	plan.Order = OrderReplayFiles(plan, spans)
	plan.Order[0], plan.Order[1] = plan.Order[1], plan.Order[0]
	if got := plan.Files(); !slices.Equal(got, plan.Order) {
		t.Errorf("explicit order = %v, want %v", orderNames(got), orderNames(plan.Order))
	}
}

// A successor whose first file opens its own numbering waits for the files
// that are provably next, so it cannot overtake a hole's filler.
func TestOrderReplayFilesHoldsBackAServersFirstTransaction(t *testing.T) {
	t.Parallel()
	plan, spans := holedPrimary(false)
	const successor = "cccccccc-0000-0000-0000-000000000003"
	plan.Segments = []ReplaySegment{
		plan.Segments[0],
		{ServerUUID: successor, Files: []string{"binlog.000001"}},
		plan.Segments[1],
	}
	spans[ReplayFile{successor, "binlog.000001"}] = ReplayFileSpan{
		FirstGTID: successor + ":1", GTIDSet: successor + ":1-5",
		FirstEventTime: time.Date(2026, 10, 20, 7, 0, 0, 0, time.UTC),
	}
	got := orderNames(OrderReplayFiles(plan, spans))
	if len(got) != 6 || got[5] != "c/binlog.000001" {
		t.Errorf("order = %v, want the successor's file last", got)
	}
}

// A file with no transactions has no place by GTID. Replayed first, its
// header events, stamped later than a time target, would stop mysqlbinlog
// before anything is applied: it keeps its place by time. A file whose first
// transaction the base backup already holds is next in line.
func TestOrderReplayFilesDoesNotLetAnEmptyFileJumpAhead(t *testing.T) {
	t.Parallel()
	for _, emptyAt := range []time.Time{time.Date(2026, 10, 20, 6, 30, 0, 0, time.UTC), {}} {
		plan := ReplayPlan{AnchorGTID: orderPrimary + ":1-10", Segments: []ReplaySegment{
			{ServerUUID: orderPrimary, Files: []string{"binlog.000001", "binlog.000002"}},
			{ServerUUID: orderReplica, Files: []string{"binlog.000001", "binlog.000002"}},
		}}
		spans := map[ReplayFile]ReplayFileSpan{
			{orderPrimary, "binlog.000001"}: span(1, 20, 0),
			{orderPrimary, "binlog.000002"}: span(21, 30, 5),
			{orderReplica, "binlog.000001"}: {FirstEventTime: emptyAt},
			{orderReplica, "binlog.000002"}: span(41, 50, 40),
		}
		got := orderNames(OrderReplayFiles(plan, spans))
		want := []string{"a/binlog.000001", "a/binlog.000002", "b/binlog.000001", "b/binlog.000002"}
		if !slices.Equal(got, want) {
			t.Errorf("empty file stamped %v: order = %v, want %v", emptyAt, got, want)
		}
	}
}

// With nothing ready, a file whose manifest carries no time does not go
// before one that does.
func TestOrderReplayFilesFallsBackToKnownTimesFirst(t *testing.T) {
	t.Parallel()
	plan := ReplayPlan{Segments: []ReplaySegment{
		{ServerUUID: orderReplica, Files: []string{"binlog.000001"}},
		{ServerUUID: orderPrimary, Files: []string{"binlog.000001"}},
	}}
	spans := map[ReplayFile]ReplayFileSpan{
		{orderReplica, "binlog.000001"}: {},
		{orderPrimary, "binlog.000001"}: span(1, 20, 0),
	}
	got := orderNames(OrderReplayFiles(plan, spans))
	if want := []string{"a/binlog.000001", "b/binlog.000001"}; !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}
