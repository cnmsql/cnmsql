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
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

func TestArchiveGapsMySQL(t *testing.T) {
	t.Parallel()
	idx := cloneHoleIndex()
	idx.CoveredGTIDSet = otherUUID + ":1-30," + testUUID + ":1-100:151-160"
	if got := ArchiveGaps(idx); !slices.Equal(got, []string{testUUID + ":101-150"}) {
		t.Fatalf("gaps = %v", got)
	}
	idx.CoveredGTIDSet = testUUID + ":1-160"
	if got := ArchiveGaps(idx); len(got) != 0 {
		t.Fatalf("a contiguous archive reported gaps %v", got)
	}
}

func TestArchiveGapsMariaDB(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{
		{ServerUUID: "a", StartGTIDSet: "0-1-1", GTIDSet: "0-1-100"},
		{ServerUUID: "b", StartGTIDSet: "0-1-151", GTIDSet: "0-2-200"},
	}}
	if got := ArchiveGaps(idx); !slices.Equal(got, []string{"0-101..150"}) {
		t.Fatalf("gaps = %v", got)
	}
	// A segment's range ends at its fork cut: the dead part does not bridge.
	idx = &objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{
		{
			ServerUUID: "a", StartGTIDSet: "0-1-1", GTIDSet: "0-1-230",
			Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 218}},
		},
		{ServerUUID: "b", StartGTIDSet: "0-2-226", GTIDSet: "0-2-300"},
	}}
	if got := ArchiveGaps(idx); !slices.Equal(got, []string{"0-219..225"}) {
		t.Fatalf("gaps = %v", got)
	}
}

// The primary's check reports the archive's gaps and coverage.
func TestForkCheckReportsGaps(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	idx := *cloneHoleIndex()
	idx.CoveredGTIDSet = otherUUID + ":1-30," + testUUID + ":1-100:151-160"
	seedIndex(t, store, idx)
	src := &executedSource{executed: otherUUID + ":1-30," + testUUID + ":1-160"}
	a, _, _ := fencedArchiver(t, store, 1, true)
	a.forks = src.source
	report, err := a.CheckForks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(report.Gaps, []string{testUUID + ":101-150"}) || report.Covered == "" {
		t.Fatalf("report gaps = %v covered = %q", report.Gaps, report.Covered)
	}
}

// cloneTailIndex: the old primary (testUUID) archived 1-100, and a successor
// re-cloned at testUUID:1-150 was promoted before replicating anything more
// from it. 101-150 never reached the archive, and nothing of testUUID follows
// it, so the covered set has no hole: only the successor's starting set shows
// the gap.
// thirdUUID is a server the archive holds nothing of.
const thirdUUID = "0b5e7f00-0000-11e1-9e33-c80aa9429562"

func cloneTailIndex() *objectstore.ArchiveIndex {
	return &objectstore.ArchiveIndex{
		Segments: []objectstore.ArchiveSegment{
			{ServerUUID: testUUID, Binlogs: []string{"binlog.000001"}, GTIDSet: testUUID + ":1-100"},
			{ServerUUID: otherUUID, Binlogs: []string{"binlog.000001"}, GTIDSet: otherUUID + ":1-30",
				PreviousGTIDSet: testUUID + ":1-150"},
		},
		CoveredGTIDSet: otherUUID + ":1-30," + testUUID + ":1-100",
	}
}

func TestArchiveGapsMySQLCloneTail(t *testing.T) {
	t.Parallel()
	if got := ArchiveGaps(cloneTailIndex()); !slices.Equal(got, []string{testUUID + ":101-150"}) {
		t.Fatalf("gaps = %v, want the clone point's unshipped tail", got)
	}
	// What predates the archive is not a gap: below its first transaction of a
	// UUID, or of a UUID it holds nothing of.
	idx := cloneTailIndex()
	idx.Segments[0].GTIDSet = testUUID + ":51-150"
	idx.CoveredGTIDSet = otherUUID + ":1-30," + testUUID + ":51-150"
	idx.Segments[1].PreviousGTIDSet = testUUID + ":1-150," + thirdUUID + ":1-9"
	if got := ArchiveGaps(idx); len(got) != 0 {
		t.Fatalf("gaps = %v, want none for history older than the archive", got)
	}
}

// The latest recovery replays the successor's segment, whose server held the
// unshipped tail: the result would lack it.
func TestLatestRecoveryRefusesACloneTail(t *testing.T) {
	t.Parallel()
	if _, err := PlanReplay(cloneTailIndex(), testUUID+":1-50", RecoveryTarget{}); !errors.Is(err, ErrArchiveGap) {
		t.Fatalf("err = %v, want ErrArchiveGap", err)
	}
	// A backup taken past the clone point holds the tail.
	if _, err := PlanReplay(cloneTailIndex(), testUUID+":1-150", RecoveryTarget{}); err != nil {
		t.Fatalf("a backup past the tail must recover: %v", err)
	}
	// A time target may stop before it.
	at := time.Unix(1700000000, 0)
	if _, err := PlanReplay(cloneTailIndex(), testUUID+":1-50", RecoveryTarget{Time: &at}); err != nil {
		t.Fatalf("a time target must plan: %v", err)
	}
}

// The archiver records what the server held before the first file its segment
// archived, once.
func TestArchiverRecordsTheSegmentStartingSet(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "two")
	writeBinlog(t, dir, "binlog.000003", "active")
	mem := newMemStore()
	previous := map[string]string{
		"binlog.000001": otherUUID + ":1-150",
		"binlog.000002": otherUUID + ":1-150," + testUUID + ":1-3",
	}
	sets := map[string]string{"binlog.000001": testUUID + ":1-3", "binlog.000002": testUUID + ":4-6"}
	scan := func(_ context.Context, path string) (ScanResult, error) {
		name := filepath.Base(path)
		return ScanResult{GTIDSet: sets[name], LastGTID: sets[name], PreviousGTIDs: previous[name]}, nil
	}
	a := newTestArchiver(t, mem, dir, scan)
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"}})
	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	if seg := readSegment(t, mem, a); seg.PreviousGTIDSet != otherUUID+":1-150" {
		t.Fatalf("segment starting set = %q, want the first file's", seg.PreviousGTIDSet)
	}
}
