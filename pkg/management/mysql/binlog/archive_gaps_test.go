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
	"slices"
	"testing"

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
		{ServerUUID: "a", StartGTIDSet: "0-1-1", GTIDSet: "0-1-230", Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 218}}},
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
