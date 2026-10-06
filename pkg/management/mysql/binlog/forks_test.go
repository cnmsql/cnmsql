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
	"reflect"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

const otherUUID = "7f2b1c90-0000-11e1-9e33-c80aa9429562"

func TestMySQLForkJudge(t *testing.T) {
	t.Parallel()
	judge, err := NewMySQLForkJudge(testUUID + ":1-218," + otherUUID + ":1-300")
	if err != nil {
		t.Fatal(err)
	}
	if judge.Authority() != testUUID+":1-218,"+otherUUID+":1-300" {
		t.Fatalf("authority = %q", judge.Authority())
	}
	cases := []struct {
		name, seg, want string
	}{
		{"lagged promotion", testUUID + ":1-219", testUUID + ":219"},
		{"nested", testUUID + ":1-200", ""},
		{"empty segment", "", ""},
		{"several holes", testUUID + ":1-221," + otherUUID + ":299-305", testUUID + ":219-221," + otherUUID + ":301-305"},
	}
	for _, tc := range cases {
		fork, err := judge.Judge(objectstore.ArchiveSegment{GTIDSet: tc.seg})
		if err != nil {
			t.Fatal(err)
		}
		got := ""
		if fork != nil {
			got = fork.GTIDSet
		}
		if got != tc.want {
			t.Fatalf("%s: disowned = %q, want %q", tc.name, got, tc.want)
		}
	}
	if _, err := NewMySQLForkJudge("uuid"); err == nil {
		t.Fatal("a malformed executed set must be rejected")
	}
	if _, err := judge.Judge(objectstore.ArchiveSegment{GTIDSet: ":1"}); err == nil {
		t.Fatal("a malformed segment set must be rejected")
	}
}

func TestMariaDBForkJudge(t *testing.T) {
	t.Parallel()
	timeline := engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-218"}}
	judge := NewMariaDBForkJudge(timeline, "0-2-300")
	if judge.Authority() != "0-2-300" {
		t.Fatalf("authority = %q", judge.Authority())
	}
	cases := []struct {
		name, seg string
		want      map[uint32]uint64
	}{
		{"dead tail", "0-1-219", map[uint32]uint64{0: 218}},
		{"canonical", "0-1-200", nil},
		{"successor", "0-2-300", nil},
		{"empty", "", nil},
	}
	for _, tc := range cases {
		fork, err := judge.Judge(objectstore.ArchiveSegment{GTIDSet: tc.seg})
		if err != nil {
			t.Fatal(err)
		}
		var got map[uint32]uint64
		if fork != nil {
			got = fork.AfterSeq
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: afterSeq = %v, want %v", tc.name, got, tc.want)
		}
	}
	noVerdict := NewMariaDBForkJudge(engine.MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}, "0-2-300")
	if fork, _ := noVerdict.Judge(objectstore.ArchiveSegment{GTIDSet: "0-1-100"}); fork != nil {
		t.Fatalf("no verdict must record nothing, got %+v", fork)
	}
	if _, err := judge.Judge(objectstore.ArchiveSegment{GTIDSet: "bad"}); err == nil {
		t.Fatal("a malformed segment position must be rejected")
	}
}

func TestMergeFork(t *testing.T) {
	t.Parallel()
	first := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC)
	later := first.Add(time.Hour)

	got, grew, err := mergeFork(nil, nil, "auth", first, "p/1")
	if err != nil || grew || got != nil {
		t.Fatalf("merging nothing = (%+v, %v, %v)", got, grew, err)
	}

	got, grew, err = mergeFork(nil, &objectstore.ArchiveFork{GTIDSet: testUUID + ":219"}, "auth1", first, "p/1")
	if err != nil || !grew {
		t.Fatalf("first record must grow: %v %v", grew, err)
	}
	want := &objectstore.ArchiveFork{GTIDSet: testUUID + ":219", AuthorityGTIDSet: "auth1", DetectedAt: first, DetectedBy: "p/1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("record = %+v, want %+v", got, want)
	}

	same, grew, err := mergeFork(got, &objectstore.ArchiveFork{GTIDSet: testUUID + ":219"}, "auth2", later, "q/2")
	if err != nil || grew || !reflect.DeepEqual(same, want) {
		t.Fatalf("an unchanged record must not grow or change: %+v %v %v", same, grew, err)
	}

	bigger, grew, err := mergeFork(got, &objectstore.ArchiveFork{GTIDSet: testUUID + ":220"}, "auth2", later, "q/2")
	if err != nil || !grew {
		t.Fatalf("a new disowned gtid must grow the record: %v %v", grew, err)
	}
	if bigger.GTIDSet != testUUID+":219-220" || bigger.AuthorityGTIDSet != "auth2" ||
		!bigger.DetectedAt.Equal(first) || bigger.DetectedBy != "p/1" {
		t.Fatalf("grown record = %+v", bigger)
	}
	if got.GTIDSet != testUUID+":219" {
		t.Fatal("mergeFork mutated the existing record")
	}

	maria, _, _ := mergeFork(nil, &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 300}}, "a", first, "p/1")
	lower, grew, _ := mergeFork(maria, &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 250, 1: 9}}, "b", later, "q/2")
	if !grew || !reflect.DeepEqual(lower.AfterSeq, map[uint32]uint64{0: 250, 1: 9}) {
		t.Fatalf("afterSeq = %v, want the lower value kept and the new domain added", lower.AfterSeq)
	}
	higher, grew, _ := mergeFork(lower, &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 280}}, "c", later, "q/2")
	if grew || higher.AfterSeq[0] != 250 {
		t.Fatalf("a higher afterSeq must not shrink the record: %v grew=%v", higher.AfterSeq, grew)
	}

	if _, _, err := mergeFork(got, &objectstore.ArchiveFork{GTIDSet: "bad"}, "a", later, "q"); err == nil {
		t.Fatal("a malformed delta must be rejected")
	}
}

func TestRenderFork(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		seg  objectstore.ArchiveSegment
		want string
	}{
		{"unforked", objectstore.ArchiveSegment{GTIDSet: "0-1-5"}, ""},
		{"mysql", objectstore.ArchiveSegment{GTIDSet: testUUID + ":1-219",
			Fork: &objectstore.ArchiveFork{GTIDSet: testUUID + ":219"}}, testUUID + ":219"},
		{"mariadb range", objectstore.ArchiveSegment{GTIDSet: "0-1-225",
			Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 218}}}, "0-1-219..0-1-225"},
		{"mariadb single", objectstore.ArchiveSegment{GTIDSet: "0-1-219",
			Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 218}}}, "0-1-219"},
		{"mariadb two domains", objectstore.ArchiveSegment{GTIDSet: "0-1-225,3-1-9",
			Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{3: 7, 0: 218}}}, "0-1-219..0-1-225,3-1-8..3-1-9"},
		{"mariadb nothing past the cut", objectstore.ArchiveSegment{GTIDSet: "0-1-200",
			Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 218}}}, ""},
	}
	for _, tc := range cases {
		if got := RenderFork(tc.seg); got != tc.want {
			t.Fatalf("%s: RenderFork = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestSegmentForksAndOldestPosition(t *testing.T) {
	t.Parallel()
	detected := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC)
	idx := &objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{
		{ServerUUID: "a", InstanceName: "demo-1", GTIDSet: "0-1-219,1-1-4",
			Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 218}, DetectedAt: detected}},
		{ServerUUID: "b", InstanceName: "demo-2", GTIDSet: "0-2-300"},
		{ServerUUID: "c", InstanceName: "demo-3", GTIDSet: "1-3-2"},
	}}
	forks := SegmentForks(idx)
	want := []SegmentFork{{ServerUUID: "a", InstanceName: "demo-1", GTIDs: "0-1-219", DetectedAt: detected}}
	if !reflect.DeepEqual(forks, want) {
		t.Fatalf("SegmentForks = %+v, want %+v", forks, want)
	}
	if got := OldestSegmentPosition(idx.Segments); got != "0-1-219,1-3-2" {
		t.Fatalf("OldestSegmentPosition = %q", got)
	}
	if got := OldestSegmentPosition([]objectstore.ArchiveSegment{{GTIDSet: testUUID + ":1-5"}}); got != "" {
		t.Fatalf("a MySQL archive has no MariaDB oldest position, got %q", got)
	}
}
