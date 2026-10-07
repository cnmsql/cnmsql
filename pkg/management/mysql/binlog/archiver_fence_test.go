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
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

const successorUUID = "bbbbbbbb-0000-0000-0000-000000000002"

// successorIndex is an index the successor (generation 5) already wrote: its
// segment holds what it inherited from testUUID and its own writes.
func successorIndex() objectstore.ArchiveIndex {
	return objectstore.ArchiveIndex{
		Generation: 5,
		Segments: []objectstore.ArchiveSegment{{
			ServerUUID: successorUUID, InstanceName: "demo-0", Binlogs: []string{"binlog.000001"},
			GTIDSet: testUUID + ":1-100," + successorUUID + ":1-50",
		}},
	}
}

func fencedArchiver(t *testing.T, store Store, generation int64, ok bool) (*Archiver, *executedSource, []BinaryLog) {
	t.Helper()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000009", "tail")
	writeBinlog(t, dir, "binlog.000010", "active")
	src := &executedSource{executed: testUUID + ":1-100"}
	a := newForkArchiver(t, store, dir, staticScan(map[string]string{"binlog.000009": testUUID + ":90-100"}), src.source)
	a.authority = func() (int64, bool) { return generation, ok }
	return a, src, MarkActive([]BinaryLog{{Name: "binlog.000009"}, {Name: "binlog.000010"}})
}

// A demoted primary finishing a pass after its successor wrote the index must
// not judge the successor's segment against its own executed set.
func TestDemotedPrimaryDoesNotJudgeANewerGeneration(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, successorIndex())
	a, src, logs := fencedArchiver(t, store, 4, true)

	res, err := a.ArchivePending(context.Background(), logs)
	if err != nil {
		t.Fatal(err)
	}
	idx := readIndex(t, store)
	for _, seg := range idx.Segments {
		if seg.Fork != nil {
			t.Fatalf("segment %s got fork %+v from a superseded primary", seg.ServerUUID, seg.Fork)
		}
	}
	if idx.Generation != 5 {
		t.Fatalf("index generation = %d, want the successor's 5 kept", idx.Generation)
	}
	if res.ForkCheck == nil || !res.ForkCheck.Superseded || res.ForkCheck.Checked {
		t.Fatalf("report = %+v, want superseded and unchecked", res.ForkCheck)
	}
	if src.calls != 0 {
		t.Fatalf("a superseded primary read its authority %d times", src.calls)
	}
	if _, err := PlanReplay(&idx, testUUID+":1-100,"+successorUUID+":1-60", RecoveryTarget{}); err != nil {
		t.Fatalf("a backup taken on the successor must stay recoverable: %v", err)
	}
}

// The current primary stamps its generation, and judges the archive.
func TestPrimaryStampsItsGeneration(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	idx := successorIndex()
	idx.Generation = 3
	seedIndex(t, store, idx)
	a, _, logs := fencedArchiver(t, store, 6, true)
	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	got := readIndex(t, store)
	if got.Generation != 6 {
		t.Fatalf("index generation = %d, want 6", got.Generation)
	}
	seg := segmentByUUID(t, got, successorUUID)
	if seg.Fork == nil || seg.Fork.GTIDSet != successorUUID+":1-50" {
		t.Fatalf("fork = %+v, want the current primary's verdict recorded", seg.Fork)
	}
}

// Until the Cluster names it currentPrimary, a writable instance does not
// archive: it does not know the generation that fences its writes.
func TestPrimaryWaitsToBeNamedBeforeArchiving(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	a, _, logs := fencedArchiver(t, store, 0, false)
	if _, err := a.ArchivePending(context.Background(), logs); !errors.Is(err, ErrNotCurrentPrimary) {
		t.Fatalf("err = %v, want ErrNotCurrentPrimary", err)
	}
	if len(store.objects) != 0 {
		t.Fatalf("archived %d objects before being named primary", len(store.objects))
	}
	if _, err := a.CheckForks(context.Background()); !errors.Is(err, ErrNotCurrentPrimary) {
		t.Fatalf("CheckForks err = %v, want ErrNotCurrentPrimary", err)
	}
}

// CheckForks on a promotion's first pass is fenced the same way.
func TestCheckForksIsFenced(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, successorIndex())
	a, _, _ := fencedArchiver(t, store, 4, true)
	report, err := a.CheckForks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Superseded || report.Checked {
		t.Fatalf("report = %+v, want superseded", report)
	}
	if seg := segmentByUUID(t, readIndex(t, store), successorUUID); seg.Fork != nil {
		t.Fatalf("fork = %+v", seg.Fork)
	}
}
