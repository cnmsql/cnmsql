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
	"slices"
	"testing"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

var testObjectStore = mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cnmsql"}

// oldSegment is a former primary's segment: it archived up to otherUUID:219,
// one transaction past what its lagged successor (this archiver) inherited.
func oldSegment(gtids string) objectstore.ArchiveSegment {
	return objectstore.ArchiveSegment{
		ServerUUID:   "old-identity",
		InstanceName: "demo-0",
		Binlogs:      []string{"binlog.000007"},
		GTIDSet:      gtids,
	}
}

func seedIndex(t *testing.T, store Store, idx objectstore.ArchiveIndex) {
	t.Helper()
	idx.ClusterName = "demo"
	key := objectstore.ArchiveIndexKey(testObjectStore, "demo")
	if err := store.PutJSON(context.Background(), testObjectStore.Bucket, key, &idx); err != nil {
		t.Fatal(err)
	}
}

func readIndex(t *testing.T, store Store) objectstore.ArchiveIndex {
	t.Helper()
	var idx objectstore.ArchiveIndex
	key := objectstore.ArchiveIndexKey(testObjectStore, "demo")
	if err := store.GetJSON(context.Background(), testObjectStore.Bucket, key, &idx); err != nil {
		t.Fatal(err)
	}
	return idx
}

// executedSource is a MySQL ForkSource over a mutable gtid_executed.
type executedSource struct {
	executed string
	err      error
	calls    int
}

func (s *executedSource) source(context.Context) (ForkJudge, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return NewMySQLForkJudge(s.executed)
}

func newForkArchiver(t *testing.T, store Store, dir string, scan Scanner, forks ForkSource) *Archiver {
	t.Helper()
	a, err := NewArchiver(ArchiverOptions{
		Store:        store,
		ObjectStore:  testObjectStore,
		ClusterName:  "demo",
		InstanceName: "demo-1",
		ServerUUID:   testUUID,
		BinlogDir:    dir,
		Scan:         scan,
		Now:          func() time.Time { return time.Unix(1700000000, 0).UTC() },
		Forks:        forks,
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// archiveOne ships binlog.000001 (holding testUUID:1-5) on a fresh archiver.
func archiveOne(t *testing.T, store Store, forks ForkSource) (*Archiver, ArchiveResult) {
	t.Helper()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "active")
	a := newForkArchiver(t, store, dir, staticScan(map[string]string{"binlog.000001": testUUID + ":1-5"}), forks)
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}})
	res, err := a.ArchivePending(context.Background(), logs)
	if err != nil {
		t.Fatal(err)
	}
	return a, res
}

func segmentByUUID(t *testing.T, idx objectstore.ArchiveIndex, uuid string) objectstore.ArchiveSegment {
	t.Helper()
	seg, ok := idx.Segment(uuid)
	if !ok {
		t.Fatalf("segment %s missing from index %+v", uuid, idx)
	}
	return *seg
}

// Case 4 of the design: the old primary uploaded otherUUID:219 before it
// crashed, and the lagged successor promoted at 218. The successor's first
// index write records the dead transaction on the segment that holds it.
func TestArchiverRecordsForkOnHoldingSegment(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	src := &executedSource{executed: otherUUID + ":1-218," + testUUID + ":1-5"}

	_, res := archiveOne(t, store, src.source)

	idx := readIndex(t, store)
	old := segmentByUUID(t, idx, "old-identity")
	if old.Fork == nil || old.Fork.GTIDSet != otherUUID+":219" {
		t.Fatalf("old segment fork = %+v, want %s:219", old.Fork, otherUUID)
	}
	if old.Fork.DetectedBy != "demo-1/"+testUUID || old.Fork.AuthorityGTIDSet != src.executed {
		t.Fatalf("fork audit fields = %+v", old.Fork)
	}
	if own := segmentByUUID(t, idx, testUUID); own.Fork != nil {
		t.Fatalf("own segment must never carry a fork, got %+v", own.Fork)
	}
	if idx.ForkCheck == nil || idx.ForkCheck.CheckedBy != "demo-1/"+testUUID {
		t.Fatalf("forkCheck = %+v", idx.ForkCheck)
	}
	if res.ForkCheck == nil || !res.ForkCheck.Checked || len(res.ForkCheck.Forks) != 1 ||
		res.ForkCheck.Forks[0].GTIDs != otherUUID+":219" {
		t.Fatalf("fork report = %+v", res.ForkCheck)
	}
}

// Case 1: a clean failover nests the old segment inside the successor's
// executed set, so nothing is recorded but the index is stamped as checked.
func TestArchiverRecordsNothingForNestedSegment(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-200")}})
	src := &executedSource{executed: otherUUID + ":1-218," + testUUID + ":1-5"}

	_, res := archiveOne(t, store, src.source)

	idx := readIndex(t, store)
	if old := segmentByUUID(t, idx, "old-identity"); old.Fork != nil {
		t.Fatalf("nested segment got a fork: %+v", old.Fork)
	}
	if idx.ForkCheck == nil || res.ForkCheck == nil || !res.ForkCheck.Checked || len(res.ForkCheck.Forks) != 0 {
		t.Fatalf("forkCheck = %+v, report = %+v", idx.ForkCheck, res.ForkCheck)
	}
}

// The checking primary's own segment holds only its own binlog. Even when its
// recorded set reads past a stale executed snapshot, it is never judged.
func TestArchiverSkipsOwnSegment(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{
		{ServerUUID: testUUID, InstanceName: "demo-1", GTIDSet: testUUID + ":1-9"},
	}})
	src := &executedSource{executed: testUUID + ":1-5"}

	archiveOne(t, store, src.source)

	if own := segmentByUUID(t, readIndex(t, store), testUUID); own.Fork != nil {
		t.Fatalf("own segment was judged: %+v", own.Fork)
	}
}

// A MySQL record narrows to what the authority still lacks: when the current
// primary holds a recorded transaction again (a former primary that held it
// was promoted after all), leaving it out of a recovery would replay later
// transactions over a state that never existed.
func TestArchiverRetractsWhatTheAuthorityHolds(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	old := oldSegment(otherUUID + ":1-221")
	old.Fork = &objectstore.ArchiveFork{GTIDSet: otherUUID + ":219-220", DetectedBy: "demo-2/x"}
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{old}})
	src := &executedSource{executed: otherUUID + ":1-220," + testUUID + ":1-5"}

	archiveOne(t, store, src.source)

	idx := readIndex(t, store)
	got := segmentByUUID(t, idx, "old-identity").Fork
	if got == nil || got.GTIDSet != otherUUID+":221" || got.DetectedBy != "demo-2/x" {
		t.Fatalf("fork = %+v, want 221 kept, 219-220 retracted, first detection kept", got)
	}
	if idx.Disowned == nil || idx.Disowned.GTIDSet != otherUUID+":221" {
		t.Fatalf("disowned = %+v, want %s:221", idx.Disowned, otherUUID)
	}
}

// A record on the primary's own segment is retracted too: it is the one the
// regained-transaction case leaves behind, and it would otherwise refuse every
// backup the new primary takes.
func TestArchiverRetractsOnItsOwnSegment(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{
		Segments: []objectstore.ArchiveSegment{{
			ServerUUID: testUUID, InstanceName: "demo-1", GTIDSet: testUUID + ":1-5",
			Fork: &objectstore.ArchiveFork{GTIDSet: testUUID + ":5", DetectedBy: "demo-2/x"},
		}},
		Disowned: &objectstore.ArchiveDisowned{GTIDSet: testUUID + ":5"},
	})
	src := &executedSource{executed: testUUID + ":1-5"}

	archiveOne(t, store, src.source)

	idx := readIndex(t, store)
	if own := segmentByUUID(t, idx, testUUID); own.Fork != nil {
		t.Fatalf("own segment fork = %+v, want retracted", own.Fork)
	}
	if idx.Disowned != nil {
		t.Fatalf("disowned = %+v, want empty", idx.Disowned)
	}
	if _, err := PlanReplay(&idx, testUUID+":1-5", RecoveryTarget{}); err != nil {
		t.Fatalf("a backup holding the regained transaction must be recoverable: %v", err)
	}
}

// The disowned record outlives the segment: retention dropping a forked
// segment keeps the dead branch it recorded.
func TestDisownedOutlivesItsSegment(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	src := &executedSource{executed: otherUUID + ":1-218," + testUUID + ":1-5"}
	archiveOne(t, store, src.source)

	idx := readIndex(t, store)
	idx.Segments = slices.DeleteFunc(idx.Segments, func(s objectstore.ArchiveSegment) bool {
		return s.ServerUUID == "old-identity"
	})
	if idx.Disowned == nil || idx.Disowned.GTIDSet != otherUUID+":219" {
		t.Fatalf("disowned = %+v", idx.Disowned)
	}
	_, err := PlanReplay(&idx, otherUUID+":1-219", RecoveryTarget{})
	if !errors.Is(err, ErrBackupOnDeadBranch) {
		t.Fatalf("err = %v, want ErrBackupOnDeadBranch after retention dropped the segment", err)
	}
}

// An index written before fork checks existed gets checked and stamped by the
// first writable pass, even when there is nothing to archive. A second check
// with nothing new writes nothing.
func TestCheckForksStampsLegacyIndexOnce(t *testing.T) {
	t.Parallel()
	store := newCountingStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-200")}})
	src := &executedSource{executed: otherUUID + ":1-218"}
	a := newForkArchiver(t, store, t.TempDir(), staticScan(nil), src.source)

	before := store.puts
	report, err := a.CheckForks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Checked || store.puts != before+1 {
		t.Fatalf("first check: report %+v, puts %d -> %d (want one write)", report, before, store.puts)
	}
	if readIndex(t, store).ForkCheck == nil {
		t.Fatal("legacy index was not stamped")
	}

	before = store.puts
	if report, err = a.CheckForks(context.Background()); err != nil || !report.Checked {
		t.Fatalf("second check: %+v %v", report, err)
	}
	if store.puts != before {
		t.Fatalf("an unchanged, already stamped index must not be rewritten (%d writes)", store.puts-before)
	}
}

// A fork record lost to an index write race (a drain or retention rewrote the
// index from an older copy) is re-detected: the transactions are still disowned.
func TestCheckForksRedetectsLostRecord(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	src := &executedSource{executed: otherUUID + ":1-218," + testUUID + ":1-5"}
	a, _ := archiveOne(t, store, src.source)

	idx := readIndex(t, store)
	for i := range idx.Segments {
		idx.Segments[i].Fork = nil
	}
	seedIndex(t, store, idx)

	report, err := a.CheckForks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := segmentByUUID(t, readIndex(t, store), "old-identity").Fork; got == nil || got.GTIDSet != otherUUID+":219" {
		t.Fatalf("lost fork not re-detected: %+v", got)
	}
	if len(report.Forks) != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestCheckForksWithoutIndexIsANoop(t *testing.T) {
	t.Parallel()
	store := newCountingStore()
	src := &executedSource{executed: testUUID + ":1-5"}
	a := newForkArchiver(t, store, t.TempDir(), staticScan(nil), src.source)
	report, err := a.CheckForks(context.Background())
	if err != nil || report.Checked || store.puts != 0 {
		t.Fatalf("report %+v err %v puts %d; want a no-op", report, err, store.puts)
	}
}

// A failing authority must not stop archiving: the index is still written,
// just not stamped as checked, and the error is reported.
func TestArchiverForkSourceErrorStillArchives(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	src := &executedSource{err: errors.New("connection refused")}

	_, res := archiveOne(t, store, src.source)

	idx := readIndex(t, store)
	if _, ok := idx.Segment(testUUID); !ok {
		t.Fatal("the archived file must still reach the index")
	}
	if idx.ForkCheck != nil {
		t.Fatal("a failed check must not stamp the index")
	}
	if res.ForkCheck == nil || res.ForkCheck.Err == nil || res.ForkCheck.Checked {
		t.Fatalf("report = %+v, want the error", res.ForkCheck)
	}
}

func TestArchiverWithoutForkSourceDoesNotCheck(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	_, res := archiveOne(t, store, nil)
	idx := readIndex(t, store)
	if idx.ForkCheck != nil || segmentByUUID(t, idx, "old-identity").Fork != nil {
		t.Fatalf("no fork source must mean no check: %+v", idx)
	}
	if res.ForkCheck != nil && res.ForkCheck.Checked {
		t.Fatalf("report = %+v", res.ForkCheck)
	}
}

// A source with no authority right now (e.g. a MariaDB primary that has not
// seen the timeline yet) skips the check without an error.
func TestArchiverForkSourceWithoutAuthoritySkips(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	none := func(context.Context) (ForkJudge, error) { return nil, nil }
	_, res := archiveOne(t, store, none)
	if idx := readIndex(t, store); idx.ForkCheck != nil {
		t.Fatal("no authority must not stamp the index")
	}
	if res.ForkCheck == nil || res.ForkCheck.Checked || res.ForkCheck.Err != nil {
		t.Fatalf("report = %+v", res.ForkCheck)
	}
}

// The authority is read for every index write, after the index: a pass that
// straddles a demotion must not judge segments that appeared after the
// authority was read.
func TestArchiverReadsAuthorityPerIndexWrite(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	dir := t.TempDir()
	for _, n := range []string{"binlog.000001", "binlog.000002", "binlog.000003", "binlog.000004"} {
		writeBinlog(t, dir, n, n)
	}
	src := &executedSource{executed: testUUID + ":1-9"}
	a := newForkArchiver(t, store, dir, staticScan(map[string]string{
		"binlog.000001": testUUID + ":1-3", "binlog.000002": testUUID + ":4-6", "binlog.000003": testUUID + ":7-9",
	}), src.source)
	logs := MarkActive([]BinaryLog{
		{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"}, {Name: "binlog.000004"},
	})
	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	if src.calls != 3 {
		t.Fatalf("authority read %d times for three index writes, want 3", src.calls)
	}
}

// The archiver writes the index with a compare-and-swap. When retention drops
// a forked segment between the archiver's read and its write, the archiver
// rebases its fold on retention's index instead of writing back the copy it
// read, which would resurrect the segment and its fork record.
func TestArchiverRebasesTheIndexOnAConcurrentWrite(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	old := oldSegment(otherUUID + ":1-219")
	old.Fork = &objectstore.ArchiveFork{GTIDSet: otherUUID + ":219"}
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{old}})
	indexKey := testObjectStore.Bucket + "/" + objectstore.ArchiveIndexKey(testObjectStore, "demo")
	store.beforeConditionalPut = func(key string) {
		if key != indexKey {
			return
		}
		store.beforeConditionalPut = nil
		seedIndex(t, store, objectstore.ArchiveIndex{}) // retention dropped every segment
	}
	src := &executedSource{executed: otherUUID + ":1-218," + testUUID + ":1-5"}

	archiveOne(t, store, src.source)

	idx := readIndex(t, store)
	if _, ok := idx.Segment("old-identity"); ok {
		t.Fatalf("the archiver resurrected the dropped segment: %+v", idx.Segments)
	}
	own, ok := idx.Segment(testUUID)
	if !ok || len(own.Binlogs) != 1 {
		t.Fatalf("the archiver's own fold was lost: %+v", idx.Segments)
	}
}
