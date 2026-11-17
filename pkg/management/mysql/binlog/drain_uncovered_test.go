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
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

func TestJudgeDrainFile(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, file, covered string
		want                drainVerdict
	}{
		{"the archive holds all of it", otherUUID + ":11-20", otherUUID + ":1-30", drainRedundant},
		{"the archive has a hole in it", otherUUID + ":11-20", otherUUID + ":1-14:18-30", drainShip},
		{"the archive has not reached it yet", otherUUID + ":11-20", otherUUID + ":1-14", drainWait},
		{"the archive has not seen that server yet", otherUUID + ":1-5", testUUID + ":1-9", drainWait},
		{"its own stranded tail", testUUID + ":10-15", testUUID + ":1-9," + otherUUID + ":1-3", drainShip},
		{"its own tail, already re-logged by the successor", testUUID + ":10-15", testUUID + ":1-15", drainRedundant},
		{"no transactions", "", otherUUID + ":1-30", drainRedundant},
	}
	for _, c := range cases {
		got, err := judgeDrainFile(c.file, c.covered, testUUID)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s: verdict = %v, want %v", c.name, got, c.want)
		}
	}
}

// drainer is a demoted former primary (testUUID) whose binlogs re-log what the
// current primary (otherUUID) writes. binlog.000001 holds 11-20, binlog.000002
// 21-30, binlog.000003 is open.
type drainer struct {
	t     *testing.T
	a     *Archiver
	store *memStore
	now   time.Time
}

func newDrainer(t *testing.T, primaryCovers string) *drainer {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"binlog.000001", "binlog.000002", "binlog.000003"} {
		writeBinlog(t, dir, name, name)
	}
	d := &drainer{t: t, store: newMemStore(), now: time.Unix(1700000000, 0).UTC()}
	d.setPrimaryCoverage(primaryCovers)
	a, err := NewArchiver(ArchiverOptions{
		Store:       d.store,
		ObjectStore: testObjectStore,
		ClusterName: "demo", InstanceName: "demo-1", ServerUUID: testUUID,
		BinlogDir: dir,
		Scan: staticScan(map[string]string{
			"binlog.000001": otherUUID + ":11-20",
			"binlog.000002": otherUUID + ":21-30",
		}),
		Now:                func() time.Time { return d.now },
		DrainOnlyUncovered: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	d.a = a
	return d
}

// setPrimaryCoverage rewrites the primary's segment, keeping the drainer's.
func (d *drainer) setPrimaryCoverage(gtids string) {
	d.t.Helper()
	idx := objectstore.ArchiveIndex{}
	key := objectstore.ArchiveIndexKey(testObjectStore, "demo")
	if ok, _ := d.store.Exists(context.Background(), testObjectStore.Bucket, key); ok {
		idx = readIndex(d.t, d.store)
	}
	primary := objectstore.ArchiveSegment{
		ServerUUID: otherUUID, InstanceName: "demo-2", Binlogs: []string{"binlog.000050"}, GTIDSet: gtids,
	}
	kept := []objectstore.ArchiveSegment{primary}
	covered := newMysqlGTIDSet()
	_ = covered.Parse(gtids)
	for _, seg := range idx.Segments {
		if seg.ServerUUID != otherUUID {
			kept = append(kept, seg)
			set := newMysqlGTIDSet()
			_ = set.Parse(seg.GTIDSet)
			covered.Union(set)
		}
	}
	idx.Segments = kept
	idx.CoveredGTIDSet = covered.String()
	seedIndex(d.t, d.store, idx)
}

func (d *drainer) drain() ArchiveResult {
	d.t.Helper()
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"}})
	res, err := d.a.DrainPending(context.Background(), logs, func(string, ScanResult) (bool, error) { return true, nil })
	if err != nil {
		d.t.Fatal(err)
	}
	// Past the coverage cache, so the next drain reads the index again.
	d.now = d.now.Add(drainCoverageRefresh + time.Second)
	return res
}

// A former primary keeps re-logging the primary's transactions. What the
// archive already holds is not shipped a second time.
func TestDrainSkipsFilesTheArchiveAlreadyHolds(t *testing.T) {
	t.Parallel()
	d := newDrainer(t, otherUUID+":1-30")
	res := d.drain()
	if got := archivedNames(d.store); len(got) != 0 {
		t.Fatalf("shipped %v, want nothing: the archive holds every transaction", got)
	}
	if res.Deferred != "" {
		t.Errorf("deferred %q: a redundant file is not a stranded one", res.Deferred)
	}
}

// A file ahead of what the primary has archived waits: the primary ships
// those transactions itself within a rotation.
func TestDrainWaitsForThePrimaryToArchiveFirst(t *testing.T) {
	t.Parallel()
	d := newDrainer(t, otherUUID+":1-20")
	d.drain()
	if got := archivedNames(d.store); len(got) != 0 {
		t.Fatalf("shipped %v before the primary had the chance to", got)
	}
	d.setPrimaryCoverage(otherUUID + ":1-30")
	d.drain()
	if got := archivedNames(d.store); len(got) != 0 {
		t.Fatalf("shipped %v although the primary archived it", got)
	}
}

// When the archive has moved past a file and still lacks some of it, the
// index has a hole (an entry lost to a stale write, a binlog the primary
// never shipped): the former primary's copy fills it.
func TestDrainShipsAFileThatFillsAHole(t *testing.T) {
	t.Parallel()
	d := newDrainer(t, otherUUID+":1-14:18-30")
	d.drain()
	if got, want := archivedNames(d.store), []string{"binlog.000001"}; !slices.Equal(got, want) {
		t.Fatalf("shipped %v, want %v", got, want)
	}
	if got, want := readIndex(t, d.store).CoveredGTIDSet, otherUUID+":1-30"; got != want {
		t.Errorf("covered = %q, want %q", got, want)
	}
}

// A hole can open after the file was judged redundant.
func TestDrainShipsAFileOnceAHoleOpens(t *testing.T) {
	t.Parallel()
	d := newDrainer(t, otherUUID+":1-30")
	d.drain()
	d.setPrimaryCoverage(otherUUID + ":1-22:26-30")
	d.drain()
	if got, want := archivedNames(d.store), []string{"binlog.000002"}; !slices.Equal(got, want) {
		t.Fatalf("shipped %v, want %v", got, want)
	}
}

// The index is not read again on every pass.
func TestDrainReusesTheCoverageItRead(t *testing.T) {
	t.Parallel()
	d := newDrainer(t, otherUUID+":1-30")
	d.drain()
	d.now = d.now.Add(-drainCoverageRefresh)
	d.setPrimaryCoverage(otherUUID + ":1-22:26-30")
	d.drain()
	if got := archivedNames(d.store); len(got) != 0 {
		t.Fatalf("shipped %v: the coverage was read again within its refresh interval", got)
	}
}
