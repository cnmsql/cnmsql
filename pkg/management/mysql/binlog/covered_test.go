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
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// promoted is a replica (testUUID) just promoted over a primary (otherUUID)
// that archived its own binlogs. While it replicated it re-logged the old
// primary's transactions: binlog.000001 holds 11-20 and binlog.000002 21-30.
// binlog.000003 holds the first transactions it authored, binlog.000004 is
// open.
type promoted struct {
	t     *testing.T
	a     *Archiver
	store *memStore
	now   time.Time
	scans int
}

func newPromoted(t *testing.T, oldPrimaryArchived string, skipCovered bool) *promoted {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"binlog.000001", "binlog.000002", "binlog.000003", "binlog.000004"} {
		writeBinlog(t, dir, name, name)
	}
	p := &promoted{t: t, store: newMemStore(), now: time.Unix(1700000000, 0).UTC()}
	if oldPrimaryArchived != "" {
		p.setOldPrimaryCoverage(oldPrimaryArchived)
	}
	a, err := NewArchiver(ArchiverOptions{
		Store:       p.store,
		ObjectStore: testObjectStore,
		ClusterName: "demo", InstanceName: "demo-1", ServerUUID: testUUID,
		BinlogDir: dir,
		Scan: countingScan(withPrevious(staticScan(map[string]string{
			"binlog.000001": otherUUID + ":11-20",
			"binlog.000002": otherUUID + ":21-30",
			"binlog.000003": testUUID + ":1-5",
		}), map[string]string{"binlog.000003": otherUUID + ":1-30"}), &p.scans),
		Now:         func() time.Time { return p.now },
		SkipCovered: skipCovered,
	})
	if err != nil {
		t.Fatal(err)
	}
	p.a = a
	return p
}

// setOldPrimaryCoverage rewrites the old primary's segment, keeping the
// promoted instance's.
func (p *promoted) setOldPrimaryCoverage(gtids string) {
	p.t.Helper()
	idx := objectstore.ArchiveIndex{}
	key := objectstore.ArchiveIndexKey(testObjectStore, "demo")
	if ok, _ := p.store.Exists(context.Background(), testObjectStore.Bucket, key); ok {
		idx = readIndex(p.t, p.store)
	}
	kept := []objectstore.ArchiveSegment{{
		ServerUUID: otherUUID, InstanceName: "demo-2", Binlogs: []string{"binlog.000050"}, GTIDSet: gtids,
	}}
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
	seedIndex(p.t, p.store, idx)
}

func (p *promoted) logs() []BinaryLog {
	return MarkActive([]BinaryLog{
		{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"}, {Name: "binlog.000004"},
	})
}

func (p *promoted) archive() ArchiveResult {
	p.t.Helper()
	res, err := p.a.ArchivePending(context.Background(), p.logs())
	if err != nil {
		p.t.Fatal(err)
	}
	return res
}

// A promoted replica does not ship again what the old primary archived. The
// file that straddles the old primary's last archived transaction carries its
// unarchived tail, so it ships whole.
func TestPromotedPrimarySkipsWhatTheArchiveHolds(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, otherUUID+":1-25", true)
	res := p.archive()

	if got, want := archivedNames(p.store), []string{"binlog.000002", "binlog.000003"}; !slices.Equal(got, want) {
		t.Fatalf("shipped %v, want %v", got, want)
	}
	idx := readIndex(t, p.store)
	listed := segmentByUUID(t, idx, testUUID).Binlogs
	if want := []string{"binlog.000002", "binlog.000003"}; !slices.Equal(listed, want) {
		t.Errorf("segment lists %v, want %v", listed, want)
	}
	if want := otherUUID + ":1-30," + testUUID + ":1-5"; !sameGTIDSet(t, idx.CoveredGTIDSet, want) {
		t.Errorf("covered = %q, want %q", idx.CoveredGTIDSet, want)
	}

	// The frontier passes the file left out: nothing is pending, and the purge
	// gate knows which transactions it holds.
	if res.LastArchivedBinlog != "binlog.000003" || pendingAfter(p.logs(), res.LastArchivedBinlog) != 0 {
		t.Errorf("frontier = %q, want every closed file accounted for", res.LastArchivedBinlog)
	}
	want := ArchivedFile{Name: "binlog.000001", GTIDSet: otherUUID + ":11-20"}
	if len(res.Files) != 3 || res.Files[0] != want {
		t.Errorf("files = %+v, want %+v first of three", res.Files, want)
	}
}

// A primary that left out the binlogs re-logging its predecessor still records
// that it had executed them: restore reads a failback from it.
func TestPromotedPrimaryRecordsWhatItHadExecuted(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, otherUUID+":1-30", true)
	p.archive()

	seg := segmentByUUID(t, readIndex(t, p.store), testUUID)
	if want := testUUID + ":1-5"; !sameGTIDSet(t, seg.GTIDSet, want) {
		t.Fatalf("segment holds %q, want only %q", seg.GTIDSet, want)
	}
	if want := otherUUID + ":1-30," + testUUID + ":1-5"; !sameGTIDSet(t, seg.SeenGTIDSet, want) {
		t.Errorf("seen = %q, want %q", seg.SeenGTIDSet, want)
	}
}

// The record of what the server had executed is an aid to restore: a set that
// does not parse never stops the archive.
func TestUnreadablePreviousGTIDsDoesNotStopArchiving(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, otherUUID+":1-30", true)
	p.a.scan = withPrevious(p.a.scan, map[string]string{"binlog.000003": otherUUID + ":1-30,"})
	p.archive()

	seg := segmentByUUID(t, readIndex(t, p.store), testUUID)
	if !slices.Equal(seg.Binlogs, []string{"binlog.000003"}) || seg.SeenGTIDSet != "" {
		t.Errorf("segment = %v seen %q, want the file archived and nothing recorded", seg.Binlogs, seg.SeenGTIDSet)
	}
}

// When every closed file is covered, the pass writes nothing and still
// accounts for all of them.
func TestPromotedPrimaryWithNothingToShip(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, otherUUID+":1-30,"+testUUID+":1-5", true)
	objects := len(p.store.objects)
	res := p.archive()

	if len(p.store.objects) != objects {
		t.Fatalf("wrote %d objects, want none", len(p.store.objects)-objects)
	}
	if res.LastArchivedBinlog != "binlog.000003" || len(res.Files) != 3 || len(res.Archived) != 0 {
		t.Errorf("result = %+v, want three files accounted for and none shipped", res)
	}
}

// A file left out is decoded once, and the index is not read again on every
// pass.
func TestCoveredFileIsJudgedCheaply(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, otherUUID+":1-30,"+testUUID+":1-5", true)
	p.archive()
	scans := p.scans

	// Within the refresh interval a change to the index goes unseen.
	p.setOldPrimaryCoverage(otherUUID + ":1-14:18-30," + testUUID + ":1-5")
	p.archive()
	if p.scans != scans {
		t.Errorf("decoded %d more files on the second pass", p.scans-scans)
	}
	if got := archivedNames(p.store); len(got) != 0 {
		t.Errorf("shipped %v: the coverage was read again within its refresh interval", got)
	}
}

// Coverage can shrink: retention drops a segment whose files all expired. A
// file left out is judged again and ships once the archive lacks it.
func TestCoveredFileShipsOnceTheCoverageShrinks(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, otherUUID+":1-30,"+testUUID+":1-5", true)
	p.archive()
	scans := p.scans

	p.setOldPrimaryCoverage(otherUUID + ":1-14:18-30," + testUUID + ":1-5")
	p.now = p.now.Add(coverageRefresh)
	p.archive()
	if got, want := archivedNames(p.store), []string{"binlog.000001"}; !slices.Equal(got, want) {
		t.Fatalf("shipped %v, want %v", got, want)
	}
	if p.scans != scans {
		t.Errorf("decoded the file again (%d scans) to ship it", p.scans-scans)
	}
}

// An index that lists nothing (none yet, or a stale read) leaves nothing out.
func TestPromotedPrimaryShipsEverythingWithoutCoverage(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, "", true)
	p.archive()
	want := []string{"binlog.000001", "binlog.000002", "binlog.000003"}
	if got := archivedNames(p.store); !slices.Equal(got, want) {
		t.Fatalf("shipped %v, want %v", got, want)
	}
}

// The coverage read while primary does not survive a demotion.
func TestForgetCoverageReadsTheIndexAgain(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, otherUUID+":1-14", true)
	p.archive()
	if got := len(archivedNames(p.store)); got != 3 {
		t.Fatalf("shipped %d files, want all three", got)
	}
	if p.a.coverageAt.IsZero() {
		t.Fatal("the coverage was never read")
	}
	p.a.ForgetCoverage()
	if !p.a.coverageAt.IsZero() {
		t.Fatal("the coverage outlived ForgetCoverage")
	}
}

// MariaDB positions are not judged: every closed file ships.
func TestSkipCoveredOffShipsEverything(t *testing.T) {
	t.Parallel()
	p := newPromoted(t, otherUUID+":1-30", false)
	p.archive()
	want := []string{"binlog.000001", "binlog.000002", "binlog.000003"}
	if got := archivedNames(p.store); !slices.Equal(got, want) {
		t.Fatalf("shipped %v, want %v", got, want)
	}
}

func sameGTIDSet(t *testing.T, got, want string) bool {
	t.Helper()
	a, b := newMysqlGTIDSet(), newMysqlGTIDSet()
	if err := a.Parse(got); err != nil {
		t.Fatal(err)
	}
	if err := b.Parse(want); err != nil {
		t.Fatal(err)
	}
	return a.Contains(b) && b.Contains(a)
}

// withPrevious adds to a scan the set the server had executed when it opened
// the file.
func withPrevious(scan Scanner, previous map[string]string) Scanner {
	return func(ctx context.Context, path string) (ScanResult, error) {
		res, err := scan(ctx, path)
		res.PreviousGTIDs = previous[filepath.Base(path)]
		return res, err
	}
}
