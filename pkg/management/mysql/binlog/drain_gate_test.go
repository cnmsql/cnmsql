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
	"database/sql"
	"slices"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-logr/logr"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// fakeView is a ClusterView with canned answers.
type fakeView struct {
	primary, position string
	known             bool
	diverged          []string
	timeline          engine.MariaDBTimeline
	hasTimeline       bool
}

func (v *fakeView) Primary() (string, string, bool) { return v.primary, v.position, v.known }
func (v *fakeView) Diverged(name string) bool       { return slices.Contains(v.diverged, name) }
func (v *fakeView) Timeline() (engine.MariaDBTimeline, bool) {
	return v.timeline, v.hasTimeline
}

// strandedSeed is the listing a former primary had while it archived.
func strandedSeed() []BinaryLog {
	return MarkActive([]BinaryLog{{Name: shippedLog}, {Name: strandedLog}})
}

// drainTick runs one non-writable tick of a demoted former primary named
// demo-1 whose source accepted it.
func drainTick(t *testing.T, loop *Loop, mock sqlmock.Sqlmock) {
	t.Helper()
	expectDemotedWithLogs(mock)
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)
}

func drainLoop(arch *Archiver, db *sql.DB, view ClusterView, mariadb bool) *Loop {
	return NewLoop(LoopOptions{
		Reader: NewReader(db), Archiver: arch, Logger: logr.Discard(),
		Replication: fakeProbe{streaming: true},
		Cluster:     view, Instance: "demo-1", MariaDB: mariadb,
	})
}

// Case 5 of the design: the stranded tail is canonical, but the current
// primary's recorded position does not show it yet. The drain defers rather
// than trusting acceptance by the source, and ships on the first tick after the
// position refresh covers it.
func TestDrainDefersUntilThePrimaryPositionCoversTheFile(t *testing.T) {
	t.Parallel()
	db, mock, arch, store := drainFixture(t)
	view := &fakeView{primary: "demo-2", position: testUUID + ":1-5", known: true}
	loop := drainLoop(arch, db, view, false)

	drainTick(t, loop, mock)
	if got, want := archivedNames(store), []string{shippedLog}; !slices.Equal(got, want) {
		t.Fatalf("archived %v, want the stranded file deferred", got)
	}
	state := loop.State()
	if state.DeferredFile != strandedLog || state.PendingFiles != 1 || state.LastArchivedBinlog != shippedLog {
		t.Fatalf("state = %+v, want %s deferred and still pending", state, strandedLog)
	}

	view.position = testUUID + ":1-9"
	drainTick(t, loop, mock)
	if got, want := archivedNames(store), []string{shippedLog, strandedLog}; !slices.Equal(got, want) {
		t.Fatalf("archived %v, want the stranded file shipped once covered", got)
	}
	if state := loop.State(); state.DeferredFile != "" || state.LastArchivedBinlog != strandedLog {
		t.Fatalf("state = %+v", state)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A diverged instance never drains, even while its replication streams (MySQL
// AUTO_POSITION accepts errant transactions).
func TestDrainBlockedForADivergedInstance(t *testing.T) {
	t.Parallel()
	db, mock, arch, store := drainFixture(t)
	view := &fakeView{primary: "demo-2", position: testUUID + ":1-9", known: true, diverged: []string{"demo-1"}}
	loop := drainLoop(arch, db, view, false)
	mock.ExpectQuery("super_read_only").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("1"))
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)
	if got, want := archivedNames(store), []string{shippedLog}; !slices.Equal(got, want) {
		t.Fatalf("a diverged instance archived %v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Without a known primary position, or when this instance is (still) named the
// primary, nothing proves the tail canonical.
func TestDrainDefersWithoutAProvingPrimary(t *testing.T) {
	t.Parallel()
	for name, view := range map[string]*fakeView{
		"no cluster view":          nil,
		"unknown primary position": {primary: "demo-2", known: false},
		"this instance is primary": {primary: "demo-1", position: testUUID + ":1-9", known: true},
		"primary has no position":  {primary: "demo-2", position: "", known: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, mock, arch, store := drainFixture(t)
			var cv ClusterView
			if view != nil {
				cv = view
			}
			loop := drainLoop(arch, db, cv, false)
			drainTick(t, loop, mock)
			if got, want := archivedNames(store), []string{shippedLog}; !slices.Equal(got, want) {
				t.Fatalf("archived %v, want the tail deferred", got)
			}
		})
	}
}

// Case 7: a replica that received the dead tail and went down keeps its stale
// position on record. Only the current primary's position counts, so that
// stale record never authorises the drain.
func TestDrainIgnoresOtherInstancesPositions(t *testing.T) {
	t.Parallel()
	db, mock, arch, store := drainFixture(t)
	// The primary never received 4-9; a stale replica record would have.
	view := &fakeView{primary: "demo-2", position: testUUID + ":1-3", known: true}
	loop := drainLoop(arch, db, view, false)
	drainTick(t, loop, mock)
	if got, want := archivedNames(store), []string{shippedLog}; !slices.Equal(got, want) {
		t.Fatalf("archived %v", got)
	}
}

// Files ship in order: a deferred file stops the pass, so a later file the
// primary happens to cover never moves the frontier past the gap.
func TestDrainStopsAtTheFirstDeferredFile(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	for _, n := range []string{"binlog.000001", "binlog.000002", "binlog.000003", "binlog.000004"} {
		writeBinlog(t, dir, n, n)
	}
	store := newMemStore()
	arch := newTestArchiver(t, store, dir, staticScan(map[string]string{
		"binlog.000001": testUUID + ":1-3",
		"binlog.000002": testUUID + ":4-9",
		"binlog.000003": otherUUID + ":1-2",
	}))
	seed := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}})
	if _, err := arch.ArchivePending(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	view := &fakeView{primary: "demo-2", position: testUUID + ":1-3," + otherUUID + ":1-2", known: true}
	loop := drainLoop(arch, db, view, false)
	mock.ExpectQuery("super_read_only").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("1"))
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(sqlmock.NewRows([]string{"Log_name", "File_size"}).
		AddRow("binlog.000001", "1").AddRow("binlog.000002", "1").AddRow("binlog.000003", "1").AddRow("binlog.000004", "1"))
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)

	if got, want := archivedNames(store), []string{"binlog.000001"}; !slices.Equal(got, want) {
		t.Fatalf("archived %v, want nothing past the deferred binlog.000002", got)
	}
	state := loop.State()
	if state.DeferredFile != "binlog.000002" || state.PendingFiles != 2 {
		t.Fatalf("state = %+v, want binlog.000002 deferred and two files pending", state)
	}
}

// A file the gate keeps deferring is decoded once, not on every tick: the scan
// is cached against the file's identity.
func TestDrainScansADeferredFileOnce(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	writeBinlog(t, dir, shippedLog, "shipped")
	writeBinlog(t, dir, strandedLog, "stranded")
	writeBinlog(t, dir, activeLog3, "active")
	scans := 0
	arch := newTestArchiver(t, newMemStore(), dir, countingScan(staticScan(map[string]string{
		shippedLog: testUUID + ":1-3", strandedLog: testUUID + ":4-9",
	}), &scans))
	if _, err := arch.ArchivePending(context.Background(), strandedSeed()); err != nil {
		t.Fatal(err)
	}
	scans = 0
	loop := drainLoop(arch, db, &fakeView{primary: "demo-2", position: testUUID + ":1-3", known: true}, false)
	for range 3 {
		drainTick(t, loop, mock)
	}
	if scans != 1 {
		t.Fatalf("the deferred file was decoded %d times over three ticks, want 1", scans)
	}
}

// The drain is a demoted instance: its own executed set is no authority, so its
// index writes never run the fork check, whatever fork source it carries.
func TestDrainNeverRunsTheForkCheck(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	writeBinlog(t, dir, shippedLog, "shipped")
	writeBinlog(t, dir, strandedLog, "stranded")
	writeBinlog(t, dir, activeLog3, "active")
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-500")}})
	src := &executedSource{executed: testUUID + ":1-9"}
	scan := staticScan(map[string]string{shippedLog: testUUID + ":1-3", strandedLog: testUUID + ":4-9"})
	seedArch := newForkArchiver(t, store, dir, scan, nil)
	if _, err := seedArch.ArchivePending(context.Background(), strandedSeed()); err != nil {
		t.Fatal(err)
	}
	arch := newForkArchiver(t, store, dir, scan, src.source)
	loop := drainLoop(arch, db, &fakeView{primary: "demo-2", position: testUUID + ":1-9", known: true}, false)
	drainTick(t, loop, mock)

	if got, want := archivedNames(store), []string{shippedLog, strandedLog}; !slices.Equal(got, want) {
		t.Fatalf("archived %v", got)
	}
	if src.calls != 0 {
		t.Fatalf("the drain read the fork authority %d times", src.calls)
	}
	if fork := segmentByUUID(t, readIndex(t, store), "old-identity").Fork; fork != nil {
		t.Fatalf("the drain recorded a fork: %+v", fork)
	}
}

// mariadbDrainFixture is drainFixture for MariaDB: server 1 archived 0-1-3 while
// primary and stranded 0-1-4..0-1-9.
func mariadbDrainFixture(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *Archiver, *memStore) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	writeBinlog(t, dir, shippedLog, "shipped")
	writeBinlog(t, dir, strandedLog, "stranded")
	writeBinlog(t, dir, activeLog3, "active")
	store := newMemStore()
	model := engine.MustForFlavor(engine.FlavorMariaDB).GTID()
	arch, err := NewArchiver(ArchiverOptions{
		Store: store, ObjectStore: testObjectStore, ClusterName: "demo", InstanceName: "demo-1",
		ServerUUID: "token-1", BinlogDir: dir,
		Scan:   staticScan(map[string]string{shippedLog: "0-1-3", strandedLog: "0-1-9"}),
		Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		NewSet: func() GTIDOps { return NewMariadbGTIDSet(model) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := arch.ArchivePending(context.Background(), strandedSeed()); err != nil {
		t.Fatal(err)
	}
	return db, mock, arch, store
}

// On MariaDB position containment is blind to forks (a dead 0-1-9 compares as
// contained in 0-2-20), so the gate also requires the file to be on the
// timeline. Off-timeline and no-verdict files defer.
func TestDrainMariaDBGate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		view    *fakeView
		shipped bool
	}{
		{"canonical tail", &fakeView{primary: "demo-2", position: "0-2-20", known: true, hasTimeline: true,
			timeline: engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-9"}}}, true},
		{"dead tail", &fakeView{primary: "demo-2", position: "0-2-20", known: true, hasTimeline: true,
			timeline: engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-5"}}}, false},
		{"no verdict", &fakeView{primary: "demo-2", position: "0-2-20", known: true, hasTimeline: true,
			timeline: engine.MariaDBTimeline{{ServerID: 2, Handoff: "0-1-50"}}}, false},
		{"no timeline", &fakeView{primary: "demo-2", position: "0-2-20", known: true}, false},
		{"on the timeline but not yet in the primary's position", &fakeView{primary: "demo-2", position: "0-1-6",
			known: true, hasTimeline: true,
			timeline: engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-9"}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, mock, arch, store := mariadbDrainFixture(t)
			loop := drainLoop(arch, db, tc.view, true)
			drainTick(t, loop, mock)
			shipped := slices.Contains(archivedNames(store), strandedLog)
			if shipped != tc.shipped {
				t.Fatalf("stranded file shipped = %v, want %v", shipped, tc.shipped)
			}
		})
	}
}
