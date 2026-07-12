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
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-logr/logr"
)

func TestLoopTickArchivesWhenPrimary(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Writable (super_read_only OFF) → primary.
	mock.ExpectQuery("super_read_only").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("0"))
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("binlog.000001", "500").
			AddRow("binlog.000002", "120"))

	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "data")
	writeBinlog(t, dir, "binlog.000002", "active")
	store := newMemStore()
	arch := newTestArchiver(t, store, dir, staticScan(map[string]string{"binlog.000001": testUUID + ":1-3"}))

	loop := NewLoop(LoopOptions{Reader: NewReader(db), Archiver: arch, Logger: logr.Discard()})
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)

	state := loop.State()
	if !state.Active {
		t.Fatal("loop should be active on primary")
	}
	if state.LastArchivedBinlog != "binlog.000001" {
		t.Fatalf("frontier = %q", state.LastArchivedBinlog)
	}
	if state.LastError != "" {
		t.Fatalf("unexpected error: %q", state.LastError)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLoopTickSkipsWhenReplica(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// super_read_only ON → replica, archiving must not run (no SHOW BINARY LOGS).
	mock.ExpectQuery("super_read_only").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("1"))

	dir := t.TempDir()
	store := newMemStore()
	arch := newTestArchiver(t, store, dir, staticScan(nil))
	loop := NewLoop(LoopOptions{Reader: NewReader(db), Archiver: arch, Logger: logr.Discard()})
	loop.mu.Lock()
	loop.state.Active = true
	loop.mu.Unlock()

	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)

	if loop.State().Active {
		t.Fatal("loop should be inactive on a replica")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// fakeProbe is a ReplicationProbe with a canned answer.
type fakeProbe struct {
	streaming bool
	err       error
}

func (p fakeProbe) Streaming(context.Context) (bool, error) { return p.streaming, p.err }

// drainFixture builds a demoted former primary: it archived binlog.000001 while
// it was writable (so it owns a segment), then died holding the closed-but-never
// -shipped binlog.000002 — the tail a crash strands. binlog.000003 is the file
// mysqld opened on restart, and must never be touched.
func drainFixture(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *Archiver, *memStore) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "shipped")
	writeBinlog(t, dir, "binlog.000002", "stranded")
	writeBinlog(t, dir, "binlog.000003", "active")
	store := newMemStore()
	arch := newTestArchiver(t, store, dir, staticScan(map[string]string{
		"binlog.000001": testUUID + ":1-3",
		"binlog.000002": testUUID + ":4-9",
	}))

	// Seed the segment: archive binlog.000001 as the primary once, so the instance
	// owns an archive identity when it is later demoted.
	seed := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}})
	if _, err := arch.ArchivePending(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	return db, mock, arch, store
}

// archivedNames lists the binlog basenames present in the store.
func archivedNames(store *memStore) []string {
	var out []string
	for key := range store.objects {
		if base := filepath.Base(key); strings.HasPrefix(base, "binlog.") &&
			!strings.HasSuffix(base, ".json") {
			out = append(out, base)
		}
	}
	sort.Strings(out)
	return out
}

// expectDemotedWithLogs primes a read-only server that reports all three files.
func expectDemotedWithLogs(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("super_read_only").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("1"))
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size"}).
			AddRow("binlog.000001", "500").
			AddRow("binlog.000002", "300").
			AddRow("binlog.000003", "120"))
}

// A demoted primary whose source accepted its GTID position is proven to be an
// ancestor of the surviving timeline, so the tail it stranded is shipped. Without
// this the transactions in binlog.000002 exist only on its PVC, and a recovery
// spanning them fails against a hole no segment can bridge.
func TestLoopDrainsStrandedTailWhenReplicating(t *testing.T) {
	t.Parallel()
	db, mock, arch, store := drainFixture(t)
	expectDemotedWithLogs(mock)

	loop := NewLoop(LoopOptions{
		Reader: NewReader(db), Archiver: arch, Logger: logr.Discard(),
		Replication: fakeProbe{streaming: true},
	})
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)

	state := loop.State()
	if state.Active {
		t.Fatal("a draining instance is not an active archiver")
	}
	if state.LastArchivedBinlog != "binlog.000002" {
		t.Fatalf("frontier = %q, want the stranded binlog.000002 shipped", state.LastArchivedBinlog)
	}
	if state.LastError != "" {
		t.Fatalf("unexpected error: %q", state.LastError)
	}
	// The active log stays untouched: a non-writable server must not archive the
	// file mysqld is still writing.
	if got, want := archivedNames(store), []string{"binlog.000001", "binlog.000002"}; !slices.Equal(got, want) {
		t.Fatalf("archived %v, want %v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// The safety property: a former primary that cannot replicate may be diverged —
// its final transactions may sit on a dead branch whose sequence numbers the
// promoted server has since reused for different transactions. Archiving them
// would let the merge-by-sequence planner replay the dead branch. So a
// non-streaming instance ships nothing, and recovery keeps failing closed.
func TestLoopDrainRefusesWhenNotReplicating(t *testing.T) {
	t.Parallel()
	db, mock, arch, store := drainFixture(t)
	// Only the writability probe runs: no SHOW BINARY LOGS, no upload.
	mock.ExpectQuery("super_read_only").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("1"))

	loop := NewLoop(LoopOptions{
		Reader: NewReader(db), Archiver: arch, Logger: logr.Discard(),
		Replication: fakeProbe{streaming: false},
	})
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)

	// The possibly-errant binlog.000002 stays on disk: unproven history never
	// enters the archive.
	if got, want := archivedNames(store), []string{"binlog.000001"}; !slices.Equal(got, want) {
		t.Fatalf("archived %v, want only the pre-demotion %v", got, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A replica that was never promoted owns no segment. Its binlogs are a re-logged
// copy of the primary's history, not stranded originals, so it stays silent —
// which is what keeps the drain from turning every replica into an archiver.
func TestLoopDrainSkipsReplicaWithoutSegment(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("super_read_only").WillReturnRows(
		sqlmock.NewRows([]string{"v"}).AddRow("1"))

	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "relogged")
	arch := newTestArchiver(t, newMemStore(), dir, staticScan(nil))

	loop := NewLoop(LoopOptions{
		Reader: NewReader(db), Archiver: arch, Logger: logr.Discard(),
		Replication: fakeProbe{streaming: true},
	})
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)

	if got := loop.State().LastArchivedBinlog; got != "" {
		t.Fatalf("a never-promoted replica archived %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFileBeforeAndPendingAfter(t *testing.T) {
	t.Parallel()
	logs := MarkActive([]BinaryLog{
		{Name: "binlog.000001"}, {Name: "binlog.000002"},
		{Name: "binlog.000003"}, {Name: "binlog.000004"},
	})
	if got := fileBefore(logs, "binlog.000003"); got != "binlog.000002" {
		t.Fatalf("fileBefore = %q", got)
	}
	if got := fileBefore(logs, "binlog.000001"); got != "" {
		t.Fatalf("fileBefore earliest = %q", got)
	}
	// Archivable = 1,2,3 (4 is active). Frontier at 2 ⇒ one pending (3).
	if got := pendingAfter(logs, "binlog.000002"); got != 1 {
		t.Fatalf("pendingAfter = %d, want 1", got)
	}
	if got := pendingAfter(logs, "binlog.000003"); got != 0 {
		t.Fatalf("pendingAfter frontier-at-last = %d, want 0", got)
	}
}
