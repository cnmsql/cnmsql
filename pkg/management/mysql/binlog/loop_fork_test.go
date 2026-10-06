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
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-logr/logr"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// idleLoop is a primary with nothing to archive (only its active log) over an
// index another primary wrote.
func idleLoop(t *testing.T, store Store, src ForkSource) (*Loop, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "active")
	arch := newForkArchiver(t, store, dir, staticScan(nil), src)
	return NewLoop(LoopOptions{Reader: NewReader(db), Archiver: arch, Logger: logr.Discard()}), mock
}

func expectWritable(mock sqlmock.Sqlmock, writable bool) {
	v := "0"
	if !writable {
		v = "1"
	}
	mock.ExpectQuery("super_read_only").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(v))
	if writable {
		mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
			sqlmock.NewRows([]string{"Log_name", "File_size"}).AddRow("binlog.000001", "100"))
	}
}

func runTick(loop *Loop) {
	var lastFlush time.Time
	var lastSize int64
	loop.tick(context.Background(), &lastFlush, &lastSize)
}

// A promotion, failback or restart checks the archive on its first writable
// pass, without waiting for a rotation to write the index. Later passes with
// nothing to write do not re-check; losing writability re-arms the check.
func TestLoopChecksForksOnFirstWritablePass(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	src := &executedSource{executed: otherUUID + ":1-218"}
	loop, mock := idleLoop(t, store, src.source)

	expectWritable(mock, true)
	runTick(loop)
	if src.calls != 1 {
		t.Fatalf("first writable pass read the authority %d times, want 1", src.calls)
	}
	if fork := segmentByUUID(t, readIndex(t, store), "old-identity").Fork; fork == nil || fork.GTIDSet != otherUUID+":219" {
		t.Fatalf("fork = %+v", fork)
	}
	state := loop.State()
	if len(state.Forks) != 1 || state.Forks[0].GTIDs != otherUUID+":219" || state.ForkCheckedAt.IsZero() {
		t.Fatalf("state forks = %+v checkedAt=%v", state.Forks, state.ForkCheckedAt)
	}

	expectWritable(mock, true)
	runTick(loop)
	if src.calls != 1 {
		t.Fatalf("an idle pass re-checked (%d reads)", src.calls)
	}
	if got := loop.State(); len(got.Forks) != 1 {
		t.Fatalf("an idle pass dropped the reported forks: %+v", got.Forks)
	}

	expectWritable(mock, false)
	runTick(loop)
	if got := loop.State(); len(got.Forks) != 0 || !got.ForkCheckedAt.IsZero() {
		t.Fatalf("a demoted instance must stop reporting forks: %+v", got)
	}

	expectWritable(mock, true)
	runTick(loop)
	if src.calls != 2 {
		t.Fatalf("a re-promotion must check again (%d reads)", src.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A restarted primary, or a new one after a failover, reports the fork records
// already in the index, not only those it detects itself.
func TestLoopReportsForksAlreadyInTheIndex(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	old := oldSegment(otherUUID + ":1-219")
	old.Fork = &objectstore.ArchiveFork{GTIDSet: otherUUID + ":219", DetectedAt: time.Unix(1600000000, 0).UTC()}
	seedIndex(t, store, objectstore.ArchiveIndex{
		Segments:  []objectstore.ArchiveSegment{old},
		ForkCheck: &objectstore.ArchiveForkCheck{CheckedAt: time.Unix(1600000000, 0).UTC(), CheckedBy: "demo-2/x"},
	})
	src := &executedSource{executed: otherUUID + ":1-218"}
	loop, mock := idleLoop(t, store, src.source)

	expectWritable(mock, true)
	runTick(loop)
	state := loop.State()
	if len(state.Forks) != 1 || !state.Forks[0].DetectedAt.Equal(time.Unix(1600000000, 0).UTC()) {
		t.Fatalf("state forks = %+v", state.Forks)
	}
}

// A failing authority is an archiving error worth surfacing, but archiving
// carries on and the next writable pass retries the check.
func TestLoopReportsForkCheckErrors(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	src := &executedSource{err: errors.New("lost connection")}
	loop, mock := idleLoop(t, store, src.source)

	expectWritable(mock, true)
	runTick(loop)
	state := loop.State()
	if !state.Active || !strings.Contains(state.LastError, "lost connection") || !state.ForkCheckedAt.IsZero() {
		t.Fatalf("state = %+v, want an active archiver reporting the fork check error", state)
	}

	src.err = nil
	src.executed = otherUUID + ":1-218"
	expectWritable(mock, true)
	runTick(loop)
	if state := loop.State(); state.LastError != "" || len(state.Forks) != 1 {
		t.Fatalf("state = %+v, want the retried check to succeed", state)
	}
}

// Without a fork source there is nothing to check, and an idle primary must not
// read the index on every tick trying to.
func TestLoopWithoutForkSourceSkipsTheCheck(t *testing.T) {
	t.Parallel()
	store := newCountingStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-219")}})
	loop, mock := idleLoop(t, store, nil)
	before := store.gets
	expectWritable(mock, true)
	runTick(loop)
	if store.gets != before {
		t.Fatalf("an idle primary without a fork source read the index %d times", store.gets-before)
	}
}
