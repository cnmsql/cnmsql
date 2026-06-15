/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package binlog

import (
	"context"
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
