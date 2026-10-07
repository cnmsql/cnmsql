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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-logr/logr"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// A primary that forced a rotation and shipped everything stamps the index's
// ArchivedThrough; while it has written nothing since, the stamp keeps up with
// the clock. Its first pass rotates out whatever the active log held when it
// started, even on an idle server.
func TestLoopStampsArchivedThrough(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "old")
	writeBinlog(t, dir, "binlog.000002", "active")
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{oldSegment(otherUUID + ":1-5")}})
	src := &executedSource{executed: otherUUID + ":1-5," + testUUID + ":1-3"}
	arch := newForkArchiver(t, store, dir, staticScan(map[string]string{"binlog.000001": testUUID + ":1-3"}), src.source)
	loop := NewLoop(LoopOptions{
		Reader: NewReader(db), Archiver: arch, Logger: logr.Discard(), FlushInterval: time.Nanosecond,
	})

	var lastFlush time.Time
	var lastSize int64
	// First pass: baseline unknown, nothing flushed, nothing stamped.
	mock.ExpectQuery("super_read_only").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(sqlmock.NewRows([]string{"Log_name", "File_size"}).
		AddRow("binlog.000001", "100"))
	loop.tick(context.Background(), &lastFlush, &lastSize)
	if got := readIndex(t, store).ArchivedThrough; !got.IsZero() {
		t.Fatalf("stamped %v before any rotation", got)
	}

	// Second pass: the idle active log is rotated out once, shipped, and the
	// archive is complete through now.
	writeBinlog(t, dir, "binlog.000002", "rotated")
	mock.ExpectQuery("super_read_only").WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(sqlmock.NewRows([]string{"Log_name", "File_size"}).
		AddRow("binlog.000001", "100"))
	mock.ExpectExec("FLUSH").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(sqlmock.NewRows([]string{"Log_name", "File_size"}).
		AddRow("binlog.000001", "100").AddRow("binlog.000002", "157"))
	before := time.Now()
	loop.tick(context.Background(), &lastFlush, &lastSize)
	got := readIndex(t, store).ArchivedThrough
	if got.Before(before) {
		t.Fatalf("archivedThrough = %v, want at least %v", got, before)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
