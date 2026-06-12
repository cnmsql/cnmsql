/*
Copyright 2026 The CNMySQL Authors.

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

	"github.com/DATA-DOG/go-sqlmock"
)

func TestReaderListBinaryLogs(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "File_size", "Encrypted"}).
			AddRow("binlog.000001", "1000", "No").
			AddRow("binlog.000002", "2000", "No").
			AddRow("binlog.000003", "500", "No"),
	)

	logs, err := NewReader(db).ListBinaryLogs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 3 {
		t.Fatalf("got %d logs", len(logs))
	}
	if !logs[2].Active || logs[0].Active || logs[1].Active {
		t.Fatalf("only the last log should be active: %+v", logs)
	}
	if logs[1].SizeBytes != 2000 {
		t.Fatalf("size = %d", logs[1].SizeBytes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderServerUUID(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("server_uuid").WillReturnRows(
		sqlmock.NewRows([]string{"@@GLOBAL.server_uuid"}).AddRow("3e11fa47-71ca-11e1-9e33-c80aa9429562"),
	)
	uuid, err := NewReader(db).ServerUUID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if uuid != "3e11fa47-71ca-11e1-9e33-c80aa9429562" {
		t.Fatalf("uuid = %q", uuid)
	}
}

func TestReaderFlushAndPurge(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectExec("FLUSH BINARY LOGS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("PURGE BINARY LOGS TO").WillReturnResult(sqlmock.NewResult(0, 0))

	r := NewReader(db)
	if err := r.FlushLogs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.PurgeLogsTo(context.Background(), "binlog.000005"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
