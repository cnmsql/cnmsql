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
