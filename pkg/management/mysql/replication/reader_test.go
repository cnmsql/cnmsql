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

package replication

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const testSourceHost = "primary.svc"

func TestParseReplicaStatusModernColumns(t *testing.T) {
	row := map[string]string{
		"Source_Host":           testSourceHost,
		"Replica_IO_Running":    "Yes",
		"Replica_SQL_Running":   "Yes",
		"Seconds_Behind_Source": "3",
		"Retrieved_Gtid_Set":    "uuid:1-50",
		"Last_Error":            "",
	}
	state := parseReplicaStatus(row)

	if !state.Configured || state.SourceHost != testSourceHost {
		t.Errorf("unexpected state: %+v", state)
	}
	if !state.IORunning || !state.SQLRunning {
		t.Errorf("threads should be running: %+v", state)
	}
	if state.SecondsBehindSource == nil || *state.SecondsBehindSource != 3 {
		t.Errorf("lag = %v", state.SecondsBehindSource)
	}
}

func TestParseReplicaStatusLegacyColumns(t *testing.T) {
	row := map[string]string{
		"Master_Host":           testSourceHost,
		"Slave_IO_Running":      "No",
		"Slave_SQL_Running":     "Yes",
		"Seconds_Behind_Master": "NULL",
		"Last_Error":            "connection refused",
	}
	state := parseReplicaStatus(row)

	if state.SourceHost != testSourceHost {
		t.Errorf("source host = %q", state.SourceHost)
	}
	if state.IORunning {
		t.Errorf("IO thread should be stopped")
	}
	if !state.SQLRunning {
		t.Errorf("SQL thread should be running")
	}
	if state.SecondsBehindSource != nil {
		t.Errorf("lag should be nil on NULL, got %v", *state.SecondsBehindSource)
	}
	if state.LastError != "connection refused" {
		t.Errorf("last error = %q", state.LastError)
	}
}

func TestReplicaStateNotConfigured(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// SHOW REPLICA STATUS returns zero rows on an unconfigured instance.
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running"}))

	m := NewManager(db, mustParse(t, "8.0.36"))
	state, err := m.ReplicaState(context.Background())
	if err != nil {
		t.Fatalf("ReplicaState: %v", err)
	}
	if state.Configured {
		t.Errorf("expected not configured")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestReplicaStateConfigured(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running", "Seconds_Behind_Source"}).
		AddRow(testSourceHost, "Yes", "Yes", "0")
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)

	m := NewManager(db, mustParse(t, "8.0.36"))
	state, err := m.ReplicaState(context.Background())
	if err != nil {
		t.Fatalf("ReplicaState: %v", err)
	}
	if !state.Configured || state.SourceHost != testSourceHost || !state.IORunning {
		t.Errorf("unexpected state: %+v", state)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestReadOnlyReadsBothFlags(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT @@GLOBAL.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("1"))
	mock.ExpectQuery("SELECT @@GLOBAL.super_read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))

	m := NewManager(db, mustParse(t, "8.0.36"))
	state, err := m.ReadOnly(context.Background())
	if err != nil {
		t.Fatalf("ReadOnly: %v", err)
	}
	if !state.ReadOnly || state.SuperReadOnly {
		t.Errorf("unexpected read-only state: %+v", state)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestReadOnlySkipsSuperOnLegacy(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Only read_only is queried before super_read_only exists.
	mock.ExpectQuery("SELECT @@GLOBAL.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))

	m := NewManager(db, mustParse(t, "5.7.7"))
	state, err := m.ReadOnly(context.Background())
	if err != nil {
		t.Fatalf("ReadOnly: %v", err)
	}
	if state.ReadOnly || state.SuperReadOnly {
		t.Errorf("unexpected read-only state: %+v", state)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestParseBoolAndYesNo(t *testing.T) {
	for _, s := range []string{"1", "ON", "true", "YES"} {
		if !parseBool(s) {
			t.Errorf("parseBool(%q) should be true", s)
		}
	}
	for _, s := range []string{"0", "OFF", "", "no"} {
		if parseBool(s) {
			t.Errorf("parseBool(%q) should be false", s)
		}
	}
	if !parseYesNo("Yes") || parseYesNo("No") {
		t.Errorf("parseYesNo broken")
	}
}
