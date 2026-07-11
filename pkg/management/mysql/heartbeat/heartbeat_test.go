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

package heartbeat

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-logr/logr"
	"github.com/go-sql-driver/mysql"
)

const (
	superReadOnlyQuery = "SELECT @@GLOBAL.super_read_only"
	readOnlyQuery      = "SELECT @@GLOBAL.read_only"
	readQuery          = "SELECT TIMESTAMPDIFF(MICROSECOND, MAX(ts), UTC_TIMESTAMP(6)) FROM `heartbeat`.`heartbeat`"
)

func newTestLoop(t *testing.T) (*Loop, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewLoop(db, Config{}, logr.Discard()), mock
}

// readOnly queues the writability probe, answered by super_read_only as MySQL
// would.
func readOnly(mock sqlmock.Sqlmock, ro bool) {
	mock.ExpectQuery(regexp.QuoteMeta(superReadOnlyQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"ro"}).AddRow(ro))
}

// TestTickOnReplicaReadsWithoutStamping proves a replica never writes to the
// heartbeat table. A stamp applied on a replica would be a transaction its
// primary never issued, which is an errant transaction, and would strand the
// replica as permanently diverged.
func TestTickOnReplicaReadsWithoutStamping(t *testing.T) {
	loop, mock := newTestLoop(t)

	readOnly(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(int64(2_500_000)))

	loop.tick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected statements: %v", err)
	}
	state := loop.State()
	if state.Writing {
		t.Error("a read-only instance reported itself as the heartbeat writer")
	}
	if !state.LagKnown || state.Lag != 2500*time.Millisecond {
		t.Errorf("lag = %v (known=%v), want 2.5s known", state.Lag, state.LagKnown)
	}
}

// TestTickOnPrimaryStampsThenReads proves the writable primary creates the table
// on first pass and stamps it.
func TestTickOnPrimaryStampsThenReads(t *testing.T) {
	loop, mock := newTestLoop(t)

	readOnly(mock, false)
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO `heartbeat`.`heartbeat`").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(int64(0)))

	loop.tick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected statements: %v", err)
	}
	if state := loop.State(); !state.Writing {
		t.Error("the writable primary did not report itself as the heartbeat writer")
	}
}

// TestStampFallsBackToReadOnlyWithoutSuperReadOnly proves the writer works on
// MariaDB, which has no super_read_only at all: asking for it fails the whole
// statement with ER_UNKNOWN_SYSTEM_VARIABLE. Without the fallback the probe
// errored on every tick, so the primary never created the table and no instance
// in a MariaDB cluster ever reported a lag.
func TestStampFallsBackToReadOnlyWithoutSuperReadOnly(t *testing.T) {
	loop, mock := newTestLoop(t)

	mock.ExpectQuery(regexp.QuoteMeta(superReadOnlyQuery)).
		WillReturnError(&mysql.MySQLError{Number: errUnknownSystemVariable, Message: "unknown system variable"})
	mock.ExpectQuery(regexp.QuoteMeta(readOnlyQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"ro"}).AddRow(false))
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(int64(1_000_000)))

	loop.tick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected statements: %v", err)
	}
	state := loop.State()
	if !state.Writing {
		t.Error("a writable MariaDB primary did not stamp the heartbeat")
	}
	if !state.LagKnown || state.Lag != time.Second {
		t.Errorf("lag = %v (known=%v), want 1s known", state.Lag, state.LagKnown)
	}
}

// TestStampOnReadOnlyMariaDBDoesNotWrite is the other half of the fallback: a
// MariaDB replica must still be recognised as read-only through read_only alone,
// or the loop would stamp on a replica and strand it with an errant transaction.
func TestStampOnReadOnlyMariaDBDoesNotWrite(t *testing.T) {
	loop, mock := newTestLoop(t)

	mock.ExpectQuery(regexp.QuoteMeta(superReadOnlyQuery)).
		WillReturnError(&mysql.MySQLError{Number: errUnknownSystemVariable, Message: "unknown system variable"})
	mock.ExpectQuery(regexp.QuoteMeta(readOnlyQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"ro"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(int64(0)))

	loop.tick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected statements: %v", err)
	}
	if loop.State().Writing {
		t.Error("a read-only MariaDB replica stamped the heartbeat table")
	}
}

// TestSchemaIsCreatedOnceProves the DDL is not replayed on every stamp: it is
// replicated, and re-issuing it every second would fill the binary log with
// no-ops.
func TestSchemaIsCreatedOnce(t *testing.T) {
	loop, mock := newTestLoop(t)

	readOnly(mock, false)
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(int64(0)))
	// Second pass: no DDL, straight to the stamp.
	readOnly(mock, false)
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(int64(0)))

	loop.tick(context.Background())
	loop.tick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected statements: %v", err)
	}
}

// TestReadWithoutTableIsNotAnError proves a replica polled before any primary
// has stamped the table reports no reading rather than an error. It is the
// normal state of a cluster that is still bootstrapping.
func TestReadWithoutTableIsNotAnError(t *testing.T) {
	loop, mock := newTestLoop(t)

	readOnly(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnError(&mysql.MySQLError{Number: errNoSuchTable, Message: "no such table"})

	loop.tick(context.Background())

	state := loop.State()
	if state.LagKnown {
		t.Error("a missing heartbeat table produced a lag reading")
	}
	if state.LastError != "" {
		t.Errorf("a missing heartbeat table was reported as an error: %q", state.LastError)
	}
}

// TestReadOfEmptyTableIsNotAnError proves MAX() over no rows (the table exists
// but nothing has stamped it) reads as no reading, not as zero lag. Zero lag
// would mean "perfectly in sync", which is the opposite of what an empty table
// tells us.
func TestReadOfEmptyTableIsNotAnError(t *testing.T) {
	loop, mock := newTestLoop(t)

	readOnly(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(nil))

	loop.tick(context.Background())

	if loop.State().LagKnown {
		t.Error("an empty heartbeat table produced a lag reading")
	}
}

// TestNegativeLagClampsToZero proves a replica whose clock trails the primary's
// reports zero rather than a negative lag, which no caller can act on.
func TestNegativeLagClampsToZero(t *testing.T) {
	loop, mock := newTestLoop(t)

	readOnly(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(int64(-750_000)))

	loop.tick(context.Background())

	state := loop.State()
	if !state.LagKnown || state.Lag != 0 {
		t.Errorf("lag = %v (known=%v), want 0 known", state.Lag, state.LagKnown)
	}
}

// TestFailedReadInvalidatesTheLastReading proves a read failure drops the lag
// rather than leaving the previous value standing. A stale lag is worse than no
// lag: the failover bound would take it for a fresh one.
func TestFailedReadInvalidatesTheLastReading(t *testing.T) {
	loop, mock := newTestLoop(t)

	readOnly(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"micros"}).AddRow(int64(1_000_000)))
	readOnly(mock, true)
	mock.ExpectQuery(regexp.QuoteMeta(readQuery)).
		WillReturnError(&mysql.MySQLError{Number: 1040, Message: "too many connections"})

	loop.tick(context.Background())
	if !loop.State().LagKnown {
		t.Fatal("the first pass produced no lag reading")
	}

	loop.tick(context.Background())
	state := loop.State()
	if state.LagKnown {
		t.Error("a failed read left the previous lag reading standing")
	}
	if state.LastError == "" {
		t.Error("a failed read recorded no error")
	}
}
