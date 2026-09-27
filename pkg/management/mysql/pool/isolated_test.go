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

package pool

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// noRows stands in for a statement result that touches no rows.
var noRows = sqlmock.NewResult(0, 0)

func newIsolatedFixture(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func TestRunIsolatedBoundsTheLockWaitAndRestoresTheSession(t *testing.T) {
	db, mock := newIsolatedFixture(t)
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION lock_wait_timeout = 15")).WillReturnResult(noRows)
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = ON")).WillReturnResult(noRows)
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL super_read_only = ON")).WillReturnResult(noRows)
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION lock_wait_timeout = DEFAULT")).WillReturnResult(noRows)

	err := RunIsolated(context.Background(), db, 15*time.Second,
		"SET GLOBAL read_only = ON", "SET GLOBAL super_read_only = ON")
	if err != nil {
		t.Fatalf("RunIsolated: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A statement that hits the bound fails the run with the statement named, and
// the session default is restored even then: the connection goes back to the
// shared pool, which the next borrower must not inherit the bound from.
func TestRunIsolatedReportsAStatementFailure(t *testing.T) {
	db, mock := newIsolatedFixture(t)
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION lock_wait_timeout = 15")).WillReturnResult(noRows)
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = ON")).
		WillReturnError(errors.New("Error 1205: Lock wait timeout exceeded"))
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION lock_wait_timeout = DEFAULT")).WillReturnResult(noRows)

	err := RunIsolated(context.Background(), db, 15*time.Second, "SET GLOBAL read_only = ON")
	if err == nil || !regexp.MustCompile(`executing "SET GLOBAL read_only = ON"`).MatchString(err.Error()) {
		t.Fatalf("err = %v, want the failing statement named", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// A failed restore surfaces on its own when the statements ran: the caller
// retries, which is safe for the idempotent statements this is used for.
func TestRunIsolatedReportsARestoreFailure(t *testing.T) {
	db, mock := newIsolatedFixture(t)
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION lock_wait_timeout = 15")).WillReturnResult(noRows)
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = ON")).WillReturnResult(noRows)
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION lock_wait_timeout = DEFAULT")).
		WillReturnError(errors.New("Error 2006: MySQL server has gone away"))

	if err := RunIsolated(context.Background(), db, 15*time.Second, "SET GLOBAL read_only = ON"); err == nil {
		t.Fatal("a failed session restore was silently dropped")
	}
}

// A lock wait bound of zero would disable the wait entirely (lock_wait_timeout
// 0 fails immediately); the smallest meaningful bound is one second.
func TestRunIsolatedClampsTheBound(t *testing.T) {
	db, mock := newIsolatedFixture(t)
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION lock_wait_timeout = 1")).WillReturnResult(noRows)
	mock.ExpectExec(regexp.QuoteMeta("SET GLOBAL read_only = ON")).WillReturnResult(noRows)
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION lock_wait_timeout = DEFAULT")).WillReturnResult(noRows)

	if err := RunIsolated(context.Background(), db, 0, "SET GLOBAL read_only = ON"); err != nil {
		t.Fatalf("RunIsolated: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRunIsolatedWithoutStatementsTouchesNothing(t *testing.T) {
	db, mock := newIsolatedFixture(t)
	if err := RunIsolated(context.Background(), db, time.Second); err != nil {
		t.Fatalf("RunIsolated: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
