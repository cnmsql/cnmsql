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

package prestop

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestWaitUntilDemotedReturnsWhenReadOnly(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// First poll: still the writable primary. Second poll: demoted to replica.
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(0))
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(1))

	start := time.Now()
	if err := WaitUntilDemoted(context.Background(), db, 5*time.Second, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) >= 5*time.Second {
		t.Fatal("WaitUntilDemoted blocked for the full timeout instead of returning on demotion")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitUntilDemotedReturnsImmediatelyForReplica(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// A replica is already read_only=ON, so the first poll returns.
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(1))

	if err := WaitUntilDemoted(context.Background(), db, 5*time.Second, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitUntilDemotedDegradesOnTimeout(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// The primary is never demoted: every poll reports read_only=OFF. The hook
	// must still return nil (never fail the Pod's termination) once the timeout
	// elapses, degrading to the operator's reactive failover path.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(0)).
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(0))

	start := time.Now()
	if err := WaitUntilDemoted(context.Background(), db, 30*time.Millisecond, 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("returned after %s, expected to wait out the ~30ms timeout", elapsed)
	}
}
