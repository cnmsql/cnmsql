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
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestPrestopProceedsWithoutCredentials proves the hook never fails or hangs
// the Pod's termination when it cannot read the control password: without a
// cluster it must return promptly, degrading to the reactive failover path.
func TestPrestopProceedsWithoutCredentials(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "default")
	t.Setenv("KUBERNETES_SERVICE_HOST", "") // not in a cluster: in-cluster config fails fast
	cmd := NewCommand()
	cmd.SetArgs([]string{"--cluster-name=demo", "--socket=/nonexistent.sock"})
	start := time.Now()
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("prestop must not fail the hook: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("prestop blocked on credentials")
	}
}

func TestWaitUntilDemotedReturnsWhenReadOnly(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// First poll: still the writable primary, with a replica to hand off to.
	// Second poll: demoted to replica.
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(0))
	expectStreamingReplicas(mock, 1)
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

// expectStreamingReplicas queues the one-off check for replicas streaming from
// this server.
func expectStreamingReplicas(mock sqlmock.Sqlmock, count int) {
	mock.ExpectQuery("information_schema.PROCESSLIST").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(count))
}

// A primary no replica streams from (a single-instance cluster) has nobody to
// hand off to, so the hook must not hold its shutdown for the whole timeout.
func TestWaitUntilDemotedReturnsImmediatelyWithoutReplicas(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(0))
	expectStreamingReplicas(mock, 0)

	start := time.Now()
	if err := WaitUntilDemoted(context.Background(), db, 5*time.Second, time.Second); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) >= time.Second {
		t.Fatal("WaitUntilDemoted waited on a primary with no replica to hand off to")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// When the replica check itself fails, the hook must keep waiting for a
// handoff rather than skip one it cannot rule out.
func TestWaitUntilDemotedKeepsWaitingWhenReplicaCheckFails(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(0))
	mock.ExpectQuery("information_schema.PROCESSLIST").WillReturnError(errors.New("access denied"))
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow(1))

	if err := WaitUntilDemoted(context.Background(), db, 5*time.Second, time.Millisecond); err != nil {
		t.Fatal(err)
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
	expectStreamingReplicas(mock, 1)
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

// TestWaitUntilDemotedHandlesOnOffSpelling proves the poll survives an engine
// that renders read_only as OFF/ON rather than 0/1. MariaDB 12 does, and
// scanning "ON" into a number fails, which the hook swallows: the demotion would
// never be seen and every drain would burn the full timeout before shutting the
// primary down, degrading a clean switchover into a reactive failover.
func TestWaitUntilDemotedHandlesOnOffSpelling(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow("OFF"))
	expectStreamingReplicas(mock, 1)
	mock.ExpectQuery("SELECT @@global.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"@@global.read_only"}).AddRow("ON"))

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
