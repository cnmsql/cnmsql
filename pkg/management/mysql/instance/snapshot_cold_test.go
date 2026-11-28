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

package instance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// armedColdController returns a fenced controller with mysqld stopped and a
// cold capture armed for "nightly".
func armedColdController(t *testing.T) (*Controller, sqlmock.Sqlmock, *fakeSupervisor, string) {
	t.Helper()
	sup := &fakeSupervisor{pid: 0}
	c, mock := newController(t, sup)
	gate := NewFenceGate()
	gate.Fence()
	c.SetFenceGate(gate)
	dataDir := t.TempDir()
	c.SetSnapshotConfig(SnapshotConfig{DataDir: dataDir})
	req := webserver.SnapshotBeginRequest{BackupName: "nightly", Cold: true}
	if _, err := c.BeginSnapshot(context.Background(), req); err != nil {
		t.Fatalf("arming a cold capture: %v", err)
	}
	return c, mock, sup, dataDir
}

func expectColdPosition(mock sqlmock.Sqlmock) {
	mock.ExpectPing()
	mock.ExpectQuery(q("SELECT @@GLOBAL.gtid_executed")).WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("uuid:1-42"))
	mock.ExpectQuery(q("SHOW MASTER STATUS")).WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position"}).AddRow("binlog.000009", "157"))
	mock.ExpectQuery(q("SELECT @@GLOBAL.version")).WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))
}

func TestColdSnapshotArmRequiresAFencedStoppedServer(t *testing.T) {
	sup := &fakeSupervisor{pid: 42}
	c, _ := newController(t, sup)
	gate := NewFenceGate()
	c.SetFenceGate(gate)
	c.SetSnapshotConfig(SnapshotConfig{DataDir: t.TempDir()})
	req := webserver.SnapshotBeginRequest{BackupName: "nightly", Cold: true}
	if _, err := c.BeginSnapshot(context.Background(), req); !errors.Is(err, webserver.ErrSnapshotNotStopped) {
		t.Fatalf("arm on a running server: err = %v", err)
	}
	gate.Fence()
	if _, err := c.BeginSnapshot(context.Background(), req); !errors.Is(err, webserver.ErrSnapshotNotStopped) {
		t.Fatalf("arm on a fenced server still running: err = %v", err)
	}
}

func TestColdSnapshotArmHoldsReplicationOnTheNextStart(t *testing.T) {
	c, _, _, dataDir := armedColdController(t)
	cold := c.SnapshotState().Cold
	if cold == nil || cold.BackupName != "nightly" || cold.Anchor != nil || cold.Error != "" {
		t.Fatalf("armed state = %+v", cold)
	}
	mysql := engine.MustForFlavor(engine.FlavorMySQL)
	got := ColdSnapshotStartArgs(dataDir, mysql, mustVersion(t, "8.0.36"))
	if !slices.Equal(got, []string{"--skip-replica-start"}) {
		t.Fatalf("MySQL 8.0.36 start args = %v", got)
	}
	mariadb := engine.MustForFlavor(engine.FlavorMariaDB)
	got = ColdSnapshotStartArgs(dataDir, mariadb, mustVersion(t, "11.4.2"))
	if !slices.Equal(got, []string{"--skip-slave-start"}) {
		t.Fatalf("MariaDB start args = %v", got)
	}
	if got := ColdSnapshotStartArgs(t.TempDir(), mysql, mustVersion(t, "8.0.36")); got != nil {
		t.Fatalf("start args without a capture = %v", got)
	}
	// Arming again for the same Backup is harmless.
	req := webserver.SnapshotBeginRequest{BackupName: "nightly", Cold: true}
	if _, err := c.BeginSnapshot(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func TestColdSnapshotCaptureReadsTheAnchorBeforeReplicationStarts(t *testing.T) {
	c, mock, sup, dataDir := armedColdController(t)

	// mysqld is still down: replication stays held and nothing is queried.
	if err := c.EnsureReplicaStarted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	sup.pid = 42
	expectColdPosition(mock)
	expectReplicaState(mock, true, false)
	mock.ExpectExec(q("START REPLICA")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := c.EnsureReplicaStarted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the anchor must be read before replication starts: %v", err)
	}
	cold := c.SnapshotState().Cold
	if cold == nil || cold.Anchor == nil || cold.Anchor.AnchorGTID != "uuid:1-42" ||
		cold.Anchor.BinlogFile != "binlog.000009" || cold.Anchor.BinlogPos != 157 {
		t.Fatalf("captured state = %+v", cold)
	}
	mysql := engine.MustForFlavor(engine.FlavorMySQL)
	if got := ColdSnapshotStartArgs(dataDir, mysql, mustVersion(t, "8.0.36")); got != nil {
		t.Fatalf("a captured anchor still holds replication: %v", got)
	}

	if err := end(c, "nightly"); err != nil {
		t.Fatalf("clearing the capture: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, coldSnapshotFile)); !os.IsNotExist(err) {
		t.Fatalf("capture record left behind: %v", err)
	}
	if err := end(c, "nightly"); !errors.Is(err, webserver.ErrSnapshotNotHeld) {
		t.Fatalf("second end: err = %v", err)
	}
}

func TestColdSnapshotCaptureGivesUpAndResumesReplication(t *testing.T) {
	c, mock, sup, _ := armedColdController(t)
	sup.pid = 42
	for attempt := 1; attempt < maxColdCaptureAttempts; attempt++ {
		mock.ExpectPing()
		mock.ExpectQuery(q("SELECT @@GLOBAL.gtid_executed")).WillReturnError(errors.New("boom"))
		if err := c.EnsureReplicaStarted(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("replication started while the capture had attempts left: %v", err)
	}
	mock.ExpectPing()
	mock.ExpectQuery(q("SELECT @@GLOBAL.gtid_executed")).WillReturnError(errors.New("boom"))
	expectReplicaState(mock, true, false)
	mock.ExpectExec(q("START REPLICA")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := c.EnsureReplicaStarted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if cold := c.SnapshotState().Cold; cold == nil || cold.Error == "" || cold.Anchor != nil {
		t.Fatalf("given-up state = %+v", cold)
	}
}

func TestColdSnapshotRecordIsAnAdoptionLeftover(t *testing.T) {
	dataDir := t.TempDir()
	if err := writeColdSnapshotRecord(dataDir, coldSnapshotRecord{BackupName: "nightly"}); err != nil {
		t.Fatal(err)
	}
	if err := removeAdoptLeftovers(dataDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, coldSnapshotFile)); !os.IsNotExist(err) {
		t.Fatalf("a restored volume kept the source's capture record: %v", err)
	}
}
