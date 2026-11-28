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
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// withHoldConn gives c a sqlmock-backed hold connection and returns its mock.
func withHoldConn(t *testing.T, c *Controller, dataDir string) sqlmock.Sqlmock {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c.openSnapshotConn = func(ctx context.Context) (*sql.Conn, func(), error) {
		conn, err := db.Conn(ctx)
		return conn, func() { _ = conn.Close() }, err
	}
	c.SetSnapshotConfig(SnapshotConfig{DataDir: dataDir})
	return mock
}

func q(s string) string { return regexp.QuoteMeta(s) }

// expectReplicaState answers the control pool's SHOW REPLICA STATUS.
func expectReplicaState(mock sqlmock.Sqlmock, replica, sqlRunning bool) {
	rows := sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"})
	if replica {
		running := "No"
		if sqlRunning {
			running = "Yes"
		}
		rows.AddRow("primary.svc", "Yes", running)
	}
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(rows)
}

// expectHold registers the FTWRL and position reads on the hold connection.
func expectHold(mock sqlmock.Sqlmock, lockWait, gtidQuery, gtid, statusQuery string) {
	mock.ExpectExec(q("SET SESSION lock_wait_timeout = " + lockWait)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(q("FLUSH TABLES WITH READ LOCK")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(q(gtidQuery)).WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(gtid))
	mock.ExpectQuery(q(statusQuery)).WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("binlog.000007", "4242", "", ""))
	mock.ExpectQuery(q("SELECT @@GLOBAL.version")).WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36-28"))
}

func TestSnapshotHoldOnReplicaStopsApplierAndWritesManifest(t *testing.T) {
	c, control := newController(t, nil)
	dir := t.TempDir()
	hold := withHoldConn(t, c, dir)

	expectReplicaState(control, true, true)
	hold.ExpectExec(q("STOP REPLICA SQL_THREAD")).WillReturnResult(sqlmock.NewResult(0, 0))
	expectHold(hold, "20", "SELECT @@GLOBAL.gtid_executed", "uuid:1-10,\nother:1-3", "SHOW MASTER STATUS")

	anchor, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "nightly", Timeout: "1m"})
	if err != nil {
		t.Fatal(err)
	}
	want := webserver.SnapshotAnchor{
		AnchorGTID: "uuid:1-10,other:1-3", BinlogFile: "binlog.000007", BinlogPos: 4242,
		Flavor: "mysql", ServerVersion: "8.0.36-28",
	}
	if *anchor != want {
		t.Fatalf("anchor = %+v, want %+v", *anchor, want)
	}
	if !c.SnapshotHeld() || c.snapshotHoldStatus().BackupName != "nightly" {
		t.Fatalf("hold not reported: %+v", c.snapshotHoldStatus())
	}

	manifest, err := ReadSnapshotManifest(dir)
	if err != nil || manifest == nil {
		t.Fatalf("manifest = %v, %v", manifest, err)
	}
	if manifest.BackupName != "nightly" || manifest.AnchorGTID != want.AnchorGTID ||
		manifest.BinlogFile != "binlog.000007" || manifest.BinlogPos != 4242 || !manifest.Online ||
		manifest.Instance != "cluster-1" || manifest.Flavor != "mysql" {
		t.Fatalf("manifest = %+v", *manifest)
	}

	hold.ExpectExec(q("UNLOCK TABLES")).WillReturnResult(sqlmock.NewResult(0, 0))
	hold.ExpectExec(q("START REPLICA SQL_THREAD")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := c.EndSnapshot(context.Background(), webserver.SnapshotEndRequest{BackupName: "nightly"}); err != nil {
		t.Fatal(err)
	}
	if c.SnapshotHeld() {
		t.Fatal("hold still reported after end")
	}
	if err := hold.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if err := control.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotBeginIsIdempotentAndRefusesAnotherBackup(t *testing.T) {
	c, control := newController(t, nil)
	hold := withHoldConn(t, c, t.TempDir())
	expectReplicaState(control, false, false)
	expectHold(hold, "20", "SELECT @@GLOBAL.gtid_executed", "uuid:1-5", "SHOW MASTER STATUS")

	first, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "a"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "a"})
	if err != nil || *again != *first {
		t.Fatalf("repeated begin = %+v, %v; want %+v", again, err, first)
	}
	if _, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "b"}); !errors.Is(err, webserver.ErrSnapshotHoldConflict) {
		t.Fatalf("begin for another backup = %v, want conflict", err)
	}
	if err := c.EndSnapshot(context.Background(), webserver.SnapshotEndRequest{BackupName: "b"}); !errors.Is(err, webserver.ErrSnapshotHoldConflict) {
		t.Fatalf("end for another backup = %v, want conflict", err)
	}
	// A primary: nothing stopped, so release only unlocks.
	hold.ExpectExec(q("UNLOCK TABLES")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := c.EndSnapshot(context.Background(), webserver.SnapshotEndRequest{BackupName: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := hold.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotHoldReleasesItselfOnTimeout(t *testing.T) {
	c, control := newController(t, nil)
	hold := withHoldConn(t, c, t.TempDir())
	expectReplicaState(control, true, true)
	hold.ExpectExec(q("STOP REPLICA SQL_THREAD")).WillReturnResult(sqlmock.NewResult(0, 0))
	expectHold(hold, "1", "SELECT @@GLOBAL.gtid_executed", "uuid:1-5", "SHOW MASTER STATUS")
	hold.ExpectExec(q("UNLOCK TABLES")).WillReturnResult(sqlmock.NewResult(0, 0))
	hold.ExpectExec(q("START REPLICA SQL_THREAD")).WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "slow", Timeout: "50ms"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.SnapshotHeld() {
		if time.Now().After(deadline) {
			t.Fatal("hold was not released on timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := c.EndSnapshot(context.Background(), webserver.SnapshotEndRequest{BackupName: "slow"}); !errors.Is(err, webserver.ErrSnapshotHoldExpired) {
		t.Fatalf("end after expiry = %v, want expired", err)
	}
	if err := hold.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotBeginUndoesAFailedLock(t *testing.T) {
	c, control := newController(t, nil)
	hold := withHoldConn(t, c, t.TempDir())
	expectReplicaState(control, true, true)
	hold.ExpectExec(q("STOP REPLICA SQL_THREAD")).WillReturnResult(sqlmock.NewResult(0, 0))
	hold.ExpectExec(q("SET SESSION lock_wait_timeout = 20")).WillReturnResult(sqlmock.NewResult(0, 0))
	hold.ExpectExec(q("FLUSH TABLES WITH READ LOCK")).WillReturnError(errors.New("Lock wait timeout exceeded"))
	hold.ExpectExec(q("UNLOCK TABLES")).WillReturnResult(sqlmock.NewResult(0, 0))
	hold.ExpectExec(q("START REPLICA SQL_THREAD")).WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "x"}); err == nil {
		t.Fatal("begin succeeded over a failed lock")
	}
	if c.SnapshotHeld() {
		t.Fatal("a failed begin left a hold")
	}
	if err := hold.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotBeginRejectsBadRequests(t *testing.T) {
	c, _ := newController(t, nil)
	if _, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "x"}); err == nil {
		t.Fatal("begin without a snapshot config succeeded")
	}
	withHoldConn(t, c, t.TempDir())
	for _, req := range []webserver.SnapshotBeginRequest{
		{},
		{BackupName: "x", Timeout: "soon"},
		{BackupName: "x", Timeout: "-1s"},
	} {
		if _, err := c.BeginSnapshot(context.Background(), req); !errors.Is(err, webserver.ErrInvalidSnapshotRequest) {
			t.Errorf("begin %+v = %v, want invalid", req, err)
		}
	}
	if err := c.EndSnapshot(context.Background(), webserver.SnapshotEndRequest{BackupName: "x"}); !errors.Is(err, webserver.ErrSnapshotNotHeld) {
		t.Fatalf("end with nothing held = %v, want not held", err)
	}
}

func TestSnapshotHoldDialects(t *testing.T) {
	t.Run("MySQL 8.4 reads SHOW BINARY LOG STATUS", func(t *testing.T) {
		db, control, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		c, err := NewController("i", db, "8.4.3", webserver.RoleUnknown, nil, engine.MustForFlavor(engine.FlavorMySQL))
		if err != nil {
			t.Fatal(err)
		}
		hold := withHoldConn(t, c, t.TempDir())
		expectReplicaState(control, false, false)
		expectHold(hold, "20", "SELECT @@GLOBAL.gtid_executed", "uuid:1-5", "SHOW BINARY LOG STATUS")
		if _, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "x"}); err != nil {
			t.Fatal(err)
		}
		if err := hold.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("MariaDB stops the slave SQL thread and reads gtid_binlog_pos", func(t *testing.T) {
		db, control, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		c, err := NewController("i", db, "11.4.3", webserver.RoleUnknown, nil, engine.MustForFlavor(engine.FlavorMariaDB))
		if err != nil {
			t.Fatal(err)
		}
		hold := withHoldConn(t, c, t.TempDir())
		control.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(
			sqlmock.NewRows([]string{"Master_Host", "Slave_IO_Running", "Slave_SQL_Running"}).AddRow("p", "Yes", "Yes"))
		hold.ExpectExec(q("STOP SLAVE SQL_THREAD")).WillReturnResult(sqlmock.NewResult(0, 0))
		expectHold(hold, "20", "SELECT @@GLOBAL.gtid_binlog_pos", "0-1-42", "SHOW MASTER STATUS")
		anchor, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "x"})
		if err != nil {
			t.Fatal(err)
		}
		if anchor.AnchorGTID != "0-1-42" || anchor.Flavor != "mariadb" {
			t.Fatalf("anchor = %+v", *anchor)
		}
		hold.ExpectExec(q("UNLOCK TABLES")).WillReturnResult(sqlmock.NewResult(0, 0))
		hold.ExpectExec(q("START SLAVE SQL_THREAD")).WillReturnResult(sqlmock.NewResult(0, 0))
		if err := c.EndSnapshot(context.Background(), webserver.SnapshotEndRequest{BackupName: "x"}); err != nil {
			t.Fatal(err)
		}
		if err := hold.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a Group Replication member keeps its applier", func(t *testing.T) {
		c, control := newController(t, nil)
		c.EnableGroupReplication()
		hold := withHoldConn(t, c, t.TempDir())
		expectHold(hold, "20", "SELECT @@GLOBAL.gtid_executed", "group:1-9", "SHOW MASTER STATUS")
		if _, err := c.BeginSnapshot(context.Background(), webserver.SnapshotBeginRequest{BackupName: "x"}); err != nil {
			t.Fatal(err)
		}
		if err := control.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		if err := hold.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSnapshotStateReportsFenceAndStoppedServer(t *testing.T) {
	sup := &fakeSupervisor{pid: 42}
	c, _ := newController(t, sup)
	gate := NewFenceGate()
	c.SetFenceGate(gate)
	if state := c.SnapshotState(); state.Fenced || state.MysqldStopped {
		t.Fatalf("running instance state = %+v", state)
	}
	gate.Fence()
	if state := c.SnapshotState(); !state.Fenced || state.MysqldStopped {
		t.Fatalf("fenced, still running state = %+v", state)
	}
	sup.pid = 0
	if state := c.SnapshotState(); !state.Fenced || !state.MysqldStopped {
		t.Fatalf("fenced and stopped state = %+v", state)
	}
}

func TestSnapshotManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if got, err := ReadSnapshotManifest(dir); got != nil || err != nil {
		t.Fatalf("missing manifest = %v, %v", got, err)
	}
	want := SnapshotManifest{BackupName: "b", AnchorGTID: "u:1-3", BinlogFile: "binlog.000002", BinlogPos: 9, Online: true, TakenAt: "t"}
	if err := WriteSnapshotManifest(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadSnapshotManifest(dir)
	if err != nil || *got != want {
		t.Fatalf("manifest = %+v, %v; want %+v", got, err, want)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != SnapshotManifestFile {
		t.Fatalf("data dir holds %v, want only the manifest", entries)
	}
	if err := os.WriteFile(filepath.Join(dir, SnapshotManifestFile), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSnapshotManifest(dir); err == nil {
		t.Fatal("a corrupt manifest decoded")
	}
}
