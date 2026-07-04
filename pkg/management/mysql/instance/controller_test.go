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
	"io"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func newController(t *testing.T, sup Supervisor) (*Controller, sqlmock.Sqlmock) {
	t.Helper()
	return newControllerWithRole(t, webserver.RoleUnknown, sup)
}

func newControllerWithRole(t *testing.T, role webserver.Role, sup Supervisor) (*Controller, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c, err := NewController("cluster-1", db, "8.0.36", role, sup, engine.MustForFlavor(engine.FlavorMySQL))
	if err != nil {
		t.Fatalf("NewController: %v", err)
	}
	return c, mock
}

// expectStatusQueries registers the queries Status issues. asReplica controls
// whether SHOW REPLICA STATUS returns a configured row.
func expectStatusQueries(mock sqlmock.Sqlmock, asReplica, ioRunning, sqlRunning bool) {
	roVal := "0"
	if asReplica {
		roVal = "1"
	}
	mock.ExpectQuery("SELECT @@GLOBAL.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(roVal))
	mock.ExpectQuery("SELECT @@GLOBAL.super_read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(roVal))

	replRows := sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"})
	if asReplica {
		yn := func(b bool) string {
			if b {
				return "Yes"
			}
			return "No"
		}
		replRows.AddRow("primary.svc", yn(ioRunning), yn(sqlRunning))
	}
	mock.ExpectQuery("SHOW REPLICA STATUS").WillReturnRows(replRows)
}

func expectBestEffortQueries(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT @@GLOBAL.version").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.0.36"))
	mock.ExpectQuery("SELECT @@GLOBAL.gtid_executed").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("uuid:1-10"))
	mock.ExpectQuery("SELECT @@GLOBAL.gtid_purged").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(""))
	mock.ExpectQuery("SELECT @@GLOBAL.rpl_semi_sync_source_enabled").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("1"))
	mock.ExpectQuery("SELECT @@GLOBAL.rpl_semi_sync_replica_enabled").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'Uptime'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("Uptime", "1234"))
}

func TestStatusPrimary(t *testing.T) {
	c, mock := newController(t, nil)

	// Status -> ReadOnly, ReplicaState, then Readyz (ping + ReplicaState), then best-effort.
	expectStatusQueries(mock, false, false, false)
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host"}))
	expectBestEffortQueries(mock)

	status, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Role != webserver.RolePrimary {
		t.Errorf("role = %q, want primary", status.Role)
	}
	if status.Replication != nil {
		t.Errorf("primary should have no replication status")
	}
	if status.GTIDExecuted != "uuid:1-10" || !status.SemiSync.SourceEnabled {
		t.Errorf("best-effort fields not populated: %+v", status)
	}
	if status.UptimeSeconds != 1234 {
		t.Errorf("uptime = %d", status.UptimeSeconds)
	}
}

func TestStatusIncludesArchiving(t *testing.T) {
	c, mock := newController(t, nil)

	expectStatusQueries(mock, false, false, false)
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host"}))
	expectBestEffortQueries(mock)

	c.SetArchivingProvider(func() *webserver.ArchivingStatus {
		return &webserver.ArchivingStatus{Active: true, LastArchivedBinlog: "binlog.000007", PendingFiles: 2}
	})

	status, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Archiving == nil || !status.Archiving.Active {
		t.Fatalf("archiving status missing: %+v", status.Archiving)
	}
	if status.Archiving.LastArchivedBinlog != "binlog.000007" || status.Archiving.PendingFiles != 2 {
		t.Fatalf("archiving status = %+v", status.Archiving)
	}
}

func TestStatusReplica(t *testing.T) {
	c, mock := newController(t, nil)

	expectStatusQueries(mock, true, true, true)
	mock.ExpectPing()
	// Readyz re-runs SHOW REPLICA STATUS.
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"}).
			AddRow("primary.svc", "Yes", "Yes"))
	expectBestEffortQueries(mock)

	status, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Role != webserver.RoleReplica {
		t.Errorf("role = %q, want replica", status.Role)
	}
	if status.Replication == nil || status.Replication.SourceHost != "primary.svc" {
		t.Errorf("replication status missing: %+v", status.Replication)
	}
	if !status.IsReady {
		t.Errorf("healthy replica should be ready")
	}
}

func TestStatusReportsLiveVersionAndUpgradeComplete(t *testing.T) {
	c, mock := newController(t, nil)

	expectStatusQueries(mock, false, false, false)
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host"}))
	// Live server version differs from the configured image version (8.0.36): the
	// instance has been upgraded, and Status must report the live value.
	mock.ExpectQuery("SELECT @@GLOBAL.version").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("8.4.3"))
	mock.ExpectQuery("SELECT @@GLOBAL.gtid_executed").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("uuid:1-10"))
	mock.ExpectQuery("SELECT @@GLOBAL.gtid_purged").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow(""))
	mock.ExpectQuery("SELECT @@GLOBAL.rpl_semi_sync_source_enabled").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))
	mock.ExpectQuery("SELECT @@GLOBAL.rpl_semi_sync_replica_enabled").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("0"))
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'Uptime'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("Uptime", "1"))

	status, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Version != "8.4.3" {
		t.Errorf("version = %q, want live 8.4.3", status.Version)
	}
	if !status.UpgradeComplete {
		t.Errorf("expected UpgradeComplete on a ready instance with a readable live version")
	}
}

func TestReadyzReplicaNotRunning(t *testing.T) {
	c, mock := newController(t, nil)
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"}).
			AddRow("primary.svc", "No", "Yes"))

	if err := c.Readyz(context.Background()); err == nil {
		t.Error("expected Readyz to fail when IO thread is down")
	}
}

func TestReadyzExpectedReplicaWithoutSource(t *testing.T) {
	c, mock := newControllerWithRole(t, webserver.RoleReplica, nil)
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"}))

	if err := c.Readyz(context.Background()); err == nil {
		t.Error("expected Readyz to fail when an expected replica has no source")
	}
}

func TestStatusExpectedReplicaWithoutSource(t *testing.T) {
	c, mock := newControllerWithRole(t, webserver.RoleReplica, nil)

	expectStatusQueries(mock, false, false, false)
	mock.ExpectPing()
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host"}))
	expectBestEffortQueries(mock)

	status, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Role != webserver.RoleUnknown {
		t.Errorf("role = %q, want unknown", status.Role)
	}
	if status.IsReady {
		t.Error("expected replica without source to be not ready")
	}
}

// Liveness does not depend on mysqld being up: a non-primary
// instance always passes without touching the database, so a stopped mysqld
// (e.g. while fenced) does not trip a kubelet restart.
func TestHealthzReplicaDoesNotPing(t *testing.T) {
	c, mock := newControllerWithRole(t, webserver.RoleReplica, nil)
	if err := c.Healthz(context.Background()); err != nil {
		t.Errorf("Healthz: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("liveness should issue no database calls: %v", err)
	}
}

// A primary liveness check exercises only the API-server isolation detector,
// never mysqld. With no detector wired it is healthy.
func TestHealthzPrimaryChecksIsolationNotMysqld(t *testing.T) {
	c, mock := newControllerWithRole(t, webserver.RolePrimary, nil)
	if err := c.Healthz(context.Background()); err != nil {
		t.Errorf("Healthz: %v", err)
	}
	c.SetIsolationDetector(NewIsolationDetector(time.Nanosecond))
	time.Sleep(time.Millisecond)
	if err := c.Healthz(context.Background()); err == nil {
		t.Error("expected isolated primary liveness to fail")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("liveness should issue no database calls: %v", err)
	}
}

// Startup gates on mysqld answering a ping, without the replication-health gate
// readiness applies.
func TestStartupzPings(t *testing.T) {
	c, mock := newController(t, nil)
	mock.ExpectPing()
	if err := c.Startupz(context.Background()); err != nil {
		t.Errorf("Startupz: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("ExpectationsWereMet: %v", err)
	}
}

type fakeSupervisor struct {
	called bool
	err    error
	pid    int
}

func (f *fakeSupervisor) Restart(context.Context) error  { f.called = true; return f.err }
func (f *fakeSupervisor) Shutdown(context.Context) error { f.called = true; return f.err }
func (f *fakeSupervisor) Pid() int                       { return f.pid }

func TestRestartUsesSupervisor(t *testing.T) {
	sup := &fakeSupervisor{}
	c, _ := newController(t, sup)
	if err := c.Restart(context.Background()); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !sup.called {
		t.Error("supervisor.Restart was not called")
	}
}

func TestRestartWithoutSupervisor(t *testing.T) {
	c, _ := newController(t, nil)
	if err := c.Restart(context.Background()); err == nil {
		t.Error("expected Restart to fail without a supervisor")
	}
}

func TestRestartInPlaceReExecsWithMysqldPID(t *testing.T) {
	t.Cleanup(func() { inPlaceUpgrading.Store(false) })
	sup := &fakeSupervisor{pid: 4242}
	c, _ := newController(t, sup)

	gotPID := make(chan int, 1)
	c.reExec = func(pid int) error { gotPID <- pid; return nil }

	if err := c.RestartInPlace(context.Background()); err != nil {
		t.Fatalf("RestartInPlace: %v", err)
	}
	if !IsInPlaceUpgrading() {
		t.Error("expected the in-flight upgrade flag to be set")
	}
	select {
	case pid := <-gotPID:
		if pid != 4242 {
			t.Errorf("re-exec pid = %d, want 4242", pid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("re-exec was never scheduled")
	}
}

func TestRestartInPlaceWithoutSupervisor(t *testing.T) {
	c, _ := newController(t, nil)
	if err := c.RestartInPlace(context.Background()); err == nil {
		t.Error("expected RestartInPlace to fail without a supervisor")
	}
}

func TestRestartInPlaceWithoutRunningMysqld(t *testing.T) {
	c, _ := newController(t, &fakeSupervisor{pid: 0})
	called := false
	c.reExec = func(int) error { called = true; return nil }
	if err := c.RestartInPlace(context.Background()); err == nil {
		t.Error("expected RestartInPlace to fail when mysqld is not running")
	}
	if called {
		t.Error("re-exec must not be scheduled when there is no mysqld PID")
	}
}

func TestUpgradeInstanceManagerWritesThenReExecs(t *testing.T) {
	t.Cleanup(func() { inPlaceUpgrading.Store(false) })
	sup := &fakeSupervisor{pid: 777}
	c, _ := newController(t, sup)

	var wroteHash string
	var wroteBody []byte
	c.writeManager = func(r io.Reader, expectedHash string) error {
		wroteHash = expectedHash
		b, _ := io.ReadAll(r)
		wroteBody = b
		return nil
	}
	gotPID := make(chan int, 1)
	c.reExecOnDisk = func(pid int) error { gotPID <- pid; return nil }

	err := c.UpgradeInstanceManager(context.Background(), strings.NewReader("new-binary"), "abc123")
	if err != nil {
		t.Fatalf("UpgradeInstanceManager: %v", err)
	}
	if wroteHash != "abc123" || string(wroteBody) != "new-binary" {
		t.Errorf("writeManager got hash=%q body=%q", wroteHash, wroteBody)
	}
	if !IsInPlaceUpgrading() {
		t.Error("expected the in-flight upgrade flag to be set")
	}
	select {
	case pid := <-gotPID:
		if pid != 777 {
			t.Errorf("re-exec pid = %d, want 777", pid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("on-disk re-exec was never scheduled")
	}
}

func TestUpgradeInstanceManagerRejectsBadBinaryWithoutReExec(t *testing.T) {
	t.Cleanup(func() { inPlaceUpgrading.Store(false) })
	c, _ := newController(t, &fakeSupervisor{pid: 777})

	c.writeManager = func(io.Reader, string) error { return errors.New("hash mismatch") }
	called := false
	c.reExecOnDisk = func(int) error { called = true; return nil }

	if err := c.UpgradeInstanceManager(context.Background(), strings.NewReader("bad"), "x"); err == nil {
		t.Error("expected UpgradeInstanceManager to fail on a bad binary")
	}
	if called {
		t.Error("re-exec must not be scheduled when the binary is rejected")
	}
}

func TestUpgradeInstanceManagerWithoutRunningMysqld(t *testing.T) {
	c, _ := newController(t, &fakeSupervisor{pid: 0})
	wrote := false
	c.writeManager = func(io.Reader, string) error { wrote = true; return nil }
	if err := c.UpgradeInstanceManager(context.Background(), strings.NewReader("x"), ""); err == nil {
		t.Error("expected UpgradeInstanceManager to fail when mysqld is not running")
	}
	if wrote {
		t.Error("must not write a new binary when there is no mysqld PID to adopt")
	}
}

func TestNewControllerRejectsBadVersion(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()
	eng := engine.MustForFlavor(engine.FlavorMySQL)
	if _, err := NewController("x", db, "not-a-version", webserver.RoleUnknown, nil, eng); err == nil {
		t.Error("expected error for invalid version")
	}
}

func TestPromoteDemoteDelegate(t *testing.T) {
	c, mock := newController(t, nil)

	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(sqlmock.NewRows([]string{"Source_Host", "Replica_IO_Running", "Replica_SQL_Running"}).
			AddRow("primary.default.svc", "Yes", "Yes"))
	mock.ExpectExec("STOP REPLICA").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("RESET REPLICA ALL").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL super_read_only = OFF").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL read_only = OFF").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := c.Promote(context.Background()); err != nil {
		t.Fatalf("Promote: %v", err)
	}

	mock.ExpectExec("SET GLOBAL read_only = ON").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL super_read_only = ON").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := c.Demote(context.Background()); err != nil {
		t.Fatalf("Demote: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestReloadAppliesDynamicParameters(t *testing.T) {
	c, mock := newController(t, nil)

	// Reload applies parameters in sorted order. A non-dynamic variable fails at
	// runtime and is reported, not fatal; a settable one is applied.
	mock.ExpectExec("SET GLOBAL innodb_buffer_pool_size = ?").
		WithArgs("1G").WillReturnError(errors.New("read-only variable"))
	mock.ExpectExec("SET GLOBAL max_connections = ?").
		WithArgs("200").WillReturnResult(sqlmock.NewResult(0, 0))

	resp, err := c.Reload(context.Background(), webserver.ReloadRequest{Parameters: map[string]string{
		"max_connections":         "200",
		"innodb_buffer_pool_size": "1G",
		// Operator-managed: skipped before ever touching MySQL.
		"server-id": "5",
	}})
	if err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if len(resp.Applied) != 1 || resp.Applied[0] != "max_connections" {
		t.Errorf("applied = %v", resp.Applied)
	}
	if resp.Skipped["server-id"] == "" {
		t.Errorf("expected server-id to be skipped as managed, got %v", resp.Skipped)
	}
	if resp.Skipped["innodb_buffer_pool_size"] == "" {
		t.Errorf("expected innodb_buffer_pool_size to be skipped, got %v", resp.Skipped)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestConfigureSemiSyncTemporarilyClearsReadOnly(t *testing.T) {
	c, mock := newController(t, nil)
	ctx := context.Background()

	mock.ExpectQuery("SELECT @@GLOBAL.read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("1"))
	mock.ExpectQuery("SELECT @@GLOBAL.super_read_only").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("1"))
	mock.ExpectExec("SET GLOBAL super_read_only = OFF").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL read_only = OFF").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSTALL PLUGIN rpl_semi_sync_source").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSTALL PLUGIN rpl_semi_sync_replica").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL rpl_semi_sync_source_enabled = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL rpl_semi_sync_replica_enabled = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL rpl_semi_sync_source_wait_for_replica_count = 1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL rpl_semi_sync_source_timeout = 10000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL read_only = ON").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET GLOBAL super_read_only = ON").WillReturnResult(sqlmock.NewResult(0, 0))

	err := configureSemiSync(ctx, c.repl, RunOptions{
		SemiSyncWaitCount:     1,
		SemiSyncTimeoutMillis: 10000,
	}, engine.MustForFlavor(engine.FlavorMySQL))
	if err != nil {
		t.Fatalf("configureSemiSync: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
