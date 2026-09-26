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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// fakeDumpScript stands in for mysqldump / mariadb-dump. FAKE_DUMP_MODE picks
// the behaviour; it records its argv and a copy of its credentials file (with
// the file's mode) under FAKE_DUMP_CAPTURE.
const fakeDumpScript = `#!/bin/sh
cap="$FAKE_DUMP_CAPTURE"
printf '%s\n' "$@" > "$cap/argv"
defaults="${1#--defaults-extra-file=}"
stat -c %a "$defaults" > "$cap/mode" 2>/dev/null
cat "$defaults" > "$cap/defaults" 2>/dev/null
case "$FAKE_DUMP_MODE" in
early)
	echo "mysqldump: Got error: 1045: Access denied for user 'cnmsql_dump'@'localhost'" >&2
	exit 2 ;;
esac
echo "-- MySQL dump 10.13  Distrib 8.4.11-11, for Linux (x86_64)"
echo "-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='mysql-bin.000007', SOURCE_LOG_POS=4242;"
echo "-- Current Database: shop"
echo "INSERT INTO t VALUES (1,'-- not a comment');"
case "$FAKE_DUMP_MODE" in
late)
	echo "mysqldump: Error 2013: Lost connection to server during query" >&2
	exit 3 ;;
slow)
	exec sleep 30 ;;
esac
echo "-- SET GLOBAL gtid_slave_pos='0-1-13';"
echo "-- Dump completed on 2026-09-25 16:29:19"
`

type dumpFixture struct {
	controller *Controller
	mock       sqlmock.Sqlmock
	capture    string
	workDir    string
}

func newDumpFixture(t *testing.T, flavor engine.Flavor, mode string) *dumpFixture {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	versionStr := "8.4.11-11"
	if flavor == engine.FlavorMariaDB {
		versionStr = "11.4.13-MariaDB-deb12-log"
	}
	eng := engine.MustForFlavor(flavor)
	c, err := NewController("shop-2", db, versionStr, webserver.RoleReplica, nil, eng)
	if err != nil {
		t.Fatal(err)
	}

	bin := t.TempDir()
	script := filepath.Join(bin, "mysqldump")
	if err := os.WriteFile(script, []byte(fakeDumpScript), 0o755); err != nil {
		t.Fatal(err)
	}
	f := &dumpFixture{controller: c, mock: mock, capture: t.TempDir(), workDir: t.TempDir()}
	t.Setenv("FAKE_DUMP_CAPTURE", f.capture)
	t.Setenv("FAKE_DUMP_MODE", mode)
	c.SetDumpConfig(DumpConfig{Engine: eng, Socket: "/var/run/mysqld/mysqld.sock", WorkDir: f.workDir, DumpPath: script})
	return f
}

func (f *dumpFixture) expectAccount(exists bool) {
	n := 0
	if exists {
		n = 1
	}
	f.mock.ExpectQuery(`SELECT COUNT\(\*\) FROM mysql.user WHERE User = \? AND Host = \?`).
		WithArgs("cnmsql_dump", "localhost").
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(n))
}

func (f *dumpFixture) expectSchemas(names ...string) {
	rows := sqlmock.NewRows([]string{"SCHEMA_NAME"})
	for _, n := range names {
		rows.AddRow(n)
	}
	f.mock.ExpectQuery(`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA`).WillReturnRows(rows)
}

func (f *dumpFixture) read(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.capture, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func (f *dumpFixture) assertWorkDirEmpty(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(f.workDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("credentials left behind in %s: %v", f.workDir, entries)
	}
}

var allSchemas = []string{"mysql", "sys", "performance_schema", "information_schema", "heartbeat", "shop", "billing"}

func TestStartDumpStreamsEveryApplicationSchema(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMySQL, "")
	f.expectAccount(true)
	f.expectSchemas(allSchemas...)

	session, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{
		Password: `p"w\d`, ExtraArgs: []string{"--max-allowed-packet=1G"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The credentials file is gone once the client has written output.
	f.assertWorkDirEmpty(t)

	info := session.Info()
	if info.Tool != "mysqldump" || info.Flavor != "mysql" || info.ServerVersion != "8.4.11-11" ||
		strings.Join(info.Databases, ",") != "billing,shop" {
		t.Fatalf("info = %+v", info)
	}

	var out bytes.Buffer
	result, err := session.Stream(context.Background(), &out)
	if err != nil {
		t.Fatal(err)
	}
	session.Close()
	if !strings.HasSuffix(out.String(), "-- Dump completed on 2026-09-25 16:29:19\n") {
		t.Fatalf("stream = %q", out.String())
	}
	if result.SnapshotBinlog != "mysql-bin.000007:4242" || result.SnapshotGTID != "" {
		t.Fatalf("mysql result = %+v, want binlog only", result)
	}

	argv := strings.Split(strings.TrimSpace(f.read(t, "argv")), "\n")
	if !strings.HasPrefix(argv[0], "--defaults-extra-file="+f.workDir) {
		t.Fatalf("argv[0] = %q", argv[0])
	}
	if strings.Join(argv[len(argv)-4:], " ") != "--max-allowed-packet=1G --databases billing shop" {
		t.Fatalf("argv tail = %v", argv)
	}
	for _, a := range argv {
		if strings.Contains(a, "p\"w") {
			t.Fatalf("password leaked into argv: %v", argv)
		}
	}
	if mode := strings.TrimSpace(f.read(t, "mode")); mode != "600" {
		t.Fatalf("credentials file mode = %s, want 600", mode)
	}
	defaults := f.read(t, "defaults")
	for _, want := range []string{
		"[client]", `user="cnmsql_dump"`, `password="p\"w\\d"`, `socket="/var/run/mysqld/mysqld.sock"`,
	} {
		if !strings.Contains(defaults, want) {
			t.Fatalf("credentials file missing %q:\n%s", want, defaults)
		}
	}
	f.assertWorkDirEmpty(t)
	if f.controller.dumpRunning.Load() {
		t.Fatal("dump slot not released")
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartDumpMariaDBReportsSnapshotGTID(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMariaDB, "")
	f.expectAccount(true)
	f.expectSchemas(allSchemas...)
	session, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{
		Password: "pw", Databases: []string{"shop"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if session.Info().Flavor != "mariadb" || strings.Join(session.Info().Databases, ",") != "shop" {
		t.Fatalf("info = %+v", session.Info())
	}
	result, err := session.Stream(context.Background(), &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if result.SnapshotGTID != "0-1-13" || result.SnapshotBinlog != "mysql-bin.000007:4242" {
		t.Fatalf("result = %+v", result)
	}
}

func TestStartDumpToolUnavailable(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMySQL, "")
	f.controller.dump.DumpPath = filepath.Join(t.TempDir(), "mysqldump")
	_, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{Password: "pw"})
	if !errors.Is(err, webserver.ErrDumpToolUnavailable) {
		t.Fatalf("err = %v, want ErrDumpToolUnavailable", err)
	}
	if !strings.Contains(err.Error(), "mysqldump") {
		t.Fatalf("error does not name the binary: %v", err)
	}
	if f.controller.dumpRunning.Load() {
		t.Fatal("a refused dump must not hold the slot")
	}
}

func TestStartDumpAllowsOneDumpAtATime(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMySQL, "slow")
	f.expectAccount(true)
	f.expectSchemas(allSchemas...)
	first, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{Password: "pw"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.controller.StartDump(context.Background(), webserver.DumpRequest{Password: "pw"})
	if !errors.Is(err, webserver.ErrDumpInProgress) {
		t.Fatalf("second dump err = %v, want ErrDumpInProgress", err)
	}
	// Closing without streaming kills the client and frees the slot.
	first.Close()
	if f.controller.dumpRunning.Load() {
		t.Fatal("slot not released after Close")
	}
	f.assertWorkDirEmpty(t)
}

func TestStartDumpAccountMissing(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMySQL, "")
	f.expectAccount(false)
	_, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{Password: "pw"})
	if !errors.Is(err, webserver.ErrDumpAccountMissing) {
		t.Fatalf("err = %v, want ErrDumpAccountMissing", err)
	}
	if f.controller.dumpRunning.Load() {
		t.Fatal("slot not released")
	}
}

func TestStartDumpExcludesTheHeartbeatSchema(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMySQL, "")
	f.expectAccount(true)
	f.expectSchemas("mysql", "sys", "heartbeat", "shop")
	session, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{Password: "pw"})
	if err != nil {
		t.Fatal(err)
	}
	session.Close()
	if strings.Join(session.Info().Databases, ",") != "shop" {
		t.Fatalf("databases = %v, want only shop", session.Info().Databases)
	}

	// The heartbeat schema cannot be dumped by name either.
	f.expectAccount(true)
	f.expectSchemas("mysql", "sys", "heartbeat", "shop")
	_, err = f.controller.StartDump(context.Background(), webserver.DumpRequest{
		Password: "pw", Databases: []string{"heartbeat"},
	})
	if !errors.Is(err, webserver.ErrInvalidDumpRequest) {
		t.Fatalf("err = %v, want ErrInvalidDumpRequest", err)
	}
	if f.controller.dumpRunning.Load() {
		t.Fatal("slot not released")
	}
}

func TestStartDumpRejectsInvalidDatabases(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requested []string
		schemas   []string
	}{
		{"unknown database", []string{"shop", "nope"}, allSchemas},
		{"system schema", []string{"mysql"}, allSchemas},
		{"operator schema", []string{"heartbeat"}, allSchemas},
		{"no application schema", nil, []string{"mysql", "sys", "heartbeat"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDumpFixture(t, engine.FlavorMySQL, "")
			f.expectAccount(true)
			f.expectSchemas(tc.schemas...)
			_, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{
				Password: "pw", Databases: tc.requested,
			})
			if !errors.Is(err, webserver.ErrInvalidDumpRequest) {
				t.Fatalf("err = %v, want ErrInvalidDumpRequest", err)
			}
			if f.controller.dumpRunning.Load() {
				t.Fatal("slot not released")
			}
			f.assertWorkDirEmpty(t)
		})
	}
}

func TestStartDumpRejectsPasswordWithLineBreak(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMySQL, "")
	_, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{Password: "a\nb"})
	if !errors.Is(err, webserver.ErrInvalidDumpRequest) {
		t.Fatalf("err = %v, want ErrInvalidDumpRequest", err)
	}
}

func TestStartDumpClientExitsBeforeOutput(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMySQL, "early")
	f.expectAccount(true)
	f.expectSchemas(allSchemas...)
	_, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{Password: "wrong"})
	if err == nil || !strings.Contains(err.Error(), "Access denied") {
		t.Fatalf("err = %v, want the client's stderr", err)
	}
	for _, sentinel := range []error{
		webserver.ErrDumpAccountMissing, webserver.ErrDumpToolUnavailable, webserver.ErrInvalidDumpRequest,
	} {
		if errors.Is(err, sentinel) {
			t.Fatalf("an early client failure is a plain error, got %v", err)
		}
	}
	if f.controller.dumpRunning.Load() {
		t.Fatal("slot not released")
	}
	f.assertWorkDirEmpty(t)
}

func TestDumpStreamReportsClientFailure(t *testing.T) {
	f := newDumpFixture(t, engine.FlavorMySQL, "late")
	f.expectAccount(true)
	f.expectSchemas(allSchemas...)
	session, err := f.controller.StartDump(context.Background(), webserver.DumpRequest{Password: "pw"})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	_, err = session.Stream(context.Background(), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "exit status 3") || !strings.Contains(err.Error(), "Lost connection") {
		t.Fatalf("err = %v", err)
	}
}

func TestCommentCaptureKeepsFirstAndLast(t *testing.T) {
	c := newCommentCapture(2)
	for _, l := range []string{"-- a\n", "-- b\n", "-- c\n", "-- d\n", "-- e\n"} {
		c.add([]byte(l))
	}
	if got := c.String(); got != "-- a\n-- b\n-- d\n-- e\n" {
		t.Fatalf("got %q", got)
	}
}
