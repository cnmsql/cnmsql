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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// fakeLoadScript stands in for the mysql / mariadb client. It records its argv
// and a copy of its credentials file (with the file's mode), copies stdin to
// FAKE_LOAD_CAPTURE/stdin, and writes "done" only once its input ended. With
// FAKE_LOAD_MODE=fail it reads its input and fails like an SQL error. With
// FAKE_LOAD_MODE=faildrop it fails like a DROP that timed out waiting for its
// metadata lock.
const fakeLoadScript = `#!/bin/sh
cap="$FAKE_LOAD_CAPTURE"
printf '%s\n' "$@" > "$cap/argv"
defaults="${1#--defaults-extra-file=}"
stat -c %a "$defaults" > "$cap/mode" 2>/dev/null
cat "$defaults" > "$cap/defaults" 2>/dev/null
cat > "$cap/stdin"
if [ "$FAKE_LOAD_MODE" = faildrop ] && grep -q '^DROP DATABASE' "$cap/stdin"; then
	echo "ERROR 1205 (HY000) at line 2: Lock wait timeout exceeded; try restarting transaction" >&2
	exit 1
fi
if [ "$FAKE_LOAD_MODE" = fail ]; then
	echo "ERROR 1146 (42S02) at line 3: Table 'shop.nope' doesn't exist" >&2
	exit 1
fi
echo done > "$cap/done"
`

// loadStream is a two-database dump in the shape both dump clients write.
const loadStream = "-- MySQL dump 10.13\n" +
	"/*!40101 SET NAMES utf8mb4 */;\n" +
	"--\n" +
	"-- Current Database: `shop`\n" +
	"--\n" +
	"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`;\n" +
	"INSERT INTO `t` VALUES (1,'-- Current Database: `billing`');\n" +
	"--\n" +
	"-- Current Database: `billing`\n" +
	"--\n" +
	"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `billing`;\n" +
	"-- Dump completed on 2026-09-26 10:00:00\n"

type loadFixture struct {
	controller *Controller
	mock       sqlmock.Sqlmock
	capture    string
	workDir    string
}

func newLoadFixture(t *testing.T, mode string) *loadFixture {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	eng := engine.MustForFlavor(engine.FlavorMySQL)
	c, err := NewController("shop-1", db, "8.4.11-11", webserver.RolePrimary, nil, eng)
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	script := filepath.Join(bin, "mysql")
	if err := os.WriteFile(script, []byte(fakeLoadScript), 0o755); err != nil {
		t.Fatal(err)
	}
	f := &loadFixture{controller: c, mock: mock, capture: t.TempDir(), workDir: t.TempDir()}
	t.Setenv("FAKE_LOAD_CAPTURE", f.capture)
	t.Setenv("FAKE_LOAD_MODE", mode)
	c.SetLoadConfig(LoadConfig{
		Engine:   eng,
		Socket:   "/var/run/mysqld/mysqld.sock",
		User:     "cnmsql_control",
		Password: `c"t\l`,
		WorkDir:  f.workDir,
		LoadPath: script,
	})
	return f
}

// expectReadOnly expects the read_only and super_read_only reads of an 8.4
// server; v is the value both report.
func (f *loadFixture) expectReadOnly(v int) {
	f.mock.ExpectQuery(`SELECT @@GLOBAL.read_only`).WillReturnRows(sqlmock.NewRows([]string{"ro"}).AddRow(v))
	f.mock.ExpectQuery(`SELECT @@GLOBAL.super_read_only`).WillReturnRows(sqlmock.NewRows([]string{"sro"}).AddRow(v))
}

func (f *loadFixture) expectObjects(db string, n int) {
	f.mock.ExpectQuery(`information_schema.TABLES WHERE TABLE_SCHEMA = \?`).
		WithArgs(db, db, db).
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(n))
}

func (f *loadFixture) read(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.capture, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func (f *loadFixture) exists(name string) bool {
	_, err := os.Stat(filepath.Join(f.capture, name))
	return err == nil
}

func (f *loadFixture) assertWorkDirEmpty(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(f.workDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("credentials left behind in %s: %v", f.workDir, entries)
	}
}

func TestLoadFailIfExistsLoadsOnlyTheSelectedSections(t *testing.T) {
	f := newLoadFixture(t, "")
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)

	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop", "shop"}, Policy: webserver.LoadPolicyFailIfExists,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Load(context.Background(), strings.NewReader(loadStream))
	session.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(result.Databases, []string{"shop"}) || result.Bytes != int64(len(loadStream)) {
		t.Errorf("result = %+v", result)
	}
	stdin := f.read(t, "stdin")
	if !strings.Contains(stdin, "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`") ||
		strings.Contains(stdin, "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `billing`") {
		t.Errorf("client input is not the shop section only:\n%s", stdin)
	}
	if !strings.HasPrefix(stdin, "-- MySQL dump") {
		t.Errorf("the header that sets the session up was dropped:\n%s", stdin)
	}
	if !f.exists("done") {
		t.Error("the client did not see the end of its input")
	}
	// The credentials are the control account's, in a 0600 file that is gone
	// after the load, and never in argv.
	if got := f.read(t, "defaults"); !strings.Contains(got, `user="cnmsql_control"`) ||
		!strings.Contains(got, `password="c\"t\\l"`) || !strings.Contains(got, "socket=") {
		t.Errorf("defaults file = %q", got)
	}
	if got := strings.TrimSpace(f.read(t, "mode")); got != "600" {
		t.Errorf("defaults file mode = %s", got)
	}
	if argv := f.read(t, "argv"); strings.Contains(argv, `c"t`) {
		t.Errorf("password in argv: %s", argv)
	}
	f.assertWorkDirEmpty(t)
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The load reads the control account's current password at start time: a
// PasswordFunc (design 030) wins over the static Password, so a rotated
// credential Secret reaches the next load.
func TestLoadUsesTheCurrentPassword(t *testing.T) {
	f := newLoadFixture(t, "")
	f.controller.load.PasswordFunc = func() string { return `f"r\l` }
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)
	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop"}, Policy: webserver.LoadPolicyFailIfExists,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Load(context.Background(), strings.NewReader(loadStream)); err != nil {
		t.Fatal(err)
	}
	session.Close()
	if got := f.read(t, "defaults"); !strings.Contains(got, `password="f\"r\\l"`) ||
		strings.Contains(got, `c\"t\\l`) {
		t.Errorf("defaults file = %q", got)
	}
	f.assertWorkDirEmpty(t)
}

func TestLoadFailIfExistsRefusesANonEmptyDatabase(t *testing.T) {
	f := newLoadFixture(t, "")
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)
	f.expectObjects("billing", 3)

	_, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop", "billing"}, Policy: webserver.LoadPolicyFailIfExists,
	})
	if !errors.Is(err, webserver.ErrDatabaseNotEmpty) || !strings.Contains(err.Error(), "billing") ||
		strings.Contains(err.Error(), "shop") {
		t.Fatalf("err = %v, want ErrDatabaseNotEmpty naming billing only", err)
	}
	// Grammar guard: the subject is the database (singular in kind), so the
	// message must read "already holds".
	if !strings.Contains(err.Error(), "billing already holds tables") {
		t.Fatalf("err = %v, want \"billing already holds tables\"", err)
	}
	if f.exists("argv") {
		t.Error("the client started although the load was refused")
	}
	f.assertWorkDirEmpty(t)

	// The refusal released the load slot.
	f.expectReadOnly(1)
	if _, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop"}, Policy: webserver.LoadPolicyFailIfExists,
	}); !errors.Is(err, webserver.ErrNotPrimary) {
		t.Fatalf("second load: %v", err)
	}
}

// The DropAndRecreate drop runs in the client's own session, bounded by a
// session lock_wait_timeout: a DROP that pends on a metadata lock must never
// occupy the instance's single control connection (issue 136), so no DROP may
// reach the control pool at all.
//
// With several databases, their base tables are locked all at once before the
// first drop: a blocker on any of them then fails the load before any database
// is gone, instead of the second drop timing out after the first succeeded.
func TestLoadDropAndRecreateDropsTheSelectedDatabases(t *testing.T) {
	f := newLoadFixture(t, "")
	f.expectReadOnly(0)
	f.mock.ExpectQuery("SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES").
		WithArgs("shop", "billing").
		WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}).
			AddRow("billing", "invoices").AddRow("shop", "orders"))

	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop", "billing"}, Policy: webserver.LoadPolicyDropAndRecreate,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Load(context.Background(), strings.NewReader(loadStream))
	session.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(result.Databases, []string{"shop", "billing"}) {
		t.Errorf("result = %+v", result)
	}
	stdin := f.read(t, "stdin")
	wantPreamble := "SET SESSION lock_wait_timeout = 10;\n" +
		"LOCK TABLES `billing`.`invoices` WRITE, `shop`.`orders` WRITE;\n" +
		"UNLOCK TABLES;\n" +
		"DROP DATABASE IF EXISTS `shop`;\n" +
		"DROP DATABASE IF EXISTS `billing`;\n" +
		"SET SESSION lock_wait_timeout = DEFAULT;\n"
	if !strings.HasPrefix(stdin, wantPreamble) {
		t.Errorf("client input does not start with the bounded drop preamble:\n%s", stdin)
	}
	if !strings.Contains(stdin, "-- MySQL dump 10.13") {
		t.Errorf("the dump stream did not follow the preamble:\n%s", stdin)
	}
	// The pool saw the read-only probe only: a DROP on the control connection
	// would be an unexpected sqlmock call and fail the run above.
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Database names are backtick-quoted with embedded backticks doubled, as the
// pool-side drop quoted them before the drop moved into the client session.
func TestLoadDropPreambleQuotesNames(t *testing.T) {
	f := newLoadFixture(t, "")
	f.expectReadOnly(0)

	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"sh`op"}, Policy: webserver.LoadPolicyDropAndRecreate,
	})
	if err != nil {
		t.Fatal(err)
	}
	// An empty stream holds no section for the database, which fails the load
	// after the client has recorded its input.
	_, loadErr := session.Load(context.Background(), strings.NewReader(""))
	session.Close()
	if loadErr == nil || !strings.Contains(loadErr.Error(), "no section for sh`op") {
		t.Fatalf("loadErr = %v, want a missing section error", loadErr)
	}
	wantQuoted := "SET SESSION lock_wait_timeout = 10;\n" +
		"DROP DATABASE IF EXISTS `sh``op`;\n" +
		"SET SESSION lock_wait_timeout = DEFAULT;\n"
	if stdin := f.read(t, "stdin"); !strings.HasPrefix(stdin, wantQuoted) {
		t.Errorf("client input is not the quoted bounded drop preamble:\n%s", stdin)
	}
}

// A drop that cannot take its metadata lock fails the load inside the bound
// instead of leaving the DROP pending server-side behind the blocker (issue
// 136): the client reports the server's error and exits, nothing is orphaned.
func TestLoadDropAndRecreateFailsFastWhenADatabaseIsInUse(t *testing.T) {
	f := newLoadFixture(t, "faildrop")
	f.expectReadOnly(0)

	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"billing"}, Policy: webserver.LoadPolicyDropAndRecreate,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = session.Load(context.Background(), strings.NewReader(loadStream))
	session.Close()
	if err == nil || !strings.Contains(err.Error(), "ERROR 1205") {
		t.Fatalf("err = %v, want the server's lock wait timeout", err)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestLoadFailIfExistsNeverDrops(t *testing.T) {
	f := newLoadFixture(t, "")
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)

	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop"}, Policy: webserver.LoadPolicyFailIfExists,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Load(context.Background(), strings.NewReader(loadStream)); err != nil {
		t.Fatal(err)
	}
	session.Close()
	if stdin := f.read(t, "stdin"); strings.Contains(stdin, "DROP DATABASE IF EXISTS") {
		t.Errorf("FailIfExists wrote a drop to the client:\n%s", stdin)
	}
}

func TestLoadRefusesAReadOnlyInstance(t *testing.T) {
	f := newLoadFixture(t, "")
	f.expectReadOnly(1)
	_, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop"}, Policy: webserver.LoadPolicyDropAndRecreate,
	})
	if !errors.Is(err, webserver.ErrNotPrimary) {
		t.Fatalf("err = %v, want ErrNotPrimary", err)
	}
	// Nothing was dropped: the DROP would be an unexpected sqlmock call.
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestLoadRejectsInvalidRequestsBeforeAnySQL(t *testing.T) {
	for name, req := range map[string]webserver.LoadRequest{
		"no database":      {Policy: webserver.LoadPolicyFailIfExists},
		"unknown policy":   {Databases: []string{"shop"}, Policy: "Merge"},
		"no policy":        {Databases: []string{"shop"}},
		"empty name":       {Databases: []string{""}, Policy: webserver.LoadPolicyFailIfExists},
		"name too long":    {Databases: []string{strings.Repeat("é", 65)}, Policy: webserver.LoadPolicyFailIfExists},
		"system schema":    {Databases: []string{"MySQL"}, Policy: webserver.LoadPolicyDropAndRecreate},
		"heartbeat schema": {Databases: []string{"Heartbeat"}, Policy: webserver.LoadPolicyDropAndRecreate},
	} {
		t.Run(name, func(t *testing.T) {
			f := newLoadFixture(t, "")
			_, err := f.controller.StartLoad(context.Background(), req)
			if !errors.Is(err, webserver.ErrInvalidLoadRequest) {
				t.Fatalf("err = %v, want ErrInvalidLoadRequest", err)
			}
			if err := f.mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestLoadAllowsOneLoadAtATime(t *testing.T) {
	f := newLoadFixture(t, "")
	req := webserver.LoadRequest{Databases: []string{"shop"}, Policy: webserver.LoadPolicyFailIfExists}
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)
	first, err := f.controller.StartLoad(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.controller.StartLoad(context.Background(), req); !errors.Is(err, webserver.ErrLoadInProgress) {
		t.Fatalf("second load: %v, want ErrLoadInProgress", err)
	}
	// Close kills the waiting client and frees the slot.
	first.Close()
	f.assertWorkDirEmpty(t)
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)
	again, err := f.controller.StartLoad(context.Background(), req)
	if err != nil {
		t.Fatalf("load after close: %v", err)
	}
	again.Close()
}

func TestLoadReportsTheClientError(t *testing.T) {
	f := newLoadFixture(t, "fail")
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)
	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop"}, Policy: webserver.LoadPolicyFailIfExists,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	_, err = session.Load(context.Background(), strings.NewReader(loadStream))
	if err == nil || !strings.Contains(err.Error(), "ERROR 1146") {
		t.Fatalf("err = %v, want the client's error", err)
	}
}

// A stream that breaks mid-way kills the client instead of ending its input,
// so the statement it was cut in the middle of never runs.
func TestLoadKillsTheClientWhenTheStreamBreaks(t *testing.T) {
	f := newLoadFixture(t, "")
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)
	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop"}, Policy: webserver.LoadPolicyFailIfExists,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	broken := io.MultiReader(strings.NewReader(loadStream[:120]), iotestErrReader{errors.New("connection reset")})
	_, err = session.Load(context.Background(), broken)
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("err = %v, want the stream error", err)
	}
	if f.exists("done") {
		t.Error("the client saw a clean end of input after a broken stream")
	}
}

func TestLoadFailsWhenASelectedDatabaseHasNoSection(t *testing.T) {
	f := newLoadFixture(t, "")
	f.expectReadOnly(0)
	f.expectObjects("shop", 0)
	f.expectObjects("crm", 0)
	session, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop", "crm"}, Policy: webserver.LoadPolicyFailIfExists,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	_, err = session.Load(context.Background(), strings.NewReader(loadStream))
	if err == nil || !strings.Contains(err.Error(), "no section for crm") {
		t.Fatalf("err = %v, want a missing section error", err)
	}
}

func TestLoadRefusesWhenTheClientIsMissing(t *testing.T) {
	f := newLoadFixture(t, "")
	f.controller.load.LoadPath = filepath.Join(t.TempDir(), "mysql")
	_, err := f.controller.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop"}, Policy: webserver.LoadPolicyFailIfExists,
	})
	if !errors.Is(err, webserver.ErrLoadToolUnavailable) {
		t.Fatalf("err = %v, want ErrLoadToolUnavailable", err)
	}
}

type iotestErrReader struct{ err error }

func (r iotestErrReader) Read([]byte) (int, error) { return 0, r.err }

func TestLoadAndDumpWithoutAnEngineAreRefused(t *testing.T) {
	c := &Controller{}
	c.SetLoadConfig(LoadConfig{User: "u", Password: "p"})
	c.SetDumpConfig(DumpConfig{})
	if _, err := c.StartLoad(context.Background(), webserver.LoadRequest{
		Databases: []string{"shop"}, Policy: webserver.LoadPolicyFailIfExists,
	}); err == nil {
		t.Error("a load without an engine was accepted")
	}
	if _, err := c.StartDump(context.Background(), webserver.DumpRequest{}); err == nil {
		t.Error("a dump without an engine was accepted")
	}
}
