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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// writeReplayFakes drops shell scripts into binDir impersonating both flavors'
// client binaries. The SQL client records everything it reads from stdin into
// applied, one line per process into pids; the binlog client echoes one line
// derived from its --start-position argument so chunks are distinguishable.
func writeReplayFakes(t *testing.T) (binDir, applied, pids string) {
	t.Helper()
	binDir = t.TempDir()
	applied = filepath.Join(t.TempDir(), "applied.sql")
	pids = filepath.Join(t.TempDir(), "pids")
	for _, name := range []string{"mysql", "mariadb"} {
		sqlBody := "cat > " + applied + "\n" +
			"echo $$ >> " + pids + "\n"
		p := filepath.Join(binDir, name)
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"+sqlBody), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"mysqlbinlog", "mariadb-binlog"} {
		decodeBody := "pos=\n" +
			"for a in \"$@\"; do\n" +
			"  case \"$a\" in --start-position=*) pos=\"${a#--start-position=}\";; esac\n" +
			"done\n" +
			"echo \"CHUNK $pos\"\n"
		p := filepath.Join(binDir, name)
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"+decodeBody), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return binDir, applied, pids
}

// readLines reads a fake-binaries output file and returns its non-empty lines.
func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for l := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func TestReplayPrologueShape(t *testing.T) {
	// The prologue is the replay's load-bearing statement sequence: FLUSH
	// PRIVILEGES must sit between SQL_LOG_BIN=0 and =1, so account-management
	// statements work while the FLUSH itself stays off the recovered timeline.
	want := "SET @@SESSION.SQL_LOG_BIN=0;\nFLUSH PRIVILEGES;\nSET @@SESSION.SQL_LOG_BIN=1;\n"
	if !strings.Contains(replayPrologue, want) {
		t.Fatalf("replay prologue lost its grant-loading, binlog-safe shape:\n%s", replayPrologue)
	}
}

func TestReplaySessionPrologueFirstAndSingleProcess(t *testing.T) {
	binDir, applied, pids := writeReplayFakes(t)

	o := &RestoreOptions{
		MysqlPath:       filepath.Join(binDir, "mysql"),
		MysqlbinlogPath: filepath.Join(binDir, "mysqlbinlog"),
		Socket:          "/cnmsql-test-nonexistent.sock",
		RootPassword:    "",
	}
	bt := engine.MustForFlavor(engine.FlavorMySQL).Backup()
	ctx := context.Background()

	sess, err := o.startReplaySession(ctx, bt)
	if err != nil {
		t.Fatalf("startReplaySession: %v", err)
	}
	// Two chunks, MariaDB-positional style: every chunk must land on the same
	// SQL client process, and the grant-loading prologue must precede them all.
	for _, pos := range []string{"100", "200"} {
		if err := sess.streamChunk(ctx, []string{"--start-position=" + pos}); err != nil {
			t.Fatalf("streamChunk(%s): %v", pos, err)
		}
	}
	if err := sess.finish(); err != nil {
		t.Fatalf("finish: %v", err)
	}

	got := readLines(t, applied)
	want := []string{
		"SET @@SESSION.SQL_LOG_BIN=0;",
		"FLUSH PRIVILEGES;",
		"SET @@SESSION.SQL_LOG_BIN=1;",
		"CHUNK 100",
		"CHUNK 200",
	}
	if len(got) != len(want) {
		t.Fatalf("replay stream got %d statements, want %d:\n%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("replay stream line %d = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}

	pidLines := readLines(t, pids)
	if len(pidLines) != 1 {
		t.Fatalf("expected one SQL client process for the whole replay, got %d: %v", len(pidLines), pidLines)
	}
}

func TestReplaySessionFlavorClientFallbacks(t *testing.T) {
	// With no explicit paths the session falls back to the engine's client
	// binaries: mysqlbinlog/mysql for MySQL, mariadb-binlog/mariadb for
	// MariaDB. Both flavors must resolve and stream identically.
	for _, tc := range []struct {
		flavor    engine.Flavor
		sqlBin    string
		decodeBin string
	}{
		{flavor: engine.FlavorMySQL, sqlBin: "mysql", decodeBin: "mysqlbinlog"},
		{flavor: engine.FlavorMariaDB, sqlBin: "mariadb", decodeBin: "mariadb-binlog"},
	} {
		t.Run(string(tc.flavor), func(t *testing.T) {
			binDir, applied, _ := writeReplayFakes(t)
			t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

			o := &RestoreOptions{Socket: "/cnmsql-test-nonexistent.sock"}
			bt := engine.MustForFlavor(tc.flavor).Backup()
			ctx := context.Background()

			sess, err := o.startReplaySession(ctx, bt)
			if err != nil {
				t.Fatalf("startReplaySession: %v", err)
			}
			if err := sess.streamChunk(ctx, []string{"--start-position=42"}); err != nil {
				t.Fatalf("streamChunk: %v", err)
			}
			if err := sess.finish(); err != nil {
				t.Fatalf("finish: %v", err)
			}

			got := readLines(t, applied)
			want := []string{
				"SET @@SESSION.SQL_LOG_BIN=0;",
				"FLUSH PRIVILEGES;",
				"SET @@SESSION.SQL_LOG_BIN=1;",
				"CHUNK 42",
			}
			if len(got) != len(want) {
				t.Fatalf("%s: replay stream got %d statements, want %d:\n%v", tc.flavor, len(got), len(want), got)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("%s: replay stream line %d = %q, want %q", tc.flavor, i, got[i], want[i])
				}
			}
		})
	}
}

func TestReplaySessionDecodeFailure(t *testing.T) {
	binDir, _, _ := writeReplayFakes(t)
	decodeBin := filepath.Join(binDir, "mysqlbinlog")
	if err := os.WriteFile(decodeBin, []byte("#!/bin/sh\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	o := &RestoreOptions{
		MysqlPath:       filepath.Join(binDir, "mysql"),
		MysqlbinlogPath: decodeBin,
		Socket:          "/cnmsql-test-nonexistent.sock",
	}
	bt := engine.MustForFlavor(engine.FlavorMySQL).Backup()
	ctx := context.Background()

	sess, err := o.startReplaySession(ctx, bt)
	if err != nil {
		t.Fatalf("startReplaySession: %v", err)
	}
	streamErr := sess.streamChunk(ctx, nil)
	if streamErr == nil {
		t.Fatal("expected decode failure, got nil")
	}
	if !strings.Contains(streamErr.Error(), "exit status 3") {
		t.Fatalf("decode error lost the binlog client exit status: %v", streamErr)
	}
	// The SQL client still receives a clean EOF and exits successfully; its
	// status must not mask the decode failure (applyReplay keeps the streaming
	// error when both fail).
	if err := sess.finish(); err != nil {
		t.Fatalf("finish after decode failure: %v", err)
	}
}

func TestReplaySessionApplyFailure(t *testing.T) {
	binDir, _, _ := writeReplayFakes(t)
	sqlBin := filepath.Join(binDir, "mysql")
	if err := os.WriteFile(sqlBin, []byte("#!/bin/sh\ncat > /dev/null\nexit 9\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	o := &RestoreOptions{
		MysqlPath:       sqlBin,
		MysqlbinlogPath: filepath.Join(binDir, "mysqlbinlog"),
		Socket:          "/cnmsql-test-nonexistent.sock",
	}
	bt := engine.MustForFlavor(engine.FlavorMySQL).Backup()
	ctx := context.Background()

	sess, err := o.startReplaySession(ctx, bt)
	if err != nil {
		t.Fatalf("startReplaySession: %v", err)
	}
	if err := sess.streamChunk(ctx, []string{"--start-position=7"}); err != nil {
		t.Fatalf("streamChunk: %v", err)
	}
	err = sess.finish()
	if err == nil {
		t.Fatal("expected SQL client failure, got nil")
	}
	if !strings.Contains(err.Error(), "apply: exit status 9") {
		t.Fatalf("finish error lost the apply exit status: %v", err)
	}
}
