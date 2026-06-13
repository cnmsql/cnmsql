/*
Copyright 2026 The CNMySQL Authors.

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

//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/binlog"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
)

// TestBinlogArchivePrimitives validates the binlog Reader and the mysqlbinlog
// Scanner against a real Percona server: SHOW BINARY LOGS active detection,
// server_uuid, forced rotation, and — crucially — that scanning real
// mysqlbinlog output recovers the same GTID set the server reports as executed.
func TestBinlogArchivePrimitives(t *testing.T) {
	ctx := context.Background()

	net, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatalf("creating network: %v", err)
	}
	defer func() { _ = net.Remove(ctx) }()

	node := startNode(ctx, t, net.Name, "binlog-primary", 1)
	defer node.close(ctx)

	// Generate a few GTID transactions, force a rotation, then a few more, so the
	// archive spans more than one immutable file plus the active tail.
	mustExec(ctx, t, node.db, "CREATE DATABASE demo")
	mustExec(ctx, t, node.db, "CREATE TABLE demo.t (id INT PRIMARY KEY)")
	mustExec(ctx, t, node.db, "INSERT INTO demo.t VALUES (1)")
	mustExec(ctx, t, node.db, "FLUSH BINARY LOGS")
	mustExec(ctx, t, node.db, "INSERT INTO demo.t VALUES (2)")
	mustExec(ctx, t, node.db, "INSERT INTO demo.t VALUES (3)")

	reader := binlog.NewReader(node.db)

	serverUUID, err := reader.ServerUUID(ctx)
	if err != nil || serverUUID == "" {
		t.Fatalf("ServerUUID: %q, %v", serverUUID, err)
	}

	logs, err := reader.ListBinaryLogs(ctx)
	if err != nil {
		t.Fatalf("ListBinaryLogs: %v", err)
	}
	if len(logs) < 2 {
		t.Fatalf("expected at least 2 binary logs, got %d", len(logs))
	}
	// Exactly one log is active, and it is the last (highest sequence).
	active := 0
	for i, l := range logs {
		if l.Active {
			active++
			if i != len(logs)-1 {
				t.Fatalf("active log is not the last entry: %+v", logs)
			}
		}
	}
	if active != 1 {
		t.Fatalf("expected exactly one active log, got %d", active)
	}

	// Writable: a fresh primary accepts writes.
	writable, err := reader.Writable(ctx)
	if err != nil || !writable {
		t.Fatalf("Writable = %v, %v; want true", writable, err)
	}

	// Scan every binary log via the real mysqlbinlog inside the container and
	// union their contributed GTID sets; the result must equal gtid_executed.
	union := replication.GTIDSet{}
	for _, l := range logs {
		out := mysqlbinlogInContainer(ctx, t, node, "/var/lib/mysql/"+l.Name)
		res, err := binlog.Scan(out)
		if err != nil {
			t.Fatalf("Scan(%s): %v", l.Name, err)
		}
		if res.GTIDSet != "" {
			parsed, err := replication.ParseGTIDSet(res.GTIDSet)
			if err != nil {
				t.Fatalf("parsing scanned set %q: %v", res.GTIDSet, err)
			}
			union.Union(parsed)
		}
	}

	var executed string
	if err := node.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		t.Fatalf("reading gtid_executed: %v", err)
	}
	expected, err := replication.ParseGTIDSet(executed)
	if err != nil {
		t.Fatalf("parsing gtid_executed %q: %v", executed, err)
	}
	if !union.Equal(expected) {
		t.Fatalf("scanned GTID union %q != gtid_executed %q", union.String(), expected.String())
	}

	// FlushLogs rotates again: the count grows and the previously-active log is
	// now immutable.
	prevActive := logs[len(logs)-1].Name
	if err := reader.FlushLogs(ctx); err != nil {
		t.Fatalf("FlushLogs: %v", err)
	}
	after, err := reader.ListBinaryLogs(ctx)
	if err != nil {
		t.Fatalf("ListBinaryLogs after flush: %v", err)
	}
	if len(after) <= len(logs) {
		t.Fatalf("flush did not add a log: before=%d after=%d", len(logs), len(after))
	}
	for _, l := range after {
		if l.Name == prevActive && l.Active {
			t.Fatalf("previously active log %s should now be immutable", prevActive)
		}
	}
}

// TestBinlogReplayToTargetGTID proves the point-in-time recovery mechanism
// end-to-end against real Percona: it generates writes on a source server,
// copies the rotated binlog files onto a fresh target, and replays a
// GTID-bounded range with the archiver's own ReplayArgs piped through
// `mysqlbinlog | mysql`. The target must end with exactly the rows up to the
// bound — the write past it must not appear.
func TestBinlogReplayToTargetGTID(t *testing.T) {
	ctx := context.Background()

	net, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatalf("creating network: %v", err)
	}
	defer func() { _ = net.Remove(ctx) }()

	source := startNode(ctx, t, net.Name, "pitr-source", 1)
	defer source.close(ctx)

	// Schema + three writes; rows 1 and 2 are the recovery target, row 3 is past it.
	mustExec(ctx, t, source.db, "CREATE DATABASE demo")
	mustExec(ctx, t, source.db, "CREATE TABLE demo.t (id INT PRIMARY KEY)")
	mustExec(ctx, t, source.db, "INSERT INTO demo.t VALUES (1)")
	mustExec(ctx, t, source.db, "INSERT INTO demo.t VALUES (2)")

	// gtid_executed here is the recovery bound: everything up to and including
	// row 2 (schema + rows 1,2). Row 3 is committed afterwards and excluded.
	includeGTIDs := gtidExecuted(ctx, t, source.db)
	mustExec(ctx, t, source.db, "INSERT INTO demo.t VALUES (3)")

	// Rotate so every file carrying the writes is immutable and shippable.
	mustExec(ctx, t, source.db, "FLUSH BINARY LOGS")
	reader := binlog.NewReader(source.db)
	logs, err := reader.ListBinaryLogs(ctx)
	if err != nil {
		t.Fatalf("ListBinaryLogs: %v", err)
	}

	target := startNode(ctx, t, net.Name, "pitr-target", 2)
	defer target.close(ctx)

	// Copy every immutable source file into the target container.
	var targetFiles []string
	for _, l := range logs {
		if l.Active {
			continue
		}
		data := copyOutOfContainer(ctx, t, source, "/var/lib/mysql/"+l.Name)
		dst := "/tmp/" + l.Name
		if err := target.container.CopyToContainer(ctx, data, dst, 0o644); err != nil {
			t.Fatalf("copying %s into target: %v", l.Name, err)
		}
		targetFiles = append(targetFiles, dst)
	}
	sort.Strings(targetFiles)

	// Replay the bounded range with the archiver's own ReplayArgs, piped through
	// `mysqlbinlog | mysql` inside the target — the exact shape instance.Restore
	// runs during recovery.
	replayArgs, err := binlog.ReplayArgs(binlog.ReplayOptions{
		Files:        targetFiles,
		IncludeGTIDs: includeGTIDs,
	})
	if err != nil {
		t.Fatalf("ReplayArgs: %v", err)
	}
	replay := fmt.Sprintf("mysqlbinlog %s | mysql -uroot -p%s",
		strings.Join(replayArgs, " "), rootPassword)
	code, out, err := target.container.Exec(ctx, []string{"sh", "-c", replay}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec replay: %v", err)
	}
	if code != 0 {
		data, _ := io.ReadAll(out)
		t.Fatalf("replay exited %d: %s", code, string(data))
	}

	// The target must hold rows 1 and 2 (the bound) and not row 3 (past it).
	rows, err := target.db.QueryContext(ctx, "SELECT id FROM demo.t ORDER BY id")
	if err != nil {
		t.Fatalf("querying target: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, id)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("recovered rows = %v, want [1 2] (row 3 must be excluded)", got)
	}
}

// gtidExecuted returns the server's current @@GLOBAL.gtid_executed.
func gtidExecuted(ctx context.Context, t *testing.T, db *sql.DB) string {
	t.Helper()
	var executed string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		t.Fatalf("reading gtid_executed: %v", err)
	}
	return executed
}

// copyOutOfContainer reads a file out of a container and returns its bytes.
func copyOutOfContainer(ctx context.Context, t *testing.T, node *perconaNode, path string) []byte {
	t.Helper()
	rc, err := node.container.CopyFileFromContainer(ctx, path)
	if err != nil {
		t.Fatalf("copying %s out of container: %v", path, err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return data
}

// mysqlbinlogInContainer runs mysqlbinlog inside the Percona container with the
// archiver's read arguments and returns its stdout (stderr discarded), so Scan
// is exercised against genuine Percona mysqlbinlog output.
func mysqlbinlogInContainer(ctx context.Context, t *testing.T, node *perconaNode, path string) io.Reader {
	t.Helper()
	args, err := binlog.ReadArgs(path)
	if err != nil {
		t.Fatal(err)
	}
	shellCmd := "mysqlbinlog"
	for _, a := range args {
		shellCmd += " " + a
	}
	shellCmd += " 2>/dev/null"

	code, reader, err := node.container.Exec(ctx, []string{"sh", "-c", shellCmd}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("exec mysqlbinlog: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading mysqlbinlog output: %v", err)
	}
	if code != 0 {
		t.Fatalf("mysqlbinlog exited %d: %s", code, string(data))
	}
	return bytes.NewReader(data)
}
