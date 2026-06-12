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
	"io"
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
