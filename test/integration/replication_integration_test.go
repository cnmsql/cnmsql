//go:build integration

/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

// Package integration holds tests that validate the management packages against
// real Percona Server containers. They require Docker and are excluded from the
// default build by the `integration` build tag. Run with:
//
//	go test -tags integration ./test/integration/...
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

const (
	perconaImage = "percona/percona-server:8.0"
	rootPassword = "rootpass"
	replUser     = "repl"
	replPassword = "replpass"
	serverVer    = "8.0.36"
)

// perconaNode is a running Percona container plus a host-side connection to it.
type perconaNode struct {
	container testcontainers.Container
	db        *sql.DB
	alias     string
}

func (n *perconaNode) close(ctx context.Context) {
	if n.db != nil {
		_ = n.db.Close()
	}
	if n.container != nil {
		_ = n.container.Terminate(ctx)
	}
}

// startNode starts a Percona container with GTID replication enabled, attached
// to the given network under the given alias, and opens a host connection.
func startNode(ctx context.Context, t *testing.T, netName, alias string, serverID int) *perconaNode {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        perconaImage,
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": rootPassword,
			"MYSQL_ROOT_HOST":     "%",
		},
		Cmd: []string{
			fmt.Sprintf("--server-id=%d", serverID),
			"--gtid-mode=ON",
			"--enforce-gtid-consistency=ON",
			"--log-bin=binlog",
			"--log-replica-updates=ON",
			"--binlog-format=ROW",
		},
		Networks:       []string{netName},
		NetworkAliases: map[string][]string{netName: {alias}},
		WaitingFor: wait.ForLog("ready for connections").
			WithOccurrence(2).
			WithStartupTimeout(3 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("starting %s: %v", alias, err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	mapped, err := container.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}

	cfg := pool.Config{
		Host:     host,
		Port:     int(mapped.Num()),
		User:     "root",
		Password: rootPassword,
	}

	// The container logs "ready for connections" during both the init phase and
	// the real start, so a host connection may still be refused briefly; retry.
	var db *sql.DB
	deadline := time.Now().Add(90 * time.Second)
	for {
		db, err = pool.Open(ctx, cfg)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connecting to %s: %v", alias, err)
		}
		time.Sleep(time.Second)
	}

	return &perconaNode{container: container, db: db, alias: alias}
}

func mustExec(ctx context.Context, t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// TestPrimaryReplicaReplication brings up two real Percona servers and verifies
// that the replication package configures GTID replication such that a write on
// the primary propagates to the replica, and that promotion clears read-only.
func TestPrimaryReplicaReplication(t *testing.T) {
	ctx := context.Background()
	ver, err := version.Parse(serverVer)
	if err != nil {
		t.Fatal(err)
	}

	net, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatalf("creating network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(ctx) })

	primary := startNode(ctx, t, net.Name, "primary", 1)
	t.Cleanup(func() { primary.close(ctx) })
	replica := startNode(ctx, t, net.Name, "replica", 2)
	t.Cleanup(func() { replica.close(ctx) })

	// Create the replication user on the primary. We use mysql_native_password
	// so the replica's IO thread can authenticate over the plaintext test
	// channel; production uses mTLS (SOURCE_SSL) instead, which sidesteps the
	// caching_sha2_password public-key exchange.
	mustExec(ctx, t, primary.db,
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s'", replUser, replPassword))
	mustExec(ctx, t, primary.db,
		fmt.Sprintf("GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%'", replUser))

	// Configure the replica to follow the primary by its network alias.
	replicaMgr := replication.NewManager(replica.db, ver)
	if err := replicaMgr.ConfigureSource(ctx, replication.SourceOptions{
		Host:         "primary",
		Port:         3306,
		User:         replUser,
		Password:     replPassword,
		AutoPosition: true,
	}); err != nil {
		t.Fatalf("ConfigureSource: %v", err)
	}

	// Wait for both replication threads to come up.
	waitForState(t, 30*time.Second, replicaMgr, func(s *replication.ReplicaState) bool {
		return s.Configured && s.IORunning && s.SQLRunning
	})

	// Write on the primary.
	mustExec(ctx, t, primary.db, "CREATE DATABASE appdb")
	mustExec(ctx, t, primary.db, "CREATE TABLE appdb.t (id INT PRIMARY KEY)")
	mustExec(ctx, t, primary.db, "INSERT INTO appdb.t VALUES (42)")

	// The write must propagate to the replica.
	waitFor(t, 30*time.Second, func() bool {
		var id int
		err := replica.db.QueryRowContext(ctx, "SELECT id FROM appdb.t WHERE id = 42").Scan(&id)
		return err == nil && id == 42
	})

	// Demote makes the replica strictly read-only.
	if err := replicaMgr.Demote(ctx); err != nil {
		t.Fatalf("Demote: %v", err)
	}
	roState, err := replicaMgr.ReadOnly(ctx)
	if err != nil {
		t.Fatalf("ReadOnly: %v", err)
	}
	if !roState.ReadOnly || !roState.SuperReadOnly {
		t.Errorf("demoted replica should be super_read_only, got %+v", roState)
	}

	// Promote the replica; it should stop replicating and become writable.
	if err := replicaMgr.Promote(ctx); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	roState, err = replicaMgr.ReadOnly(ctx)
	if err != nil {
		t.Fatalf("ReadOnly after promote: %v", err)
	}
	if roState.ReadOnly || roState.SuperReadOnly {
		t.Errorf("promoted instance must be writable, got %+v", roState)
	}
	mustExec(ctx, t, replica.db, "INSERT INTO appdb.t VALUES (100)")
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// waitForState polls the replica state until cond is satisfied, logging the last
// observed state (including any replication error) on timeout.
func waitForState(t *testing.T, timeout time.Duration, mgr *replication.Manager, cond func(*replication.ReplicaState) bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *replication.ReplicaState
	for time.Now().Before(deadline) {
		state, err := mgr.ReplicaState(context.Background())
		if err == nil {
			last = state
			if cond(state) {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("replica did not reach desired state within %s; last=%+v", timeout, last)
}
