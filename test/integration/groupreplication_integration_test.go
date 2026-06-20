//go:build integration

/*
Copyright 2026 The CloudNative MySQL Authors.

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

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

// grGroupName is a fixed, valid group_replication_group_name UUID for the test.
const grGroupName = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

// startGRNode starts a Percona container configured for Group Replication
// (plugin loaded, group name/seeds/local-address pinned, single-primary), the
// same shape the operator renders, and opens a host connection.
func startGRNode(ctx context.Context, t *testing.T, netName, alias string, serverID int) *perconaNode {
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
			"--plugin-load-add=group_replication.so",
			"--group_replication_group_name=" + grGroupName,
			fmt.Sprintf("--group_replication_local_address=%s:33061", alias),
			fmt.Sprintf("--group_replication_group_seeds=%s:33061", alias),
			"--group_replication_start_on_boot=OFF",
			"--group_replication_bootstrap_group=OFF",
			"--group_replication_single_primary_mode=ON",
			"--group_replication_enforce_update_everywhere_checks=OFF",
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

	cfg := pool.Config{Host: host, Port: int(mapped.Num()), User: "root", Password: rootPassword}
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

// TestGroupReplicationSingleMemberBootstrap is the M-GR.2 acceptance test: a real
// Percona member with the operator-shaped GR config bootstraps a one-member group
// via the groupreplication.Manager and reports exactly one ONLINE PRIMARY member,
// the signal the operator observes to set bootstrapped/currentPrimary.
func TestGroupReplicationSingleMemberBootstrap(t *testing.T) {
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

	node := startGRNode(ctx, t, net.Name, "gr-1", 1)
	t.Cleanup(func() { node.close(ctx) })

	// Configure the distributed-recovery channel credentials. They are unused while
	// bootstrapping (no donor) but GR expects the channel to exist.
	mustExec(ctx, t, node.db,
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s'", replUser, replPassword))
	mustExec(ctx, t, node.db, fmt.Sprintf("GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%'", replUser))
	mustExec(ctx, t, node.db, fmt.Sprintf(
		"CHANGE REPLICATION SOURCE TO SOURCE_USER='%s', SOURCE_PASSWORD='%s' FOR CHANNEL 'group_replication_recovery'",
		replUser, replPassword))

	mgr := groupreplication.NewManager(node.db, ver)

	// Before bootstrap the member is not in any group.
	view, err := mgr.ReadGroupView(ctx)
	if err != nil {
		t.Fatalf("reading group view before bootstrap: %v", err)
	}
	if view.Configured {
		t.Fatalf("expected no group membership before bootstrap, got %+v", view.Members)
	}

	// Bootstrap the one-member group (the exactly-once sequence).
	if err := mgr.Bootstrap(ctx); err != nil {
		t.Fatalf("bootstrapping group: %v", err)
	}

	// The member must converge to a single ONLINE PRIMARY.
	deadline := time.Now().Add(90 * time.Second)
	for {
		view, err = mgr.ReadGroupView(ctx)
		if err != nil {
			t.Fatalf("reading group view: %v", err)
		}
		if member, ok := view.Primary(); ok && member.State == groupreplication.MemberStateOnline {
			if len(view.Members) != 1 {
				t.Fatalf("members = %d, want exactly 1", len(view.Members))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("member never reached ONLINE PRIMARY; last view: %+v", view.Members)
		}
		time.Sleep(2 * time.Second)
	}

	// The bootstrapped primary must be writable (read_only cleared by GR).
	var superReadOnly int
	if err := node.db.QueryRowContext(ctx, "SELECT @@global.super_read_only").Scan(&superReadOnly); err != nil {
		t.Fatalf("reading super_read_only: %v", err)
	}
	if superReadOnly != 0 {
		t.Fatal("the single-primary group's PRIMARY must be writable")
	}

	// A second Bootstrap must be a safe no-op (idempotent): START on an already
	// ONLINE member does not create a second group.
	if err := mgr.Bootstrap(ctx); err != nil {
		t.Logf("second bootstrap returned (tolerated): %v", err)
	}
	view, err = mgr.ReadGroupView(ctx)
	if err != nil {
		t.Fatalf("reading group view after re-bootstrap: %v", err)
	}
	if len(view.Members) != 1 {
		t.Fatalf("members after re-bootstrap = %d, want 1 (no split)", len(view.Members))
	}
}
