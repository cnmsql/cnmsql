//go:build integration

/*
Copyright 2026 The CloudNative M,ySQL Authors.

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

package integration

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	tcnetwork "github.com/testcontainers/testcontainers-go/network"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
)

// TestUserAndDatabaseManagement drives the user.Manager (the same code path the
// instance manager's control API uses) against a real Percona server, covering
// the declarative operations the managed-roles and Database controllers rely on:
// create/alter/drop user with privilege and password changes, and create/drop
// database. It asserts both the observed state (ListUsers/ListDatabases) and the
// real authentication/authorization effects on the server.
func TestUserAndDatabaseManagement(t *testing.T) {
	ctx := context.Background()

	net, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatalf("creating network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(ctx) })

	node := startNode(ctx, t, net.Name, "mysql", 1)
	t.Cleanup(func() { node.close(ctx) })

	mgr := user.NewManager(node.db)

	const (
		schema = "appdb"
		uname  = "appuser"
		pass1  = "p4ssw0rd-one"
		pass2  = "p4ssw0rd-two"
	)

	// --- CreateDatabase -----------------------------------------------------
	if err := mgr.CreateDatabase(ctx, user.CreateDatabaseRequest{
		Name: schema, CharacterSet: "utf8mb4", Collation: "utf8mb4_0900_ai_ci",
	}); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if !listDatabasesContains(ctx, t, mgr, schema) {
		t.Fatalf("ListDatabases does not contain %q after create", schema)
	}

	// --- CreateUser with a schema-scoped grant ------------------------------
	if err := mgr.CreateUser(ctx, user.CreateUserRequest{
		Name:     uname,
		Host:     "%",
		Password: pass1,
		Privileges: []user.Privilege{
			{Privileges: []string{"SELECT", "INSERT", "CREATE"}, On: schema + ".*"},
		},
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	grants := userGrants(ctx, t, mgr, uname, "%")
	if !grantsContain(grants, "SELECT", schema) || !grantsContain(grants, "INSERT", schema) {
		t.Fatalf("grants %v missing SELECT/INSERT on %q", grants, schema)
	}

	// The new account can authenticate with its password and use its privileges.
	userDB := openAs(ctx, t, node, uname, pass1, schema)
	mustExec(ctx, t, userDB, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(ctx, t, userDB, "INSERT INTO t VALUES (1)")
	_ = userDB.Close()

	// --- AlterUser: rotate the password -------------------------------------
	if err := mgr.AlterUser(ctx, user.AlterUserRequest{
		Name: uname, Host: "%", Password: ptr(pass2),
	}); err != nil {
		t.Fatalf("AlterUser password: %v", err)
	}
	if canAuthenticate(ctx, node, uname, pass1, schema) {
		t.Errorf("old password still authenticates after rotation")
	}
	if !canAuthenticate(ctx, node, uname, pass2, schema) {
		t.Errorf("new password does not authenticate after rotation")
	}

	// --- AlterUser: narrow the privileges to SELECT only --------------------
	// REVOKE the previous grant first (GRANT is additive); the controller does
	// the same when a privilege set shrinks. Here we assert the builder applies
	// the new GRANT and the server reflects it.
	mustExec(ctx, t, node.db, "REVOKE INSERT ON "+schema+".* FROM '"+uname+"'@'%'")
	if err := mgr.AlterUser(ctx, user.AlterUserRequest{
		Name: uname, Host: "%",
		Privileges: &[]user.Privilege{{Privileges: []string{"SELECT"}, On: schema + ".*"}},
	}); err != nil {
		t.Fatalf("AlterUser privileges: %v", err)
	}
	grants = userGrants(ctx, t, mgr, uname, "%")
	if grantsContain(grants, "INSERT", schema) {
		t.Errorf("INSERT still present after narrowing to SELECT: %v", grants)
	}
	if !grantsContain(grants, "SELECT", schema) {
		t.Errorf("SELECT missing after privilege change: %v", grants)
	}

	// --- DropUser -----------------------------------------------------------
	if err := mgr.DropUser(ctx, user.DropUserRequest{Name: uname, Host: "%"}); err != nil {
		t.Fatalf("DropUser: %v", err)
	}
	if userListed(ctx, t, mgr, uname) {
		t.Errorf("user %q still listed after DropUser", uname)
	}
	if canAuthenticate(ctx, node, uname, pass2, schema) {
		t.Errorf("dropped user can still authenticate")
	}

	// --- DropDatabase -------------------------------------------------------
	if err := mgr.DropDatabase(ctx, user.DropDatabaseRequest{Name: schema}); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	if listDatabasesContains(ctx, t, mgr, schema) {
		t.Errorf("ListDatabases still contains %q after drop", schema)
	}
}

func ptr[T any](v T) *T { return &v }

func listDatabasesContains(ctx context.Context, t *testing.T, mgr *user.Manager, name string) bool {
	t.Helper()
	resp, err := mgr.ListDatabases(ctx)
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	return slices.Contains(resp.Databases, name)
}

func userListed(ctx context.Context, t *testing.T, mgr *user.Manager, name string) bool {
	t.Helper()
	resp, err := mgr.ListUsers(ctx)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	for _, u := range resp.Users {
		if u.Name == name {
			return true
		}
	}
	return false
}

func userGrants(ctx context.Context, t *testing.T, mgr *user.Manager, name, host string) []string {
	t.Helper()
	resp, err := mgr.ListUsers(ctx)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	for _, u := range resp.Users {
		if u.Name == name && u.Host == host {
			return u.Grants
		}
	}
	t.Fatalf("user %s@%s not found in ListUsers", name, host)
	return nil
}

// grantsContain reports whether any SHOW GRANTS line grants the privilege on a
// target naming the schema (the exact target text varies by server version,
// e.g. "`appdb`.*").
func grantsContain(grants []string, privilege, schema string) bool {
	for _, g := range grants {
		if strings.Contains(g, privilege) && strings.Contains(g, schema) {
			return true
		}
	}
	return false
}

// openAs opens a host connection authenticating as the given account. It uses
// tls=skip-verify so caching_sha2_password can complete its exchange over the
// server's auto-generated TLS (production verifies the CA instead).
func openAs(ctx context.Context, t *testing.T, node *perconaNode, acct, password, database string) *sql.DB {
	t.Helper()
	db, err := openUser(ctx, node, acct, password, database)
	if err != nil {
		t.Fatalf("connecting as %s: %v", acct, err)
	}
	return db
}

func canAuthenticate(ctx context.Context, node *perconaNode, acct, password, database string) bool {
	db, err := openUser(ctx, node, acct, password, database)
	if err != nil {
		return false
	}
	_ = db.Close()
	return true
}

func openUser(ctx context.Context, node *perconaNode, acct, password, database string) (*sql.DB, error) {
	host, err := node.container.Host(ctx)
	if err != nil {
		return nil, err
	}
	mapped, err := node.container.MappedPort(ctx, "3306")
	if err != nil {
		return nil, err
	}
	tctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return pool.Open(tctx, pool.Config{
		Host:     host,
		Port:     int(mapped.Num()),
		User:     acct,
		Password: password,
		Database: database,
		Params:   map[string]string{"tls": "skip-verify"},
	})
}
