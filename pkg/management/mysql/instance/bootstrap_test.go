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

package instance

import (
	"strings"
	"testing"
)

func joinStmts(t *testing.T, p BootstrapParams) string {
	t.Helper()
	stmts, err := BootstrapStatements(p)
	if err != nil {
		t.Fatalf("BootstrapStatements: %v", err)
	}
	return strings.Join(stmts, "\n")
}

func TestBootstrapMinimal(t *testing.T) {
	out := joinStmts(t, BootstrapParams{RootPassword: "secret"})
	if !strings.Contains(out, "ALTER USER 'root'@'localhost' IDENTIFIED BY 'secret'") {
		t.Errorf("root password not set:\n%s", out)
	}
	if !strings.Contains(out, "FLUSH PRIVILEGES") {
		t.Errorf("missing flush:\n%s", out)
	}
	if strings.Contains(out, "CREATE DATABASE") {
		t.Errorf("should not create a database when none requested:\n%s", out)
	}
}

func TestBootstrapFull(t *testing.T) {
	out := joinStmts(t, BootstrapParams{
		RootPassword:        "rootpw",
		Database:            "app",
		AppUser:             "appuser",
		AppPassword:         "apppw",
		CharacterSet:        "utf8mb4",
		Collation:           "utf8mb4_0900_ai_ci",
		ReplicationUser:     "repl",
		ReplicationPassword: "replpw",
		PostInitSQL:         []string{"CREATE TABLE app.t (id INT)"},
	})

	for _, want := range []string{
		"CREATE DATABASE IF NOT EXISTS `app` CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci`",
		"CREATE USER IF NOT EXISTS 'appuser'@'%' IDENTIFIED BY 'apppw'",
		"GRANT ALL PRIVILEGES ON `app`.* TO 'appuser'@'%'",
		"CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'replpw'",
		"GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'",
		"CREATE TABLE app.t (id INT)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestBootstrapControlUserWithDynamicPrivileges(t *testing.T) {
	out := joinStmts(t, BootstrapParams{
		RootPassword:              "rootpw",
		ControlUser:               "control",
		ControlPassword:           "ctlpw",
		SupportsDynamicPrivileges: true,
	})
	wantGrant := "GRANT SERVICE_CONNECTION_ADMIN, CONNECTION_ADMIN, SYSTEM_VARIABLES_ADMIN, " +
		"REPLICATION_SLAVE_ADMIN, BACKUP_ADMIN, CLONE_ADMIN ON *.* TO 'control'@'%'"
	for _, want := range []string{
		"CREATE USER IF NOT EXISTS 'control'@'%' IDENTIFIED BY 'ctlpw'",
		"GRANT ALL PRIVILEGES ON *.* TO 'control'@'%' WITH GRANT OPTION",
		wantGrant,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestBootstrapControlUserWithoutDynamicPrivileges(t *testing.T) {
	out := joinStmts(t, BootstrapParams{
		RootPassword:    "rootpw",
		ControlUser:     "control",
		ControlPassword: "ctlpw",
	})
	if strings.Contains(out, "SERVICE_CONNECTION_ADMIN") {
		t.Errorf("legacy server should not get dynamic privilege grants:\n%s", out)
	}
	if !strings.Contains(out, "GRANT ALL PRIVILEGES ON *.* TO 'control'@'%'") {
		t.Errorf("control user should still get ALL PRIVILEGES:\n%s", out)
	}
}

func TestBootstrapControlUserValidation(t *testing.T) {
	if _, err := BootstrapStatements(BootstrapParams{RootPassword: "x", ControlUser: "control"}); err == nil {
		t.Error("expected error when control password missing")
	}
}

func TestBootstrapReplicationX509(t *testing.T) {
	out := joinStmts(t, BootstrapParams{
		RootPassword:           "rootpw",
		ReplicationUser:        "repl",
		ReplicationRequireX509: true,
	})
	if !strings.Contains(out, "CREATE USER IF NOT EXISTS 'repl'@'%' REQUIRE X509") {
		t.Errorf("expected X509 replication user:\n%s", out)
	}
}

func TestBootstrapPostInitOrdering(t *testing.T) {
	stmts, err := BootstrapStatements(BootstrapParams{
		RootPassword: "x",
		PostInitSQL:  []string{"SELECT 1", "SELECT 2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// postInitSQL must come after FLUSH PRIVILEGES.
	flush, one := -1, -1
	for i, s := range stmts {
		switch s {
		case "FLUSH PRIVILEGES":
			flush = i
		case "SELECT 1":
			one = i
		}
	}
	if flush == -1 || one == -1 || one < flush {
		t.Errorf("postInitSQL should run after FLUSH PRIVILEGES: %v", stmts)
	}
}

func TestBootstrapValidation(t *testing.T) {
	cases := []BootstrapParams{
		{},                                   // no root password
		{RootPassword: "x", Database: "app"}, // partial app config
		{RootPassword: "x", AppUser: "u", AppPassword: "p"}, // missing database
		{RootPassword: "x", ReplicationUser: "repl"},        // repl without password/x509
	}
	for i, p := range cases {
		if _, err := BootstrapStatements(p); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
}

func TestBootstrapEscaping(t *testing.T) {
	out := joinStmts(t, BootstrapParams{RootPassword: `a'b\c`})
	if !strings.Contains(out, `IDENTIFIED BY 'a\'b\\c'`) {
		t.Errorf("password not escaped:\n%s", out)
	}
}
