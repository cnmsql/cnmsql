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

package user

import (
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func TestCreateUserStatements(t *testing.T) {
	stmts, err := CreateUserStatements(CreateUserRequest{
		Name:               "app",
		Host:               "%",
		Password:           "s3cr3t",
		MaxUserConnections: 5,
		RequireTLS:         "x509",
		Privileges: []Privilege{
			{Privileges: []string{"SELECT", "INSERT"}, On: "appdb.*"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("got %d statements, want 2: %v", len(stmts), stmts)
	}
	create := stmts[0]
	for _, want := range []string{
		"CREATE USER IF NOT EXISTS 'app'@'%'",
		"IDENTIFIED BY 's3cr3t'",
		"REQUIRE X509",
		"MAX_USER_CONNECTIONS 5",
	} {
		if !strings.Contains(create, want) {
			t.Errorf("create statement %q missing %q", create, want)
		}
	}
	if want := "GRANT SELECT, INSERT ON appdb.* TO 'app'@'%'"; stmts[1] != want {
		t.Errorf("grant = %q, want %q", stmts[1], want)
	}
}

func TestCreateUserRevokesFollowGrants(t *testing.T) {
	stmts, err := CreateUserStatements(CreateUserRequest{
		Name:     "admin",
		Host:     "%",
		Password: "p",
		Privileges: []Privilege{
			{Privileges: []string{"SELECT", "INSERT"}, On: "*.*"},
		},
		Revokes: []Privilege{
			{Privileges: []string{"INSERT"}, On: "mysql.*"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3: %v", len(stmts), stmts)
	}
	if want := "GRANT SELECT, INSERT ON *.* TO 'admin'@'%'"; stmts[1] != want {
		t.Errorf("grant = %q, want %q", stmts[1], want)
	}
	// Revoke must come after the grant so the carve-out is not re-widened.
	if want := "REVOKE IF EXISTS INSERT ON mysql.* FROM 'admin'@'%'"; stmts[2] != want {
		t.Errorf("revoke = %q, want %q", stmts[2], want)
	}
}

func TestAlterUserRevokesFollowGrants(t *testing.T) {
	privs := []Privilege{{Privileges: []string{"SELECT"}, On: "*.*"}}
	revokes := []Privilege{{Privileges: []string{"UPDATE"}, On: "mysql.*"}}
	stmts, err := AlterUserStatements(AlterUserRequest{
		Name: "admin", Host: "%", Privileges: &privs, Revokes: &revokes,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("got %d statements, want 2: %v", len(stmts), stmts)
	}
	if want := "GRANT SELECT ON *.* TO 'admin'@'%'"; stmts[0] != want {
		t.Errorf("grant = %q, want %q", stmts[0], want)
	}
	if want := "REVOKE IF EXISTS UPDATE ON mysql.* FROM 'admin'@'%'"; stmts[1] != want {
		t.Errorf("revoke = %q, want %q", stmts[1], want)
	}
}

func TestCreateUserSuperuserSupersedesPrivileges(t *testing.T) {
	stmts, err := CreateUserStatements(CreateUserRequest{
		Name:      "admin",
		Superuser: true,
		Privileges: []Privilege{
			{Privileges: []string{"SELECT"}, On: "db.*"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("got %d statements, want 2", len(stmts))
	}
	if want := "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION"; stmts[1] != want {
		t.Errorf("grant = %q, want %q", stmts[1], want)
	}
}

func TestCreateUserDefaultHostAndNoLimits(t *testing.T) {
	stmts, err := CreateUserStatements(CreateUserRequest{Name: "u", Password: "p"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("got %d statements, want 1 (no grants): %v", len(stmts), stmts)
	}
	for _, want := range []string{"'u'@'%'", "REQUIRE NONE", "MAX_USER_CONNECTIONS 0"} {
		if !strings.Contains(stmts[0], want) {
			t.Errorf("statement %q missing %q", stmts[0], want)
		}
	}
}

func TestCreateUserInvalidRequireTLS(t *testing.T) {
	if _, err := CreateUserStatements(CreateUserRequest{Name: "u", RequireTLS: "bogus"}); err == nil {
		t.Fatal("expected error for invalid requireTLS")
	}
}

func TestPasswordEscaping(t *testing.T) {
	stmts, err := CreateUserStatements(CreateUserRequest{Name: "u", Password: `a'b\c`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := `IDENTIFIED BY 'a\'b\\c'`; !strings.Contains(stmts[0], want) {
		t.Errorf("statement %q missing escaped password %q", stmts[0], want)
	}
}

func TestAlterUserOnlyTouchesSetFields(t *testing.T) {
	stmts, err := AlterUserStatements(AlterUserRequest{
		Name:               "app",
		Host:               "10.0.0.1",
		Password:           ptr("newpw"),
		MaxUserConnections: ptr(int32(10)),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("got %d statements, want 1: %v", len(stmts), stmts)
	}
	for _, want := range []string{
		"ALTER USER 'app'@'10.0.0.1'",
		"IDENTIFIED BY 'newpw'",
		"WITH MAX_USER_CONNECTIONS 10",
	} {
		if !strings.Contains(stmts[0], want) {
			t.Errorf("statement %q missing %q", stmts[0], want)
		}
	}
	if strings.Contains(stmts[0], "REQUIRE") {
		t.Errorf("statement %q should not touch REQUIRE", stmts[0])
	}
}

func TestAlterUserNoFieldsNoStatements(t *testing.T) {
	stmts, err := AlterUserStatements(AlterUserRequest{Name: "app"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 0 {
		t.Fatalf("got %d statements, want 0: %v", len(stmts), stmts)
	}
}

func TestAlterUserPrivilegesIssuesGrants(t *testing.T) {
	stmts, err := AlterUserStatements(AlterUserRequest{
		Name:       "app",
		Privileges: &[]Privilege{{Privileges: []string{"ALL"}, On: "db.*"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("got %d statements, want 1: %v", len(stmts), stmts)
	}
	if want := "GRANT ALL ON db.* TO 'app'@'%'"; stmts[0] != want {
		t.Errorf("grant = %q, want %q", stmts[0], want)
	}
}

func TestDropUserStatement(t *testing.T) {
	if got, want := DropUserStatement("app", ""), "DROP USER IF EXISTS 'app'@'%'"; got != want {
		t.Errorf("drop = %q, want %q", got, want)
	}
}

func TestCreateDatabaseStatement(t *testing.T) {
	got := CreateDatabaseStatement(CreateDatabaseRequest{
		Name:         "my-db",
		CharacterSet: "utf8mb4",
		Collation:    "utf8mb4_0900_ai_ci",
	})
	want := "CREATE DATABASE IF NOT EXISTS `my-db` CHARACTER SET `utf8mb4` COLLATE `utf8mb4_0900_ai_ci`"
	if got != want {
		t.Errorf("create db = %q, want %q", got, want)
	}
}

func TestCreateDatabaseStatementMinimal(t *testing.T) {
	if got, want := CreateDatabaseStatement(CreateDatabaseRequest{Name: "db"}),
		"CREATE DATABASE IF NOT EXISTS `db`"; got != want {
		t.Errorf("create db = %q, want %q", got, want)
	}
}

func TestDropDatabaseStatement(t *testing.T) {
	if got, want := DropDatabaseStatement("db`x"), "DROP DATABASE IF EXISTS `db``x`"; got != want {
		t.Errorf("drop db = %q, want %q", got, want)
	}
}
