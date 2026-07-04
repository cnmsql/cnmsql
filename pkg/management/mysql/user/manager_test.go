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
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func newManager(t *testing.T) (*Manager, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewManager(db), mock
}

func TestManagerCreateUserExecutesAllStatements(t *testing.T) {
	m, mock := newManager(t)
	mock.ExpectExec("CREATE USER IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("GRANT SELECT ON db.* TO 'app'@'%'")).WillReturnResult(sqlmock.NewResult(0, 0))

	err := m.CreateUser(context.Background(), CreateUserRequest{
		Name:       "app",
		Password:   "pw",
		Privileges: []Privilege{{Privileges: []string{"SELECT"}, On: "db.*"}},
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestManagerMariaDBToleratesNonexistingGrantRevoke(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := NewManagerWithDialect(db, MariaDBDialect)

	mock.ExpectExec("CREATE USER IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("GRANT SELECT ON *.* TO 'app'@'%'")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// MariaDB emits a plain REVOKE; a re-applied revoke of an absent grant
	// returns ER_NONEXISTING_GRANT (1141), which must be tolerated as a no-op.
	mock.ExpectExec(regexp.QuoteMeta("REVOKE INSERT ON mysql.* FROM 'app'@'%'")).
		WillReturnError(&mysql.MySQLError{Number: 1141, Message: "no such grant"})

	err = m.CreateUser(context.Background(), CreateUserRequest{
		Name: "app", Host: "%", Password: "pw",
		Privileges: []Privilege{{Privileges: []string{"SELECT"}, On: "*.*"}},
		Revokes:    []Privilege{{Privileges: []string{"INSERT"}, On: "mysql.*"}},
	})
	if err != nil {
		t.Fatalf("CreateUser should tolerate non-existing grant: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestManagerMariaDBPropagatesOtherRevokeErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := NewManagerWithDialect(db, MariaDBDialect)

	mock.ExpectExec("CREATE USER IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))
	// A non-grant error (e.g. access denied) must still surface.
	mock.ExpectExec(regexp.QuoteMeta("REVOKE INSERT ON mysql.* FROM 'app'@'%'")).
		WillReturnError(&mysql.MySQLError{Number: 1045, Message: "access denied"})

	err = m.CreateUser(context.Background(), CreateUserRequest{
		Name: "app", Host: "%", Password: "pw",
		Revokes: []Privilege{{Privileges: []string{"INSERT"}, On: "mysql.*"}},
	})
	if err == nil {
		t.Fatal("CreateUser should propagate a non-grant revoke error")
	}
}

func TestManagerListUsers(t *testing.T) {
	m, mock := newManager(t)
	userRows := sqlmock.NewRows([]string{
		"User", "Host", "max_user_connections", "max_questions",
		"max_updates", "max_connections", "ssl_type",
	}).AddRow("app", "%", 5, 0, 0, 0, "X509")
	mock.ExpectQuery("FROM mysql.user").WillReturnRows(userRows)
	mock.ExpectQuery("SHOW GRANTS FOR").WillReturnRows(
		sqlmock.NewRows([]string{"Grants"}).AddRow("GRANT SELECT ON `db`.* TO `app`@`%`"))

	resp, err := m.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if len(resp.Users) != 1 {
		t.Fatalf("got %d users, want 1", len(resp.Users))
	}
	u := resp.Users[0]
	if u.Name != "app" || u.Host != "%" || u.MaxUserConnections != 5 {
		t.Errorf("unexpected user: %+v", u)
	}
	if u.RequireTLS != "x509" {
		t.Errorf("requireTLS = %q, want x509", u.RequireTLS)
	}
	if len(u.Grants) != 1 {
		t.Errorf("grants = %v, want 1", u.Grants)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestManagerListDatabases(t *testing.T) {
	m, mock := newManager(t)
	mock.ExpectQuery("information_schema.schemata").WillReturnRows(
		sqlmock.NewRows([]string{"schema_name"}).AddRow("app").AddRow("reports"))

	resp, err := m.ListDatabases(context.Background())
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(resp.Databases) != 2 || resp.Databases[0] != "app" {
		t.Errorf("unexpected databases: %v", resp.Databases)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestManagerRejectsReservedUsers(t *testing.T) {
	// No SQL is expected: the guard must reject before touching the connection.
	reserved := []string{
		"cnmsql_control", "cnmsql_repl",
		"cnmsql_backup", "root", "mysql.sys",
	}
	for _, name := range reserved {
		m, _ := newManager(t)
		ctx := context.Background()
		if err := m.CreateUser(ctx, CreateUserRequest{Name: name, Password: "pw"}); err == nil {
			t.Errorf("CreateUser(%q) = nil, want error", name)
		}
		if err := m.AlterUser(ctx, AlterUserRequest{Name: name}); err == nil {
			t.Errorf("AlterUser(%q) = nil, want error", name)
		}
		if err := m.DropUser(ctx, DropUserRequest{Name: name}); err == nil {
			t.Errorf("DropUser(%q) = nil, want error", name)
		}
	}
}

func TestManagerRejectsDroppingSystemDatabases(t *testing.T) {
	for _, name := range []string{"mysql", "information_schema", "performance_schema", "sys", "SYS"} {
		m, _ := newManager(t)
		if err := m.DropDatabase(context.Background(), DropDatabaseRequest{Name: name}); err == nil {
			t.Errorf("DropDatabase(%q) = nil, want error", name)
		}
	}
}

func TestManagerDropDatabase(t *testing.T) {
	m, mock := newManager(t)
	mock.ExpectExec(regexp.QuoteMeta("DROP DATABASE IF EXISTS `app`")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := m.DropDatabase(context.Background(), DropDatabaseRequest{Name: "app"}); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
