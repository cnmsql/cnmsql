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

package user

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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
		"cloudnative-mysql_control", "cloudnative-mysql_repl",
		"cloudnative-mysql_backup", "root", "mysql.sys",
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
