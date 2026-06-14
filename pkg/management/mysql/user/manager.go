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

package user

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
)

// Manager executes user and database management statements against a mysqld
// connection. The statement text is produced by the pure builders in this
// package, so Manager stays a thin, ordered executor.
type Manager struct {
	conn pool.Connection
}

// NewManager builds a Manager bound to a connection.
func NewManager(conn pool.Connection) *Manager {
	return &Manager{conn: conn}
}

func (m *Manager) exec(ctx context.Context, stmt string) error {
	if _, err := m.conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("executing %q: %w", stmt, err)
	}
	return nil
}

func (m *Manager) execAll(ctx context.Context, stmts []string) error {
	for _, stmt := range stmts {
		if err := m.exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// CreateUser creates the user and applies its grants.
func (m *Manager) CreateUser(ctx context.Context, req CreateUserRequest) error {
	if IsReservedUser(req.Name) {
		return fmt.Errorf("refusing to create reserved account %q", req.Name)
	}
	stmts, err := CreateUserStatements(req)
	if err != nil {
		return err
	}
	return m.execAll(ctx, stmts)
}

// AlterUser applies the non-nil fields of the request to an existing user.
func (m *Manager) AlterUser(ctx context.Context, req AlterUserRequest) error {
	if IsReservedUser(req.Name) {
		return fmt.Errorf("refusing to alter reserved account %q", req.Name)
	}
	stmts, err := AlterUserStatements(req)
	if err != nil {
		return err
	}
	return m.execAll(ctx, stmts)
}

// DropUser removes the user.
func (m *Manager) DropUser(ctx context.Context, req DropUserRequest) error {
	if IsReservedUser(req.Name) {
		return fmt.Errorf("refusing to drop reserved account %q", req.Name)
	}
	return m.exec(ctx, DropUserStatement(req.Name, req.Host))
}

// ListUsers reads the managed users, their resource limits, TLS requirement,
// and grants.
func (m *Manager) ListUsers(ctx context.Context) (*ListUsersResponse, error) {
	rows, err := m.conn.QueryContext(ctx, ListUsersQuery())
	if err != nil {
		return nil, fmt.Errorf("listing users: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var users []UserInfo
	for rows.Next() {
		var (
			info    UserInfo
			sslType string
		)
		if err := rows.Scan(&info.Name, &info.Host, &info.MaxUserConnections,
			&info.MaxQueriesPerHour, &info.MaxUpdatesPerHour, &info.MaxConnectionsPerHour,
			&sslType); err != nil {
			return nil, fmt.Errorf("scanning user row: %w", err)
		}
		info.RequireTLS = sslTypeToRequireTLS(sslType)
		users = append(users, info)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range users {
		grants, err := m.showGrants(ctx, users[i].Name, users[i].Host)
		if err != nil {
			return nil, err
		}
		users[i].Grants = grants
	}
	return &ListUsersResponse{Users: users}, nil
}

func (m *Manager) showGrants(ctx context.Context, name, host string) ([]string, error) {
	rows, err := m.conn.QueryContext(ctx, ShowGrantsQuery(name, host))
	if err != nil {
		return nil, fmt.Errorf("showing grants for %s@%s: %w", name, host, err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, fmt.Errorf("scanning grant row: %w", err)
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// CreateDatabase creates the schema with optional charset/collation.
func (m *Manager) CreateDatabase(ctx context.Context, req CreateDatabaseRequest) error {
	return m.exec(ctx, CreateDatabaseStatement(req))
}

// DropDatabase drops the schema.
func (m *Manager) DropDatabase(ctx context.Context, req DropDatabaseRequest) error {
	if IsReservedDatabase(req.Name) {
		return fmt.Errorf("refusing to drop system database %q", req.Name)
	}
	return m.exec(ctx, DropDatabaseStatement(req.Name))
}

// ListDatabases reads the user schemas, excluding the MySQL system schemas.
func (m *Manager) ListDatabases(ctx context.Context) (*ListDatabasesResponse, error) {
	rows, err := m.conn.QueryContext(ctx, ListDatabasesQuery())
	if err != nil {
		return nil, fmt.Errorf("listing databases: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var dbs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning database row: %w", err)
		}
		dbs = append(dbs, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &ListDatabasesResponse{Databases: dbs}, nil
}

// sslTypeToRequireTLS maps the mysql.user.ssl_type column to a RequireTLS value.
func sslTypeToRequireTLS(sslType string) string {
	switch sslType {
	case "X509":
		return requireX509
	case "ANY", "SPECIFIED":
		return requireSSL
	default:
		return requireNone
	}
}

// ensure the connection interface is what we expect (compile-time aid).
var _ interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
} = (pool.Connection)(nil)
