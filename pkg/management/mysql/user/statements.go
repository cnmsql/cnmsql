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

// Package user builds and executes the SQL that manages MySQL users and
// databases declaratively. The statement builders here are pure so they can be
// unit-tested without a running server. cloudnative-mysql targets Percona Server 8.0/8.4/
// 9.x only, so the modern syntax (CREATE USER IF NOT EXISTS, ALTER USER ...
// IDENTIFIED BY, REQUIRE X509) is always used; no MySQL 5.x branches exist.
package user

import (
	"fmt"
	"strings"
)

// RequireTLS values accepted on managed roles and reported in UserInfo.
const (
	requireNone = "none"
	requireSSL  = "ssl"
	requireX509 = "x509"
)

// reservedUsers are the operator- and server-owned accounts that must never be
// created, altered or dropped through the declarative/control API. They mirror
// the account names the operator provisions during bootstrap (see
// internal/controller constants and instance/bootstrap.go); touching them would
// break the control plane, replication, backups or monitoring.
var reservedUsers = map[string]struct{}{
	"cloudnative-mysql_control": {},
	"cloudnative-mysql_repl":    {},
	"cloudnative-mysql_backup":  {},
	"cloudnative-mysql_metrics": {},
	"root":                      {},
}

// IsReservedUser reports whether name is an operator/server-managed account that
// the user-management API must refuse to mutate. The internal mysql.* accounts
// (mysql.sys, mysql.session, mysql.infoschema) are covered by the prefix check.
func IsReservedUser(name string) bool {
	if strings.HasPrefix(name, "mysql.") {
		return true
	}
	_, ok := reservedUsers[name]
	return ok
}

// reservedDatabases are the MySQL system schemas that must never be dropped.
var reservedDatabases = map[string]struct{}{
	"mysql":              {},
	"information_schema": {},
	"performance_schema": {},
	"sys":                {},
}

// IsReservedDatabase reports whether name is a MySQL system schema that the
// database-management API must refuse to drop.
func IsReservedDatabase(name string) bool {
	_, ok := reservedDatabases[strings.ToLower(name)]
	return ok
}

// CreateUserRequest describes the desired state of a MySQL user to create.
type CreateUserRequest struct {
	Name                  string      `json:"name"`
	Host                  string      `json:"host"`
	Password              string      `json:"password"`
	Superuser             bool        `json:"superuser"`
	MaxUserConnections    int32       `json:"maxUserConnections"`
	MaxQueriesPerHour     int32       `json:"maxQueriesPerHour"`
	MaxUpdatesPerHour     int32       `json:"maxUpdatesPerHour"`
	MaxConnectionsPerHour int32       `json:"maxConnectionsPerHour"`
	RequireTLS            string      `json:"requireTLS"`
	Privileges            []Privilege `json:"privileges,omitempty"`
}

// AlterUserRequest describes a mutation of an existing user. Nil fields are left
// untouched; non-nil fields are applied.
type AlterUserRequest struct {
	Name                  string       `json:"name"`
	Host                  string       `json:"host"`
	Password              *string      `json:"password,omitempty"`
	Superuser             *bool        `json:"superuser,omitempty"`
	MaxUserConnections    *int32       `json:"maxUserConnections,omitempty"`
	MaxQueriesPerHour     *int32       `json:"maxQueriesPerHour,omitempty"`
	MaxUpdatesPerHour     *int32       `json:"maxUpdatesPerHour,omitempty"`
	MaxConnectionsPerHour *int32       `json:"maxConnectionsPerHour,omitempty"`
	RequireTLS            *string      `json:"requireTLS,omitempty"`
	Privileges            *[]Privilege `json:"privileges,omitempty"`
}

// DropUserRequest identifies a user to remove.
type DropUserRequest struct {
	Name string `json:"name"`
	Host string `json:"host"`
}

// Privilege is a grant of one or more privileges on a target.
type Privilege struct {
	// Privileges is the grant list (SELECT, INSERT, ALL, ...).
	Privileges []string `json:"privileges"`
	// On is the target (e.g. "*.*", "mydb.*"). Defaults to "*.*".
	On string `json:"on,omitempty"`
}

// UserInfo is the observed state of a user, used by the operator to diff
// against the desired managed-role spec.
type UserInfo struct {
	Name                  string   `json:"name"`
	Host                  string   `json:"host"`
	MaxUserConnections    int32    `json:"maxUserConnections"`
	MaxQueriesPerHour     int32    `json:"maxQueriesPerHour"`
	MaxUpdatesPerHour     int32    `json:"maxUpdatesPerHour"`
	MaxConnectionsPerHour int32    `json:"maxConnectionsPerHour"`
	RequireTLS            string   `json:"requireTLS"`
	Grants                []string `json:"grants"`
}

// ListUsersResponse is the JSON body returned by GET /user/list.
type ListUsersResponse struct {
	Users []UserInfo `json:"users"`
}

// CreateDatabaseRequest describes a schema to create.
type CreateDatabaseRequest struct {
	Name         string `json:"name"`
	CharacterSet string `json:"characterSet,omitempty"`
	Collation    string `json:"collation,omitempty"`
}

// DropDatabaseRequest identifies a schema to drop.
type DropDatabaseRequest struct {
	Name string `json:"name"`
}

// ListDatabasesResponse is the JSON body returned by GET /database/list.
type ListDatabasesResponse struct {
	Databases []string `json:"databases"`
}

// account renders the 'name'@'host' clause, defaulting the host to '%'.
func account(name, host string) string {
	if host == "" {
		host = "%"
	}
	return quote(name) + "@" + quote(host)
}

// requireClause maps a RequireTLS value to its REQUIRE clause. An empty or
// unknown value yields REQUIRE NONE.
func requireClause(requireTLS string) (string, error) {
	switch strings.ToLower(requireTLS) {
	case "", requireNone:
		return "REQUIRE NONE", nil
	case requireSSL:
		return "REQUIRE SSL", nil
	case requireX509:
		return "REQUIRE X509", nil
	default:
		return "", fmt.Errorf("invalid requireTLS value %q", requireTLS)
	}
}

// resourceOptions renders the WITH MAX_* clause from the four resource limits. A
// zero value means "no limit" and is rendered as 0, matching MySQL semantics.
func resourceOptions(maxUserConns, maxQueries, maxUpdates, maxConnsPerHour int32) string {
	return fmt.Sprintf(
		"WITH MAX_QUERIES_PER_HOUR %d MAX_UPDATES_PER_HOUR %d MAX_CONNECTIONS_PER_HOUR %d MAX_USER_CONNECTIONS %d",
		maxQueries, maxUpdates, maxConnsPerHour, maxUserConns,
	)
}

// grantStatements builds GRANT statements for a privilege list against an
// account. A superuser grant supersedes any explicit privileges.
func grantStatements(name, host string, superuser bool, privileges []Privilege) ([]string, error) {
	acct := account(name, host)
	if superuser {
		return []string{fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO %s WITH GRANT OPTION", acct)}, nil
	}
	var stmts []string
	for _, p := range privileges {
		if len(p.Privileges) == 0 {
			return nil, fmt.Errorf("privilege entry has no privileges")
		}
		on := p.On
		if on == "" {
			on = "*.*"
		}
		stmts = append(stmts, fmt.Sprintf("GRANT %s ON %s TO %s",
			strings.Join(p.Privileges, ", "), on, acct))
	}
	return stmts, nil
}

// CreateUserStatements builds the CREATE USER statement followed by any GRANT
// statements needed to satisfy the request.
func CreateUserStatements(req CreateUserRequest) ([]string, error) {
	require, err := requireClause(req.RequireTLS)
	if err != nil {
		return nil, err
	}
	create := fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s %s %s",
		account(req.Name, req.Host), quote(req.Password), require,
		resourceOptions(req.MaxUserConnections, req.MaxQueriesPerHour, req.MaxUpdatesPerHour, req.MaxConnectionsPerHour))

	grants, err := grantStatements(req.Name, req.Host, req.Superuser, req.Privileges)
	if err != nil {
		return nil, err
	}
	stmts := make([]string, 0, 1+len(grants))
	stmts = append(stmts, create)
	return append(stmts, grants...), nil
}

// AlterUserStatements builds the ALTER USER statements (and GRANT statements for
// privilege changes) for the non-nil fields of the request.
func AlterUserStatements(req AlterUserRequest) ([]string, error) {
	acct := account(req.Name, req.Host)
	var stmts []string

	// Build a single ALTER USER for the attributes that fit on it.
	var clauses []string
	if req.Password != nil {
		clauses = append(clauses, "IDENTIFIED BY "+quote(*req.Password))
	}
	if req.RequireTLS != nil {
		require, err := requireClause(*req.RequireTLS)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, require)
	}
	if req.MaxUserConnections != nil || req.MaxQueriesPerHour != nil ||
		req.MaxUpdatesPerHour != nil || req.MaxConnectionsPerHour != nil {
		var res []string
		if req.MaxQueriesPerHour != nil {
			res = append(res, fmt.Sprintf("MAX_QUERIES_PER_HOUR %d", *req.MaxQueriesPerHour))
		}
		if req.MaxUpdatesPerHour != nil {
			res = append(res, fmt.Sprintf("MAX_UPDATES_PER_HOUR %d", *req.MaxUpdatesPerHour))
		}
		if req.MaxConnectionsPerHour != nil {
			res = append(res, fmt.Sprintf("MAX_CONNECTIONS_PER_HOUR %d", *req.MaxConnectionsPerHour))
		}
		if req.MaxUserConnections != nil {
			res = append(res, fmt.Sprintf("MAX_USER_CONNECTIONS %d", *req.MaxUserConnections))
		}
		clauses = append(clauses, "WITH "+strings.Join(res, " "))
	}
	if len(clauses) > 0 {
		stmts = append(stmts, fmt.Sprintf("ALTER USER %s %s", acct, strings.Join(clauses, " ")))
	}

	if req.Privileges != nil {
		superuser := req.Superuser != nil && *req.Superuser
		grants, err := grantStatements(req.Name, req.Host, superuser, *req.Privileges)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, grants...)
	} else if req.Superuser != nil && *req.Superuser {
		stmts = append(stmts, fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO %s WITH GRANT OPTION", acct))
	}

	return stmts, nil
}

// DropUserStatement builds DROP USER IF EXISTS for the account.
func DropUserStatement(name, host string) string {
	return "DROP USER IF EXISTS " + account(name, host)
}

// CreateDatabaseStatement builds CREATE DATABASE IF NOT EXISTS with optional
// charset/collation.
func CreateDatabaseStatement(req CreateDatabaseRequest) string {
	stmt := "CREATE DATABASE IF NOT EXISTS " + quoteIdent(req.Name)
	if req.CharacterSet != "" {
		stmt += " CHARACTER SET " + quoteIdent(req.CharacterSet)
	}
	if req.Collation != "" {
		stmt += " COLLATE " + quoteIdent(req.Collation)
	}
	return stmt
}

// DropDatabaseStatement builds DROP DATABASE IF EXISTS.
func DropDatabaseStatement(name string) string {
	return "DROP DATABASE IF EXISTS " + quoteIdent(name)
}

// ListDatabasesQuery returns user schemas, excluding the MySQL system schemas.
func ListDatabasesQuery() string {
	return "SELECT schema_name FROM information_schema.schemata " +
		"WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') " +
		"ORDER BY schema_name"
}

// ListUsersQuery returns the user accounts and their resource limits and TLS
// requirement from mysql.user, excluding reserved system accounts.
func ListUsersQuery() string {
	return "SELECT User, Host, max_user_connections, max_questions, max_updates, " +
		"max_connections, ssl_type FROM mysql.user " +
		"WHERE User NOT LIKE 'mysql.%' AND User <> 'root' " +
		"AND User NOT IN ('cloudnative-mysql_control', 'cloudnative-mysql_repl', " +
		"'cloudnative-mysql_backup', 'cloudnative-mysql_metrics') " +
		"ORDER BY User, Host"
}

// ShowGrantsQuery returns SHOW GRANTS for the account.
func ShowGrantsQuery(name, host string) string {
	return "SHOW GRANTS FOR " + account(name, host)
}

// quote single-quotes a string literal, escaping backslashes and single quotes.
func quote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// quoteIdent backtick-quotes a SQL identifier, escaping embedded backticks.
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
