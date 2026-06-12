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
	"fmt"
	"strings"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
)

// BootstrapParams describes the desired state of a freshly initialised server.
type BootstrapParams struct {
	// RootPassword is set on root@localhost. Required.
	RootPassword string
	// Database, AppUser and AppPassword create the application schema and owner.
	// All three must be set together, or all empty to skip.
	Database    string
	AppUser     string
	AppPassword string
	// CharacterSet and Collation apply to the application database.
	CharacterSet string
	Collation    string
	// ReplicationUser and ReplicationPassword create the replication account
	// used by replicas. Both must be set together, or empty to skip.
	ReplicationUser     string
	ReplicationPassword string
	// ReplicationRequireX509 creates the replication user requiring a client
	// certificate (mTLS) rather than password authentication.
	ReplicationRequireX509 bool
	// BackupUser and BackupPassword create the account XtraBackup uses, running
	// locally on the primary, to take the physical backup streamed to joining
	// replicas. Both must be set together, or empty to skip.
	BackupUser     string
	BackupPassword string
	// ControlUser and ControlPassword create the privileged account the instance
	// manager uses for monitoring and lifecycle over the admin interface. Both
	// must be set together, or empty to skip.
	ControlUser     string
	ControlPassword string
	// SupportsDynamicPrivileges enables the MySQL 8.0+ dynamic privilege grants
	// the control user needs (admin-interface access, super_read_only, etc.).
	SupportsDynamicPrivileges bool
	// MySQLVersion selects the SQL dialect (e.g. older MySQL lacks
	// CREATE USER ... IF NOT EXISTS and sets the root password differently).
	// Defaults to modern syntax when empty.
	MySQLVersion string
	// PostInitSQL is run verbatim, in order, after the managed statements.
	PostInitSQL []string
}

// controlDynamicPrivileges are the MySQL 8.0+ dynamic privileges the control
// user needs: connect on the administrative interface, use the reserved
// connection slot, toggle super_read_only, manage replication and take backups.
var controlDynamicPrivileges = []string{
	"SERVICE_CONNECTION_ADMIN",
	"CONNECTION_ADMIN",
	"SYSTEM_VARIABLES_ADMIN",
	"REPLICATION_SLAVE_ADMIN",
	"BACKUP_ADMIN",
	"CLONE_ADMIN",
}

// Validate checks the parameters are internally consistent.
func (p BootstrapParams) Validate() error {
	if p.RootPassword == "" {
		return fmt.Errorf("bootstrap: root password is required")
	}
	if (p.Database != "" || p.AppUser != "" || p.AppPassword != "") &&
		(p.Database == "" || p.AppUser == "" || p.AppPassword == "") {
		return fmt.Errorf("bootstrap: database, appUser and appPassword must be set together")
	}
	if p.ReplicationUser != "" && p.ReplicationPassword == "" && !p.ReplicationRequireX509 {
		return fmt.Errorf("bootstrap: replication user needs a password or requireX509")
	}
	if (p.BackupUser != "") != (p.BackupPassword != "") {
		return fmt.Errorf("bootstrap: backupUser and backupPassword must be set together")
	}
	if (p.ControlUser != "") != (p.ControlPassword != "") {
		return fmt.Errorf("bootstrap: controlUser and controlPassword must be set together")
	}
	return nil
}

// BootstrapStatements returns the ordered SQL run against a freshly initialised
// server (connected as the passwordless root over the local socket) to bring it
// to the desired state. The statements are idempotent where MySQL allows it.
func BootstrapStatements(p BootstrapParams) ([]string, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	d := newBootstrapDialect(p.MySQLVersion)
	var stmts []string

	// Secure the root account.
	stmts = append(stmts, d.setRootPassword(p.RootPassword))

	// Remove anonymous accounts if an older initializer created them; left in
	// place, ''@'localhost' shadows real users on local connections. A no-op on
	// servers initialised without anonymous users.
	stmts = append(stmts, "DELETE FROM mysql.user WHERE User = ''")

	// Application database and owner.
	if p.Database != "" {
		create := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdent(p.Database))
		if p.CharacterSet != "" {
			create += " CHARACTER SET " + quoteIdent(p.CharacterSet)
		}
		if p.Collation != "" {
			create += " COLLATE " + quoteIdent(p.Collation)
		}
		stmts = append(stmts,
			create,
			d.createUser(p.AppUser, "IDENTIFIED BY "+quoteString(p.AppPassword)),
			fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%'",
				quoteIdent(p.Database), escapeName(p.AppUser)),
		)
	}

	// Replication account.
	if p.ReplicationUser != "" {
		idClause := "IDENTIFIED BY " + quoteString(p.ReplicationPassword)
		if p.ReplicationRequireX509 {
			idClause = "REQUIRE X509"
		}
		stmts = append(stmts, d.createUser(p.ReplicationUser, idClause))
		stmts = append(stmts,
			fmt.Sprintf("GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%'",
				escapeName(p.ReplicationUser)),
		)
	}

	// Backup account used by XtraBackup on the primary to clone replicas. The
	// static grants cover all supported versions (FLUSH TABLES WITH READ LOCK,
	// reading binlog position); BACKUP_ADMIN (8.0+) enables LOCK INSTANCE FOR
	// BACKUP and is added only where dynamic privileges exist.
	if p.BackupUser != "" {
		stmts = append(stmts,
			d.createUser(p.BackupUser, "IDENTIFIED BY "+quoteString(p.BackupPassword)),
			fmt.Sprintf("GRANT RELOAD, PROCESS, LOCK TABLES, REPLICATION CLIENT ON *.* TO '%s'@'%%'",
				escapeName(p.BackupUser)),
		)
		if p.SupportsDynamicPrivileges {
			// XtraBackup 8.0 needs BACKUP_ADMIN plus SELECT on these
			// performance_schema tables (log position, keyring state, group
			// membership); without them it aborts with ER_TABLEACCESS_DENIED.
			// All three are 8.0-only, gated with the dynamic-privilege check.
			stmts = append(stmts,
				fmt.Sprintf("GRANT BACKUP_ADMIN ON *.* TO '%s'@'%%'", escapeName(p.BackupUser)),
				fmt.Sprintf("GRANT SELECT ON performance_schema.log_status TO '%s'@'%%'", escapeName(p.BackupUser)),
				fmt.Sprintf("GRANT SELECT ON performance_schema.keyring_component_status TO '%s'@'%%'", escapeName(p.BackupUser)),
				fmt.Sprintf("GRANT SELECT ON performance_schema.replication_group_members TO '%s'@'%%'", escapeName(p.BackupUser)),
			)
		}
	}

	// Control account used by the instance manager.
	if p.ControlUser != "" {
		stmts = append(stmts,
			d.createUser(p.ControlUser, "IDENTIFIED BY "+quoteString(p.ControlPassword)),
			fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION",
				escapeName(p.ControlUser)),
		)
		if p.SupportsDynamicPrivileges {
			stmts = append(stmts, fmt.Sprintf("GRANT %s ON *.* TO '%s'@'%%'",
				strings.Join(controlDynamicPrivileges, ", "), escapeName(p.ControlUser)))
		}
	}

	stmts = append(stmts, "FLUSH PRIVILEGES")
	stmts = append(stmts, p.PostInitSQL...)

	return stmts, nil
}

// bootstrapDialect captures version-specific SQL differences. Older MySQL
// releases predate CREATE USER ... IF NOT EXISTS and ALTER USER ... IDENTIFIED
// BY (both 5.7.6+), so they fall back to plain CREATE USER and SET PASSWORD.
type bootstrapDialect struct {
	ifNotExists  bool
	alterForPass bool
}

func newBootstrapDialect(versionStr string) bootstrapDialect {
	// Default to modern syntax when the version is unknown.
	if versionStr == "" {
		return bootstrapDialect{ifNotExists: true, alterForPass: true}
	}
	v, err := version.Parse(versionStr)
	if err != nil {
		return bootstrapDialect{ifNotExists: true, alterForPass: true}
	}
	modern := v.AtLeast(5, 7, 6)
	return bootstrapDialect{ifNotExists: modern, alterForPass: modern}
}

func (d bootstrapDialect) createUser(name, idClause string) string {
	keyword := "CREATE USER "
	if d.ifNotExists {
		keyword += "IF NOT EXISTS "
	}
	return fmt.Sprintf("%s'%s'@'%%' %s", keyword, escapeName(name), idClause)
}

func (d bootstrapDialect) setRootPassword(password string) string {
	return d.setUserPassword("root", "localhost", password)
}

// setUserPassword returns the statement that resets an existing account's
// password, using ALTER USER on modern servers and SET PASSWORD on older ones.
func (d bootstrapDialect) setUserPassword(user, host, password string) string {
	if d.alterForPass {
		return fmt.Sprintf("ALTER USER '%s'@'%s' IDENTIFIED BY %s",
			escapeName(user), host, quoteString(password))
	}
	return fmt.Sprintf("SET PASSWORD FOR '%s'@'%s' = PASSWORD(%s)",
		escapeName(user), host, quoteString(password))
}

// quoteString single-quotes a SQL string literal, escaping backslashes and
// single quotes.
func quoteString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// quoteIdent backtick-quotes a SQL identifier, escaping embedded backticks.
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// escapeName escapes a value used inside a single-quoted user name (without
// adding the surrounding quotes, which the callers supply).
func escapeName(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}
