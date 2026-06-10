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
	// ControlUser and ControlPassword create the privileged account the instance
	// manager uses for monitoring and lifecycle over the admin interface. Both
	// must be set together, or empty to skip.
	ControlUser     string
	ControlPassword string
	// SupportsDynamicPrivileges enables the MySQL 8.0+ dynamic privilege grants
	// the control user needs (admin-interface access, super_read_only, etc.).
	SupportsDynamicPrivileges bool
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

	var stmts []string

	// Secure the root account.
	stmts = append(stmts, fmt.Sprintf(
		"ALTER USER 'root'@'localhost' IDENTIFIED BY %s", quoteString(p.RootPassword)))

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
			fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY %s",
				escapeName(p.AppUser), quoteString(p.AppPassword)),
			fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%'",
				quoteIdent(p.Database), escapeName(p.AppUser)),
		)
	}

	// Replication account.
	if p.ReplicationUser != "" {
		var create string
		if p.ReplicationRequireX509 {
			create = fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' REQUIRE X509",
				escapeName(p.ReplicationUser))
		} else {
			create = fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY %s",
				escapeName(p.ReplicationUser), quoteString(p.ReplicationPassword))
		}
		stmts = append(stmts,
			create,
			fmt.Sprintf("GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%'",
				escapeName(p.ReplicationUser)),
		)
	}

	// Control account used by the instance manager.
	if p.ControlUser != "" {
		stmts = append(stmts,
			fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY %s",
				escapeName(p.ControlUser), quoteString(p.ControlPassword)),
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
