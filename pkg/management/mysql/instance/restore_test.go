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

package instance

import (
	"strings"
	"testing"
)

func TestCredentialReconcileStatements(t *testing.T) {
	stmts := credentialReconcileStatements(
		"8.4.0", "rootpw",
		"cloudnative-mysql_control", "ctlpw",
		"cloudnative-mysql_backup", "bkppw",
	)
	out := strings.Join(stmts, "\n")

	// FLUSH PRIVILEGES must come first so the grant system is re-enabled after
	// --skip-grant-tables before any ALTER USER runs.
	if len(stmts) == 0 || stmts[0] != "FLUSH PRIVILEGES" {
		t.Fatalf("expected FLUSH PRIVILEGES first, got: %v", stmts)
	}
	for _, want := range []string{
		"ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootpw'",
		"ALTER USER 'cloudnative-mysql_control'@'%' IDENTIFIED BY 'ctlpw'",
		"ALTER USER 'cloudnative-mysql_backup'@'%' IDENTIFIED BY 'bkppw'",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("reconcile statements missing %q:\n%s", want, out)
		}
	}
	// The replication account uses mTLS, so it must never be reset here.
	if strings.Contains(out, "cloudnative-mysql_repl") {
		t.Fatalf("replication account must not be reset:\n%s", out)
	}
}

func TestCredentialReconcileStatementsEmptyWhenNoPasswords(t *testing.T) {
	if stmts := credentialReconcileStatements(
		"8.4.0", "",
		"cloudnative-mysql_control", "",
		"cloudnative-mysql_backup", "",
	); stmts != nil {
		t.Fatalf("expected no statements without passwords, got: %v", stmts)
	}
}

func TestCredentialReconcileStatementsLegacy(t *testing.T) {
	stmts := credentialReconcileStatements("5.7.5", "rootpw", "", "", "", "")
	out := strings.Join(stmts, "\n")
	if !strings.Contains(out, "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('rootpw')") {
		t.Fatalf("expected legacy SET PASSWORD syntax:\n%s", out)
	}
}
