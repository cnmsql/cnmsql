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

func TestCredentialReconcileStatements(t *testing.T) {
	stmts := credentialReconcileStatements("8.4.0", "rootpw", "cnmysql_control", "ctlpw", "cnmysql_backup", "bkppw")
	out := strings.Join(stmts, "\n")

	// FLUSH PRIVILEGES must come first so the grant system is re-enabled after
	// --skip-grant-tables before any ALTER USER runs.
	if len(stmts) == 0 || stmts[0] != "FLUSH PRIVILEGES" {
		t.Fatalf("expected FLUSH PRIVILEGES first, got: %v", stmts)
	}
	for _, want := range []string{
		"ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootpw'",
		"ALTER USER 'cnmysql_control'@'%' IDENTIFIED BY 'ctlpw'",
		"ALTER USER 'cnmysql_backup'@'%' IDENTIFIED BY 'bkppw'",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("reconcile statements missing %q:\n%s", want, out)
		}
	}
	// The replication account uses mTLS, so it must never be reset here.
	if strings.Contains(out, "cnmysql_repl") {
		t.Fatalf("replication account must not be reset:\n%s", out)
	}
}

func TestCredentialReconcileStatementsEmptyWhenNoPasswords(t *testing.T) {
	if stmts := credentialReconcileStatements("8.4.0", "", "cnmysql_control", "", "cnmysql_backup", ""); stmts != nil {
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
