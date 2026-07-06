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

package instance

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// TestMariaDBPITRNotBlocked verifies that MariaDB point-in-time recovery is no
// longer blocked at the guard; it proceeds to the replay planner which may fail
// on a later step (e.g. missing bucket) but not with the old "not yet supported" error.
func TestMariaDBPITRNotBlocked(t *testing.T) {
	t.Setenv("CNMSQL_FLAVOR", "mariadb")
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, bootstrapSentinel), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	err := Restore(context.Background(), RestoreOptions{
		Store:         &objectstore.Client{},
		Bucket:        "b",
		ArchiveKey:    "k",
		DataDir:       dataDir,
		BackupDir:     t.TempDir(),
		SourceCluster: "prod",
	})
	if err == nil {
		t.Fatal("expected error (missing binlog-info), got nil")
	}
	if strings.Contains(err.Error(), "not yet supported for MariaDB") {
		t.Fatalf("MariaDB PITR should no longer be blocked; got %v", err)
	}
}

func TestCredentialReconcileStatements(t *testing.T) {
	stmts := credentialReconcileStatements(
		"8.4.0", "rootpw",
		"cnmsql_control", "ctlpw",
		"cnmsql_backup", "bkppw",
	)
	out := strings.Join(stmts, "\n")

	// FLUSH PRIVILEGES must come first so the grant system is re-enabled after
	// --skip-grant-tables before any ALTER USER runs.
	if len(stmts) == 0 || stmts[0] != "FLUSH PRIVILEGES" {
		t.Fatalf("expected FLUSH PRIVILEGES first, got: %v", stmts)
	}
	for _, want := range []string{
		"ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootpw'",
		"ALTER USER 'cnmsql_control'@'%' IDENTIFIED BY 'ctlpw'",
		"ALTER USER 'cnmsql_backup'@'%' IDENTIFIED BY 'bkppw'",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("reconcile statements missing %q:\n%s", want, out)
		}
	}
	// The replication account uses mTLS, so it must never be reset here.
	if strings.Contains(out, "cnmsql_repl") {
		t.Fatalf("replication account must not be reset:\n%s", out)
	}
}

func TestCredentialReconcileStatementsEmptyWhenNoPasswords(t *testing.T) {
	if stmts := credentialReconcileStatements(
		"8.4.0", "",
		"cnmsql_control", "",
		"cnmsql_backup", "",
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

func TestFindAnchorIndex(t *testing.T) {
	// downloadReplayFiles names every file "<serverUUID>_<binlogName>", so the
	// anchor's bare name must match on the "_<name>" suffix — never on a substring
	// or a bare (unprefixed) name that the downloader never produces.
	files := []string{
		"uuid-a_binlog.000001",
		"uuid-a_binlog.000002",
		"uuid-b_binlog.000001",
	}
	tests := []struct {
		name   string
		anchor string
		want   int
	}{
		{name: "matches suffix", anchor: "binlog.000002", want: 1},
		// Two servers both number from 000001; the first timeline occurrence wins.
		{name: "collision picks first in timeline order", anchor: "binlog.000001", want: 0},
		{name: "absent", anchor: "binlog.000009", want: -1},
		// A bare (unprefixed) name is never how the downloader stores files.
		{name: "no substring match", anchor: "uuid-a_binlog.000001", want: -1},
	}
	for _, tc := range tests {
		if got := findAnchorIndex(files, tc.anchor); got != tc.want {
			t.Errorf("%s: findAnchorIndex(_, %q) = %d, want %d", tc.name, tc.anchor, got, tc.want)
		}
	}
}
