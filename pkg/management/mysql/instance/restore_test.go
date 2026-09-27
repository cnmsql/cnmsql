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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/engine"
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

// Issue 138: a physical backup taken from a replica carries the source's
// replication metadata (mysql.slave_master_info, the applier metadata and the
// relay-log position it references) but no relay logs. If a restored instance
// kept that metadata, its runner would run START REPLICA against missing relay
// logs (Error 1872) and crash-loop — and even usable metadata would make a
// restored primary replicate from the backup source's host. The restore must
// therefore clear the inherited metadata with RESET REPLICA ALL in the source's
// own dialect.
func TestRestoredReplicaResetStatement(t *testing.T) {
	tests := []struct {
		name    string
		flavor  engine.Flavor
		version string
		want    string
	}{
		{
			name:    "modern MySQL uses replica terminology",
			flavor:  engine.FlavorMySQL,
			version: "8.4.0",
			want:    "RESET REPLICA ALL",
		},
		{
			name:    "MySQL before replica terminology",
			flavor:  engine.FlavorMySQL,
			version: "8.0.22",
			want:    "RESET SLAVE ALL",
		},
		{
			name:    "MariaDB keeps slave syntax",
			flavor:  engine.FlavorMariaDB,
			version: "11.4.3",
			want:    "RESET SLAVE ALL",
		},
		{
			name:    "unknown version defaults to modern syntax",
			flavor:  engine.FlavorMySQL,
			version: "",
			want:    "RESET REPLICA ALL",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := restoredReplicaResetStatement(engine.MustForFlavor(tc.flavor), tc.version)
			if got != tc.want {
				t.Errorf("restoredReplicaResetStatement(%s, %q) = %q, want %q", tc.flavor, tc.version, got, tc.want)
			}
		})
	}
}

func TestRestoredServerStatementsResetReplicaBeforeCredentials(t *testing.T) {
	eng := engine.MustForFlavor(engine.FlavorMySQL)
	stmts := restoredServerStatements(eng, "8.4.0", "rootpw",
		"cnmsql_control", "ctlpw", "cnmsql_backup", "bkppw")
	out := strings.Join(stmts, "\n")

	// The first statement must clear the inherited replication metadata: the
	// restored instance is a new primary, never a replica of the backup source.
	if stmts[0] != "RESET REPLICA ALL" {
		t.Fatalf("expected RESET REPLICA ALL first, got: %v", stmts)
	}
	// FLUSH PRIVILEGES must precede the ALTER USERs after --skip-grant-tables.
	if !strings.Contains(out, "FLUSH PRIVILEGES") ||
		!strings.Contains(out, "ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootpw'") {
		t.Fatalf("credential reconcile statements lost:\n%s", out)
	}
	// The restore path never starts replication: the instance boots as a
	// standalone primary and the operator decides any replication later.
	if strings.Contains(out, "START REPLICA") || strings.Contains(out, "START SLAVE") {
		t.Fatalf("restore path must never start replication:\n%s", out)
	}
}

// Without any passwords to reconcile, the reset alone must still give the
// temporary server work: the metadata reset runs on every restore, not only
// when credentials are provided.
func TestRestoredServerStatementsRunWithoutPasswords(t *testing.T) {
	eng := engine.MustForFlavor(engine.FlavorMySQL)
	stmts := restoredServerStatements(eng, "8.4.0", "", "", "", "", "")
	if len(stmts) != 1 || stmts[0] != "RESET REPLICA ALL" {
		t.Fatalf("expected only the replica reset without passwords, got: %v", stmts)
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
		name         string
		anchor       string
		anchorServer string
		want         int
		wantAmbig    bool
	}{
		{name: "matches suffix under one server", anchor: "binlog.000002", want: 1},
		// Two servers both number from 000001: a GTID-less anchor cannot pick one.
		{name: "collision across servers is ambiguous", anchor: "binlog.000001", wantAmbig: true},
		// A recorded anchor server disambiguates the collision to its own file.
		{name: "recorded server picks first", anchor: "binlog.000001", anchorServer: "uuid-a", want: 0},
		{name: "recorded server picks second", anchor: "binlog.000001", anchorServer: "uuid-b", want: 2},
		// Recorded server whose anchor file was purged: absent, not ambiguous.
		{name: "recorded server absent", anchor: "binlog.000001", anchorServer: "uuid-c", want: -1},
		{name: "absent", anchor: "binlog.000009", want: -1},
		// A bare (unprefixed) name is never how the downloader stores files.
		{name: "no substring match", anchor: "uuid-a_binlog.000001", want: -1},
	}
	for _, tc := range tests {
		got, err := findAnchorIndex(files, tc.anchor, tc.anchorServer)
		if tc.wantAmbig {
			if !errors.Is(err, ErrAmbiguousAnchor) {
				t.Errorf("%s: findAnchorIndex(_, %q) err = %v, want ErrAmbiguousAnchor", tc.name, tc.anchor, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: findAnchorIndex(_, %q) unexpected err = %v", tc.name, tc.anchor, err)
		}
		if got != tc.want {
			t.Errorf("%s: findAnchorIndex(_, %q) = %d, want %d", tc.name, tc.anchor, got, tc.want)
		}
	}
}
