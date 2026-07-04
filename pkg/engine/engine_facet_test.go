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

package engine

import (
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/config"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

func mustVersion(t *testing.T, s string) version.Version {
	t.Helper()
	v, err := version.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return v
}

// versionMatrix is the set of versions the MySQL engine must be verified against
// to prove "no behaviour change" — the engine result must equal the underlying
// version/replication function's result at each entry.
var versionMatrix = []string{"8.0.22", "8.0.26", "8.4.0", "9.0.0"}

func TestMySQLVersionFacet(t *testing.T) {
	eng := MustForFlavor(FlavorMySQL)

	raw := "8.0.36-16"
	got, err := eng.ParseServerVersion(raw)
	if err != nil {
		t.Fatalf("ParseServerVersion(%q) error: %v", raw, err)
	}
	want, err := version.Parse(raw)
	if err != nil {
		t.Fatalf("version.Parse(%q) error: %v", raw, err)
	}
	if got != want {
		t.Errorf("ParseServerVersion(%q) = %v, want %v", raw, got, want)
	}

	for _, s := range versionMatrix {
		v := mustVersion(t, s)
		gotSeries := eng.Series(v)
		wantSeries := v.Series()
		if gotSeries != wantSeries {
			t.Errorf("Series(%s) = %v, want %v", s, gotSeries, wantSeries)
		}
	}

	if len(eng.UpgradeChain()) != len(version.UpgradeSeriesChain) {
		t.Errorf("UpgradeChain len = %d, want %d", len(eng.UpgradeChain()), len(version.UpgradeSeriesChain))
	}
	for i := range eng.UpgradeChain() {
		if eng.UpgradeChain()[i] != version.UpgradeSeriesChain[i] {
			t.Errorf("UpgradeChain[%d] = %v, want %v", i, eng.UpgradeChain()[i], version.UpgradeSeriesChain[i])
		}
	}

	pairs := []struct{ from, to string }{
		{"8.0.22", "8.0.22"},
		{"8.0.36", "8.4.0"},
		{"8.4.0", "9.0.0"},
	}
	for _, p := range pairs {
		fv, tv := mustVersion(t, p.from), mustVersion(t, p.to)
		gotErr := eng.CheckUpgrade(fv, tv)
		wantErr := version.CheckUpgrade(fv, tv)
		if (gotErr == nil) != (wantErr == nil) {
			t.Errorf("CheckUpgrade(%s, %s) error = %v, want %v", p.from, p.to, gotErr, wantErr)
		}
	}
}

func TestMySQLReplDialect(t *testing.T) {
	eng := MustForFlavor(FlavorMySQL)
	r := eng.Repl()

	for _, s := range versionMatrix {
		v := mustVersion(t, s)

		opts := replication.SourceOptions{
			Host:         "source.example.com",
			Port:         3306,
			User:         "repl",
			Password:     `pass'word`,
			AutoPosition: true,
		}
		got := r.ChangeSource(v, opts)
		want := replication.ChangeSourceStatement(v, opts)
		if got != want {
			t.Errorf("ChangeSource(%s) =\n  %q\nwant\n  %q", s, got, want)
		}

		if got, want := r.StartReplica(v), replication.StartReplicaStatement(v); got != want {
			t.Errorf("StartReplica(%s) = %q, want %q", s, got, want)
		}
		if got, want := r.StopReplica(v), replication.StopReplicaStatement(v); got != want {
			t.Errorf("StopReplica(%s) = %q, want %q", s, got, want)
		}
		if got, want := r.ResetReplica(v, true), replication.ResetReplicaStatement(v, true); got != want {
			t.Errorf("ResetReplica(%s, true) = %q, want %q", s, got, want)
		}
		if got, want := r.ResetReplica(v, false), replication.ResetReplicaStatement(v, false); got != want {
			t.Errorf("ResetReplica(%s, false) = %q, want %q", s, got, want)
		}
		if got, want := r.ShowReplicaStatus(v), replication.ShowReplicaStatusStatement(v); got != want {
			t.Errorf("ShowReplicaStatus(%s) = %q, want %q", s, got, want)
		}
		if got, want := r.ResetBinaryLogs(v), replication.ResetBinaryLogsStatement(v); got != want {
			t.Errorf("ResetBinaryLogs(%s) = %q, want %q", s, got, want)
		}
	}
}

func TestMySQLSemiSyncFacet(t *testing.T) {
	eng := MustForFlavor(FlavorMySQL)
	if !eng.SemiSyncIsPlugin() {
		t.Error("MySQL SemiSyncIsPlugin() = false, want true")
	}

	for _, s := range versionMatrix {
		v := mustVersion(t, s)
		got := eng.SemiSync(v)
		want := v.SemiSync()
		if got != want {
			t.Errorf("SemiSync(%s) = %+v, want %+v", s, got, want)
		}
	}
}

func TestMySQLCapabilityFacet(t *testing.T) {
	eng := MustForFlavor(FlavorMySQL)

	for _, s := range versionMatrix {
		v := mustVersion(t, s)
		if got, want := eng.HasAdminInterface(v), v.HasAdminInterface(); got != want {
			t.Errorf("HasAdminInterface(%s) = %v, want %v", s, got, want)
		}
		if got, want := eng.UsesResetBinaryLogsAndGtids(v), v.UsesResetBinaryLogsAndGtids(); got != want {
			t.Errorf("UsesResetBinaryLogsAndGtids(%s) = %v, want %v", s, got, want)
		}
		if got, want := eng.UsesReplicaTerminology(v), v.UsesReplicaTerminology(); got != want {
			t.Errorf("UsesReplicaTerminology(%s) = %v, want %v", s, got, want)
		}
	}
}

func TestMySQLConfigFacet(t *testing.T) {
	eng := MustForFlavor(FlavorMySQL)

	// IsGroupReplicationManagedKey
	grKeys := []string{"group_replication_group_name", "group_replication_local_address", "plugin_load_add"}
	nonGRKeys := []string{"server_id", "read_only", "datadir", "port"}
	for _, k := range grKeys {
		if got, want := eng.IsGroupReplicationManagedKey(k), config.IsGroupReplicationManagedKey(k); got != want {
			t.Errorf("IsGroupReplicationManagedKey(%q) = %v, want %v", k, got, want)
		}
	}
	for _, k := range nonGRKeys {
		if got, want := eng.IsGroupReplicationManagedKey(k), config.IsGroupReplicationManagedKey(k); got != want {
			t.Errorf("IsGroupReplicationManagedKey(%q) = %v, want %v", k, got, want)
		}
	}

	// BinlogExpire
	for _, s := range versionMatrix {
		v := mustVersion(t, s)
		gotName, gotVal := eng.BinlogExpire(v, 86400)
		wantName, wantVal := config.BinlogExpire(v, 86400)
		if gotName != wantName || gotVal != wantVal {
			t.Errorf("BinlogExpire(%s, 86400) = (%q, %q), want (%q, %q)", s, gotName, gotVal, wantName, wantVal)
		}
	}
}

func TestMariaDBReplDialect(t *testing.T) {
	eng := MustForFlavor(FlavorMariaDB)
	r := eng.Repl()

	v := mustVersion(t, "11.4.0")

	if got, want := r.StartReplica(v), "START SLAVE"; got != want {
		t.Errorf("StartReplica = %q, want %q", got, want)
	}
	if got, want := r.StopReplica(v), "STOP SLAVE"; got != want {
		t.Errorf("StopReplica = %q, want %q", got, want)
	}
	if got, want := r.ShowReplicaStatus(v), "SHOW SLAVE STATUS"; got != want {
		t.Errorf("ShowReplicaStatus = %q, want %q", got, want)
	}
	if got, want := r.ResetBinaryLogs(v), "RESET MASTER"; got != want {
		t.Errorf("ResetBinaryLogs = %q, want %q", got, want)
	}
	if got, want := r.ResetReplica(v, true), "RESET SLAVE ALL"; got != want {
		t.Errorf("ResetReplica(all) = %q, want %q", got, want)
	}
	if got, want := r.ResetReplica(v, false), "RESET SLAVE"; got != want {
		t.Errorf("ResetReplica = %q, want %q", got, want)
	}
}

func TestMariaDBFacets(t *testing.T) {
	eng := MustForFlavor(FlavorMariaDB)

	if eng.HasSuperReadOnly() {
		t.Error("MariaDB HasSuperReadOnly() = true, want false")
	}
	if eng.SupportsGroupReplication() {
		t.Error("MariaDB SupportsGroupReplication() = true, want false")
	}
	if eng.SemiSyncIsPlugin() {
		t.Error("MariaDB SemiSyncIsPlugin() = true, want false")
	}

	v := mustVersion(t, "11.4.0")

	if eng.HasAdminInterface(v) {
		t.Error("MariaDB HasAdminInterface() = true, want false")
	}
	if eng.HasLogReplicaUpdates(v) {
		t.Error("MariaDB HasLogReplicaUpdates() = true, want false")
	}
	if eng.UsesResetBinaryLogsAndGtids(v) {
		t.Error("MariaDB UsesResetBinaryLogsAndGtids() = true, want false")
	}
	if eng.UsesReplicaTerminology(v) {
		t.Error("MariaDB UsesReplicaTerminology() = true, want false")
	}

	// MariaDB never owns GR keys.
	if eng.IsGroupReplicationManagedKey("group_replication_group_name") {
		t.Error("MariaDB IsGroupReplicationManagedKey(gr) = true, want false")
	}
	if eng.IsGroupReplicationManagedKey("plugin_load_add") {
		t.Error("MariaDB IsGroupReplicationManagedKey(plugin_load_add) = true, want false")
	}

	// SemiSync uses master/slave naming regardless of version.
	n := eng.SemiSync(v)
	if n.EnabledVarSource != "rpl_semi_sync_master_enabled" {
		t.Errorf("MariaDB SemiSync EnabledVarSource = %q, want rpl_semi_sync_master_enabled", n.EnabledVarSource)
	}
	if n.EnabledVarReplica != "rpl_semi_sync_slave_enabled" {
		t.Errorf("MariaDB SemiSync EnabledVarReplica = %q, want rpl_semi_sync_slave_enabled", n.EnabledVarReplica)
	}
}

func TestMariaDBVersionFacet(t *testing.T) {
	eng := MustForFlavor(FlavorMariaDB)

	// ParseServerVersion with a real MariaDB @@version string.
	raw := "11.4.3-MariaDB-1:11.4.3+maria~ubu2404"
	got, err := eng.ParseServerVersion(raw)
	if err != nil {
		t.Fatalf("ParseServerVersion(%q) error: %v", raw, err)
	}
	want := version.Version{Major: 11, Minor: 4, Patch: 3}
	if got != want {
		t.Errorf("ParseServerVersion(%q) = %v, want %v", raw, got, want)
	}

	// Series: all 11.x runtime versions map to catalog series 11.4.
	seriesTests := []struct {
		runtime string
		series  version.Version
	}{
		{"10.6.0", version.Version{Major: 10, Minor: 6}},
		{"10.11.3", version.Version{Major: 10, Minor: 11}},
		{"11.4.0", version.Version{Major: 11, Minor: 4}},
		{"11.4.3", version.Version{Major: 11, Minor: 4}},
	}
	for _, tc := range seriesTests {
		v := mustVersion(t, tc.runtime)
		got := eng.Series(v)
		if got != tc.series {
			t.Errorf("Series(%s) = %v, want %v", tc.runtime, got, tc.series)
		}
	}

	// Upgrade chain.
	chain := eng.UpgradeChain()
	expectedChain := []version.Version{
		{Major: 10, Minor: 6},
		{Major: 10, Minor: 11},
		{Major: 11, Minor: 4},
		{Major: 12, Minor: 3},
	}
	if len(chain) != len(expectedChain) {
		t.Fatalf("UpgradeChain len = %d, want %d", len(chain), len(expectedChain))
	}
	for i := range chain {
		if chain[i] != expectedChain[i] {
			t.Errorf("UpgradeChain[%d] = %v, want %v", i, chain[i], expectedChain[i])
		}
	}

	// CheckUpgrade validates against the MariaDB chain.
	upgradeTests := []struct {
		from, to string
		wantErr  bool
	}{
		{"10.6.1", "10.11.0", false},
		{"10.11.0", "11.4.0", false},
		{"11.4.0", "12.3.0", false},
		{"10.6.1", "11.4.0", true},  // skips 10.11
		{"11.4.0", "10.11.0", true}, // downgrade
		{"5.7.0", "10.6.0", true},   // unknown source
		{"10.6.0", "10.6.5", false}, // patch bump within series
	}
	for _, tc := range upgradeTests {
		fv, tv := mustVersion(t, tc.from), mustVersion(t, tc.to)
		err := eng.CheckUpgrade(fv, tv)
		if tc.wantErr && err == nil {
			t.Errorf("CheckUpgrade(%s → %s): expected error", tc.from, tc.to)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("CheckUpgrade(%s → %s): unexpected error: %v", tc.from, tc.to, err)
		}
	}
}

func TestMariaDBDefaults(t *testing.T) {
	eng := MustForFlavor(FlavorMariaDB)

	if got, want := eng.DefaultImage(), "ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4"; got != want {
		t.Errorf("DefaultImage() = %q, want %q", got, want)
	}

	tests := []struct {
		tag     string
		want    string
		wantErr bool
	}{
		{"10.6", "10.6.18", false},
		{"10.11", "10.11.8", false},
		{"11.4", "11.4.3", false},
		{"12.3", "12.3.0", false},
		{"9.0", "", true},
	}
	for _, tc := range tests {
		got, err := eng.DefaultServerVersion(tc.tag)
		if tc.wantErr {
			if err == nil {
				t.Errorf("DefaultServerVersion(%q): expected error", tc.tag)
			}
			continue
		}
		if err != nil {
			t.Errorf("DefaultServerVersion(%q): unexpected error: %v", tc.tag, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DefaultServerVersion(%q) = %q, want %q", tc.tag, got, tc.want)
		}
	}

	if got, want := eng.DefaultAuthenticationPlugin(), "mysql_native_password"; got != want {
		t.Errorf("DefaultAuthenticationPlugin() = %q, want %q", got, want)
	}
}

func TestMariaDBLifecycle(t *testing.T) {
	eng := MustForFlavor(FlavorMariaDB)

	if got, want := eng.InitBinary(), "mariadb-install-db"; got != want {
		t.Errorf("InitBinary() = %q, want %q", got, want)
	}

	if got, want := eng.ServerdCommand(), "mariadbd"; got != want {
		t.Errorf("ServerdCommand() = %q, want %q", got, want)
	}

	args := eng.InitDataDirArgs("/var/lib/mysql")
	if len(args) != 3 {
		t.Fatalf("InitDataDirArgs length = %d, want 3", len(args))
	}
	if args[0] != "--datadir=/var/lib/mysql" {
		t.Errorf("InitDataDirArgs[0] = %q", args[0])
	}
	if args[1] != "--auth-root-authentication-method=normal" {
		t.Errorf("InitDataDirArgs[1] = %q", args[1])
	}
	if args[2] != "--skip-test-db" {
		t.Errorf("InitDataDirArgs[2] = %q", args[2])
	}

	if len(eng.UpgradeArgs()) != 0 {
		t.Errorf("UpgradeArgs() should be empty for MariaDB")
	}
}

func TestMySQLDefaults(t *testing.T) {
	eng := MustForFlavor(FlavorMySQL)

	if got, want := eng.DefaultImage(), "ghcr.io/cnmsql/cnmsql-instance:8.0"; got != want {
		t.Errorf("DefaultImage() = %q, want %q", got, want)
	}

	tests := []struct {
		tag     string
		want    string
		wantErr bool
	}{
		{"8.0", "8.0.46", false},
		{"8.4", "8.4.0", false},
		{"9.x", "9.6.0", false},
		{"10.6", "", true},
	}
	for _, tc := range tests {
		got, err := eng.DefaultServerVersion(tc.tag)
		if tc.wantErr {
			if err == nil {
				t.Errorf("DefaultServerVersion(%q): expected error", tc.tag)
			}
			continue
		}
		if err != nil {
			t.Errorf("DefaultServerVersion(%q): unexpected error: %v", tc.tag, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DefaultServerVersion(%q) = %q, want %q", tc.tag, got, tc.want)
		}
	}

	if got, want := eng.DefaultAuthenticationPlugin(), "caching_sha2_password"; got != want {
		t.Errorf("DefaultAuthenticationPlugin() = %q, want %q", got, want)
	}
}

func TestMySQLLifecycle(t *testing.T) {
	eng := MustForFlavor(FlavorMySQL)

	if got, want := eng.InitBinary(), "mysqld"; got != want {
		t.Errorf("InitBinary() = %q, want %q", got, want)
	}

	if got, want := eng.ServerdCommand(), "mysqld"; got != want {
		t.Errorf("ServerdCommand() = %q, want %q", got, want)
	}

	args := eng.InitDataDirArgs("/var/lib/mysql")
	if len(args) != 2 {
		t.Fatalf("InitDataDirArgs length = %d, want 2", len(args))
	}
	if args[0] != "--initialize-insecure" {
		t.Errorf("InitDataDirArgs[0] = %q", args[0])
	}
	if args[1] != "--datadir=/var/lib/mysql" {
		t.Errorf("InitDataDirArgs[1] = %q", args[1])
	}

	if len(eng.UpgradeArgs()) != 0 {
		t.Errorf("UpgradeArgs() should be empty for MySQL")
	}
}
