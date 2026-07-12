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
	"strings"
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
		if got, want := r.SeedReplicaPosition("uuid:1-10"), replication.SetGTIDPurgedStatement("uuid:1-10"); got != want {
			t.Errorf("SeedReplicaPosition(%s) = %q, want %q", s, got, want)
		}
		if got, want := r.SemiSyncNaming(v), v.SemiSync(); got != want {
			t.Errorf("SemiSyncNaming(%s) = %+v, want %+v", s, got, want)
		}
	}

	// GTID position is read from gtid_executed on MySQL (version-independent).
	if got, want := r.GTIDExecutedQuery(), "SELECT @@GLOBAL.gtid_executed"; got != want {
		t.Errorf("GTIDExecutedQuery() = %q, want %q", got, want)
	}
	if got, want := r.GTIDPurgedQuery(), "SELECT @@GLOBAL.gtid_purged"; got != want {
		t.Errorf("GTIDPurgedQuery() = %q, want %q", got, want)
	}
	if got, want := r.ServerIdentityQuery(), "SELECT @@GLOBAL.server_uuid"; got != want {
		t.Errorf("ServerIdentityQuery() = %q, want %q", got, want)
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
	if !eng.SupportsDynamicPrivileges() {
		t.Error("MySQL SupportsDynamicPrivileges() = false, want true")
	}
	if got := eng.TLSReloadStatement(); got != "ALTER INSTANCE RELOAD TLS" {
		t.Errorf("MySQL TLSReloadStatement() = %q, want ALTER INSTANCE RELOAD TLS", got)
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

	// ChangeSource keeps MASTER_* terminology and seeds GTID auto-positioning
	// with MASTER_USE_GTID=current_pos — never the MySQL SOURCE_/AUTO_POSITION form.
	stmt := r.ChangeSource(v, replication.SourceOptions{Host: "h", User: "u", AutoPosition: true})
	wantStmt := replication.MariaDBChangeSourceStatement(
		replication.SourceOptions{Host: "h", User: "u", AutoPosition: true})
	if got, want := stmt, wantStmt; got != want {
		t.Errorf("ChangeSource = %q, want %q", got, want)
	}
	if !strings.Contains(stmt, "CHANGE MASTER TO") || !strings.Contains(stmt, "MASTER_USE_GTID=current_pos") {
		t.Errorf("ChangeSource = %q, want CHANGE MASTER TO ... MASTER_USE_GTID=current_pos", stmt)
	}

	// GTID position and replica seeding use MariaDB's gtid_current_pos /
	// gtid_slave_pos, not MySQL's gtid_executed / gtid_purged.
	if got, want := r.GTIDExecutedQuery(), "SELECT @@gtid_current_pos"; got != want {
		t.Errorf("GTIDExecutedQuery() = %q, want %q", got, want)
	}
	if got, want := r.SeedReplicaPosition("0-1-100"), "SET GLOBAL gtid_slave_pos = '0-1-100'"; got != want {
		t.Errorf("SeedReplicaPosition() = %q, want %q", got, want)
	}
	// MariaDB has no gtid_purged (empty → caller skips) and no server_uuid
	// (server_id is the archive-partition identity).
	if got := r.GTIDPurgedQuery(); got != "" {
		t.Errorf("GTIDPurgedQuery() = %q, want empty", got)
	}
	if got, want := r.ServerIdentityQuery(), "SELECT @@GLOBAL.server_id"; got != want {
		t.Errorf("ServerIdentityQuery() = %q, want %q", got, want)
	}

	// Semi-sync naming uses source/replica spelling.
	if naming := r.SemiSyncNaming(v); naming.EnabledVarSource != "rpl_semi_sync_source_enabled" ||
		naming.EnabledVarReplica != "rpl_semi_sync_replica_enabled" {
		t.Errorf("SemiSyncNaming() = %+v, want source/replica spelling", naming)
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
	if eng.SupportsDynamicPrivileges() {
		t.Error("MariaDB SupportsDynamicPrivileges() = true, want false")
	}
	if got := eng.TLSReloadStatement(); got != "FLUSH LOCAL SSL" {
		t.Errorf("MariaDB TLSReloadStatement() = %q, want FLUSH LOCAL SSL", got)
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

	// SemiSync uses source/replica naming.
	n := eng.SemiSync(v)
	if n.EnabledVarSource != "rpl_semi_sync_source_enabled" {
		t.Errorf("MariaDB SemiSync EnabledVarSource = %q, want rpl_semi_sync_source_enabled", n.EnabledVarSource)
	}
	if n.EnabledVarReplica != "rpl_semi_sync_replica_enabled" {
		t.Errorf("MariaDB SemiSync EnabledVarReplica = %q, want rpl_semi_sync_replica_enabled", n.EnabledVarReplica)
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
		{"10.11.0", "11.4.0", false},
		{"11.4.0", "12.3.0", false},
		{"10.11.0", "12.3.0", true},   // skips 11.4
		{"11.4.0", "10.11.0", true},   // downgrade
		{"5.7.0", "10.11.0", true},    // unknown source
		{"10.11.0", "10.11.5", false}, // patch bump within series
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

	// MariaDB rejects gtid_mode/enforce_gtid_consistency; it pins gtid_strict_mode.
	gtid := eng.GTIDConfigSettings()
	if len(gtid) != 1 || gtid[0] != [2]string{"gtid_strict_mode", "ON"} {
		t.Errorf("GTIDConfigSettings() = %v, want [[gtid_strict_mode ON]]", gtid)
	}
}

func TestMariaDBLifecycle(t *testing.T) {
	eng := MustForFlavor(FlavorMariaDB)

	if got, want := eng.InitBinary(), mariadbInitBinary; got != want {
		t.Errorf("InitBinary() = %q, want %q", got, want)
	}

	if got, want := eng.ServerdCommand(), mariadbServerdBinary; got != want {
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

	args = eng.UpgradeArgs("/var/run/mysqld/mysqld.sock", "root")
	if len(args) != 3 {
		t.Fatalf("UpgradeArgs() returned %d args, want 3", len(args))
	}
	if args[0] != "--force" {
		t.Errorf("UpgradeArgs[0] = %q, want --force", args[0])
	}
	if args[1] != "--socket=/var/run/mysqld/mysqld.sock" {
		t.Errorf("UpgradeArgs[1] = %q", args[1])
	}
	if args[2] != "-uroot" {
		t.Errorf("UpgradeArgs[2] = %q, want -uroot", args[2])
	}

	if eng.UpgradeBinary() != "mariadb-upgrade" {
		t.Errorf("UpgradeBinary() = %q, want mariadb-upgrade", eng.UpgradeBinary())
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

	// MySQL enables GTID via gtid_mode + enforce_gtid_consistency.
	gtid := eng.GTIDConfigSettings()
	want := [][2]string{{"gtid_mode", "ON"}, {"enforce_gtid_consistency", "ON"}}
	if len(gtid) != len(want) || gtid[0] != want[0] || gtid[1] != want[1] {
		t.Errorf("GTIDConfigSettings() = %v, want %v", gtid, want)
	}
}

func TestMySQLLifecycle(t *testing.T) {
	eng := MustForFlavor(FlavorMySQL)

	if got, want := eng.InitBinary(), defaultMySQLdBinary; got != want {
		t.Errorf("InitBinary() = %q, want %q", got, want)
	}

	if got, want := eng.ServerdCommand(), defaultMySQLdBinary; got != want {
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

	if len(eng.UpgradeArgs("", "")) != 0 {
		t.Errorf("UpgradeArgs() should be empty for MySQL")
	}
	if eng.UpgradeBinary() != "" {
		t.Errorf("UpgradeBinary() = %q, want empty", eng.UpgradeBinary())
	}
}
