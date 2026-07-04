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
