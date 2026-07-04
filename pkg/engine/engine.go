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

// Package engine is the single source of truth for every decision that differs
// between the database engines this operator can drive ("flavors"): today
// MySQL (Percona Server for MySQL) and MariaDB.
//
// Historically the divergence axis was the server *version*
// (pkg/management/mysql/version): call sites branched on
// ver.UsesReplicaTerminology(), ver.SemiSync(), etc. That is insufficient for a
// second engine whose GTID model, replication dialect and backup tool differ
// regardless of version. Engine widens the axis to a (flavor, version) pair and
// funnels each flavor-dependent decision through one interface.
//
// This is the foundation described in design/026-mariadb-support.md. It starts
// with the highest-risk facet — the GTID model — plus the cheap capability
// booleans; the interface grows (config rendering, lifecycle commands, backup
// tool) as later milestones land.
package engine

import (
	"fmt"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/config"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// Flavor selects the database engine a Cluster runs. It mirrors
// api/v1alpha1.Flavor but is duplicated here so the engine package does not
// depend on the API types (the in-Pod instance manager selects an Engine from an
// env var, without the CR).
type Flavor string

const (
	// FlavorMySQL is Percona Server for MySQL, the default and original engine.
	FlavorMySQL Flavor = "mysql"
	// FlavorMariaDB is MariaDB Server.
	FlavorMariaDB Flavor = "mariadb"
)

// Engine is the contract every flavor implements. Implementations are
// stateless value types selected once per reconcile (from spec.flavor) or per
// instance-manager boot (from the CNMSQL_FLAVOR env var).
type Engine interface {
	// Flavor identifies the engine.
	Flavor() Flavor

	// GTID interprets the engine's GTID position/set strings. This is the
	// engine-divergent heart of switchover, failover and errant-transaction
	// (diverged-instance) detection.
	GTID() GTIDModel

	// HasSuperReadOnly reports whether the server supports super_read_only.
	// MySQL does; MariaDB does not (only read_only exists), which changes how
	// the operator enforces read-only on replicas and fenced instances.
	HasSuperReadOnly() bool

	// SupportsGroupReplication gates spec.replication.mode=groupReplication.
	// MySQL supports it; MariaDB does not (its quorum story is Galera, which
	// this operator does not yet manage).
	SupportsGroupReplication() bool

	// --- versioning ---

	// ParseServerVersion normalizes a raw @@version string into a comparable
	// Version. MariaDB reports "11.4.3-MariaDB-..." which must be parsed into
	// a valid version.Version.
	ParseServerVersion(raw string) (version.Version, error)

	// Series maps a runtime version to its catalog/upgrade series.
	Series(version.Version) version.Version

	// UpgradeChain returns the ordered, single-hop-only series chain for this
	// flavor.
	UpgradeChain() []version.Version

	// CheckUpgrade validates a version transition along the flavor's upgrade
	// chain.
	CheckUpgrade(from, to version.Version) error

	// DefaultImage returns the default fully-qualified container image for
	// the latest supported series of this flavor.
	DefaultImage() string

	// DefaultServerVersion resolves the concrete server version string for a
	// catalog series tag (e.g. "8.0" → "8.0.46", "11.4" → "11.4.3").
	DefaultServerVersion(tag string) (string, error)

	// --- replication SQL dialect ---

	// Repl returns the replication SQL dialect for this flavor.
	Repl() ReplDialect

	// --- semi-sync ---

	// SemiSync returns the semi-sync naming appropriate for the server version.
	SemiSync(version.Version) version.SemiSyncNaming

	// SemiSyncIsPlugin reports whether semi-sync is loaded as a plugin (MySQL)
	// or is built-in (MariaDB).
	SemiSyncIsPlugin() bool

	// --- capability ---

	// HasAdminInterface reports whether the server supports the administrative
	// network interface (admin_address/admin_port). MySQL 8.0.14+; MariaDB
	// does not.
	HasAdminInterface(version.Version) bool

	// HasLogReplicaUpdates reports whether the server uses the
	// log_replica_updates variable name instead of log_slave_updates.
	// MySQL 8.0+; MariaDB always uses log_slave_updates.
	HasLogReplicaUpdates(version.Version) bool

	// UsesResetBinaryLogsAndGtids reports whether the server uses the modern
	// "RESET BINARY LOGS AND GTIDS" syntax (MySQL 8.4.0+) vs "RESET MASTER".
	UsesResetBinaryLogsAndGtids(version.Version) bool

	// UsesReplicaTerminology reports whether the server uses SOURCE/REPLICA
	// terminology (MySQL 8.0.23+) vs MASTER/SLAVE. MariaDB never adopted it.
	UsesReplicaTerminology(version.Version) bool

	// --- config ---

	// IsGroupReplicationManagedKey reports whether a normalized config key
	// falls in the operator-owned Group Replication namespace.
	IsGroupReplicationManagedKey(normalized string) bool

	// BinlogExpire returns the version-appropriate binlog expiry variable name
	// and value for the given expiry seconds.
	BinlogExpire(ver version.Version, seconds int) (name, value string)

	// GTIDConfigSettings returns the ordered my.cnf key/value pairs that enable
	// the engine's GTID mode. MySQL needs gtid_mode/enforce_gtid_consistency;
	// MariaDB has no such variables (GTID is inherent with the binlog) and only
	// pins gtid_strict_mode. Emitting the MySQL variables to mariadbd aborts
	// startup with "unknown variable", so this must be flavor-selected.
	GTIDConfigSettings() [][2]string

	// DefaultAuthenticationPlugin returns the default server authentication
	// plugin: caching_sha2_password for MySQL 8.0+, mysql_native_password for
	// MariaDB.
	DefaultAuthenticationPlugin() string

	// --- lifecycle commands ---

	// InitBinary returns the name of the binary used to initialize a fresh
	// data directory ("mysqld" for MySQL, "mariadb-install-db" for MariaDB).
	InitBinary() string

	// InitDataDirArgs returns the arguments passed to InitBinary to
	// initialize a fresh data directory (system tables).
	InitDataDirArgs(datadir string) []string

	// UpgradeArgs returns the arguments passed to the upgrade binary to
	// migrate system tables across a major/minor version boundary.
	UpgradeArgs() []string

	// ServerdCommand returns the name of the server daemon binary
	// ("mysqld" for MySQL, "mariadbd" for MariaDB).
	ServerdCommand() string
}

// ForFlavor returns the Engine for a flavor. An empty flavor resolves to MySQL
// so pre-flavor Clusters (whose spec.flavor defaults to "mysql") and callers
// that have not yet been threaded through the API keep the original behaviour.
func ForFlavor(f Flavor) (Engine, error) {
	switch f {
	case FlavorMySQL, "":
		return mysqlEngine{}, nil
	case FlavorMariaDB:
		return mariadbEngine{}, nil
	default:
		return nil, fmt.Errorf("unknown engine flavor %q", f)
	}
}

// MustForFlavor is ForFlavor for call sites that have already validated the
// flavor (e.g. behind an admission webhook enum). It panics on an unknown
// flavor.
func MustForFlavor(f Flavor) Engine {
	e, err := ForFlavor(f)
	if err != nil {
		panic(err)
	}
	return e
}

// --- MySQL engine ---

type mysqlEngine struct{}

func (mysqlEngine) Flavor() Flavor                 { return FlavorMySQL }
func (mysqlEngine) GTID() GTIDModel                { return mysqlGTID{} }
func (mysqlEngine) HasSuperReadOnly() bool         { return true }
func (mysqlEngine) SupportsGroupReplication() bool { return true }

// versioning

func (mysqlEngine) ParseServerVersion(raw string) (version.Version, error) {
	return version.Parse(raw)
}

func (mysqlEngine) Series(v version.Version) version.Version {
	return v.Series()
}

func (mysqlEngine) UpgradeChain() []version.Version {
	return version.UpgradeSeriesChain
}

func (mysqlEngine) CheckUpgrade(from, to version.Version) error {
	return version.CheckUpgrade(from, to)
}

func (mysqlEngine) DefaultImage() string {
	return "ghcr.io/cnmsql/cnmsql-instance:8.0"
}

func (mysqlEngine) DefaultServerVersion(tag string) (string, error) {
	return mysqlDefaultServerVersion(tag)
}

// replication dialect

func (mysqlEngine) Repl() ReplDialect {
	return mysqlReplDialect{}
}

// semi-sync

func (mysqlEngine) SemiSync(v version.Version) version.SemiSyncNaming {
	return v.SemiSync()
}

func (mysqlEngine) SemiSyncIsPlugin() bool { return true }

// capability

func (mysqlEngine) HasAdminInterface(v version.Version) bool {
	return v.HasAdminInterface()
}

func (mysqlEngine) HasLogReplicaUpdates(v version.Version) bool {
	return v.HasLogReplicaUpdates()
}

func (mysqlEngine) UsesResetBinaryLogsAndGtids(v version.Version) bool {
	return v.UsesResetBinaryLogsAndGtids()
}

func (mysqlEngine) UsesReplicaTerminology(v version.Version) bool {
	return v.UsesReplicaTerminology()
}

// config

func (mysqlEngine) IsGroupReplicationManagedKey(normalized string) bool {
	return config.IsGroupReplicationManagedKey(normalized)
}

func (mysqlEngine) BinlogExpire(ver version.Version, seconds int) (string, string) {
	return config.BinlogExpire(ver, seconds)
}

func (mysqlEngine) GTIDConfigSettings() [][2]string {
	return [][2]string{
		{"gtid_mode", "ON"},
		{"enforce_gtid_consistency", "ON"},
	}
}

func (mysqlEngine) DefaultAuthenticationPlugin() string {
	return "caching_sha2_password"
}

func (mysqlEngine) InitBinary() string {
	return "mysqld"
}

func (mysqlEngine) InitDataDirArgs(datadir string) []string {
	return []string{"--initialize-insecure", "--datadir=" + datadir}
}

func (mysqlEngine) UpgradeArgs() []string {
	return nil
}

func (mysqlEngine) ServerdCommand() string {
	return "mysqld"
}

// --- MariaDB engine ---

type mariadbEngine struct{}

func (mariadbEngine) Flavor() Flavor                 { return FlavorMariaDB }
func (mariadbEngine) GTID() GTIDModel                { return mariadbGTID{} }
func (mariadbEngine) HasSuperReadOnly() bool         { return false }
func (mariadbEngine) SupportsGroupReplication() bool { return false }

// versioning — MariaDB has its own series chain (10.6 → 10.11 → 11.4 → 12.3)
// and normalizes @@version strings that carry a "-MariaDB-" suffix.

var mariadbUpgradeChain = []version.Version{
	{Major: 10, Minor: 6},
	{Major: 10, Minor: 11},
	{Major: 11, Minor: 4},
	{Major: 12, Minor: 3},
}

func (mariadbEngine) ParseServerVersion(raw string) (version.Version, error) {
	return version.Parse(raw)
}

func (mariadbEngine) Series(v version.Version) version.Version {
	return version.Version{Major: v.Major, Minor: v.Minor}
}

func (mariadbEngine) UpgradeChain() []version.Version {
	c := make([]version.Version, len(mariadbUpgradeChain))
	copy(c, mariadbUpgradeChain)
	return c
}

func (mariadbEngine) CheckUpgrade(from, to version.Version) error {
	return checkUpgradeChain(from, to, mariadbUpgradeChain, "MariaDB")
}

func (mariadbEngine) DefaultImage() string {
	return "ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4"
}

func (mariadbEngine) DefaultServerVersion(tag string) (string, error) {
	return mariadbDefaultServerVersion(tag)
}

// replication dialect

func (mariadbEngine) Repl() ReplDialect {
	return mariadbReplDialect{}
}

// semi-sync — MariaDB uses master/slave naming and semi-sync is built-in (no
// INSTALL PLUGIN).

func (mariadbEngine) SemiSync(version.Version) version.SemiSyncNaming {
	// Semi-sync is built into the MariaDB server: there is no INSTALL PLUGIN and
	// no shared library, so Plugin*/Lib* are intentionally left empty. Reading
	// them without gating on SemiSyncIsPlugin() (which is false) is a bug.
	return version.SemiSyncNaming{
		EnabledVarSource:  "rpl_semi_sync_master_enabled",
		EnabledVarReplica: "rpl_semi_sync_slave_enabled",
		WaitForCountVar:   "rpl_semi_sync_master_wait_for_slave_count",
		TimeoutVar:        "rpl_semi_sync_master_timeout",
	}
}

func (mariadbEngine) SemiSyncIsPlugin() bool { return false }

// capability

func (mariadbEngine) HasAdminInterface(version.Version) bool {
	return false
}

func (mariadbEngine) HasLogReplicaUpdates(version.Version) bool {
	return false
}

func (mariadbEngine) UsesResetBinaryLogsAndGtids(version.Version) bool {
	return false
}

func (mariadbEngine) UsesReplicaTerminology(version.Version) bool {
	return false
}

// config

func (mariadbEngine) IsGroupReplicationManagedKey(normalized string) bool {
	_ = normalized
	return false
}

func (mariadbEngine) BinlogExpire(ver version.Version, seconds int) (string, string) {
	return config.BinlogExpire(ver, seconds)
}

func (mariadbEngine) GTIDConfigSettings() [][2]string {
	// MariaDB has no gtid_mode/enforce_gtid_consistency: GTID is inherent once
	// the binary log is enabled. gtid_strict_mode is the safety analog of
	// enforce_gtid_consistency, refusing out-of-order GTIDs.
	return [][2]string{
		{"gtid_strict_mode", "ON"},
	}
}

func (mariadbEngine) DefaultAuthenticationPlugin() string {
	return "mysql_native_password"
}

func (mariadbEngine) InitBinary() string {
	return "mariadb-install-db"
}

func (mariadbEngine) InitDataDirArgs(datadir string) []string {
	return []string{
		"--datadir=" + datadir,
		"--auth-root-authentication-method=normal",
		"--skip-test-db",
	}
}

func (mariadbEngine) UpgradeArgs() []string {
	return nil
}

func (mariadbEngine) ServerdCommand() string {
	return "mariadbd"
}

// --- version helpers ---

// checkUpgradeChain validates a version transition along the given series chain.
// It is the flavour-agnostic equivalent of version.CheckUpgrade (which is
// hard-coded to the MySQL chain).
func checkUpgradeChain(from, to version.Version, chain []version.Version, label string) error {
	if from.Series() == to.Series() {
		return nil
	}
	fromIdx := -1
	toIdx := -1
	for i, e := range chain {
		if e == from.Series() {
			fromIdx = i
		}
		if e == to.Series() {
			toIdx = i
		}
	}
	if fromIdx == -1 {
		return fmt.Errorf("unsupported source %s series %d.%d", label, from.Major, from.Minor)
	}
	if toIdx == -1 {
		return fmt.Errorf("unsupported target %s series %d.%d", label, to.Major, to.Minor)
	}
	if toIdx < fromIdx {
		return fmt.Errorf("downgrade from %s %d.%d to %d.%d is not supported",
			label, from.Major, from.Minor, to.Major, to.Minor)
	}
	if toIdx > fromIdx+1 {
		next := chain[fromIdx+1]
		return fmt.Errorf("cannot upgrade from %s %d.%d directly to %d.%d: upgrade to %d.%d first",
			label, from.Major, from.Minor, to.Major, to.Minor, next.Major, next.Minor)
	}
	return nil
}

// mysqlDefaultServerVersion resolves a MySQL catalog series tag to the concrete
// default server version.
func mysqlDefaultServerVersion(tag string) (string, error) {
	switch tag {
	case "8.0":
		return "8.0.46", nil
	case "8.4":
		return "8.4.0", nil
	case "9.x":
		return "9.6.0", nil
	default:
		return "", fmt.Errorf("unsupported MySQL series %q", tag)
	}
}

// mariadbDefaultServerVersion resolves a MariaDB catalog series tag to the
// concrete default server version.
func mariadbDefaultServerVersion(tag string) (string, error) {
	switch tag {
	case "10.6":
		return "10.6.18", nil
	case "10.11":
		return "10.11.8", nil
	case "11.4":
		return "11.4.3", nil
	case "12.3":
		return "12.3.0", nil
	default:
		return "", fmt.Errorf("unsupported MariaDB series %q", tag)
	}
}
