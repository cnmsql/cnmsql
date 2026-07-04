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

// --- MariaDB engine ---

type mariadbEngine struct{}

func (mariadbEngine) Flavor() Flavor                 { return FlavorMariaDB }
func (mariadbEngine) GTID() GTIDModel                { return mariadbGTID{} }
func (mariadbEngine) HasSuperReadOnly() bool         { return false }
func (mariadbEngine) SupportsGroupReplication() bool { return false }

// versioning — delegates to MySQL version functions for now; M-MDB.3 adds
// MariaDB-specific series chain and version parsing.

func (mariadbEngine) ParseServerVersion(raw string) (version.Version, error) {
	return version.Parse(raw)
}

func (mariadbEngine) Series(v version.Version) version.Version {
	return v.Series()
}

func (mariadbEngine) UpgradeChain() []version.Version {
	return version.UpgradeSeriesChain
}

func (mariadbEngine) CheckUpgrade(from, to version.Version) error {
	return version.CheckUpgrade(from, to)
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
