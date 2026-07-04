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

import "fmt"

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

// --- MariaDB engine ---

type mariadbEngine struct{}

func (mariadbEngine) Flavor() Flavor                 { return FlavorMariaDB }
func (mariadbEngine) GTID() GTIDModel                { return mariadbGTID{} }
func (mariadbEngine) HasSuperReadOnly() bool         { return false }
func (mariadbEngine) SupportsGroupReplication() bool { return false }
