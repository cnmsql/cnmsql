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

package replication

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/pool"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// Dialect is the replication SQL dialect a Manager executes: the verbs and
// syntax for CHANGE MASTER/SOURCE, START/STOP/RESET replica, SHOW ... STATUS,
// RESET MASTER/BINARY LOGS, GTID-position queries and replica-position seeding.
// It is declared here (not imported from engine) so the replication package
// avoids an import cycle with engine, which imports this package for the
// statement builders. engine.ReplDialect matches this shape structurally, so
// engine.ForFlavor(...).Repl() can be passed to NewManagerWithDialect.
type Dialect interface {
	ChangeSource(v version.Version, opts SourceOptions) string
	StartReplica(v version.Version) string
	StopReplica(v version.Version) string
	ResetReplica(v version.Version, all bool) string
	ShowReplicaStatus(v version.Version) string
	ResetBinaryLogs(v version.Version) string
	GTIDExecutedQuery() string
	// GTIDPurgedQuery reads the purged-GTID set. Empty means the flavor has no
	// such concept (MariaDB) and the caller should skip the read.
	GTIDPurgedQuery() string
	// ServerIdentityQuery reads the stable per-instance identity used to
	// partition binary-log archive segments (MySQL server_uuid; MariaDB
	// server_id, as MariaDB has no server_uuid).
	ServerIdentityQuery() string
	SeedReplicaPosition(pos string) string
	SemiSyncNaming(v version.Version) version.SemiSyncNaming
	HasSuperReadOnly() bool
}

// mysqlDialect delegates to the version-aware statement builders in this
// package. It is the default when no dialect is injected via
// NewManagerWithDialect (which M-MDB.2 wires from engine.ForFlavor(...).Repl()).
type mysqlDialect struct{}

func (mysqlDialect) ChangeSource(v version.Version, opts SourceOptions) string {
	return ChangeSourceStatement(v, opts)
}
func (mysqlDialect) StartReplica(v version.Version) string {
	return StartReplicaStatement(v)
}
func (mysqlDialect) StopReplica(v version.Version) string {
	return StopReplicaStatement(v)
}
func (mysqlDialect) ResetReplica(v version.Version, all bool) string {
	return ResetReplicaStatement(v, all)
}
func (mysqlDialect) ShowReplicaStatus(v version.Version) string {
	return ShowReplicaStatusStatement(v)
}
func (mysqlDialect) ResetBinaryLogs(v version.Version) string {
	return ResetBinaryLogsStatement(v)
}
func (mysqlDialect) GTIDExecutedQuery() string {
	return "SELECT @@GLOBAL.gtid_executed"
}
func (mysqlDialect) GTIDPurgedQuery() string {
	return "SELECT @@GLOBAL.gtid_purged"
}
func (mysqlDialect) ServerIdentityQuery() string {
	return "SELECT @@GLOBAL.server_uuid"
}
func (mysqlDialect) SeedReplicaPosition(pos string) string {
	return SetGTIDPurgedStatement(pos)
}
func (mysqlDialect) SemiSyncNaming(v version.Version) version.SemiSyncNaming {
	return v.SemiSync()
}
func (mysqlDialect) HasSuperReadOnly() bool { return true }

// Manager executes replication and role-transition statements against a mysqld
// connection. The statement text is produced by a replDialect, whose default is
// the version-aware builders in this package.
type Manager struct {
	conn    pool.Connection
	version version.Version
	repl    Dialect
}

// NewManager builds a Manager bound to a connection and server version. The
// replication dialect defaults to the built-in MySQL/Percona statement builders.
func NewManager(conn pool.Connection, v version.Version) *Manager {
	return &Manager{conn: conn, version: v, repl: mysqlDialect{}}
}

// NewManagerWithDialect builds a Manager that executes statements through the
// supplied Dialect (e.g. engine.ForFlavor(...).Repl()) instead of the default
// MySQL/Percona builders. A nil dialect falls back to the MySQL default.
func NewManagerWithDialect(conn pool.Connection, v version.Version, d Dialect) *Manager {
	if d == nil {
		d = mysqlDialect{}
	}
	return &Manager{conn: conn, version: v, repl: d}
}

func (m *Manager) exec(ctx context.Context, stmt string) error {
	if _, err := m.conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("executing %q: %w", stmt, err)
	}
	return nil
}

// ConfigureSource points the replica at the given source and starts
// replication: STOP REPLICA, CHANGE REPLICATION SOURCE, START REPLICA.
func (m *Manager) ConfigureSource(ctx context.Context, opts SourceOptions) error {
	return m.configureSource(ctx, opts, true)
}

// configureSource runs STOP REPLICA, CHANGE REPLICATION SOURCE and, when start
// is true, START REPLICA.
func (m *Manager) configureSource(ctx context.Context, opts SourceOptions, start bool) error {
	if err := m.exec(ctx, m.repl.StopReplica(m.version)); err != nil {
		return err
	}
	if err := m.exec(ctx, m.repl.ChangeSource(m.version, opts)); err != nil {
		return err
	}
	if !start {
		return nil
	}
	return m.exec(ctx, m.repl.StartReplica(m.version))
}

// ProvisionFromBackup configures a freshly restored replica: it resets the
// binary logs and GTID history, seeds the replica position (SET GLOBAL
// gtid_purged for MySQL, gtid_slave_pos for MariaDB) so replication resumes
// from the backup point, then points the replica at the source. gtidPurged
// may be empty for a non-GTID backup.
//
// It deliberately does NOT start replication: this runs on the throwaway
// temporary server (started with --skip-slave-start), and the real instance
// resumes replication from the persisted source config on its next boot.
// Starting here is redundant on the throwaway server, so we configure-only.
func (m *Manager) ProvisionFromBackup(ctx context.Context, gtidPurged string, opts SourceOptions) error {
	if err := m.exec(ctx, m.repl.ResetBinaryLogs(m.version)); err != nil {
		return err
	}
	if gtidPurged != "" {
		if err := m.exec(ctx, m.repl.SeedReplicaPosition(gtidPurged)); err != nil {
			return err
		}
	}
	return m.configureSource(ctx, opts, false)
}

// ResetBinaryLogs clears the binary logs and the GTID execution history,
// resetting gtid_executed to empty. Group Replication distributed recovery uses
// it on a freshly initialised joiner so the transactions initdb authored locally
// are not seen as errant transactions (which would block a clone from a donor).
func (m *Manager) ResetBinaryLogs(ctx context.Context) error {
	return m.exec(ctx, m.repl.ResetBinaryLogs(m.version))
}

// StartReplica starts the replication threads.
func (m *Manager) StartReplica(ctx context.Context) error {
	return m.exec(ctx, m.repl.StartReplica(m.version))
}

// EnsureReplicaConfigured makes sure this server follows the requested source.
// It configures a missing source, otherwise it starts stopped replication
// threads. It leaves already-running replicas untouched.
func (m *Manager) EnsureReplicaConfigured(ctx context.Context, opts SourceOptions) error {
	state, err := m.ReplicaState(ctx)
	if err != nil {
		return err
	}
	if !state.Configured {
		return m.ConfigureSource(ctx, opts)
	}
	if !sameSourceHost(state.SourceHost, opts.Host) {
		return m.ConfigureSource(ctx, opts)
	}
	if state.IORunning && state.SQLRunning {
		return nil
	}
	return m.StartReplica(ctx)
}

func sameSourceHost(current, desired string) bool {
	return strings.EqualFold(strings.TrimSuffix(current, "."), strings.TrimSuffix(desired, "."))
}

// EnsureReplicaStarted starts replication when this server has a configured
// source but one of the replication threads is not running. It is a no-op on
// primaries and on replicas that are already applying.
func (m *Manager) EnsureReplicaStarted(ctx context.Context) error {
	state, err := m.ReplicaState(ctx)
	if err != nil {
		return err
	}
	if !state.Configured || (state.IORunning && state.SQLRunning) {
		return nil
	}
	return m.StartReplica(ctx)
}

// StopReplica stops the replication threads.
func (m *Manager) StopReplica(ctx context.Context) error {
	return m.exec(ctx, m.repl.StopReplica(m.version))
}

// ResetReplica clears replication configuration. With all=true it also removes
// connection settings (RESET REPLICA ALL).
func (m *Manager) ResetReplica(ctx context.Context, all bool) error {
	return m.exec(ctx, m.repl.ResetReplica(m.version, all))
}

// SetReadOnly toggles read_only.
func (m *Manager) SetReadOnly(ctx context.Context, on bool) error {
	return m.exec(ctx, SetReadOnlyStatement(on))
}

// SetSuperReadOnly toggles super_read_only when supported by the server. The
// flavor must have the feature at all (MariaDB does not, and its dialect reports
// false) and the running version must be recent enough (MySQL gained
// super_read_only in 5.7.8).
func (m *Manager) SetSuperReadOnly(ctx context.Context, on bool) error {
	if !m.repl.HasSuperReadOnly() || !m.version.HasSuperReadOnly() {
		return nil
	}
	return m.exec(ctx, SetSuperReadOnlyStatement(on))
}

// Promote transitions a replica to primary: stop and fully reset replication
// when a source is configured, then clear the read-only flags. A freshly
// bootstrapped primary has no replica metadata, so promotion only needs to clear
// read-only mode.
func (m *Manager) Promote(ctx context.Context) error {
	state, err := m.ReplicaState(ctx)
	if err != nil {
		return err
	}
	if state.Configured {
		if err := m.StopReplica(ctx); err != nil {
			return err
		}
		if err := m.ResetReplica(ctx, true); err != nil {
			return err
		}
	}
	// super_read_only must be cleared before read_only.
	if err := m.SetSuperReadOnly(ctx, false); err != nil {
		return err
	}
	return m.SetReadOnly(ctx, false)
}

// Demote makes the instance read-only, the first step of turning a primary into
// a replica. read_only must be set before super_read_only.
func (m *Manager) Demote(ctx context.Context) error {
	if err := m.SetReadOnly(ctx, true); err != nil {
		return err
	}
	return m.SetSuperReadOnly(ctx, true)
}

// InstallSemiSyncSource installs the semi-sync source plugin, ignoring the
// error raised when it is already installed.
func (m *Manager) InstallSemiSyncSource(ctx context.Context) error {
	return m.installPlugin(ctx, InstallSemiSyncSourceStatement(m.version))
}

// InstallSemiSyncReplica installs the semi-sync replica plugin, ignoring the
// error raised when it is already installed.
func (m *Manager) InstallSemiSyncReplica(ctx context.Context) error {
	return m.installPlugin(ctx, InstallSemiSyncReplicaStatement(m.version))
}

// EnableSemiSync turns the source and replica semi-sync plugins on at runtime.
func (m *Manager) EnableSemiSync(ctx context.Context) error {
	naming := m.repl.SemiSyncNaming(m.version)
	if err := m.exec(ctx, SetGlobalStatement(naming.EnabledVarSource, "1")); err != nil {
		return err
	}
	return m.exec(ctx, SetGlobalStatement(naming.EnabledVarReplica, "1"))
}

// SetSemiSyncWaitForReplicaCount sets, at runtime, how many replica
// acknowledgements the semi-sync source waits for before committing
// (rpl_semi_sync_source_wait_for_replica_count / the legacy slave-count
// variable). The operator lowers this below minSyncReplicas while replicas are
// unhealthy under "preferred" data durability, then restores it as they recover.
func (m *Manager) SetSemiSyncWaitForReplicaCount(ctx context.Context, count int) error {
	naming := m.repl.SemiSyncNaming(m.version)
	return m.exec(ctx, SetGlobalStatement(naming.WaitForCountVar, strconv.Itoa(count)))
}

// SetSemiSyncTimeoutMillis sets the source wait timeout at runtime.
func (m *Manager) SetSemiSyncTimeoutMillis(ctx context.Context, timeoutMillis int) error {
	naming := m.repl.SemiSyncNaming(m.version)
	return m.exec(ctx, SetGlobalStatement(naming.TimeoutVar, strconv.Itoa(timeoutMillis)))
}

func (m *Manager) installPlugin(ctx context.Context, stmt string) error {
	if _, err := m.conn.ExecContext(ctx, stmt); err != nil && !isPluginAlreadyInstalled(err) {
		return fmt.Errorf("executing %q: %w", stmt, err)
	}
	return nil
}

// isPluginAlreadyInstalled reports whether the error means the plugin is
// already loaded, so re-installing a plugin is idempotent.
func isPluginAlreadyInstalled(err error) bool {
	switch mysqlErrorNumber(err) {
	case errPluginInstalled, errFunctionAlreadyExists:
		return true
	default:
		return false
	}
}
