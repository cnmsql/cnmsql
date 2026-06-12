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

package replication

import (
	"context"
	"fmt"
	"strings"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
)

// Manager executes replication and role-transition statements against a mysqld
// connection. The statement text is produced by the version-aware builders in
// this package, so Manager stays a thin, ordered executor.
type Manager struct {
	conn    pool.Connection
	version version.Version
}

// NewManager builds a Manager bound to a connection and server version.
func NewManager(conn pool.Connection, v version.Version) *Manager {
	return &Manager{conn: conn, version: v}
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
	if err := m.exec(ctx, StopReplicaStatement(m.version)); err != nil {
		return err
	}
	if err := m.exec(ctx, ChangeSourceStatement(m.version, opts)); err != nil {
		return err
	}
	if !start {
		return nil
	}
	return m.exec(ctx, StartReplicaStatement(m.version))
}

// ProvisionFromBackup configures a freshly restored replica: it resets the
// binary logs and GTID history, sets gtid_purged to the backup's GTID set so
// auto-positioning resumes from the backup point, then points the replica at
// the source. gtidPurged may be empty for a non-GTID backup.
//
// It deliberately does NOT start replication: this runs on the throwaway
// temporary server (started with --skip-slave-start), and the real instance
// resumes replication from the persisted source config on its next boot.
// Starting here is redundant on the throwaway server, so we configure-only.
func (m *Manager) ProvisionFromBackup(ctx context.Context, gtidPurged string, opts SourceOptions) error {
	if err := m.exec(ctx, ResetBinaryLogsStatement(m.version)); err != nil {
		return err
	}
	if gtidPurged != "" {
		if err := m.exec(ctx, SetGTIDPurgedStatement(gtidPurged)); err != nil {
			return err
		}
	}
	return m.configureSource(ctx, opts, false)
}

// StartReplica starts the replication threads.
func (m *Manager) StartReplica(ctx context.Context) error {
	return m.exec(ctx, StartReplicaStatement(m.version))
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
	return m.exec(ctx, StopReplicaStatement(m.version))
}

// ResetReplica clears replication configuration. With all=true it also removes
// connection settings (RESET REPLICA ALL).
func (m *Manager) ResetReplica(ctx context.Context, all bool) error {
	return m.exec(ctx, ResetReplicaStatement(m.version, all))
}

// SetReadOnly toggles read_only.
func (m *Manager) SetReadOnly(ctx context.Context, on bool) error {
	return m.exec(ctx, SetReadOnlyStatement(on))
}

// SetSuperReadOnly toggles super_read_only when supported by the server.
func (m *Manager) SetSuperReadOnly(ctx context.Context, on bool) error {
	if !m.version.HasSuperReadOnly() {
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

func (m *Manager) installPlugin(ctx context.Context, stmt string) error {
	if _, err := m.conn.ExecContext(ctx, stmt); err != nil && !isPluginAlreadyInstalled(err) {
		return fmt.Errorf("executing %q: %w", stmt, err)
	}
	return nil
}

// isPluginAlreadyInstalled reports whether the error is MySQL error 1968
// (ER_PLUGIN_INSTALLED) so re-installing a plugin is idempotent.
func isPluginAlreadyInstalled(err error) bool {
	return mysqlErrorNumber(err) == errPluginInstalled
}
