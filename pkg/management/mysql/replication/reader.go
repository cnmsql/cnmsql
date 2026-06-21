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
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// ReplicaState is the parsed result of SHOW REPLICA/SLAVE STATUS.
type ReplicaState struct {
	// Configured is false when the instance has no replication source set up
	// (the status query returns no rows).
	Configured bool
	// SourceHost is the configured replication source host.
	SourceHost string
	// IORunning and SQLRunning are the replication thread states.
	IORunning  bool
	SQLRunning bool
	// SecondsBehindSource is the replication lag; nil when not replicating.
	SecondsBehindSource *int64
	// LastError holds the most recent replication error, if any.
	LastError string
	// RetrievedGTIDSet is the set of GTIDs received from the source.
	RetrievedGTIDSet string
}

// ReadOnlyState holds the read-only flags of the server.
type ReadOnlyState struct {
	ReadOnly      bool
	SuperReadOnly bool
}

// GTIDExecuted returns the global gtid_executed set.
func (m *Manager) GTIDExecuted(ctx context.Context) (string, error) {
	return m.scalarString(ctx, "SELECT @@GLOBAL.gtid_executed")
}

// GTIDPurged returns the global gtid_purged set.
func (m *Manager) GTIDPurged(ctx context.Context) (string, error) {
	return m.scalarString(ctx, "SELECT @@GLOBAL.gtid_purged")
}

// ServerUUID returns the server's UUID.
func (m *Manager) ServerUUID(ctx context.Context) (string, error) {
	return m.scalarString(ctx, "SELECT @@GLOBAL.server_uuid")
}

// ReadOnly returns the read_only / super_read_only flags. super_read_only is
// reported false on servers that do not support it.
func (m *Manager) ReadOnly(ctx context.Context) (ReadOnlyState, error) {
	var state ReadOnlyState
	ro, err := m.scalarString(ctx, "SELECT @@GLOBAL.read_only")
	if err != nil {
		return state, err
	}
	state.ReadOnly = parseBool(ro)

	if m.version.HasSuperReadOnly() {
		sro, err := m.scalarString(ctx, "SELECT @@GLOBAL.super_read_only")
		if err != nil {
			return state, err
		}
		state.SuperReadOnly = parseBool(sro)
	}
	return state, nil
}

// SemiSyncState reports whether the semi-sync source/replica plugins are
// enabled, using the version-appropriate variable names.
type SemiSyncState struct {
	SourceEnabled  bool
	ReplicaEnabled bool
}

// SemiSyncStatus reads the semi-sync enabled flags. Missing variables (plugins
// not installed) are reported as disabled rather than an error.
func (m *Manager) SemiSyncStatus(ctx context.Context) (SemiSyncState, error) {
	naming := m.version.SemiSync()
	source, err := m.optionalGlobalBool(ctx, naming.EnabledVarSource)
	if err != nil {
		return SemiSyncState{}, err
	}
	replica, err := m.optionalGlobalBool(ctx, naming.EnabledVarReplica)
	if err != nil {
		return SemiSyncState{}, err
	}
	return SemiSyncState{SourceEnabled: source, ReplicaEnabled: replica}, nil
}

// Uptime returns the mysqld uptime in seconds.
func (m *Manager) Uptime(ctx context.Context) (int64, error) {
	var name string
	var value sql.NullString
	row := m.conn.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Uptime'")
	if err := row.Scan(&name, &value); err != nil {
		return 0, fmt.Errorf("reading uptime: %w", err)
	}
	if !value.Valid {
		return 0, nil
	}
	uptime, err := strconv.ParseInt(value.String, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing uptime %q: %w", value.String, err)
	}
	return uptime, nil
}

// optionalGlobalBool reads a global variable as a bool, treating an unknown
// variable (plugin not installed) as false.
func (m *Manager) optionalGlobalBool(ctx context.Context, name string) (bool, error) {
	query := fmt.Sprintf("SELECT @@GLOBAL.%s", name)
	var value sql.NullString
	if err := m.conn.QueryRowContext(ctx, query).Scan(&value); err != nil {
		if mysqlErrorNumber(err) == errUnknownSystemVariable {
			return false, nil
		}
		return false, fmt.Errorf("query %q: %w", query, err)
	}
	return parseBool(value.String), nil
}

func (m *Manager) scalarString(ctx context.Context, query string) (string, error) {
	var value sql.NullString
	if err := m.conn.QueryRowContext(ctx, query).Scan(&value); err != nil {
		return "", fmt.Errorf("query %q: %w", query, err)
	}
	return value.String, nil
}

// ReplicaState runs SHOW REPLICA/SLAVE STATUS and parses it, coping with the
// column renames introduced in MySQL 8.0.22.
func (m *Manager) ReplicaState(ctx context.Context) (*ReplicaState, error) {
	query := ShowReplicaStatusStatement(m.version)
	rows, err := m.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", query, err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("reading columns: %w", err)
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return &ReplicaState{Configured: false}, nil
	}

	row, err := scanRowToMap(rows, cols)
	if err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return parseReplicaStatus(row), nil
}

// scanRowToMap scans the current row into a column->value string map.
func scanRowToMap(rows *sql.Rows, cols []string) (map[string]string, error) {
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}
	out := make(map[string]string, len(cols))
	for i, col := range cols {
		out[col] = string(raw[i])
	}
	return out, nil
}

// parseReplicaStatus extracts a ReplicaState from a SHOW REPLICA STATUS row,
// trying both the modern (Replica_/Source_) and legacy (Slave_/Master_) column
// names.
func parseReplicaStatus(row map[string]string) *ReplicaState {
	state := &ReplicaState{Configured: true}

	state.SourceHost = firstNonEmpty(row, "Source_Host", "Master_Host")
	state.IORunning = parseYesNo(firstNonEmpty(row, "Replica_IO_Running", "Slave_IO_Running"))
	state.SQLRunning = parseYesNo(firstNonEmpty(row, "Replica_SQL_Running", "Slave_SQL_Running"))
	state.RetrievedGTIDSet = firstNonEmpty(row, "Retrieved_Gtid_Set")
	state.LastError = firstNonEmpty(row, "Last_Error", "Last_IO_Error", "Last_SQL_Error")

	if lag := firstNonEmpty(row, "Seconds_Behind_Source", "Seconds_Behind_Master"); lag != "" && lag != "NULL" {
		if v, err := strconv.ParseInt(lag, 10, 64); err == nil {
			state.SecondsBehindSource = &v
		}
	}

	return state
}

func firstNonEmpty(row map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := row[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

func parseYesNo(s string) bool {
	return strings.EqualFold(strings.TrimSpace(s), "Yes")
}

// parseBool interprets MySQL boolean-ish values ("1", "ON", "true").
func parseBool(s string) bool {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "1", "ON", "TRUE", "YES":
		return true
	default:
		return false
	}
}
