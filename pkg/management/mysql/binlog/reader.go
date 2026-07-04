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

package binlog

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/pool"
)

// defaultServerIdentityQuery reads the MySQL server_uuid. MariaDB has no
// server_uuid, so callers on that flavor construct the Reader with server_id via
// NewReaderWithIdentityQuery.
const defaultServerIdentityQuery = "SELECT @@GLOBAL.server_uuid"

// Reader queries the local mysqld for binary-log state and issues the
// archiver's flush/purge statements. It is a thin executor over a pool
// Connection so it stays unit-testable with sqlmock.
type Reader struct {
	conn pool.Connection
	// identityQuery reads the stable per-instance value that partitions this
	// instance's archive segment (MySQL server_uuid; MariaDB server_id).
	identityQuery string
}

// NewReader builds a Reader bound to a mysqld connection, reading the MySQL
// server_uuid as the archive-partition identity.
func NewReader(conn pool.Connection) *Reader {
	return &Reader{conn: conn, identityQuery: defaultServerIdentityQuery}
}

// NewReaderWithIdentityQuery builds a Reader that reads its archive-partition
// identity via the given query. MariaDB passes "SELECT @@GLOBAL.server_id"
// because it has no server_uuid. An empty query falls back to the MySQL default.
func NewReaderWithIdentityQuery(conn pool.Connection, identityQuery string) *Reader {
	if identityQuery == "" {
		identityQuery = defaultServerIdentityQuery
	}
	return &Reader{conn: conn, identityQuery: identityQuery}
}

// ServerUUID returns the server's stable archive-partition identity (MySQL
// server_uuid; MariaDB server_id), which partitions this instance's segment of
// the archive.
func (r *Reader) ServerUUID(ctx context.Context) (string, error) {
	var uuid sql.NullString
	if err := r.conn.QueryRowContext(ctx, r.identityQuery).Scan(&uuid); err != nil {
		return "", fmt.Errorf("binlog: reading server identity: %w", err)
	}
	if !uuid.Valid || uuid.String == "" {
		return "", fmt.Errorf("binlog: server identity is empty")
	}
	return uuid.String, nil
}

// Writable reports whether the server currently accepts writes, i.e. it is the
// confirmed primary. Only the in-Pod role reconciler clears super_read_only on
// the primary, so a writable server is the archive's authoritative source. It
// reads super_read_only when available and falls back to read_only.
func (r *Reader) Writable(ctx context.Context) (bool, error) {
	var value sql.NullString
	err := r.conn.QueryRowContext(ctx, "SELECT @@GLOBAL.super_read_only").Scan(&value)
	if err != nil {
		// Older servers (pre-5.7.8) lack super_read_only; fall back to read_only.
		if err2 := r.conn.QueryRowContext(ctx, "SELECT @@GLOBAL.read_only").Scan(&value); err2 != nil {
			return false, fmt.Errorf("binlog: reading read_only flags: %w", err2)
		}
	}
	return !parseMySQLBool(value.String), nil
}

func parseMySQLBool(s string) bool {
	switch s {
	case "1", "ON", "on", "true", "TRUE", "Yes", "YES":
		return true
	default:
		return false
	}
}

// ListBinaryLogs runs SHOW BINARY LOGS and returns the entries with the active
// (currently-written) log flagged. The active log is the highest-sequence one.
func (r *Reader) ListBinaryLogs(ctx context.Context) ([]BinaryLog, error) {
	rows, err := r.conn.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("binlog: SHOW BINARY LOGS: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("binlog: reading columns: %w", err)
	}

	var logs []BinaryLog
	for rows.Next() {
		raw := make([]sql.RawBytes, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("binlog: scanning row: %w", err)
		}
		entry := BinaryLog{}
		for i, col := range cols {
			switch col {
			case "Log_name":
				entry.Name = string(raw[i])
			case "File_size":
				if v, err := strconv.ParseInt(string(raw[i]), 10, 64); err == nil {
					entry.SizeBytes = v
				}
			}
		}
		if entry.Name != "" {
			logs = append(logs, entry)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("binlog: iterating rows: %w", err)
	}
	return MarkActive(logs), nil
}

// FlushLogs forces mysqld to rotate the active binary log so the previous
// segment becomes immutable and archivable. This is the RPO trigger.
func (r *Reader) FlushLogs(ctx context.Context) error {
	if _, err := r.conn.ExecContext(ctx, FlushLogsStatement()); err != nil {
		return fmt.Errorf("binlog: flushing logs: %w", err)
	}
	return nil
}

// PurgeLogsTo purges binary logs up to (not including) upTo. Callers must only
// pass a file the archive has already captured, keeping the purge gate honest.
func (r *Reader) PurgeLogsTo(ctx context.Context, upTo string) error {
	stmt, err := PurgeLogsStatement(upTo)
	if err != nil {
		return err
	}
	if _, err := r.conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("binlog: purging logs to %q: %w", upTo, err)
	}
	return nil
}
