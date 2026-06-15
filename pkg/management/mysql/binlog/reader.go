/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package binlog

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
)

// Reader queries the local mysqld for binary-log state and issues the
// archiver's flush/purge statements. It is a thin executor over a pool
// Connection so it stays unit-testable with sqlmock.
type Reader struct {
	conn pool.Connection
}

// NewReader builds a Reader bound to a mysqld connection.
func NewReader(conn pool.Connection) *Reader {
	return &Reader{conn: conn}
}

// ServerUUID returns the server's server_uuid, which partitions this instance's
// segment of the archive.
func (r *Reader) ServerUUID(ctx context.Context) (string, error) {
	var uuid sql.NullString
	if err := r.conn.QueryRowContext(ctx, "SELECT @@GLOBAL.server_uuid").Scan(&uuid); err != nil {
		return "", fmt.Errorf("binlog: reading server_uuid: %w", err)
	}
	if !uuid.Valid || uuid.String == "" {
		return "", fmt.Errorf("binlog: server_uuid is empty")
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
