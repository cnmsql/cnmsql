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

// Package heartbeat measures end-to-end replication delay in wall-clock time.
//
// The writable primary's instance manager stamps the current UTC time into a
// small replicated table once per interval. Every instance manager reads the
// table back and subtracts the newest stamp it has applied from its own clock.
// On a replica the difference is the age of the most recent write it has caught
// up to, which is what "how many seconds of writes would we lose" actually
// means. This is the mechanism Percona's pt-heartbeat popularised, reimplemented
// in the instance manager so no sidecar or cron is needed.
//
// It exists because the server's own Seconds_Behind_Source cannot answer that
// question. That column times the SQL applier against the events it has already
// received, so a replica that stopped receiving anything reads zero once its
// relay log drains, however far the primary ran ahead without it. It is also
// NULL whenever the IO thread is disconnected, which is precisely the state
// every replica is in the moment its primary dies. The heartbeat has neither
// blind spot: when replication stops, the stamps stop arriving and the measured
// age grows on its own.
//
// The table shape is pt-heartbeat's on purpose, so the heartbeat collector that
// already ships in mysqld_exporter scrapes it without configuration.
package heartbeat

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

const (
	// DefaultSchema and DefaultTable match pt-heartbeat's defaults, which are
	// also the defaults the mysqld_exporter heartbeat collector looks for.
	DefaultSchema = "heartbeat"
	DefaultTable  = "heartbeat"
	// DefaultInterval is how often the primary stamps the table. It bounds the
	// resolution of every lag reading: a replica in perfect sync still reports an
	// age of up to one interval simply because the next stamp has not been
	// written yet.
	DefaultInterval = time.Second
)

// tsLayout is pt-heartbeat's wire format, stored in a varchar rather than a
// DATETIME. It is fixed width and big-endian in significance, so a lexical MAX()
// over the column is also the chronological maximum, which is what lets a single
// aggregate find the newest stamp across every primary that ever wrote one.
const tsLayout = "%Y-%m-%dT%H:%i:%S.%f"

// Config configures the heartbeat loop.
type Config struct {
	// Schema and Table locate the heartbeat table. Empty values take the
	// pt-heartbeat defaults.
	Schema string
	Table  string
	// Interval is the stamping period. Zero takes DefaultInterval.
	Interval time.Duration
}

func (c Config) schema() string {
	if c.Schema == "" {
		return DefaultSchema
	}
	return c.Schema
}

func (c Config) table() string {
	if c.Table == "" {
		return DefaultTable
	}
	return c.Table
}

func (c Config) interval() time.Duration {
	if c.Interval <= 0 {
		return DefaultInterval
	}
	return c.Interval
}

// State is the loop's current view, served into the instance status.
type State struct {
	// Writing is true when this instance is the one stamping the table, i.e. it
	// is the writable primary.
	Writing bool
	// Lag is the age of the newest stamp this instance has applied, measured
	// against its own clock. Valid only when LagKnown is true.
	//
	// While the primary is alive and stamping, this is the replication delay.
	// Once the primary stops, no new stamps arrive and the value grows by one
	// second per second, so a reader must not treat a large value during an
	// outage as evidence that transactions were lost. Subtracting how long the
	// primary has been down recovers the delay as it stood when the primary died,
	// which is the part that corresponds to lost writes.
	Lag time.Duration
	// LagKnown is false before the first successful read, and while the table is
	// missing or empty (no primary has ever stamped it).
	LagKnown bool
	// SampledAt is when Lag was last computed.
	SampledAt time.Time
	// LastError is the most recent read or write failure, empty when the last
	// pass succeeded.
	LastError string
}

// Loop stamps the heartbeat table on the primary and reads it back everywhere.
// One runs in every Pod; which of them writes is decided by the server, not by
// the loop (see tick).
type Loop struct {
	db  *sql.DB
	cfg Config
	log logr.Logger

	mu    sync.RWMutex
	state State

	// schemaReady records that the DDL has been applied by this process, so the
	// CREATE statements are not replayed on every stamp.
	schemaReady bool
	// now is the clock, overridable in tests.
	now func() time.Time
}

// NewLoop builds a heartbeat loop against the local server.
func NewLoop(db *sql.DB, cfg Config, log logr.Logger) *Loop {
	return &Loop{db: db, cfg: cfg, log: log, now: time.Now}
}

// State returns the loop's current view.
func (l *Loop) State() State {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.state
}

// Run stamps and reads the heartbeat until the context is cancelled. It never
// returns an error: a heartbeat that cannot be written or read degrades the lag
// reading to unknown, which callers already have to handle, and must not take
// the instance manager down with it.
func (l *Loop) Run(ctx context.Context) {
	ticker := time.NewTicker(l.cfg.interval())
	defer ticker.Stop()
	for {
		l.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// tick runs one stamp-and-read pass.
func (l *Loop) tick(ctx context.Context) {
	writing, err := l.stamp(ctx)
	if err != nil {
		l.fail(err)
		return
	}
	lag, known, err := l.read(ctx)
	if err != nil {
		l.fail(err)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state = State{
		Writing:   writing,
		Lag:       lag,
		LagKnown:  known,
		SampledAt: l.now(),
	}
}

func (l *Loop) fail(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Keep the last good reading's shape but drop its validity: a stale lag is
	// worse than no lag, because a caller cannot tell how old it is.
	l.state.LagKnown = false
	l.state.LastError = err.Error()
	l.log.V(1).Info("Heartbeat pass failed", "error", err)
}

// stamp writes the current UTC time into the heartbeat table, and reports
// whether this instance did the writing.
//
// Only the writable primary may write: a stamp applied on a replica would be a
// transaction the primary never issued, which is an errant transaction and would
// mark the replica diverged for good. Two things prevent that. The loop asks the
// server whether it is read-only first, and even if that raced a demotion, the
// server itself refuses the write under super_read_only. The read-only case is
// therefore not an error, it is the normal state of every replica in the
// cluster.
func (l *Loop) stamp(ctx context.Context) (bool, error) {
	writable, err := l.writable(ctx)
	if err != nil {
		return false, err
	}
	if !writable {
		return false, nil
	}
	if err := l.ensureSchema(ctx); err != nil {
		return false, err
	}
	// One row per server_id, so a promoted primary starts its own row rather than
	// overwriting the dead one's. Readers take the newest stamp across all rows,
	// which makes the handover seamless and leaves the old row as harmless
	// history.
	stamp := fmt.Sprintf(
		"INSERT INTO %s (server_id, ts) VALUES (@@server_id, DATE_FORMAT(UTC_TIMESTAMP(6), '%s')) "+
			"ON DUPLICATE KEY UPDATE ts = VALUES(ts)",
		l.qualifiedTable(), tsLayout,
	)
	if _, err := l.db.ExecContext(ctx, stamp); err != nil {
		return false, fmt.Errorf("stamping heartbeat: %w", err)
	}
	return true, nil
}

// writable reports whether the server currently accepts writes, which is true
// only on the confirmed primary.
//
// MariaDB has no super_read_only: it is a MySQL variable, and asking for it
// there fails the whole statement with ER_UNKNOWN_SYSTEM_VARIABLE. So the probe
// asks for it, and falls back to read_only when the server has never heard of
// it. Very old MySQL (pre-5.7.8) lacks it for the same reason and takes the same
// path. This mirrors the archiver's Writable, which needs the answer for the
// same reason: only the primary may generate transactions.
//
// The flag is read as a string, never straight into a bool: engines disagree on
// how they render it. MySQL and MariaDB through 11.x answer 0/1, MariaDB 12
// answers OFF/ON, and a bool Scan of "OFF" fails outright, which would leave
// every server looking unwritable and stop the primary stamping at all.
func (l *Loop) writable(ctx context.Context) (bool, error) {
	var readOnly sql.NullString
	err := l.db.QueryRowContext(ctx, "SELECT @@GLOBAL.super_read_only").Scan(&readOnly)
	if err != nil {
		if err2 := l.db.QueryRowContext(ctx, "SELECT @@GLOBAL.read_only").Scan(&readOnly); err2 != nil {
			return false, fmt.Errorf("reading server read-only state: %w", err2)
		}
	}
	return !parseServerBool(readOnly.String), nil
}

// parseServerBool reads a server boolean in any of the spellings the engines
// use for it: 0/1, OFF/ON, and the mixed-case variants.
func parseServerBool(s string) bool {
	switch strings.ToUpper(s) {
	case "1", "ON", "TRUE", "YES":
		return true
	default:
		return false
	}
}

// read returns the age of the newest stamp this instance has applied.
//
// The subtraction happens in the server, between two naive UTC datetimes, so it
// never passes through a time zone: the stamp is written as UTC and compared
// against UTC_TIMESTAMP. That also means the reading is only as good as the
// clocks agree between the primary's node and this one. Kubernetes nodes are
// NTP-disciplined in practice and skew shows up as a constant offset, but a node
// with a badly wrong clock will report a badly wrong lag, exactly as pt-heartbeat
// would.
func (l *Loop) read(ctx context.Context) (time.Duration, bool, error) {
	query := fmt.Sprintf(
		"SELECT TIMESTAMPDIFF(MICROSECOND, MAX(ts), UTC_TIMESTAMP(6)) FROM %s",
		l.qualifiedTable(),
	)
	var micros sql.NullInt64
	err := l.db.QueryRowContext(ctx, query).Scan(&micros)
	switch {
	case isMissingTable(err):
		// No primary has stamped yet (a cluster that has never had one, or one
		// still bootstrapping). Not an error, just nothing to report.
		return 0, false, nil
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("reading heartbeat: %w", err)
	case !micros.Valid:
		// The table exists but holds no rows: MAX() over nothing is NULL.
		return 0, false, nil
	}
	// A replica whose clock runs behind the primary's can compute a negative age.
	// Report it as zero rather than as a negative lag, which no caller can use.
	return max(time.Duration(micros.Int64)*time.Microsecond, 0), true, nil
}

// ensureSchema creates the heartbeat table if it is not there. It runs on the
// primary only (stamp gates it), so the DDL reaches the replicas through the
// binary log like any other statement.
func (l *Loop) ensureSchema(ctx context.Context) error {
	if l.schemaReady {
		return nil
	}
	create := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdent(l.cfg.schema())),
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s ("+
				"ts varchar(26) NOT NULL, "+
				"server_id int unsigned NOT NULL PRIMARY KEY"+
				") ENGINE=InnoDB",
			l.qualifiedTable(),
		),
	}
	for _, stmt := range create {
		if _, err := l.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("creating heartbeat table: %w", err)
		}
	}
	l.schemaReady = true
	return nil
}

func (l *Loop) qualifiedTable() string {
	return quoteIdent(l.cfg.schema()) + "." + quoteIdent(l.cfg.table())
}

// quoteIdent backtick-quotes an identifier, doubling any embedded backtick.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
