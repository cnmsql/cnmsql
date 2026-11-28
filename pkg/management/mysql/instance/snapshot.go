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

package instance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/pool"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// SnapshotManifestFile is the file a volume-snapshot hold writes at the root of
// the data directory. It travels inside the snapshot and is what a restore from
// it reads the anchor from, the role xtrabackup_binlog_info plays for an
// xbstream archive (design 045).
const SnapshotManifestFile = "cnmsql-snapshot.json"

// DefaultSnapshotHoldTimeout bounds a hold whose request names no timeout.
const DefaultSnapshotHoldTimeout = 2 * time.Minute

// maxSnapshotHoldTimeout caps a requested hold: a global read lock held longer
// than this is an outage, not a backup.
const maxSnapshotHoldTimeout = 30 * time.Minute

// snapshotLockWait bounds how long FLUSH TABLES WITH READ LOCK waits behind
// running statements. Writes queue behind a pending FTWRL, so a long query
// would otherwise turn the wait into a write stall of its own length.
const snapshotLockWait = 20 * time.Second

// SnapshotManifest is the content of SnapshotManifestFile.
type SnapshotManifest struct {
	BackupName    string `json:"backupName"`
	AnchorGTID    string `json:"anchorGTID"`
	BinlogFile    string `json:"binlogFile,omitempty"`
	BinlogPos     int64  `json:"binlogPos,omitempty"`
	Flavor        string `json:"flavor,omitempty"`
	ServerVersion string `json:"serverVersion,omitempty"`
	Instance      string `json:"instance,omitempty"`
	// TakenAt is when the position was read (RFC 3339).
	TakenAt string `json:"takenAt"`
	// Online is true for a hold under a global read lock, false for an anchor
	// derived from a cleanly stopped server.
	Online bool `json:"online"`
}

// ReadSnapshotManifest reads SnapshotManifestFile from dataDir. It returns
// (nil, nil) when the file does not exist.
func ReadSnapshotManifest(dataDir string) (*SnapshotManifest, error) {
	content, err := os.ReadFile(filepath.Join(dataDir, SnapshotManifestFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest SnapshotManifest
	if err := json.Unmarshal(content, &manifest); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", SnapshotManifestFile, err)
	}
	return &manifest, nil
}

// WriteSnapshotManifest writes manifest to dataDir durably (see
// writeDataDirFile), so the snapshot cut after the call returns always holds
// the complete file.
func WriteSnapshotManifest(dataDir string, manifest SnapshotManifest) error {
	content, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return writeDataDirFile(dataDir, SnapshotManifestFile, append(content, '\n'))
}

// writeDataDirFile writes name under dataDir durably: a temporary file, fsync,
// rename, then an fsync of the directory.
func writeDataDirFile(dataDir, name string, content []byte) error {
	target := filepath.Join(dataDir, name)
	tmp, err := os.CreateTemp(dataDir, "."+strings.TrimPrefix(name, ".")+".*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o640); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), target); err != nil {
		return err
	}
	dir, err := os.Open(dataDir)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

// SnapshotConfig enables the /cluster/snapshot routes.
type SnapshotConfig struct {
	// Engine selects the flavor's statements.
	Engine engine.Engine
	// DataDir is where the manifest is written.
	DataDir string
	// Socket, User and Password (or PasswordFunc) open the hold's dedicated
	// connection as the control account. It is not taken from the control
	// pool: on a single-connection pool it would starve every other control
	// call for the length of the hold.
	Socket       string
	User         string
	Password     string
	PasswordFunc func() string
}

// snapshotHold is an active hold.
type snapshotHold struct {
	backupName string
	conn       *sql.Conn
	closeConn  func()
	// stoppedApplier is set when the hold stopped the replica SQL thread and
	// must start it again on release.
	stoppedApplier bool
	since, expires time.Time
	timer          *time.Timer
	anchor         webserver.SnapshotAnchor
}

// SetSnapshotConfig enables the /cluster/snapshot routes.
func (c *Controller) SetSnapshotConfig(cfg SnapshotConfig) {
	c.snapshot = &cfg
	if c.openSnapshotConn == nil {
		c.openSnapshotConn = cfg.open
	}
}

// open opens the hold's dedicated connection.
func (cfg *SnapshotConfig) open(ctx context.Context) (*sql.Conn, func(), error) {
	db, err := pool.Connect(pool.Config{
		Socket:       cfg.Socket,
		User:         cfg.User,
		Password:     cfg.Password,
		PasswordFunc: cfg.PasswordFunc,
		MaxOpenConns: 1,
	})
	if err != nil {
		return nil, nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return conn, func() {
		_ = conn.Close()
		_ = db.Close()
	}, nil
}

// SnapshotHeld reports whether a snapshot hold is active. The heartbeat writer
// skips its stamp while it is: the stamp would queue behind the global read
// lock and hold a control connection for as long as the lock.
func (c *Controller) SnapshotHeld() bool {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	return c.hold != nil
}

// snapshotHoldStatus is the active hold for the instance status, nil when
// none.
func (c *Controller) snapshotHoldStatus() *webserver.SnapshotHoldStatus {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	if c.hold == nil {
		return nil
	}
	return &webserver.SnapshotHoldStatus{
		BackupName: c.hold.backupName,
		Since:      c.hold.since.UTC().Format(time.RFC3339),
		ExpiresAt:  c.hold.expires.UTC().Format(time.RFC3339),
	}
}

// SnapshotState reports the hold and whether mysqld is fenced and stopped,
// without querying mysqld.
func (c *Controller) SnapshotState() webserver.SnapshotState {
	state := webserver.SnapshotState{Hold: c.snapshotHoldStatus()}
	if c.fence != nil {
		state.Fenced = c.fence.IsFenced()
	}
	if c.supervisor != nil {
		state.MysqldStopped = c.supervisor.Pid() == 0
	}
	state.Cold = c.coldSnapshotStatus()
	return state
}

// BeginSnapshot quiesces the instance for a volume snapshot: it stops the
// replica applier, takes FLUSH TABLES WITH READ LOCK on a dedicated
// connection, reads the position under it, writes the manifest into the data
// directory and arms a timer that releases the hold on its own. A repeated
// begin for the same Backup returns the anchor already taken.
func (c *Controller) BeginSnapshot(
	ctx context.Context, req webserver.SnapshotBeginRequest,
) (*webserver.SnapshotAnchor, error) {
	if c.snapshot == nil {
		return nil, errors.New("volume snapshots are not available in this context")
	}
	if req.BackupName == "" {
		return nil, fmt.Errorf("%w: backupName is required", webserver.ErrInvalidSnapshotRequest)
	}
	if req.Cold {
		if err := c.armColdSnapshot(ctx, req.BackupName); err != nil {
			return nil, err
		}
		return &webserver.SnapshotAnchor{}, nil
	}
	timeout := DefaultSnapshotHoldTimeout
	if req.Timeout != "" {
		d, err := time.ParseDuration(req.Timeout)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%w: timeout %q is not a positive duration", webserver.ErrInvalidSnapshotRequest, req.Timeout)
		}
		timeout = min(d, maxSnapshotHoldTimeout)
	}

	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	if c.hold != nil {
		if c.hold.backupName == req.BackupName {
			anchor := c.hold.anchor
			return &anchor, nil
		}
		return nil, fmt.Errorf("%w: held for %q", webserver.ErrSnapshotHoldConflict, c.hold.backupName)
	}
	delete(c.expiredHolds, req.BackupName)

	log := logf.FromContext(ctx).WithName("snapshot").WithValues("backup", req.BackupName)
	hold, err := c.takeSnapshotHold(ctx, req.BackupName, timeout)
	if err != nil {
		log.Error(err, "Could not quiesce the instance for a volume snapshot")
		return nil, err
	}
	backupName := req.BackupName
	hold.timer = time.AfterFunc(timeout, func() { c.expireSnapshotHold(backupName, hold) })
	c.hold = hold
	log.Info("Holding the instance for a volume snapshot",
		"anchorGTID", hold.anchor.AnchorGTID, "binlogFile", hold.anchor.BinlogFile,
		"binlogPos", hold.anchor.BinlogPos, "timeout", timeout.String(), "stoppedApplier", hold.stoppedApplier)
	anchor := hold.anchor
	return &anchor, nil
}

// takeSnapshotHold runs the quiesce. On any failure it undoes what it did.
func (c *Controller) takeSnapshotHold(
	ctx context.Context, backupName string, timeout time.Duration,
) (_ *snapshotHold, err error) {
	conn, closeConn, err := c.openSnapshotConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("opening the snapshot connection: %w", err)
	}
	hold := &snapshotHold{backupName: backupName, conn: conn, closeConn: closeConn}
	defer func() {
		if err != nil {
			// The begin's own error is what the caller needs; the release
			// closes the connection, which drops the lock either way.
			_ = c.releaseSnapshotHold(context.WithoutCancel(ctx), hold)
		}
	}()

	// A Group Replication member cannot stop its applier without leaving the
	// group: FTWRL alone blocks the applier for the hold, which flow control
	// tolerates for the bounded time.
	if !c.groupReplication {
		state, err := c.repl.ReplicaState(ctx)
		if err != nil {
			return nil, fmt.Errorf("reading the replica state: %w", err)
		}
		if state.Configured && state.SQLRunning {
			if _, err := conn.ExecContext(ctx, c.replicaSQLThreadStatement("STOP")); err != nil {
				return nil, fmt.Errorf("stopping the replica SQL thread: %w", err)
			}
			hold.stoppedApplier = true
		}
	}

	wait := max(int(min(snapshotLockWait, timeout).Seconds()), 1)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", wait)); err != nil {
		return nil, fmt.Errorf("bounding the lock wait: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		return nil, fmt.Errorf("taking the global read lock: %w", err)
	}

	anchor, err := c.readSnapshotPosition(ctx, conn)
	if err != nil {
		return nil, err
	}
	hold.anchor = anchor
	hold.since = time.Now()
	hold.expires = hold.since.Add(timeout)
	if err := WriteSnapshotManifest(c.snapshot.DataDir, SnapshotManifest{
		BackupName:    backupName,
		AnchorGTID:    anchor.AnchorGTID,
		BinlogFile:    anchor.BinlogFile,
		BinlogPos:     anchor.BinlogPos,
		Flavor:        anchor.Flavor,
		ServerVersion: anchor.ServerVersion,
		Instance:      c.name,
		TakenAt:       hold.since.UTC().Format(time.RFC3339),
		Online:        true,
	}); err != nil {
		return nil, fmt.Errorf("writing %s: %w", SnapshotManifestFile, err)
	}
	return hold, nil
}

// replicaSQLThreadStatement is START/STOP REPLICA SQL_THREAD in the server's
// dialect.
func (c *Controller) replicaSQLThreadStatement(verb string) string {
	if !c.mariadb && c.version.UsesReplicaTerminology() {
		return verb + " REPLICA SQL_THREAD"
	}
	return verb + " SLAVE SQL_THREAD"
}

// readSnapshotPosition reads the GTID position, binlog coordinates and server
// version on the held connection, or on the control pool for a cold capture.
func (c *Controller) readSnapshotPosition(ctx context.Context, conn rowQueryer) (webserver.SnapshotAnchor, error) {
	anchor := webserver.SnapshotAnchor{Flavor: string(engine.FlavorMySQL), ServerVersion: c.versionStr}
	gtidQuery := "SELECT @@GLOBAL.gtid_executed"
	statusQuery := "SHOW MASTER STATUS"
	if c.mariadb {
		anchor.Flavor = string(engine.FlavorMariaDB)
		gtidQuery = "SELECT @@GLOBAL.gtid_binlog_pos"
	} else if c.version.AtLeast(8, 2, 0) {
		statusQuery = "SHOW BINARY LOG STATUS"
	}
	var gtid sql.NullString
	if err := conn.QueryRowContext(ctx, gtidQuery).Scan(&gtid); err != nil {
		return anchor, fmt.Errorf("reading the GTID position: %w", err)
	}
	// gtid_executed renders its set across lines.
	anchor.AnchorGTID = strings.ReplaceAll(strings.ReplaceAll(gtid.String, "\n", ""), " ", "")

	file, pos, err := readBinlogStatus(ctx, conn, statusQuery)
	if err != nil {
		return anchor, err
	}
	anchor.BinlogFile, anchor.BinlogPos = file, pos

	var live sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.version").Scan(&live); err == nil && live.String != "" {
		anchor.ServerVersion = live.String
	}
	return anchor, nil
}

// queryer runs a query on a connection or a pool.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// rowQueryer also reads single rows.
type rowQueryer interface {
	queryer
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// readBinlogStatus reads File and Position from SHOW BINARY LOG STATUS / SHOW
// MASTER STATUS, whose other columns vary by flavor and version. A server
// without binary logging returns no row, and an empty position.
func readBinlogStatus(ctx context.Context, conn queryer, query string) (string, int64, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return "", 0, fmt.Errorf("reading the binary log position: %w", err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	if !rows.Next() {
		return "", 0, rows.Err()
	}
	values := make([]sql.RawBytes, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return "", 0, err
	}
	var file string
	var pos int64
	for i, column := range columns {
		switch strings.ToLower(column) {
		case "file":
			file = string(values[i])
		case "position":
			if pos, err = strconv.ParseInt(string(values[i]), 10, 64); err != nil {
				return "", 0, fmt.Errorf("parsing binary log position %q: %w", values[i], err)
			}
		}
	}
	return file, pos, rows.Err()
}

// EndSnapshot releases the hold for req.BackupName, or clears its cold
// capture.
func (c *Controller) EndSnapshot(ctx context.Context, req webserver.SnapshotEndRequest) error {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	if c.hold == nil || c.hold.backupName != req.BackupName {
		if cleared, err := c.clearColdSnapshot(ctx, req.BackupName); cleared || err != nil {
			return err
		}
		if _, ok := c.expiredHolds[req.BackupName]; ok {
			return webserver.ErrSnapshotHoldExpired
		}
		if c.hold != nil {
			return fmt.Errorf("%w: held for %q", webserver.ErrSnapshotHoldConflict, c.hold.backupName)
		}
		return webserver.ErrSnapshotNotHeld
	}
	hold := c.hold
	c.hold = nil
	hold.timer.Stop()
	err := c.releaseSnapshotHold(ctx, hold)
	logf.FromContext(ctx).WithName("snapshot").Info("Released the volume snapshot hold",
		"backup", req.BackupName, "heldFor", time.Since(hold.since).Round(time.Millisecond).String())
	return err
}

// expireSnapshotHold releases a hold whose timeout elapsed before end, and
// remembers it so the end that comes later fails the Backup instead of
// completing an anchor that may not match the cut.
func (c *Controller) expireSnapshotHold(backupName string, hold *snapshotHold) {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	if c.hold != hold {
		return
	}
	c.hold = nil
	if c.expiredHolds == nil {
		c.expiredHolds = map[string]struct{}{}
	}
	c.expiredHolds[backupName] = struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := c.releaseSnapshotHold(ctx, hold)
	log := logf.Log.WithName("snapshot").WithValues("backup", backupName)
	if err != nil {
		log.Error(err, "Released an expired volume snapshot hold with errors")
		return
	}
	log.Info("Released an expired volume snapshot hold",
		"heldFor", time.Since(hold.since).Round(time.Millisecond).String())
}

// releaseSnapshotHold unlocks the tables, restarts the applier when the hold
// stopped it, and closes the connection; closing it alone would drop the lock,
// so the close always runs. It is safe on a partially taken hold.
func (c *Controller) releaseSnapshotHold(ctx context.Context, hold *snapshotHold) error {
	var errs []error
	if _, err := hold.conn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		errs = append(errs, fmt.Errorf("unlocking tables: %w", err))
	}
	if hold.stoppedApplier {
		if _, err := hold.conn.ExecContext(ctx, c.replicaSQLThreadStatement("START")); err != nil {
			errs = append(errs, fmt.Errorf("starting the replica SQL thread: %w", err))
		}
	}
	hold.closeConn()
	return errors.Join(errs...)
}
