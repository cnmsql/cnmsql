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
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// BackupConfig configures the streaming physical backup an instance serves to
// joining replicas. XtraBackup must run where the data files live, so the
// backup is always taken locally and streamed to the caller.
type BackupConfig struct {
	// XtrabackupPath overrides the backup binary. Empty (the default) selects the
	// engine's tool: xtrabackup for MySQL, mariabackup for MariaDB.
	XtrabackupPath string
	// DataDir is the data directory to back up.
	DataDir string
	// Socket connects xtrabackup to the local mysqld for locking and binlog
	// coordinates.
	Socket string
	// User and Password authenticate the local backup account.
	User     string
	Password string
	// WorkDir is a writable scratch directory xtrabackup uses for transient
	// metadata while streaming (default os.TempDir()).
	WorkDir string
	// Compress streams a compressed archive (requires qpress/zstd on both ends).
	Compress bool
	// Parallel sets the number of copy threads (0 = tool default).
	Parallel int
}

func (c *BackupConfig) applyDefaults() {
	if c.WorkDir == "" {
		c.WorkDir = os.TempDir()
	}
}

// SetBackupConfig enables the streaming backup endpoint on the controller. With
// it unset, BackupStream returns an error and the control API still advertises
// the route (the handler reports the misconfiguration).
func (c *Controller) SetBackupConfig(cfg BackupConfig) {
	cfg.applyDefaults()
	c.backup = &cfg
}

// maxBinlogPosTailBytes bounds the mariabackup-stderr tail retained to parse the
// backup's binlog coordinates. The coordinate line is short and printed last, so a
// modest tail comfortably contains it.
const maxBinlogPosTailBytes = 64 << 10

// BackupStream runs a physical backup against the local data directory and copies
// the archive to w. It satisfies webserver.BackupStreamer. For MariaDB it also
// resolves the base backup's GTID anchor (see resolveAnchorGTID) and returns it, so
// point-in-time recovery gets a fully-specified anchor even on 10.11 whose
// binlog-info file carries only file+position.
func (c *Controller) BackupStream(ctx context.Context, w io.Writer) (webserver.BackupResult, error) {
	if c.backup == nil {
		return webserver.BackupResult{}, errors.New("backup streaming is not configured on this instance")
	}

	flavor := engine.Flavor(os.Getenv("CNMSQL_FLAVOR"))
	bt := engine.MustForFlavor(flavor).Backup()

	args, err := bt.BackupArgs(engine.BackupOpts{
		TargetDir: c.backup.WorkDir,
		Socket:    c.backup.Socket,
		User:      c.backup.User,
		Password:  c.backup.Password,
		Parallel:  c.backup.Parallel,
		Stream:    true,
		Compress:  c.backup.Compress,
	})
	if err != nil {
		return webserver.BackupResult{}, err
	}

	binary := c.backup.XtrabackupPath
	if binary == "" {
		binary = bt.BackupBinary()
	}

	// Tee stderr: still log it, but also keep the tail so the binlog-coordinate line
	// (printed once the consistent point is fixed, at the end) can be parsed.
	logWriter := newProcessLogWriter(logf.FromContext(ctx).WithName(binary).WithValues(
		"instance", c.name,
		"dataDir", c.backup.DataDir,
	), "stderr")
	tail := newTailWriter(maxBinlogPosTailBytes)

	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout = w
	cmd.Stderr = io.MultiWriter(logWriter, tail)
	if err := cmd.Run(); err != nil {
		return webserver.BackupResult{}, err
	}

	// Only MariaDB needs anchor resolution; MySQL's xtrabackup_binlog_info already
	// carries gtid_executed.
	if flavor != engine.FlavorMariaDB {
		return webserver.BackupResult{}, nil
	}
	anchorGTID, err := c.resolveAnchorGTID(ctx, tail.String())
	if err != nil {
		return webserver.BackupResult{}, err
	}
	// Record the incarnation the backup was taken from: its archive-partition token
	// (the same identity the continuous archiver keys binlog objects under). A
	// GTID-less recovery uses it to pick the anchor binlog when a re-clone/failover
	// left several incarnations numbering their binlogs from 000001.
	anchorServer, err := ReadArchiveID(c.backup.DataDir)
	if err != nil {
		return webserver.BackupResult{}, fmt.Errorf("backup: reading archive identity: %w", err)
	}
	return webserver.BackupResult{AnchorGTID: anchorGTID, AnchorServerUUID: anchorServer}, nil
}

// resolveAnchorGTID turns the base backup's recorded binlog coordinates into a GTID
// position. When mariabackup already reported a GTID (11.1+), it is used directly.
// Otherwise (10.11) the exact file+position is converted on the source via
// BINLOG_GTID_POS — a pure function of the immutable binlog history up to that
// offset, so the result is race-free regardless of writes after the backup.
//
// BINLOG_GTID_POS distinguishes two empty-ish outcomes, and they are not the same:
//   - SQL NULL means the file+offset is not a resolvable binlog position (the binlog
//     was purged/rotated away, the offset is past EOF, or the coordinates are
//     malformed). That is deterministic — retrying will not recover it — so it is a
//     hard error; the backup fails closed rather than shipping an anchor that would
//     make recovery replay from genesis.
//   - A valid empty string means the position genuinely precedes the first GTID (a
//     backup at genesis); that is a legitimate empty anchor and returned as "".
//
// Transient query errors (connection blips) are retried briefly.
func (c *Controller) resolveAnchorGTID(ctx context.Context, stderr string) (string, error) {
	file, pos, gtid, ok := parseMariabackupBinlogPos(stderr)
	if !ok {
		return "", fmt.Errorf("backup: could not parse binlog coordinates from mariabackup output")
	}
	if gtid != "" {
		return gtid, nil
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		var resolved sql.NullString
		err := c.conn.QueryRowContext(ctx, "SELECT BINLOG_GTID_POS(?, ?)", file, pos).Scan(&resolved)
		if err != nil {
			lastErr = err
			continue
		}
		if !resolved.Valid {
			// NULL: not a resolvable position. Deterministic, so do not retry.
			return "", fmt.Errorf(
				"backup: BINLOG_GTID_POS(%q, %d) returned NULL; the backup's binlog position is not resolvable (purged, rotated, or invalid)",
				file, pos)
		}
		// Valid — possibly the empty genesis position, possibly a real GTID set.
		return resolved.String, nil
	}
	return "", fmt.Errorf("backup: resolving anchor GTID via BINLOG_GTID_POS(%q, %d): %w", file, pos, lastErr)
}

// Ensure Controller advertises the optional streaming backup capability.
var _ webserver.BackupStreamer = (*Controller)(nil)
