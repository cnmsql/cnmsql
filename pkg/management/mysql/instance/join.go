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

package instance

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/xtrabackup"
)

// JoinOptions configures provisioning a replica from a physical backup.
//
// The backup itself is produced on the source (xtrabackup must run where the
// source's data files live) and made available to the replica under BackupDir;
// the operator orchestrates that transport. Join then prepares the backup,
// restores it into DataDir, and configures GTID replication so that, once the
// main mysqld starts, replication resumes automatically.
type JoinOptions struct {
	// XtrabackupPath is the xtrabackup binary (default "xtrabackup").
	XtrabackupPath string
	// MysqldPath is the mysqld binary (default "mysqld").
	MysqldPath string
	// BackupDir holds the streamed backup to restore from.
	BackupDir string
	// DataDir is the replica data directory to restore into.
	DataDir string
	// ConfigFile is the defaults file for the temporary server.
	ConfigFile string
	// Socket is the unix socket for the temporary server.
	Socket string
	// Version is the MySQL server version (e.g. "8.0.36").
	Version string
	// RootUser and RootPassword authenticate to the temporary server. After a
	// physical clone these are the source's credentials.
	RootUser     string
	RootPassword string
	// Source describes how to reach the replication source.
	Source replication.SourceOptions
	// ReadyTimeout bounds waiting for the temporary server.
	ReadyTimeout time.Duration
}

func (o *JoinOptions) applyDefaults() {
	if o.XtrabackupPath == "" {
		o.XtrabackupPath = defaultXtrabackupBinary
	}
	if o.MysqldPath == "" {
		o.MysqldPath = defaultMysqldBinary
	}
	if o.RootUser == "" {
		o.RootUser = "root"
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = 120 * time.Second
	}
}

// Join provisions a replica from the backup under BackupDir. It is idempotent:
// a no-op if the data directory is already initialised.
func Join(ctx context.Context, opts JoinOptions) error {
	opts.applyDefaults()

	if IsInitialized(opts.DataDir) {
		return nil
	}

	ver, err := version.Parse(opts.Version)
	if err != nil {
		return err
	}

	// 1. Prepare the backup into a consistent state.
	prepareArgs, err := xtrabackup.PrepareArgs(opts.BackupDir)
	if err != nil {
		return err
	}
	if err := runCommand(ctx, opts.XtrabackupPath, prepareArgs); err != nil {
		return fmt.Errorf("xtrabackup prepare: %w", err)
	}

	// 2. Restore into the (empty) data directory.
	if err := os.MkdirAll(opts.DataDir, 0o750); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}
	copyBackArgs, err := xtrabackup.CopyBackArgs(opts.BackupDir, opts.DataDir)
	if err != nil {
		return err
	}
	if err := runCommand(ctx, opts.XtrabackupPath, copyBackArgs); err != nil {
		return fmt.Errorf("xtrabackup copy-back: %w", err)
	}

	// 3. Read the backup's GTID position.
	binlogInfo, err := readBinlogInfo(opts.BackupDir)
	if err != nil {
		return err
	}

	// 4. Configure replication on a temporary server so it persists in the data
	// directory and resumes when the main server starts.
	return opts.configureReplication(ctx, ver, binlogInfo.GTIDSet)
}

// configureReplication starts a temporary socket-only server and provisions GTID
// replication from the backup point.
func (o *JoinOptions) configureReplication(ctx context.Context, ver version.Version, gtidPurged string) error {
	args := []string{}
	if o.ConfigFile != "" {
		args = append(args, "--defaults-file="+o.ConfigFile)
	}
	args = append(args,
		"--datadir="+o.DataDir,
		"--socket="+o.Socket,
		"--skip-networking",
		// GTID replication is mandatory; ensure it is enabled even if the
		// temporary server is started without the rendered configuration. The
		// CHANGE REPLICATION SOURCE ... AUTO_POSITION and SET gtid_purged below
		// both require it.
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
		"--log-bin=binlog",
	)
	// Do not start replication on the temporary server; we configure it and let
	// the real server start it. The option was renamed slave→replica in 8.0.26;
	// 5.6 only knows --skip-slave-start, 9.x only --skip-replica-start.
	if ver.UsesReplicaTerminology() {
		args = append(args, "--skip-replica-start")
	} else {
		args = append(args, "--skip-slave-start")
	}
	// log_slave_updates was renamed to log_replica_updates in 8.0.
	if ver.HasLogReplicaUpdates() {
		args = append(args, "--log-replica-updates=ON")
	} else {
		args = append(args, "--log-slave-updates")
	}

	sup := NewProcessSupervisor(o.MysqldPath, args, WithShutdownTimeout(o.ReadyTimeout))
	if err := sup.Start(ctx); err != nil {
		return fmt.Errorf("starting temporary server: %w", err)
	}
	defer func() { _ = sup.Shutdown(ctx) }()

	db, err := waitForSocket(ctx, o.Socket, o.RootUser, o.RootPassword, o.ReadyTimeout)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	mgr := replication.NewManager(db, ver)
	return mgr.ProvisionFromBackup(ctx, gtidPurged, o.Source)
}

// readBinlogInfo reads and parses xtrabackup_binlog_info from the backup.
func readBinlogInfo(backupDir string) (xtrabackup.BinlogInfo, error) {
	path := filepath.Join(backupDir, "xtrabackup_binlog_info")
	content, err := os.ReadFile(path) //nolint:gosec // path derived from operator-provided backup dir
	if err != nil {
		return xtrabackup.BinlogInfo{}, fmt.Errorf("reading %s: %w", path, err)
	}
	return xtrabackup.ParseBinlogInfo(string(content))
}

// runCommand runs an external command, forwarding output to the process stdio.
func runCommand(ctx context.Context, binary string, args []string) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
