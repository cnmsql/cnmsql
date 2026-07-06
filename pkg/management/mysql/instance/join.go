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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// JoinOptions configures provisioning a replica from a physical backup.
//
// The backup itself is produced on the source (xtrabackup must run where the
// source's data files live) and made available to the replica under BackupDir;
// the operator orchestrates that transport. Join then prepares the backup,
// restores it into DataDir, and configures GTID replication so that, once the
// main mysqld starts, replication resumes automatically.
type JoinOptions struct {
	// XtrabackupPath overrides the backup binary. Empty (the default) selects the
	// engine's tool: xtrabackup for MySQL, mariabackup for MariaDB.
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
	log := logf.FromContext(ctx).WithName("instance-join").WithValues(
		"dataDir", opts.DataDir,
		"backupDir", opts.BackupDir,
		"socket", opts.Socket,
		"version", opts.Version,
		"sourceHost", opts.Source.Host,
	)
	log.Info("Starting replica join")

	if IsInitialized(opts.DataDir) {
		log.Info("Data directory already initialized")
		return nil
	}

	ver, err := version.Parse(opts.Version)
	if err != nil {
		return err
	}

	eng := engine.MustForFlavor(engine.Flavor(os.Getenv("CNMSQL_FLAVOR")))
	bt := eng.Backup()

	if opts.MysqldPath == defaultMysqldBinary {
		opts.MysqldPath = eng.ServerdCommand()
	}

	// 1. Prepare the backup into a consistent state.
	prepareArgs, err := bt.PrepareArgs(opts.BackupDir)
	if err != nil {
		return err
	}
	log.Info("Preparing backup")
	binary := opts.XtrabackupPath
	if binary == "" {
		binary = bt.BackupBinary()
	}
	if err := runCommand(ctx, binary, prepareArgs); err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	// 2. Restore into the data directory. Clear it fully first — a previous
	// failed copy-back may have left files behind, and mariabackup refuses to
	// write into a non-empty target dir.
	if err := os.MkdirAll(opts.DataDir, 0o750); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}
	if err := purgeDataDir(opts.DataDir); err != nil {
		return fmt.Errorf("purging data dir: %w", err)
	}
	copyBackArgs, err := bt.CopyBackArgs(opts.BackupDir, opts.DataDir)
	if err != nil {
		return err
	}
	log.Info("Restoring backup")
	if err := runCommand(ctx, binary, copyBackArgs); err != nil {
		return fmt.Errorf("copy-back: %w", err)
	}

	// The cloned data dir carries the source's archive identity (MariaDB token /
	// MySQL auto.cnf); replace it so this replica archives under its own incarnation
	// and never collides with the source's objects. For MySQL this must run before
	// the temporary server below starts, so mysqld mints a fresh server_uuid.
	if err := resetArchiveIdentity(opts.DataDir, eng.Flavor()); err != nil {
		return fmt.Errorf("resetting archive identity: %w", err)
	}

	// 3. Read the backup's GTID position.
	binlogInfo, err := readBinlogInfoWithTool(opts.BackupDir, bt)
	if err != nil {
		return err
	}
	log.Info("Read backup binlog info", "hasGTIDSet", binlogInfo.GTIDSet != "")

	// 4. Configure replication on a temporary server so it persists in the data
	// directory and resumes when the main server starts.
	if err := opts.configureReplication(ctx, eng, ver, binlogInfo.GTIDSet); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(opts.DataDir, bootstrapSentinel), nil, 0o600); err != nil {
		return fmt.Errorf("marking data directory as bootstrapped: %w", err)
	}
	log.Info("Completed replica join")
	return nil
}

// configureReplication starts a temporary socket-only server and provisions GTID
// replication from the backup point.
func (o *JoinOptions) configureReplication(
	ctx context.Context, eng engine.Engine, ver version.Version, gtidPurged string,
) error {
	log := logf.FromContext(ctx).WithName("instance-join")
	args := []string{}
	if o.ConfigFile != "" {
		args = append(args, "--defaults-file="+o.ConfigFile)
	}
	args = append(args,
		"--datadir="+o.DataDir,
		"--socket="+o.Socket,
		"--skip-networking",
		"--log-bin=binlog",
	)
	args = append(args, eng.GTIDStartupArgs()...)
	// Do not start replication on the temporary server; we configure it and let
	// the real server start it. The option was renamed slave→replica in 8.0.26.
	if o.MysqldPath == defaultMysqldBinary {
		o.MysqldPath = eng.ServerdCommand()
	}
	if eng.UsesReplicaTerminology(ver) {
		args = append(args, "--skip-replica-start")
	} else {
		args = append(args, "--skip-slave-start")
	}
	// log_slave_updates was renamed to log_replica_updates in 8.0.
	if eng.HasLogReplicaUpdates(ver) {
		args = append(args, "--log-replica-updates=ON")
	} else {
		args = append(args, "--log-slave-updates")
	}

	stdout, stderr := newProcessLogWriters(log.WithName("temporary-mysqld"))
	sup := NewProcessSupervisor(o.MysqldPath, args,
		WithShutdownTimeout(o.ReadyTimeout),
		WithOutput(stdout, stderr))
	log.Info("Starting temporary mysqld to configure replication", "socket", o.Socket)
	if err := sup.Start(ctx); err != nil {
		return fmt.Errorf("starting temporary server: %w", err)
	}
	defer func() { _ = sup.Shutdown(ctx) }()

	db, err := waitForSocket(ctx, o.Socket, o.RootUser, o.RootPassword, o.ReadyTimeout)
	if err != nil {
		return err
	}
	log.Info("Connected to temporary mysqld")
	defer func() { _ = db.Close() }()

	mgr := replication.NewManagerWithDialect(db, ver, eng.Repl())
	log.Info("Provisioning replication from backup", "sourceHost", o.Source.Host)
	return mgr.ProvisionFromBackup(ctx, gtidPurged, o.Source)
}

// readBinlogInfoWithTool reads and parses the backup tool's binlog-info file. A
// missing file is not an error: mariabackup only writes mariadb_backup_binlog_info
// when the source has a non-empty binlog GTID position, so a primary whose data
// was authored out of the binlog (initdb, --skip-log-bin bootstrap) produces a
// backup without one. That means an empty replica start position, which
// ProvisionFromBackup handles by skipping the seed and following the source from
// the beginning.
func readBinlogInfoWithTool(backupDir string, bt engine.BackupTool) (engine.BinlogInfo, error) {
	for _, name := range bt.BinlogInfoFileNames() {
		path := filepath.Join(backupDir, name)
		content, err := os.ReadFile(path) //nolint:gosec // path derived from operator-provided backup dir
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return engine.BinlogInfo{}, fmt.Errorf("reading %s: %w", path, err)
		}
		return bt.ParseBinlogInfo(string(content))
	}
	return engine.BinlogInfo{}, nil
}

// persistBinlogInfo copies the backup tool's binlog-info file from the scratch
// backup dir into the durable data dir so point-in-time replay can recover the
// base-backup anchor GTID after an init-container restart wipes the scratch dir.
// A missing source file is not an error: an empty-position backup writes none.
func persistBinlogInfo(bt engine.BackupTool, backupDir, dataDir string) error {
	// Copy whichever candidate name the backup tool actually wrote, preserving that
	// name so readAnchorGTID (which tries the same candidates) finds it in the data
	// dir. MariaBackup < 11.1 writes the legacy xtrabackup_binlog_info name.
	for _, name := range bt.BinlogInfoFileNames() {
		content, err := os.ReadFile(filepath.Join(backupDir, name)) //nolint:gosec // path derived from operator-provided backup dir
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		return os.WriteFile(filepath.Join(dataDir, name), content, 0o600)
	}
	// No binlog-info file under any name: an empty-position backup writes none.
	return nil
}

// runCommand runs an external command, forwarding output to the process stdio.
func runCommand(ctx context.Context, binary string, args []string) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	stdout, stderr := newProcessLogWriters(logf.FromContext(ctx).WithName("process").WithValues("process", binary))
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}
