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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/binlog"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/xtrabackup"
)

// RestoreOptions configures bootstrapping a primary's data directory from a
// physical backup held in object storage.
//
// Recovery is the object-store analogue of Join: it extracts the archive,
// prepares it, and restores it into DataDir. Unlike Join it configures no
// replication — recovery produces a standalone primary that replicas then clone
// from in the usual way.
type RestoreOptions struct {
	// Store reads the archive (and optional metadata) from object storage.
	Store *objectstore.Client
	// Bucket and ArchiveKey locate the xbstream archive.
	Bucket     string
	ArchiveKey string
	// MetadataKey, when set, is read first to discover the archive checksum and
	// whether it is compressed; it also lets recovery verify integrity.
	MetadataKey string
	// BackupDir is scratch space the archive is extracted into.
	BackupDir string
	// DataDir is the data directory to restore into.
	DataDir string
	// XBStreamPath is the xbstream binary (default "xbstream").
	XBStreamPath string
	// XtrabackupPath is the xtrabackup binary (default "xtrabackup").
	XtrabackupPath string
	// Compress forces decompression after extraction. When MetadataKey is set the
	// archive's recorded compression flag takes precedence.
	Compress bool
	// VerifyChecksum compares the downloaded archive against the metadata SHA256.
	VerifyChecksum bool

	// The fields below drive the post-restore credential reconcile. The restored
	// data carries the source cluster's internal accounts with the source's
	// passwords; the recovery cluster generates fresh secrets, so the accounts
	// must be reset to those before the instance manager can connect. When
	// RootPassword is empty the reconcile is skipped entirely.
	MysqldPath string
	ConfigFile string
	Socket     string
	Version    string
	// ReadyTimeout bounds the temporary reconcile server startup.
	ReadyTimeout time.Duration
	// RootPassword resets root@localhost. Its presence enables the reconcile.
	RootPassword string
	// ControlUser/ControlPassword and BackupUser/BackupPassword reset the
	// instance-manager and XtraBackup accounts when both halves are set.
	ControlUser     string
	ControlPassword string
	BackupUser      string
	BackupPassword  string

	// The fields below drive point-in-time recovery (M7.2): after the base
	// backup is restored, archived binlogs are downloaded and replayed up to the
	// recovery target. Replay is enabled only when SourceCluster is set.
	//
	// ObjectStore identifies the bucket/path the binlog archive lives under so
	// per-file keys can be reconstructed (the Client alone does not carry it).
	ObjectStore mysqlv1alpha1.S3ObjectStore
	// SourceCluster is the name of the cluster whose archive is replayed. Its
	// presence enables the replay step. For same-cluster DR it is the original
	// cluster name; the archive keys are partitioned under it.
	SourceCluster string
	// Target bounds the replay (targetTime / targetGTID / latest).
	Target binlog.RecoveryTarget
	// MysqlbinlogPath and MysqlPath are the binaries used to decode and apply the
	// archived binlogs (default "mysqlbinlog" / "mysql").
	MysqlbinlogPath string
	MysqlPath       string
}

func (o *RestoreOptions) applyDefaults() {
	if o.XBStreamPath == "" {
		o.XBStreamPath = "xbstream"
	}
	if o.XtrabackupPath == "" {
		o.XtrabackupPath = defaultXtrabackupBinary
	}
	if o.MysqldPath == "" {
		o.MysqldPath = "mysqld"
	}
	if o.Socket == "" {
		o.Socket = "/var/run/mysqld/mysqld.sock"
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = 2 * time.Minute
	}
	if o.MysqlbinlogPath == "" {
		o.MysqlbinlogPath = "mysqlbinlog"
	}
	if o.MysqlPath == "" {
		o.MysqlPath = "mysql"
	}
}

// Restore downloads the archive from object storage, extracts and prepares it,
// then restores it into DataDir. It is idempotent: a no-op if the data
// directory is already initialised.
func Restore(ctx context.Context, opts RestoreOptions) error {
	opts.applyDefaults()
	log := logf.FromContext(ctx).WithName("instance-restore").WithValues(
		"dataDir", opts.DataDir,
		"backupDir", opts.BackupDir,
		"bucket", opts.Bucket,
		"archiveKey", opts.ArchiveKey,
	)
	log.Info("Starting restore from object store")

	if opts.Store == nil {
		return fmt.Errorf("restore: object-store client is required")
	}
	if opts.Bucket == "" || opts.ArchiveKey == "" {
		return fmt.Errorf("restore: bucket and archive key are required")
	}
	if opts.DataDir == "" || opts.BackupDir == "" {
		return fmt.Errorf("restore: data dir and backup dir are required")
	}

	// Base restore is idempotent on the data directory: once copy-back has run it
	// is not repeated. Point-in-time replay, however, must still run on a retry
	// (an init-container restart after copy-back leaves the data initialised but
	// the binlogs un-replayed), so it lives outside this guard and is gated by its
	// own sentinel below.
	if IsInitialized(opts.DataDir) {
		log.Info("Data directory already initialized; skipping base restore")
	} else if err := opts.restoreBase(ctx); err != nil {
		return err
	} else if err := os.WriteFile(filepath.Join(opts.DataDir, bootstrapSentinel), nil, 0o600); err != nil {
		return fmt.Errorf("marking restored data directory as bootstrapped: %w", err)
	}

	// 6. Point-in-time recovery: replay archived binlogs onto the restored data
	// up to the recovery target. Enabled only when a source cluster is set; a
	// plain M6 restore leaves the data at the base-backup point. Reentrant: a
	// sentinel on the (durable) data directory makes a retry skip a completed
	// replay rather than re-applying already-executed GTIDs.
	if opts.SourceCluster != "" {
		if err := opts.maybeReplay(ctx); err != nil {
			return fmt.Errorf("replaying archived binlogs: %w", err)
		}
	}

	log.Info("Completed restore from object store")
	return nil
}

// restoreBase extracts, prepares and copy-backs the base backup into the data
// directory, then resets the restored internal accounts to this cluster's
// credentials. It runs only when the data directory is not yet initialised.
func (opts RestoreOptions) restoreBase(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("instance-restore")

	compress := opts.Compress
	var expectedSHA256 string
	if opts.MetadataKey != "" {
		var metadata objectstore.BackupMetadata
		if err := opts.Store.GetJSON(ctx, opts.Bucket, opts.MetadataKey, &metadata); err != nil {
			return fmt.Errorf("reading backup metadata: %w", err)
		}
		compress = metadata.Compressed
		expectedSHA256 = metadata.SHA256
		log.Info("Loaded backup metadata",
			"backupID", metadata.BackupID, "compressed", compress, "sizeBytes", metadata.SizeBytes)
	}

	if err := os.MkdirAll(opts.BackupDir, 0o750); err != nil {
		return fmt.Errorf("creating backup dir: %w", err)
	}

	// 1. Stream the archive out of object storage straight into `xbstream -x`,
	// checksumming in flight so it can be verified against the metadata.
	checksum, err := opts.extract(ctx)
	if err != nil {
		return err
	}
	if opts.VerifyChecksum && expectedSHA256 != "" && checksum != expectedSHA256 {
		return fmt.Errorf("restore: archive checksum mismatch: got %s, want %s", checksum, expectedSHA256)
	}

	// 2. Optionally decompress the extracted archive.
	if compress {
		decompressArgs, err := xtrabackup.DecompressArgs(opts.BackupDir)
		if err != nil {
			return err
		}
		log.Info("Decompressing backup")
		if err := runCommand(ctx, opts.XtrabackupPath, decompressArgs); err != nil {
			return fmt.Errorf("xtrabackup decompress: %w", err)
		}
	}

	// 3. Prepare the backup into a consistent state.
	prepareArgs, err := xtrabackup.PrepareArgs(opts.BackupDir)
	if err != nil {
		return err
	}
	log.Info("Preparing backup")
	if err := runCommand(ctx, opts.XtrabackupPath, prepareArgs); err != nil {
		return fmt.Errorf("xtrabackup prepare: %w", err)
	}

	// 4. Restore into the (empty) data directory. ext4-backed PVCs ship a
	// lost+found directory at the mount root; copy-back aborts on a non-empty
	// data dir, so clear it first.
	if err := os.MkdirAll(opts.DataDir, 0o750); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}
	if err := removeLostFound(opts.DataDir); err != nil {
		return err
	}
	copyBackArgs, err := xtrabackup.CopyBackArgs(opts.BackupDir, opts.DataDir)
	if err != nil {
		return err
	}
	log.Info("Restoring backup")
	if err := runCommand(ctx, opts.XtrabackupPath, copyBackArgs); err != nil {
		return fmt.Errorf("xtrabackup copy-back: %w", err)
	}

	// 5. Reset the restored internal accounts to this cluster's credentials so the
	// instance manager (and XtraBackup) can authenticate against the recovered
	// data. Skipped when no root password is provided.
	if opts.RootPassword != "" {
		if err := opts.reconcileCredentials(ctx); err != nil {
			return fmt.Errorf("reconciling restored credentials: %w", err)
		}
	}
	return nil
}

// credentialReconcileStatements returns the SQL that resets the restored
// internal accounts to the recovery cluster's generated passwords. The
// replication account is intentionally left untouched: it authenticates with
// mTLS (REQUIRE X509), so no password is exposed to the Pod.
func credentialReconcileStatements(
	version, rootPassword, controlUser, controlPassword, backupUser, backupPassword string,
) []string {
	if rootPassword == "" && controlPassword == "" && backupPassword == "" {
		return nil
	}
	d := newBootstrapDialect(version)
	// FLUSH PRIVILEGES re-enables the grant system after --skip-grant-tables so
	// the subsequent ALTER USER statements take effect.
	stmts := []string{"FLUSH PRIVILEGES"}
	if rootPassword != "" {
		stmts = append(stmts, d.setUserPassword("root", "localhost", rootPassword))
	}
	if controlUser != "" && controlPassword != "" {
		stmts = append(stmts, d.setUserPassword(controlUser, "%", controlPassword))
	}
	if backupUser != "" && backupPassword != "" {
		stmts = append(stmts, d.setUserPassword(backupUser, "%", backupPassword))
	}
	return stmts
}

// reconcileCredentials starts a temporary socket-only, --skip-grant-tables
// server over the restored data directory and resets the internal accounts to
// this cluster's passwords, then shuts it down.
func (o *RestoreOptions) reconcileCredentials(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("instance-restore")
	stmts := credentialReconcileStatements(
		o.Version, o.RootPassword, o.ControlUser, o.ControlPassword, o.BackupUser, o.BackupPassword)
	if len(stmts) == 0 {
		return nil
	}

	args := []string{}
	if o.ConfigFile != "" {
		args = append(args, "--defaults-file="+o.ConfigFile)
	}
	args = append(args,
		"--datadir="+o.DataDir,
		"--socket="+o.Socket,
		"--skip-networking",
		"--skip-grant-tables",
	)

	stdout, stderr := newProcessLogWriters(log.WithName("temporary-mysqld"))
	sup := NewProcessSupervisor(o.MysqldPath, args,
		WithShutdownTimeout(o.ReadyTimeout),
		WithOutput(stdout, stderr))
	log.Info("Starting temporary mysqld to reconcile restored credentials", "socket", o.Socket)
	if err := sup.Start(ctx); err != nil {
		return fmt.Errorf("starting temporary server: %w", err)
	}
	defer func() { _ = sup.Shutdown(ctx) }()

	db, err := waitForSocket(ctx, o.Socket, "root", "", o.ReadyTimeout)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	log.Info("Reconciling restored credentials", "statements", len(stmts))
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("credential reconcile statement failed: %w", err)
		}
	}
	return nil
}

// extract downloads the archive and pipes it into `xbstream -x`, returning the
// SHA256 of the downloaded bytes.
func (o *RestoreOptions) extract(ctx context.Context) (string, error) {
	log := logf.FromContext(ctx).WithName("instance-restore")
	extractArgs, err := xtrabackup.ExtractArgs(o.BackupDir)
	if err != nil {
		return "", err
	}

	pipeReader, pipeWriter := io.Pipe()
	hasher := objectstore.NewSHA256Writer(pipeWriter)

	extract := exec.CommandContext(ctx, o.XBStreamPath, extractArgs...)
	extract.Stdin = pipeReader
	extractOut, extractErr := newProcessLogWriters(log.WithName("xbstream"))
	extract.Stdout = extractOut
	extract.Stderr = extractErr
	log.Info("Extracting backup stream", "binary", o.XBStreamPath)
	if err := extract.Start(); err != nil {
		_ = pipeReader.CloseWithError(err)
		return "", fmt.Errorf("starting xbstream: %w", err)
	}

	// Download into the pipe; closing it signals EOF to xbstream.
	downloadErr := make(chan error, 1)
	go func() {
		_, err := o.Store.Download(ctx, o.Bucket, o.ArchiveKey, hasher)
		// Surface any download error to the reader so xbstream fails too.
		_ = pipeWriter.CloseWithError(err)
		downloadErr <- err
	}()

	waitErr := extract.Wait()
	dlErr := <-downloadErr
	if dlErr != nil {
		return "", dlErr
	}
	if waitErr != nil {
		return "", fmt.Errorf("xbstream extract: %w", waitErr)
	}
	return hasher.SumHex(), nil
}
