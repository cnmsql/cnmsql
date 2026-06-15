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

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/binlog"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/objectstore"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/xtrabackup"
)

// binlogInfoFile is the XtraBackup artifact holding the base backup's binlog
// position and GTID set — the anchor the replay starts from. copy-back leaves a
// copy in the data directory, so it survives an init-container restart even
// though the scratch backup dir (an emptyDir) does not.
const binlogInfoFile = "xtrabackup_binlog_info"

// pitrSentinelFile marks, on the durable data directory, that point-in-time
// replay has completed. It makes the replay reentrant: a retry skips it instead
// of re-applying already-executed GTIDs (which mysqld rejects).
const pitrSentinelFile = ".cloudnative-mysql-pitr-done"

// maybeReplay runs the point-in-time replay unless a previous attempt already
// completed it (sentinel present on the data directory).
func (o *RestoreOptions) maybeReplay(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("instance-pitr")
	sentinel := filepath.Join(o.DataDir, pitrSentinelFile)
	if _, err := os.Stat(sentinel); err == nil {
		log.Info("Point-in-time replay already completed; skipping", "sentinel", sentinel)
		return nil
	}
	if err := o.replayBinlogs(ctx); err != nil {
		return err
	}
	if err := os.WriteFile(sentinel, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o640); err != nil {
		return fmt.Errorf("pitr: writing completion sentinel: %w", err)
	}
	return nil
}

// replayBinlogs performs point-in-time recovery: it reads the base backup's
// anchor GTID, loads the cluster archive index, plans the segments/files to
// replay up to the recovery target, downloads them, and applies them onto the
// restored data directory via a temporary socket-only mysqld.
//
// It is the M7.2 complement to Restore's base-backup steps. gtid_executed
// advances as each archived transaction is applied (mysqlbinlog emits
// SET GTID_NEXT per transaction), so the recovered server ends at exactly the
// target point with a coherent GTID history.
func (o *RestoreOptions) replayBinlogs(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("instance-pitr").WithValues(
		"sourceCluster", o.SourceCluster, "bucket", o.ObjectStore.Bucket)

	if o.ObjectStore.Bucket == "" {
		return fmt.Errorf("pitr: object store bucket is required for binlog replay")
	}

	anchor, err := o.readAnchorGTID()
	if err != nil {
		return err
	}
	log.Info("Read base backup anchor GTID", "anchorGTID", anchor)

	// Load the cluster-level archive index: the ordered timeline of UUID segments.
	indexKey := objectstore.ArchiveIndexKey(o.ObjectStore, o.SourceCluster)
	var index objectstore.ArchiveIndex
	if err := o.Store.GetJSON(ctx, o.ObjectStore.Bucket, indexKey, &index); err != nil {
		return fmt.Errorf("pitr: reading archive index %q: %w", indexKey, err)
	}

	plan, err := binlog.PlanReplay(&index, anchor, o.Target)
	if err != nil {
		return fmt.Errorf("pitr: planning replay: %w", err)
	}
	if len(plan.Segments) == 0 {
		log.Info("No archived binlogs to replay; data is already at the recovery target")
		return nil
	}

	// Download every planned file in timeline order.
	replayDir := filepath.Join(o.BackupDir, "binlog-replay")
	if err := os.MkdirAll(replayDir, 0o750); err != nil {
		return fmt.Errorf("pitr: creating replay scratch dir: %w", err)
	}
	files, err := o.downloadReplayFiles(ctx, replayDir, plan)
	if err != nil {
		return err
	}
	log.Info("Downloaded archived binlogs", "files", len(files),
		"stopDatetime", plan.StopDatetime, "includeGTIDs", plan.IncludeGTIDs)

	return o.applyReplay(ctx, plan, files)
}

// readAnchorGTID parses the base backup's xtrabackup_binlog_info for the GTID
// set the restore landed at. It prefers the copy in the data directory (durable
// on the PVC, so it survives an init-container restart that lost the scratch
// backup dir) and falls back to the scratch dir. A missing file (older
// XtraBackup, or no GTIDs yet) yields an empty anchor: replay from the start.
func (o *RestoreOptions) readAnchorGTID() (string, error) {
	content, err := os.ReadFile(filepath.Join(o.DataDir, binlogInfoFile))
	if os.IsNotExist(err) {
		content, err = os.ReadFile(filepath.Join(o.BackupDir, binlogInfoFile))
	}
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("pitr: reading %s: %w", binlogInfoFile, err)
	}
	info, err := xtrabackup.ParseBinlogInfo(string(content))
	if err != nil {
		return "", fmt.Errorf("pitr: parsing %s: %w", binlogInfoFile, err)
	}
	return info.GTIDSet, nil
}

// downloadReplayFiles pulls every planned segment file into replayDir, returning
// the local paths in timeline order. Keys are partitioned by server UUID, so two
// segments' like-named files (both produce a binlog.000004) never collide on disk.
func (o *RestoreOptions) downloadReplayFiles(
	ctx context.Context, replayDir string, plan binlog.ReplayPlan,
) ([]string, error) {
	var files []string
	for _, seg := range plan.Segments {
		for _, name := range seg.Files {
			keys, err := objectstore.BuildBinlogKeys(o.ObjectStore, o.SourceCluster, seg.ServerUUID, name)
			if err != nil {
				return nil, fmt.Errorf("pitr: building key for %s/%s: %w", seg.ServerUUID, name, err)
			}
			local := filepath.Join(replayDir, seg.ServerUUID+"_"+name)
			if err := o.downloadTo(ctx, keys.BinlogKey, local); err != nil {
				return nil, err
			}
			files = append(files, local)
		}
	}
	return files, nil
}

// downloadTo streams one object to a local file.
func (o *RestoreOptions) downloadTo(ctx context.Context, key, local string) error {
	f, err := os.Create(local) //nolint:gosec // local path is operator-derived, not user input
	if err != nil {
		return fmt.Errorf("pitr: creating %s: %w", local, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := o.Store.Download(ctx, o.ObjectStore.Bucket, key, f); err != nil {
		return fmt.Errorf("pitr: downloading %s: %w", key, err)
	}
	return nil
}

// applyReplay starts a temporary socket-only mysqld over the restored data
// directory and pipes `mysqlbinlog <files> | mysql` into it, bounded by the
// recovery target. The temp server runs with --skip-grant-tables (same pattern
// as reconcileCredentials) so the mysql client connects as root without a
// password; GTID tracking is independent of the grant system.
func (o *RestoreOptions) applyReplay(ctx context.Context, plan binlog.ReplayPlan, files []string) error {
	log := logf.FromContext(ctx).WithName("instance-pitr")

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
	log.Info("Starting temporary mysqld to replay archived binlogs", "socket", o.Socket)
	if err := sup.Start(ctx); err != nil {
		return fmt.Errorf("pitr: starting temporary server: %w", err)
	}
	defer func() { _ = sup.Shutdown(ctx) }()

	// Wait until the socket accepts connections before streaming the replay.
	db, err := waitForSocket(ctx, o.Socket, "root", "", o.ReadyTimeout)
	if err != nil {
		return fmt.Errorf("pitr: %w", err)
	}
	_ = db.Close()

	replayArgs, err := binlog.ReplayArgs(binlog.ReplayOptions{
		Files:        files,
		StopDatetime: plan.StopDatetime,
		IncludeGTIDs: plan.IncludeGTIDs,
		ExcludeGTIDs: plan.ExcludeGTIDs,
	})
	if err != nil {
		return fmt.Errorf("pitr: building replay args: %w", err)
	}

	log.Info("Replaying archived binlogs into restored data", "files", len(files))
	return o.pipeReplay(ctx, replayArgs)
}

// pipeReplay runs `mysqlbinlog <replayArgs> | mysql --socket ...`, decoding the
// archived binlogs and applying them to the temporary server. Both child
// processes' stderr is captured as structured logs; the binlog stream itself is
// a data path and never logged.
func (o *RestoreOptions) pipeReplay(ctx context.Context, replayArgs []string) error {
	log := logf.FromContext(ctx).WithName("instance-pitr")

	pr, pw := io.Pipe()

	decode := exec.CommandContext(ctx, o.MysqlbinlogPath, replayArgs...)
	decode.Stdout = pw
	_, decodeErr := newProcessLogWriters(log.WithName("mysqlbinlog"))
	decode.Stderr = decodeErr

	apply := exec.CommandContext(ctx, o.MysqlPath,
		"--socket="+o.Socket, "--user=root", "--binary-mode")
	apply.Stdin = pr
	// MYSQL_PWD keeps the (empty here) password off the argv; harmless under
	// --skip-grant-tables but keeps the invocation consistent.
	apply.Env = append(os.Environ(), "MYSQL_PWD="+o.RootPassword)
	applyOut, applyErr := newProcessLogWriters(log.WithName("mysql"))
	apply.Stdout = applyOut
	apply.Stderr = applyErr

	if err := apply.Start(); err != nil {
		_ = pr.CloseWithError(err)
		return fmt.Errorf("pitr: starting mysql: %w", err)
	}
	if err := decode.Start(); err != nil {
		_ = pw.CloseWithError(err)
		return fmt.Errorf("pitr: starting mysqlbinlog: %w", err)
	}

	decErr := decode.Wait()
	// Closing the writer signals EOF (or the decode error) to the mysql client.
	_ = pw.CloseWithError(decErr)
	appErr := apply.Wait()
	_ = pr.CloseWithError(appErr)

	if decErr != nil {
		return fmt.Errorf("pitr: mysqlbinlog: %w", decErr)
	}
	if appErr != nil {
		return fmt.Errorf("pitr: mysql apply: %w", appErr)
	}
	return nil
}
