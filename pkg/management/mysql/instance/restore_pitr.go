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
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/binlog"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// pitrSentinelFile marks, on the durable data directory, that point-in-time
// replay has completed. It makes the replay reentrant: a retry skips it instead
// of re-applying already-executed GTIDs (which mysqld rejects).
const pitrSentinelFile = ".cnmsql-pitr-done"

// maybeReplay runs the point-in-time replay unless a previous attempt already
// completed it (sentinel present on the data directory).
func (o *RestoreOptions) maybeReplay(ctx context.Context, bt engine.BackupTool, eng engine.Engine) error {
	log := logf.FromContext(ctx).WithName("instance-pitr")
	sentinel := filepath.Join(o.DataDir, pitrSentinelFile)
	if _, err := os.Stat(sentinel); err == nil {
		log.Info("Point-in-time replay already completed; skipping", "sentinel", sentinel)
		return nil
	}
	if err := o.replayBinlogs(ctx, bt, eng); err != nil {
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
func (o *RestoreOptions) replayBinlogs(ctx context.Context, bt engine.BackupTool, eng engine.Engine) error {
	log := logf.FromContext(ctx).WithName("instance-pitr").WithValues(
		"sourceCluster", o.SourceCluster, "bucket", o.ObjectStore.Bucket)

	if o.ObjectStore.Bucket == "" {
		return fmt.Errorf("pitr: object store bucket is required for binlog replay")
	}

	anchor, err := o.readAnchorGTID(bt)
	if err != nil {
		return err
	}
	log.Info("Read base backup anchor GTID", "anchorGTID", anchor.GTIDSet)

	// Load the cluster-level archive index: the ordered timeline of UUID segments.
	indexKey := objectstore.ArchiveIndexKey(o.ObjectStore, o.SourceCluster)
	var index objectstore.ArchiveIndex
	if err := o.Store.GetJSON(ctx, o.ObjectStore.Bucket, indexKey, &index); err != nil {
		return fmt.Errorf("pitr: reading archive index %q: %w", indexKey, err)
	}

	var plan binlog.ReplayPlan
	if eng.Flavor() == engine.FlavorMariaDB {
		plan, err = binlog.PlanReplayWithModel(&index, anchor.GTIDSet, o.Target, eng.GTID())
		if err != nil {
			return fmt.Errorf("pitr: planning MariaDB replay: %w", err)
		}
		plan.AnchorFile = anchor.File
		plan.StartPosition = anchor.Position
		// mariadb-binlog cannot filter by GTID, so a targetGTID recovery is bounded
		// positionally: resolve the target (and the anchor already applied by the
		// base backup) to a single domain's sequence numbers, and let the executor
		// derive byte offsets by scanning the downloaded binlogs.
		if o.Target.GTID != "" {
			domain, targetSeq, ok, err := binlog.SingleDomainMariaGTID(o.Target.GTID)
			if err != nil {
				return fmt.Errorf("pitr: %w", err)
			}
			if ok {
				plan.MariaDBPositional = true
				plan.MariaDBDomain = domain
				plan.MariaDBTargetSeq = targetSeq
				plan.MariaDBAnchorSeq = binlog.MariaSeqForDomain(anchor.GTIDSet, domain)
			}
		}
	} else {
		plan, err = binlog.PlanReplay(&index, anchor.GTIDSet, o.Target)
		if err != nil {
			return fmt.Errorf("pitr: planning replay: %w", err)
		}
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

	return o.applyReplay(ctx, bt, eng, plan, files)
}

// readAnchorGTID parses the base backup's binlog-info file and returns the
// full binlog info (file, position, GTID set) the restore landed at. It prefers
// the copy in the data directory (durable on the PVC, so it survives an
// init-container restart that lost the scratch backup dir) and falls back to the
// scratch dir.
func (o *RestoreOptions) readAnchorGTID(bt engine.BackupTool) (engine.BinlogInfo, error) {
	infoFile := bt.BinlogInfoFileName()
	content, err := os.ReadFile(filepath.Join(o.DataDir, infoFile))
	if os.IsNotExist(err) {
		content, err = os.ReadFile(filepath.Join(o.BackupDir, infoFile))
	}
	if err != nil {
		if os.IsNotExist(err) {
			return engine.BinlogInfo{}, nil
		}
		return engine.BinlogInfo{}, fmt.Errorf("pitr: reading %s: %w", infoFile, err)
	}
	return bt.ParseBinlogInfo(string(content))
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
// directory and pipes the binlog client output into the SQL client, bounded by
// the recovery target. The temp server runs with --skip-grant-tables (same
// pattern as reconcileCredentials) so the client connects as root without a
// password; GTID tracking is independent of the grant system.
func (o *RestoreOptions) applyReplay(
	ctx context.Context, bt engine.BackupTool, eng engine.Engine, plan binlog.ReplayPlan, files []string,
) error {
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

	isMariaDB := eng.Flavor() == engine.FlavorMariaDB

	// A MariaDB targetGTID recovery is bounded by byte offsets (mariadb-binlog has
	// no --include-gtids), computed by scanning the downloaded binlogs.
	if isMariaDB && plan.MariaDBPositional {
		return o.replayMariadbPositional(ctx, bt, plan, files)
	}

	replayFiles := files
	startPos := plan.StartPosition

	// For MariaDB positional replay: find the anchor file in the downloaded files
	// and skip everything before it. Downloaded files are stored as
	// <serverUUID>_<binlogName>, so we match on the suffix.
	if isMariaDB && plan.AnchorFile != "" {
		found := false
		for i, f := range files {
			if strings.HasSuffix(f, "_"+plan.AnchorFile) || filepath.Base(f) == plan.AnchorFile {
				replayFiles = files[i:]
				found = true
				break
			}
		}
		if !found {
			// The anchor binlog was rotated/purged before archiving, so its byte
			// offset means nothing in any downloaded file. Applying it to a
			// different file lands mid-event and corrupts the stream, so start from
			// the beginning instead.
			startPos = 0
		}
	}

	replayArgs, err := binlog.ReplayArgs(binlog.ReplayOptions{
		Files:         replayFiles,
		StopDatetime:  plan.StopDatetime,
		IncludeGTIDs:  plan.IncludeGTIDs,
		ExcludeGTIDs:  plan.ExcludeGTIDs,
		StartPosition: startPos,
		MariaDB:       isMariaDB,
	})
	if err != nil {
		return fmt.Errorf("pitr: building replay args: %w", err)
	}

	log.Info("Replaying archived binlogs into restored data", "files", len(files))
	return o.runReplayChunk(ctx, bt, replayArgs)
}

// replayMariadbPositional executes a MariaDB targetGTID recovery as byte-offset-
// bounded chunks. It scans each downloaded binlog for its transaction boundaries,
// plans the ordered chunks that cover (anchorSeq, targetSeq], and streams each
// chunk into the already-running temporary mysqld. Because a stop offset requires a
// single file, the last chunk is the target file bounded by --stop-position; the
// server's GTID state advances across chunks so the result ends exactly at target.
func (o *RestoreOptions) replayMariadbPositional(
	ctx context.Context, bt engine.BackupTool, plan binlog.ReplayPlan, files []string,
) error {
	log := logf.FromContext(ctx).WithName("instance-pitr")

	boundaries := make([][]binlog.TxnBoundary, len(files))
	for i, f := range files {
		b, err := binlog.MariadbTxnBoundaries(ctx, o.MysqlbinlogPath, f)
		if err != nil {
			return fmt.Errorf("pitr: scanning binlog boundaries in %s: %w", f, err)
		}
		boundaries[i] = b
	}

	chunks, err := binlog.PlanMariadbPositional(
		files, boundaries, plan.MariaDBDomain, plan.MariaDBAnchorSeq, plan.MariaDBTargetSeq)
	if err != nil {
		return fmt.Errorf("pitr: planning MariaDB positional replay: %w", err)
	}
	if len(chunks) == 0 {
		log.Info("No MariaDB transactions to replay past the anchor; data is already at the recovery target")
		return nil
	}

	for i, chunk := range chunks {
		replayArgs, err := binlog.ReplayArgs(binlog.ReplayOptions{
			Files:         chunk.Files,
			StartPosition: chunk.StartPosition,
			StopPosition:  chunk.StopPosition,
			MariaDB:       true,
		})
		if err != nil {
			return fmt.Errorf("pitr: building replay args for chunk %d: %w", i, err)
		}
		log.Info("Replaying MariaDB binlog chunk", "chunk", i, "files", len(chunk.Files),
			"startPosition", chunk.StartPosition, "stopPosition", chunk.StopPosition)
		if err := o.runReplayChunk(ctx, bt, replayArgs); err != nil {
			return err
		}
	}
	return nil
}

// runReplayChunk runs the binlog client piped into the SQL client for one bounded
// set of files, decoding the archived binlogs and applying them to the temporary
// server. It is invoked once per replay chunk (MariaDB positional recovery streams
// several in sequence against the same server); both child processes' stderr is
// captured as structured logs, while the binlog stream itself is a data path and
// never logged.
func (o *RestoreOptions) runReplayChunk(ctx context.Context, bt engine.BackupTool, replayArgs []string) error {
	log := logf.FromContext(ctx).WithName("instance-pitr")

	pr, pw := io.Pipe()

	decodeBin := o.MysqlbinlogPath
	if decodeBin == "" {
		decodeBin = bt.BinlogClientBinary()
	}
	decode := exec.CommandContext(ctx, decodeBin, replayArgs...)
	decode.Stdout = pw
	_, decodeErr := newProcessLogWriters(log.WithName(decodeBin))
	decode.Stderr = decodeErr

	sqlBin := o.MysqlPath
	if sqlBin == "" {
		sqlBin = bt.SQLClientBinary()
	}
	apply := exec.CommandContext(ctx, sqlBin,
		"--socket="+o.Socket, "--user=root", "--binary-mode")
	apply.Stdin = pr
	// MYSQL_PWD keeps the (empty here) password off the argv; harmless under
	// --skip-grant-tables but keeps the invocation consistent.
	apply.Env = append(os.Environ(), "MYSQL_PWD="+o.RootPassword)
	applyOut, applyErr := newProcessLogWriters(log.WithName(sqlBin))
	apply.Stdout = applyOut
	apply.Stderr = applyErr

	if err := apply.Start(); err != nil {
		_ = pr.CloseWithError(err)
		return fmt.Errorf("pitr: starting %s: %w", sqlBin, err)
	}
	if err := decode.Start(); err != nil {
		_ = pw.CloseWithError(err)
		return fmt.Errorf("pitr: starting %s: %w", decodeBin, err)
	}

	decErr := decode.Wait()
	_ = pw.CloseWithError(decErr)
	appErr := apply.Wait()
	_ = pr.CloseWithError(appErr)

	if decErr != nil {
		return fmt.Errorf("pitr: %s: %w", decodeBin, decErr)
	}
	if appErr != nil {
		return fmt.Errorf("pitr: %s apply: %w", sqlBin, appErr)
	}
	return nil
}
