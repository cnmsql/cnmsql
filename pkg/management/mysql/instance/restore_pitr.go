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
	"errors"
	"fmt"
	"io"
	"math"
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
	// The backup metadata carries two anchor facts the in-archive binlog-info file
	// cannot: the fully-specified anchor GTID (a MariaDB 10.11 backup's binlog-info
	// has only file+position; the GTID was resolved at backup time via
	// BINLOG_GTID_POS) and the archive identity of the incarnation the backup was
	// taken from. Load it once so both are available.
	var anchorServer string
	if o.MetadataKey != "" {
		var metadata objectstore.BackupMetadata
		if err := o.Store.GetJSON(ctx, o.Bucket, o.MetadataKey, &metadata); err != nil {
			return fmt.Errorf("pitr: reading backup metadata %q: %w", o.MetadataKey, err)
		}
		anchorServer = metadata.AnchorServerUUID
		// Prefer the metadata GTID when the in-archive anchor has none, so the ordinary
		// GTID-based planner path applies instead of the positional fallback.
		if anchor.GTIDSet == "" && metadata.AnchorGTID != "" {
			anchor.GTIDSet = metadata.AnchorGTID
			log.Info("Using backup-time resolved anchor GTID from metadata", "anchorGTID", anchor.GTIDSet)
		}
	}
	log.Info("Read base backup anchor GTID", "anchorGTID", anchor.GTIDSet, "anchorServer", anchorServer)

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
		plan.AnchorServerUUID = anchorServer
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
				// When the index carries per-segment GTID ranges, prune the download to
				// the minimal set of segments whose union covers (anchorSeq, targetSeq],
				// failing closed on a gap. A GTID-less archive (no segment GTIDSet) has no
				// ranges to select on, so keep every segment and let the boundary-scanning
				// replay planner be the authority. MariaDBAnchorSeq may be 0 here (10.11
				// backup with no GTID); selection from 0 over-includes rather than
				// under-includes, and the replay planner trims with the derived anchor.
				if segmentsHaveGTIDRanges(index.Segments) {
					selected, err := binlog.SelectMariadbSegments(
						index.Segments, domain, plan.MariaDBAnchorSeq, targetSeq)
					if err != nil {
						return fmt.Errorf("pitr: selecting MariaDB segments: %w", err)
					}
					plan.Segments = selected
				}
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
// scratch dir. Each candidate filename is tried in both directories, since
// MariaBackup's binlog-info file name is version-dependent (mariadb_backup_binlog_info
// on 11.1+, xtrabackup_binlog_info before).
func (o *RestoreOptions) readAnchorGTID(bt engine.BackupTool) (engine.BinlogInfo, error) {
	for _, dir := range []string{o.DataDir, o.BackupDir} {
		for _, name := range bt.BinlogInfoFileNames() {
			content, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return engine.BinlogInfo{}, fmt.Errorf("pitr: reading %s: %w", name, err)
			}
			return bt.ParseBinlogInfo(string(content))
		}
	}
	// No binlog-info file under any name: an empty-position backup (fresh primary).
	return engine.BinlogInfo{}, nil
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

// segmentsHaveGTIDRanges reports whether the archive index carries per-segment GTID
// coverage, i.e. at least one segment has a non-empty GTIDSet. A GTID-less archive
// (10.11 mariabackup, or files with no GTID events) has none, so range-based segment
// selection cannot apply and every segment must be kept for boundary-scan replay.
func segmentsHaveGTIDRanges(segments []objectstore.ArchiveSegment) bool {
	for i := range segments {
		if segments[i].GTIDSet != "" {
			return true
		}
	}
	return false
}

// ErrAmbiguousAnchor is returned when a GTID-less base backup's anchor file name
// matches downloaded binlogs from more than one server. Every server numbers its
// binlogs from 000001, so a bare name like "binlog.000004" can name two different
// files across a failover; without a GTID (or identity) to disambiguate, replaying
// from the wrong one would re-apply or skip transactions, so recovery fails closed.
var ErrAmbiguousAnchor = errors.New(
	"pitr: base backup anchor file name matches binlogs from more than one server; cannot disambiguate a GTID-less anchor")

// findAnchorIndex returns the index of the downloaded binlog that is the base
// backup's anchor file, or -1 if none is present. downloadReplayFiles names every
// file "<serverUUID>_<binlogName>", so the anchor's bare name matches on the
// "_<name>" suffix.
//
// When anchorServer is set (the backup metadata recorded the incarnation the backup
// was taken from) the match is pinned to that server's file directly, which resolves
// the cross-incarnation collision a re-clone/failover produces. When it is empty
// (legacy backups) and the name matches files under more than one distinct
// "<serverUUID>_" prefix, it returns ErrAmbiguousAnchor rather than guessing.
func findAnchorIndex(files []string, anchorFile, anchorServer string) (int, error) {
	first := -1
	firstServer := ""
	for i, f := range files {
		base := filepath.Base(f)
		if !strings.HasSuffix(base, "_"+anchorFile) {
			continue
		}
		server := strings.TrimSuffix(base, "_"+anchorFile)
		if anchorServer != "" {
			// A recorded anchor server names exactly the incarnation to replay from;
			// ignore like-named files from any other incarnation.
			if server == anchorServer {
				return i, nil
			}
			continue
		}
		if first < 0 {
			first, firstServer = i, server
			continue
		}
		if server != firstServer {
			return -1, ErrAmbiguousAnchor
		}
	}
	// With a recorded anchor server that matched nothing, the anchor incarnation's
	// binlog was purged/rotated before archiving; report absent so the caller applies
	// its rotated-anchor fallback rather than failing closed.
	return first, nil
}

// applyReplay starts a temporary socket-only mysqld over the restored data
// directory and pipes the binlog client output into the SQL client, bounded by
// the recovery target. The temp server runs with --skip-grant-tables (same
// pattern as reconcileCredentials) so the client connects as root without a
// password; GTID tracking is independent of the grant system.
//
// The whole replay streams through one SQL client session started before the
// grant tables are loaded (replaySession): that session both opens the
// connection and loads the grant tables, so replayed account-management
// statements execute on it no matter what the stream does to the accounts.
func (o *RestoreOptions) applyReplay(
	ctx context.Context, bt engine.BackupTool, eng engine.Engine, plan binlog.ReplayPlan, files []string,
) (err error) {
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

	sess, err := o.startReplaySession(ctx, bt)
	if err != nil {
		return err
	}
	// Always reap the session's SQL client, and let its exit status surface only
	// when the streaming reported no error: a mid-stream decode failure
	// truncates the stream, which makes the client exit too, and the streaming
	// error is the root cause worth reporting.
	defer func() {
		if finErr := sess.finish(); err == nil {
			err = finErr
		}
	}()

	isMariaDB := eng.Flavor() == engine.FlavorMariaDB

	// A MariaDB targetGTID recovery is bounded by byte offsets (mariadb-binlog has
	// no --include-gtids), computed by scanning the downloaded binlogs.
	if isMariaDB && plan.MariaDBPositional {
		return o.replayMariadbPositional(ctx, plan, files, sess)
	}

	replayFiles := files
	startPos := plan.StartPosition

	// For MariaDB positional replay: find the anchor file in the downloaded files
	// and skip everything before it.
	if isMariaDB && plan.AnchorFile != "" {
		ai, anchorErr := findAnchorIndex(files, plan.AnchorFile, plan.AnchorServerUUID)
		if anchorErr != nil {
			return anchorErr
		}
		if ai >= 0 {
			replayFiles = files[ai:]
		} else {
			// The anchor binlog was rotated/purged before archiving, so its byte
			// offset means nothing in any downloaded file. Applying it to a
			// different file lands mid-event and corrupts the stream, so start from
			// the beginning instead.
			startPos = 0
		}
	}

	replayArgs, argsErr := binlog.ReplayArgs(binlog.ReplayOptions{
		Files:         replayFiles,
		StopDatetime:  plan.StopDatetime,
		IncludeGTIDs:  plan.IncludeGTIDs,
		ExcludeGTIDs:  plan.ExcludeGTIDs,
		StartPosition: startPos,
		MariaDB:       isMariaDB,
	})
	if argsErr != nil {
		return fmt.Errorf("pitr: building replay args: %w", argsErr)
	}

	log.Info("Replaying archived binlogs into restored data", "files", len(files))
	return sess.streamChunk(ctx, replayArgs)
}

// replayMariadbPositional executes a MariaDB targetGTID recovery as byte-offset-
// bounded chunks. It scans each downloaded binlog for its transaction boundaries,
// plans the ordered chunks that cover (anchorSeq, targetSeq], and streams each
// chunk into the session's already-connected SQL client. Because a stop offset
// requires a single file, the last chunk is the target file bounded by
// --stop-position; the server's GTID state advances across chunks so the result
// ends exactly at target.
func (o *RestoreOptions) replayMariadbPositional(
	ctx context.Context, plan binlog.ReplayPlan, files []string, sess *replaySession,
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

	// When the base backup's binlog-info file carried no GTID (10.11 mariabackup
	// writes only file+position), MariaDBAnchorSeq is 0 and the planner would rewind
	// replay to genesis, re-applying transactions already in the backup (duplicate
	// keys, etc.). Recover the anchor sequence from the recorded binlog position by
	// scanning the anchor file's transaction boundaries.
	anchorSeq := plan.MariaDBAnchorSeq
	if anchorSeq == 0 && plan.AnchorFile != "" {
		ai, err := findAnchorIndex(files, plan.AnchorFile, plan.AnchorServerUUID)
		if err != nil {
			return err
		}
		if ai >= 0 {
			anchorSeq = binlog.AnchorSeqFromBoundaries(boundaries[ai], plan.MariaDBDomain, plan.StartPosition)
			// The base backup also contains every transaction in the source server's
			// earlier binlogs. Those carry lower sequences than the anchor file, so
			// normally they don't change the result — except when the backup was taken
			// just after a log rotation, leaving no transaction before StartPosition in
			// the anchor file itself. Without folding them in, anchorSeq would stay 0
			// and replay would rewind to genesis. Same-server files are matched by the
			// "<serverUUID>_" prefix the downloader assigns; a different server's
			// like-named binlog carries an unrelated sequence range and must not count.
			srvPrefix := strings.TrimSuffix(filepath.Base(files[ai]), plan.AnchorFile)
			for i := range ai {
				if !strings.HasPrefix(filepath.Base(files[i]), srvPrefix) {
					continue
				}
				if s := binlog.AnchorSeqFromBoundaries(boundaries[i], plan.MariaDBDomain, math.MaxInt64); s > anchorSeq {
					anchorSeq = s
				}
			}
			log.Info("Derived MariaDB anchor sequence from backup binlog position",
				"anchorFile", plan.AnchorFile, "position", plan.StartPosition, "anchorSeq", anchorSeq)
		}
	}

	chunks, err := binlog.PlanMariadbPositional(
		files, boundaries, plan.MariaDBDomain, anchorSeq, plan.MariaDBTargetSeq)
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
		if err := sess.streamChunk(ctx, replayArgs); err != nil {
			return err
		}
	}
	return nil
}

// replayPrologue primes the replay's SQL stream. The temporary server runs with
// --skip-grant-tables, which leaves the grant tables unloaded, so every
// account-management statement in the replayed binlogs (CREATE USER, GRANT,
// ALTER USER, DROP USER, SET PASSWORD — e.g. a user granted during the
// backup-to-target window) fails with ER_OPTION_PREVENTS_STATEMENT
// (ERROR 1290) until the grant tables are loaded. FLUSH PRIVILEGES loads them,
// on the already-established connection, and re-enables those statements.
//
// The FLUSH runs with the session's binary logging disabled: on MySQL it would
// otherwise be written to the temporary server's binary log as a GTID
// transaction of the restored identity, polluting the replication timeline the
// recovery just reconstructed (reconcileCredentials guards the same way with
// --skip-log-bin).
const replayPrologue = "SET @@SESSION.SQL_LOG_BIN=0;\n" +
	"FLUSH PRIVILEGES;\n" +
	"SET @@SESSION.SQL_LOG_BIN=1;\n"

// replaySession is the single SQL client session a whole replay streams
// through. One connection for the entire replay (not one per chunk) is a
// correctness requirement: the connection is established while the temporary
// server still runs --skip-grant-tables, and the stream opens with the
// replayPrologue, which loads the restored grant tables on that same session.
// The session keeps the skip-grants authority it received at connect time, so
// the replayed account-management statements execute; any connection opened
// after the FLUSH would instead authenticate normally against the restored
// data's own accounts, whose passwords the recovery flow does not control.
// The MariaDB positional path streams several chunks, so all chunks share
// this one process.
type replaySession struct {
	o      *RestoreOptions
	bt     engine.BackupTool
	sqlBin string
	apply  *exec.Cmd
	pr     *io.PipeReader
	pw     *io.PipeWriter
}

// startReplaySession starts the replay's SQL client with the grant-loading
// prologue already queued on its stdin.
func (o *RestoreOptions) startReplaySession(ctx context.Context, bt engine.BackupTool) (*replaySession, error) {
	log := logf.FromContext(ctx).WithName("instance-pitr")

	sqlBin := o.MysqlPath
	if sqlBin == "" {
		sqlBin = bt.SQLClientBinary()
	}
	pr, pw := io.Pipe()
	apply := exec.CommandContext(ctx, sqlBin,
		"--socket="+o.Socket, "--user=root", "--binary-mode")
	apply.Stdin = pr
	// MYSQL_PWD keeps the (empty here) password off the argv; the connection is
	// established under --skip-grant-tables, before grant checking exists.
	apply.Env = append(os.Environ(), "MYSQL_PWD="+o.RootPassword)
	applyOut, applyErr := newProcessLogWriters(log.WithName(sqlBin))
	apply.Stdout = applyOut
	apply.Stderr = applyErr

	if err := apply.Start(); err != nil {
		_ = pr.CloseWithError(err)
		_ = pw.CloseWithError(err)
		return nil, fmt.Errorf("pitr: starting %s: %w", sqlBin, err)
	}
	if _, err := pw.Write([]byte(replayPrologue)); err != nil {
		// A write fails only once the read side is gone, i.e. the client exited;
		// reap it before reporting.
		_ = pw.CloseWithError(err)
		_ = apply.Wait()
		return nil, fmt.Errorf("pitr: priming %s stream: %w", sqlBin, err)
	}
	return &replaySession{o: o, bt: bt, sqlBin: sqlBin, apply: apply, pr: pr, pw: pw}, nil
}

// streamChunk runs the binlog client for one bounded set of files into the
// session's persistent SQL client, decoding the archived binlogs and applying
// them to the temporary server. It is invoked once per replay chunk (MariaDB
// positional recovery streams several in sequence against the same session);
// the binlog client's stderr is captured as structured logs, while the binlog
// stream itself is a data path and is never logged.
func (s *replaySession) streamChunk(ctx context.Context, replayArgs []string) error {
	log := logf.FromContext(ctx).WithName("instance-pitr")

	decodeBin := s.o.MysqlbinlogPath
	if decodeBin == "" {
		decodeBin = s.bt.BinlogClientBinary()
	}
	decode := exec.CommandContext(ctx, decodeBin, replayArgs...)
	decode.Stdout = s.pw
	_, decodeErrW := newProcessLogWriters(log.WithName(decodeBin))
	decode.Stderr = decodeErrW

	if err := decode.Start(); err != nil {
		return fmt.Errorf("pitr: starting %s: %w", decodeBin, err)
	}
	if decErr := decode.Wait(); decErr != nil {
		return fmt.Errorf("pitr: %s: %w", decodeBin, decErr)
	}
	return nil
}

// finish closes the replay stream and reaps the SQL client. It must be called
// exactly once per session (applyReplay defers it); when the stream was
// truncated by an earlier decode failure, the client's own exit status is
// secondary and the caller reports the streaming error instead.
func (s *replaySession) finish() error {
	_ = s.pw.Close() // EOF: the client drains any remaining stream and exits
	appErr := s.apply.Wait()
	_ = s.pr.Close()
	if appErr != nil {
		return fmt.Errorf("pitr: %s apply: %w", s.sqlBin, appErr)
	}
	return nil
}
