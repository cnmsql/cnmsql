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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/sqldump"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/tail"
)

// ImportMarkerName is written into the data directory once an import has
// finished, so a restarted init container does not load the dump again.
const ImportMarkerName = ".cnmsql-import-done"

// maxImportStderrTailBytes bounds the SQL client output kept for the error
// message when a load fails.
const maxImportStderrTailBytes = 4 << 10

// ImportOptions configures loading a logical backup into a freshly initialised
// data directory.
type ImportOptions struct {
	// Store reads the dump and its manifest.
	Store *objectstore.Client
	// Bucket, DumpKey and ManifestKey locate the dump.sql.zst object and its
	// logical.json manifest.
	Bucket      string
	DumpKey     string
	ManifestKey string
	// Databases loads only these schemas. Empty loads the whole dump.
	Databases []string
	// PostImportSQL runs as root after the load.
	PostImportSQL []string
	// Engine selects the SQL client and its arguments.
	Engine engine.Engine

	// MysqldPath, ConfigFile, DataDir and Socket start the temporary server.
	MysqldPath string
	ConfigFile string
	DataDir    string
	Socket     string
	// LoadPath overrides the SQL client. Empty selects the engine's client:
	// mysql for MySQL, mariadb for MariaDB.
	LoadPath string
	// WorkDir holds the client's credentials file while it runs (default
	// os.TempDir()).
	WorkDir string
	// RootPassword is root@localhost's password, set by initdb.
	RootPassword string
	// ReadyTimeout bounds the temporary server startup.
	ReadyTimeout time.Duration
	// ShutdownTimeout bounds the temporary server's clean shutdown, which has a
	// whole load to flush. A server killed after it is only crash recovery:
	// every loaded statement was committed.
	ShutdownTimeout time.Duration
}

func (o *ImportOptions) applyDefaults() {
	if o.MysqldPath == "" {
		o.MysqldPath = "mysqld"
	}
	if o.Socket == "" {
		o.Socket = "/var/run/mysqld/mysqld.sock"
	}
	if o.WorkDir == "" {
		o.WorkDir = os.TempDir()
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = 2 * time.Minute
	}
	if o.ShutdownTimeout == 0 {
		o.ShutdownTimeout = 10 * time.Minute
	}
}

// errImportStreamClosed closes the download pipe when the load stopped
// reading it, so the download's own error can be told apart from it.
var errImportStreamClosed = errors.New("import: stream closed")

// importMarker is the content of the import marker file, for whoever finds it
// later.
type importMarker struct {
	BackupID    string    `json:"backupID"`
	ClusterName string    `json:"clusterName"`
	BackupName  string    `json:"backupName"`
	Databases   []string  `json:"databases"`
	CompletedAt time.Time `json:"completedAt"`
}

// Import loads a logical backup into DataDir, which initdb has just
// initialised. It starts a temporary socket-only server with binary logging
// off, streams the dump into the engine's SQL client as root (verifying the
// checksum and keeping only the selected databases on the way), runs the
// post-import SQL, and stops the server. A marker file then makes a later run
// a no-op. A run that fails half-way is safe to repeat: the dump drops and
// recreates every table it loads.
func Import(ctx context.Context, opts ImportOptions) error {
	opts.applyDefaults()
	log := logf.FromContext(ctx).WithName("instance-import").WithValues(
		"bucket", opts.Bucket, "dumpKey", opts.DumpKey)

	switch {
	case opts.Store == nil:
		return errors.New("import: object-store client is required")
	case opts.Bucket == "" || opts.DumpKey == "" || opts.ManifestKey == "":
		return errors.New("import: bucket, dump key and manifest key are required")
	case opts.DataDir == "":
		return errors.New("import: data dir is required")
	case opts.Engine == nil:
		return errors.New("import: engine is required")
	case opts.RootPassword == "":
		return errors.New("import: the root password is required")
	case strings.ContainsAny(opts.RootPassword, "\n\r"):
		return errors.New("import: the root password cannot contain a line break")
	}

	marker := filepath.Join(opts.DataDir, ImportMarkerName)
	if _, err := os.Stat(marker); err == nil {
		log.Info("Logical backup already imported, skipping", "marker", marker)
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("import: checking %s: %w", marker, err)
	}

	var meta objectstore.LogicalBackupMetadata
	if err := opts.Store.GetJSON(ctx, opts.Bucket, opts.ManifestKey, &meta); err != nil {
		return fmt.Errorf("import: reading manifest %s: %w", opts.ManifestKey, err)
	}
	if err := meta.CheckImportable(string(opts.Engine.Flavor()), opts.Databases); err != nil {
		return fmt.Errorf("import: %w", err)
	}

	client := opts.LoadPath
	if client == "" {
		client = opts.Engine.Logical().LoadBinary()
	}
	clientPath, err := exec.LookPath(client)
	if err != nil {
		return fmt.Errorf("import: %s is not in this instance image: %w", client, err)
	}

	args := []string{}
	if opts.ConfigFile != "" {
		args = append(args, "--defaults-file="+opts.ConfigFile)
	}
	args = append(args,
		"--datadir="+opts.DataDir,
		"--socket="+opts.Socket,
		"--skip-networking",
		// The load is not replicated: replicas clone the loaded primary, and
		// the dump carries no GTIDs that must survive. Without a binary log the
		// load is faster and the first archived binlog is not the size of it.
		"--skip-log-bin",
		// Imported events must not fire against a half-loaded schema.
		"--event-scheduler=OFF",
		// Accept the largest statement the SQL client sends.
		fmt.Sprintf("--max-allowed-packet=%d", engine.MaxLoadPacketBytes),
	)
	stdout, stderr := newProcessLogWriters(log.WithName("temporary-mysqld"))
	sup := NewProcessSupervisor(opts.MysqldPath, args,
		WithShutdownTimeout(opts.ShutdownTimeout),
		WithOutput(stdout, stderr))
	log.Info("Starting temporary mysqld to import a logical backup", "socket", opts.Socket)
	if err := sup.Start(ctx); err != nil {
		return fmt.Errorf("import: starting temporary server: %w", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = sup.Shutdown(ctx)
		}
	}()

	db, err := waitForSocket(ctx, opts.Socket, "root", opts.RootPassword, opts.ReadyTimeout)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	defer func() { _ = db.Close() }()

	log.Info("Loading logical backup", "backupID", meta.BackupID, "sourceCluster", meta.ClusterName,
		"sourceVersion", meta.ServerVersion, "databases", opts.Databases)
	started := time.Now()
	if err := opts.load(ctx, clientPath, meta); err != nil {
		return err
	}
	log.Info("Loaded logical backup", "duration", time.Since(started).Round(time.Second).String())

	for i, stmt := range opts.PostImportSQL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("import: postImportSQL[%d]: %w", i, err)
		}
	}
	_ = db.Close()

	log.Info("Stopping temporary mysqld")
	stopped = true
	if err := sup.Shutdown(ctx); err != nil {
		return fmt.Errorf("import: stopping temporary server: %w", err)
	}

	databases := opts.Databases
	if len(databases) == 0 {
		databases = meta.Databases
	}
	payload, err := json.Marshal(importMarker{
		BackupID:    meta.BackupID,
		ClusterName: meta.ClusterName,
		BackupName:  meta.BackupName,
		Databases:   databases,
		CompletedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	if err := writeFileSync(marker, payload); err != nil {
		return fmt.Errorf("import: writing %s: %w", marker, err)
	}
	log.Info("Imported logical backup", "backupID", meta.BackupID)
	return nil
}

// load streams the dump from the object store into the SQL client: download,
// checksum, zstd decode, database filter, client stdin. On success the client
// has run every statement and the checksum matches the manifest.
func (o *ImportOptions) load(ctx context.Context, clientPath string, meta objectstore.LogicalBackupMetadata) error {
	log := logf.FromContext(ctx).WithName("instance-import")

	dir, err := os.MkdirTemp(o.WorkDir, "cnmsql-import-")
	if err != nil {
		return fmt.Errorf("import: creating work directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	defaults := filepath.Join(dir, "client.cnf")
	if err := os.WriteFile(defaults, clientDefaultsFile("root", o.RootPassword, o.Socket), 0o600); err != nil {
		return fmt.Errorf("import: writing credentials file: %w", err)
	}

	// Cancelling clientCtx kills the client, so a broken stream never lets it
	// run the statement it was cut in the middle of.
	clientCtx, killClient := context.WithCancel(ctx)
	defer killClient()
	tool := filepath.Base(clientPath)
	stderrTail := tail.NewWriter(maxImportStderrTailBytes)
	cmd := exec.CommandContext(clientCtx, clientPath, o.Engine.Logical().LoadArgs(defaults)...)
	cmd.Stdout = newProcessLogWriter(log.WithName(tool), "stdout")
	cmd.Stderr = io.MultiWriter(newProcessLogWriter(log.WithName(tool), "stderr"), stderrTail)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("import: starting %s: %w", tool, err)
	}
	clientFailed := func(waitErr error) error {
		return fmt.Errorf("import: %s failed: %v: %s", tool, waitErr, strings.TrimSpace(stderrTail.String()))
	}

	pr, pw := io.Pipe()
	hash := objectstore.NewSHA256Writer(pw)
	downloaded := make(chan error, 1)
	go func() {
		_, err := o.Store.Download(ctx, o.Bucket, o.DumpKey, hash)
		_ = pw.CloseWithError(err)
		downloaded <- err
	}()

	in := &stickyErrWriter{w: stdin}
	kept, copyErr := decodeAndFilter(in, pr, o.Databases)
	// Unblock the download if the copy stopped early; a no-op once it is done.
	_ = pr.CloseWithError(errImportStreamClosed)
	downloadErr := <-downloaded
	if errors.Is(downloadErr, errImportStreamClosed) {
		downloadErr = nil
	}

	if copyErr != nil {
		if in.err != nil {
			// The client stopped reading: it exited on an SQL error, which its
			// own output explains better than the broken pipe.
			_ = stdin.Close()
			return clientFailed(cmd.Wait())
		}
		killClient()
		_ = stdin.Close()
		_ = cmd.Wait()
		if downloadErr != nil {
			return fmt.Errorf("import: downloading %s: %w", o.DumpKey, downloadErr)
		}
		return fmt.Errorf("import: reading %s: %w", o.DumpKey, copyErr)
	}

	// The whole stream went through. A checksum mismatch or a missing section
	// is reported only after the client ran it, so a corrupted dump fails the
	// import (and a retry loads it again) without being marked done.
	if err := stdin.Close(); err != nil {
		return fmt.Errorf("import: closing %s input: %w", tool, err)
	}
	if err := cmd.Wait(); err != nil {
		return clientFailed(err)
	}
	if meta.SHA256 != "" && hash.SumHex() != meta.SHA256 {
		return fmt.Errorf("import: %s checksum mismatch: got %s, manifest has %s", o.DumpKey, hash.SumHex(), meta.SHA256)
	}
	for _, db := range o.Databases {
		if !slices.Contains(kept, db) {
			return fmt.Errorf("import: the manifest lists database %q but the dump has no section for it", db)
		}
	}
	return nil
}

// decodeAndFilter decompresses the dump and copies the selected databases'
// sections to w.
func decodeAndFilter(w io.Writer, compressed io.Reader, databases []string) ([]string, error) {
	dec, err := objectstore.NewZstdReader(compressed)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dec.Close() }()
	return sqldump.FilterDatabases(w, dec, databases)
}

// stickyErrWriter records the first write error, so the caller can tell a
// consumer that went away from a producer that failed.
type stickyErrWriter struct {
	w   io.Writer
	err error
}

func (s *stickyErrWriter) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	n, err := s.w.Write(p)
	if err != nil {
		s.err = err
	}
	return n, err
}

// writeFileSync writes a file and flushes it to disk before returning.
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
