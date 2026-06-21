/*
Copyright 2026 The CloudNative MySQL Authors.

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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

// InitOptions configures a fresh data-directory initialisation.
type InitOptions struct {
	// MysqldPath is the mysqld binary (default "mysqld").
	MysqldPath string
	// Version is the MySQL server version (e.g. "8.0.36"). It selects the
	// initialisation method and the bootstrap SQL dialect.
	Version string
	// DataDir is the data directory to initialise.
	DataDir string
	// ConfigFile is the defaults file passed to mysqld.
	ConfigFile string
	// Socket is the unix socket the temporary server listens on.
	Socket string
	// Bootstrap is the desired post-initialisation state.
	Bootstrap BootstrapParams
	// ReadyTimeout bounds how long to wait for the temporary server to accept
	// connections.
	ReadyTimeout time.Duration
}

func (o *InitOptions) applyDefaults() {
	if o.MysqldPath == "" {
		o.MysqldPath = defaultMysqldBinary
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = 60 * time.Second
	}
}

// bootstrapSentinel is created atomically after a complete bootstrap so the
// next run can reliably distinguish a fully initialized directory from one
// where initialization was interrupted partway.
const bootstrapSentinel = ".cloudnative-mysql-bootstrapped"

// hasSystemSchema reports whether the data directory contains the mysql system
// schema laid down by --initialize, regardless of whether bootstrap completed.
func hasSystemSchema(dataDir string) bool {
	info, err := os.Stat(filepath.Join(dataDir, "mysql"))
	return err == nil && info.IsDir()
}

// IsInitialized reports whether the data directory is fully initialised
// (bootstrap completed). It checks for a sentinel file written only after a
// successful bootstrap, so a directory with system tables but no sentinel
// (interrupted initialization) is considered uninitialised.
func IsInitialized(dataDir string) bool {
	_, err := os.Stat(filepath.Join(dataDir, bootstrapSentinel))
	return err == nil
}

// Initialize initialises a fresh data directory and applies the bootstrap
// statements. It is a no-op (returns nil) if the directory is already
// initialised, making it safe to run on every pod start.
func Initialize(ctx context.Context, opts InitOptions) error {
	opts.applyDefaults()
	log := logf.FromContext(ctx).WithName("instance-initdb").WithValues(
		"dataDir", opts.DataDir,
		"socket", opts.Socket,
		"version", opts.Version,
	)
	log.Info("Starting data directory initialization")

	// Propagate the version into the bootstrap so its SQL dialect matches.
	if opts.Bootstrap.MySQLVersion == "" {
		opts.Bootstrap.MySQLVersion = opts.Version
	}

	if err := opts.Bootstrap.Validate(); err != nil {
		return err
	}

	ver, err := version.Parse(opts.Version)
	if err != nil {
		return err
	}
	if !ver.AtLeast(5, 7, 0) {
		return fmt.Errorf("MySQL versions older than 5.7 are not supported")
	}

	if IsInitialized(opts.DataDir) {
		log.Info("Data directory already initialized")
		return nil
	}

	// If system tables exist but the sentinel is missing, a previous
	// initialization was interrupted after --initialize-insecure but before
	// (or during) bootstrap cleanup. Wipe the partial state so we re-run
	// both --initialize and bootstrap from scratch.
	if hasSystemSchema(opts.DataDir) {
		log.Info("Data directory has system tables without bootstrap sentinel; cleaning up partial state")
		if err := purgeDataDir(opts.DataDir); err != nil {
			log.Error(err, "Failed to clean partial data directory")
		}
	}

	if err := os.MkdirAll(opts.DataDir, 0o750); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}
	log.Info("Created data directory")

	if err := opts.runInitialize(ctx); err != nil {
		// A failed --initialize leaves a half-written data directory (auto.cnf,
		// the mysql/ system-schema dir, tablespaces). Without cleanup a retry
		// would find the partial state, attempt --initialize over it (which
		// fails), and wedge. Wipe it so a retry starts from a clean slate.
		if cleanErr := purgeDataDir(opts.DataDir); cleanErr != nil {
			log.Error(cleanErr, "Failed to clean partial data directory after a failed initialize")
		}
		return err
	}

	if err := opts.runBootstrap(ctx); err != nil {
		// --initialize already laid down the mysql/ system schema, so
		// hasSystemSchema would now report the directory as initialized and the
		// next attempt would skip straight past bootstrap, leaving a
		// half-initialized server (system tables present, but no operator
		// accounts and a passwordless root). Wipe the partial state so a retry
		// re-runs both --initialize and the bootstrap SQL.
		if cleanErr := purgeDataDir(opts.DataDir); cleanErr != nil {
			log.Error(cleanErr, "Failed to clean partial data directory after a failed bootstrap")
		}
		return err
	}

	if err := os.WriteFile(filepath.Join(opts.DataDir, bootstrapSentinel), nil, 0o600); err != nil {
		return fmt.Errorf("marking data directory as bootstrapped: %w", err)
	}
	log.Info("Completed data directory initialization")
	return nil
}

// purgeDataDir empties a data directory in place (keeping the directory itself,
// which may be a mount point) so a failed initialization can be retried cleanly.
func purgeDataDir(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("reading data dir: %w", err)
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dataDir, entry.Name())); err != nil {
			return fmt.Errorf("removing %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// runInitialize lays down the system tables.
func (o *InitOptions) runInitialize(ctx context.Context) error {
	return o.runMysqldInitialize(ctx)
}

// runMysqldInitialize runs `mysqld --initialize-insecure`.
func (o *InitOptions) runMysqldInitialize(ctx context.Context) error {
	logf.FromContext(ctx).WithName("instance-initdb").Info("Running mysqld initialize", "binary", o.MysqldPath)
	args := []string{}
	if o.ConfigFile != "" {
		// --initialize ignores plugin_load_add, so group_replication.so is never
		// loaded and the group_replication_* settings become unknown variables
		// that abort initialization. Feed mysqld a copy of the config with the GR
		// block stripped out (it is meaningless during --initialize anyway).
		initConfig, err := o.writeInitConfig()
		if err != nil {
			return err
		}
		defer func() { _ = os.Remove(initConfig) }()
		args = append(args, "--defaults-file="+initConfig)
	}
	args = append(args,
		"--initialize-insecure",
		"--datadir="+o.DataDir,
	)
	return runStdio(ctx, o.MysqldPath, args, "mysqld --initialize-insecure")
}

// writeInitConfig writes a temporary copy of the runtime config with the Group
// Replication block stripped, suitable for `mysqld --initialize`, and returns
// its path. The caller is responsible for removing it.
func (o *InitOptions) writeInitConfig() (string, error) {
	content, err := os.ReadFile(o.ConfigFile)
	if err != nil {
		return "", fmt.Errorf("reading config file: %w", err)
	}
	stripped := config.StripGroupReplication(string(content))
	f, err := os.CreateTemp("", "mysqld-init-*.cnf")
	if err != nil {
		return "", fmt.Errorf("creating init config: %w", err)
	}
	if _, err := f.WriteString(stripped); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("writing init config: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("closing init config: %w", err)
	}
	return f.Name(), nil
}

func runStdio(ctx context.Context, binary string, args []string, what string) error {
	cmd := exec.CommandContext(ctx, binary, args...)
	stdout, stderr := newProcessLogWriters(logf.FromContext(ctx).WithName("process").WithValues("process", what))
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// runBootstrap starts a temporary socket-only server, applies the bootstrap
// statements as the passwordless root, then shuts it down.
func (o *InitOptions) runBootstrap(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("instance-initdb")
	args := []string{}
	if o.ConfigFile != "" {
		args = append(args, "--defaults-file="+o.ConfigFile)
	}
	args = append(args,
		"--datadir="+o.DataDir,
		"--socket="+o.Socket,
		"--skip-networking",
		// The bootstrap SQL creates accounts and sets the root password, so the
		// temporary server must be writable. A replica's rendered config carries
		// read_only/super_read_only=ON (and a Group Replication member always
		// renders as a replica until the group elects it primary), which would make
		// every bootstrap statement fail with ER_OPTION_PREVENTS_STATEMENT. Override
		// both off on the command line so the temporary server is writable
		// regardless of the member's eventual role.
		"--read-only=OFF",
		"--super-read-only=OFF",
	)

	stdout, stderr := newProcessLogWriters(log.WithName("temporary-mysqld"))
	sup := NewProcessSupervisor(o.MysqldPath, args,
		WithShutdownTimeout(o.ReadyTimeout),
		WithOutput(stdout, stderr))
	log.Info("Starting temporary mysqld for bootstrap", "socket", o.Socket)
	if err := sup.Start(ctx); err != nil {
		return fmt.Errorf("starting temporary server: %w", err)
	}
	defer func() { _ = sup.Shutdown(ctx) }()

	db, err := waitForSocket(ctx, o.Socket, "root", "", o.ReadyTimeout)
	if err != nil {
		return err
	}
	log.Info("Connected to temporary mysqld")
	defer func() { _ = db.Close() }()

	stmts, err := BootstrapStatements(o.Bootstrap)
	if err != nil {
		return err
	}
	log.Info("Applying bootstrap SQL", "statements", len(stmts))
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("bootstrap statement failed: %w", err)
		}
	}
	return nil
}

// waitForSocket opens a connection over the socket for the given credentials,
// retrying until the server is ready or the timeout elapses.
func waitForSocket(ctx context.Context, socket, user, password string, timeout time.Duration) (*sql.DB, error) {
	cfg := pool.Config{Socket: socket, User: user, Password: password}
	deadline := time.Now().Add(timeout)
	for {
		db, err := pool.Open(ctx, cfg)
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("temporary server not ready within %s: %w", timeout, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
