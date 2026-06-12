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
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
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

// IsInitialized reports whether the data directory already contains a MySQL
// system schema.
func IsInitialized(dataDir string) bool {
	info, err := os.Stat(filepath.Join(dataDir, "mysql"))
	return err == nil && info.IsDir()
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

	if err := os.MkdirAll(opts.DataDir, 0o750); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}
	log.Info("Created data directory")

	if err := opts.runInitialize(ctx); err != nil {
		return err
	}

	if err := opts.runBootstrap(ctx); err != nil {
		return err
	}
	log.Info("Completed data directory initialization")
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
		args = append(args, "--defaults-file="+o.ConfigFile)
	}
	args = append(args,
		"--initialize-insecure",
		"--datadir="+o.DataDir,
	)
	return runStdio(ctx, o.MysqldPath, args, "mysqld --initialize-insecure")
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
