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

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
)

// InitOptions configures a fresh data-directory initialisation.
type InitOptions struct {
	// MysqldPath is the mysqld binary (default "mysqld").
	MysqldPath string
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
		o.MysqldPath = "mysqld"
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = 60 * time.Second
	}
}

// IsInitialized reports whether the data directory already contains a MySQL
// system schema (the "mysql" subdirectory).
func IsInitialized(dataDir string) bool {
	info, err := os.Stat(filepath.Join(dataDir, "mysql"))
	return err == nil && info.IsDir()
}

// Initialize initialises a fresh data directory and applies the bootstrap
// statements. It is a no-op (returns nil) if the directory is already
// initialised, making it safe to run on every pod start.
func Initialize(ctx context.Context, opts InitOptions) error {
	opts.applyDefaults()

	if err := opts.Bootstrap.Validate(); err != nil {
		return err
	}

	if IsInitialized(opts.DataDir) {
		return nil
	}

	if err := os.MkdirAll(opts.DataDir, 0o750); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	if err := opts.runInitialize(ctx); err != nil {
		return err
	}

	return opts.runBootstrap(ctx)
}

// runInitialize runs `mysqld --initialize-insecure` to lay down the system
// tables with a passwordless root@localhost.
func (o *InitOptions) runInitialize(ctx context.Context) error {
	args := []string{}
	if o.ConfigFile != "" {
		args = append(args, "--defaults-file="+o.ConfigFile)
	}
	args = append(args,
		"--initialize-insecure",
		"--datadir="+o.DataDir,
	)

	cmd := exec.CommandContext(ctx, o.MysqldPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysqld --initialize-insecure: %w", err)
	}
	return nil
}

// runBootstrap starts a temporary socket-only server, applies the bootstrap
// statements as the passwordless root, then shuts it down.
func (o *InitOptions) runBootstrap(ctx context.Context) error {
	args := []string{}
	if o.ConfigFile != "" {
		args = append(args, "--defaults-file="+o.ConfigFile)
	}
	args = append(args,
		"--datadir="+o.DataDir,
		"--socket="+o.Socket,
		"--skip-networking",
	)

	sup := NewProcessSupervisor(o.MysqldPath, args, WithShutdownTimeout(o.ReadyTimeout))
	if err := sup.Start(ctx); err != nil {
		return fmt.Errorf("starting temporary server: %w", err)
	}
	defer func() { _ = sup.Shutdown(ctx) }()

	db, err := waitForSocket(ctx, o.Socket, o.ReadyTimeout)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	stmts, err := BootstrapStatements(o.Bootstrap)
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("bootstrap statement failed: %w", err)
		}
	}
	return nil
}

// waitForSocket opens a passwordless root connection over the socket, retrying
// until the server is ready or the timeout elapses.
func waitForSocket(ctx context.Context, socket string, timeout time.Duration) (*sql.DB, error) {
	cfg := pool.Config{Socket: socket, User: "root"}
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
