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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/sqldump"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// LoadConfig configures the loads an instance serves on POST /cluster/load
// (a LogicalRestore into a running cluster). The load runs as the control
// account: loading definers through the binlog needs SUPER, so no narrower
// account can do it (design 029 §2.1).
type LoadConfig struct {
	// Engine selects the SQL client and its arguments.
	Engine engine.Engine
	// Socket is the local mysqld socket the client connects through.
	Socket string
	// User and Password are the control account's credentials.
	User     string
	Password string
	// WorkDir holds the per-load credentials file (default os.TempDir()).
	WorkDir string
	// LoadPath overrides the SQL client. Empty selects the engine's client.
	LoadPath string
}

// SetLoadConfig enables POST /cluster/load on the controller.
func (c *Controller) SetLoadConfig(cfg LoadConfig) {
	if cfg.WorkDir == "" {
		cfg.WorkDir = os.TempDir()
	}
	c.load = &cfg
}

// maxDatabaseNameChars is the server's limit on a schema name, the same bound
// the LogicalRestore CRD sets: the instance checks it too, as the authority on
// what it loads.
const maxDatabaseNameChars = 64

// maxLoadStderrTailBytes bounds the SQL client output kept for the error of a
// failed load.
const maxLoadStderrTailBytes = 8 << 10

// StartLoad checks that a load can run, applies its policy and starts the SQL
// client. Every refusal happens before anything is changed: a missing client,
// a load already running, a read-only instance, a bad request, or (with
// FailIfExists) a selected database that holds objects. DropAndRecreate drops
// the selected databases here, before the stream is read.
func (c *Controller) StartLoad(ctx context.Context, req webserver.LoadRequest) (webserver.LoadSession, error) {
	if c.load == nil || c.load.Engine == nil {
		return nil, errors.New("loads are not configured on this instance")
	}
	cfg := c.load
	databases, err := validateLoadRequest(req)
	if err != nil {
		return nil, err
	}

	binary := cfg.LoadPath
	if binary == "" {
		binary = cfg.Engine.Logical().LoadBinary()
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return nil, fmt.Errorf("%w: %s is not in this instance image", webserver.ErrLoadToolUnavailable, binary)
	}
	if cfg.User == "" || cfg.Password == "" {
		return nil, errors.New("load: the control account credentials are not configured")
	}
	if strings.ContainsAny(cfg.Password, "\n\r") {
		return nil, errors.New("load: the control account password cannot contain a line break")
	}

	if !c.loadRunning.CompareAndSwap(false, true) {
		return nil, webserver.ErrLoadInProgress
	}
	tool := filepath.Base(binary)
	log := logf.FromContext(ctx).WithName(tool).WithValues("instance", c.name)
	session := &loadSession{controller: c, tool: tool, databases: databases, log: log}
	started := false
	defer func() {
		if !started {
			session.Close()
		}
	}()

	if err := c.checkWritable(ctx); err != nil {
		return nil, err
	}

	// Everything that can fail is prepared before the policy runs: once
	// DropAndRecreate has dropped a database, only starting the client is left.
	session.dir, err = os.MkdirTemp(cfg.WorkDir, "cnmsql-load-")
	if err != nil {
		return nil, fmt.Errorf("load: creating work directory: %w", err)
	}
	defaults := filepath.Join(session.dir, "client.cnf")
	if err := os.WriteFile(defaults, clientDefaultsFile(cfg.User, cfg.Password, cfg.Socket), 0o600); err != nil {
		return nil, fmt.Errorf("load: writing credentials file: %w", err)
	}

	session.stderr = newTailWriter(maxLoadStderrTailBytes)
	session.cmd = exec.CommandContext(ctx, path, cfg.Engine.Logical().LoadArgs(defaults)...)
	session.cmd.Stdout = newProcessLogWriter(log, "stdout")
	session.cmd.Stderr = io.MultiWriter(newProcessLogWriter(log, "stderr"), session.stderr)
	stdin, err := session.cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	if err := c.applyLoadPolicy(ctx, req.Policy, databases); err != nil {
		return nil, err
	}
	log.Info("Starting logical load", "databases", databases, "policy", req.Policy)
	if err := session.cmd.Start(); err != nil {
		return nil, fmt.Errorf("load: starting %s: %w", tool, err)
	}
	session.running = true
	session.stdin = stdin
	started = true
	return session, nil
}

// validateLoadRequest checks the request and returns its databases without
// duplicates, in request order.
func validateLoadRequest(req webserver.LoadRequest) ([]string, error) {
	switch req.Policy {
	case webserver.LoadPolicyFailIfExists, webserver.LoadPolicyDropAndRecreate:
	default:
		return nil, fmt.Errorf("%w: unknown policy %q", webserver.ErrInvalidLoadRequest, req.Policy)
	}
	if len(req.Databases) == 0 {
		return nil, fmt.Errorf("%w: no database selected", webserver.ErrInvalidLoadRequest)
	}
	var out []string
	for _, db := range req.Databases {
		switch {
		case db == "" || utf8.RuneCountInString(db) > maxDatabaseNameChars || !utf8.ValidString(db):
			return nil, fmt.Errorf("%w: %q is not a valid database name (1 to %d characters)",
				webserver.ErrInvalidLoadRequest, db, maxDatabaseNameChars)
		case isExcludedSchema(db):
			return nil, fmt.Errorf("%w: %q is a system or operator schema and cannot be loaded",
				webserver.ErrInvalidLoadRequest, db)
		}
		if !slices.Contains(out, db) {
			out = append(out, db)
		}
	}
	return out, nil
}

// checkWritable refuses a read-only instance: an async replica, a Group
// Replication secondary or a demoted primary. The replication reader parses
// both spellings of the flag (MariaDB 12 reports read_only as OFF/ON).
func (c *Controller) checkWritable(ctx context.Context) error {
	state, err := c.repl.ReadOnly(ctx)
	if err != nil {
		return fmt.Errorf("load: checking read_only: %w", err)
	}
	if state.ReadOnly || state.SuperReadOnly {
		return fmt.Errorf("%w: %s is read-only", webserver.ErrNotPrimary, c.name)
	}
	return nil
}

// applyLoadPolicy refuses non-empty databases (FailIfExists) or drops the
// selected databases (DropAndRecreate).
func (c *Controller) applyLoadPolicy(ctx context.Context, policy string, databases []string) error {
	if policy == webserver.LoadPolicyDropAndRecreate {
		log := logf.FromContext(ctx).WithValues("instance", c.name)
		for _, db := range databases {
			if _, err := c.conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(db)); err != nil {
				return fmt.Errorf("load: dropping database %q: %w", db, err)
			}
			log.Info("Dropped database before the load", "database", db)
		}
		return nil
	}
	nonEmpty, err := c.nonEmptyDatabases(ctx, databases)
	if err != nil {
		return err
	}
	if len(nonEmpty) > 0 {
		return fmt.Errorf("%w: %s already hold tables, views, routines or events; "+
			"use the DropAndRecreate policy to replace them", webserver.ErrDatabaseNotEmpty,
			strings.Join(nonEmpty, ", "))
	}
	return nil
}

// nonEmptyDatabases returns the databases that exist and hold a table, a view,
// a routine or an event. Triggers belong to tables.
func (c *Controller) nonEmptyDatabases(ctx context.Context, databases []string) ([]string, error) {
	var out []string
	for _, db := range databases {
		var n int
		if err := c.conn.QueryRowContext(ctx,
			"SELECT (SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?) + "+
				"(SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?) + "+
				"(SELECT COUNT(*) FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ?)",
			db, db, db,
		).Scan(&n); err != nil {
			return nil, fmt.Errorf("load: inspecting database %q: %w", db, err)
		}
		if n > 0 {
			out = append(out, db)
		}
	}
	return out, nil
}

type loadSession struct {
	controller *Controller
	tool       string
	databases  []string
	log        logr.Logger
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stderr     *tailWriter
	dir        string
	running    bool
	closeOnce  sync.Once
}

// Load streams r through the database filter into the SQL client and waits for
// it. A read error kills the client rather than closing its input: at end of
// input the client would run the statement it was cut in the middle of.
func (s *loadSession) Load(_ context.Context, r io.Reader) (webserver.LoadResult, error) {
	started := time.Now()
	body := &countingReader{r: r}
	in := &stickyErrWriter{w: s.stdin}
	kept, filterErr := sqldump.FilterDatabases(in, body, s.databases)
	if filterErr != nil {
		if in.err != nil {
			// The client stopped reading: it exited on an SQL error, which its
			// own output explains better than the broken pipe.
			_ = s.stdin.Close()
			return webserver.LoadResult{}, s.clientFailed(s.wait())
		}
		s.kill()
		return webserver.LoadResult{}, fmt.Errorf("load: reading the stream after %d bytes: %w", body.n, filterErr)
	}
	if err := s.stdin.Close(); err != nil {
		// The client went away as its input ended; its exit status and
		// output say why.
		return webserver.LoadResult{}, s.clientFailed(errors.Join(err, s.wait()))
	}
	if err := s.wait(); err != nil {
		return webserver.LoadResult{}, s.clientFailed(err)
	}
	s.removeDir()

	var missing []string
	for _, db := range s.databases {
		if !slices.Contains(kept, db) {
			missing = append(missing, db)
		}
	}
	if len(missing) > 0 {
		return webserver.LoadResult{}, fmt.Errorf("load: the stream holds no section for %s",
			strings.Join(missing, ", "))
	}
	s.log.Info("Logical load finished", "databases", kept, "bytes", body.n,
		"duration", time.Since(started).Round(time.Second).String())
	return webserver.LoadResult{Databases: kept, Bytes: body.n}, nil
}

func (s *loadSession) clientFailed(waitErr error) error {
	return fmt.Errorf("load: %s failed: %v: %s", s.tool, waitErr, strings.TrimSpace(s.stderr.String()))
}

// Close stops a client that is still running, removes the credentials, and
// frees the instance's load slot.
func (s *loadSession) Close() {
	s.closeOnce.Do(func() {
		s.kill()
		s.removeDir()
		s.controller.loadRunning.Store(false)
	})
}

func (s *loadSession) kill() {
	if s.running && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_ = s.wait()
	}
}

func (s *loadSession) wait() error {
	if !s.running {
		return nil
	}
	s.running = false
	return s.cmd.Wait()
}

func (s *loadSession) removeDir() {
	if s.dir != "" {
		_ = os.RemoveAll(s.dir)
		s.dir = ""
	}
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Ensure Controller advertises the optional logical load capability.
var _ webserver.LoadStreamer = (*Controller)(nil)
