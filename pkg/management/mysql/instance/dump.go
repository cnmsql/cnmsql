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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/heartbeat"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// DumpConfig configures the logical dumps an instance serves on
// POST /cluster/dump. It needs no flag of its own: everything comes from what
// the instance manager already knows, so enabling it never changes the Pod
// spec.
type DumpConfig struct {
	// Engine selects the dump client and its arguments.
	Engine engine.Engine
	// Socket is the local mysqld socket the dump connects through.
	Socket string
	// WorkDir holds the per-dump credentials file (default os.TempDir()).
	WorkDir string
	// DumpPath overrides the dump binary. Empty selects the engine's tool.
	DumpPath string
	// HeartbeatSchema is the replication-lag heartbeat schema to exclude from
	// dumps. Empty takes heartbeat.DefaultSchema.
	HeartbeatSchema string
}

// SetDumpConfig enables POST /cluster/dump on the controller.
func (c *Controller) SetDumpConfig(cfg DumpConfig) {
	if cfg.WorkDir == "" {
		cfg.WorkDir = os.TempDir()
	}
	if cfg.HeartbeatSchema == "" {
		cfg.HeartbeatSchema = heartbeat.DefaultSchema
	}
	c.dump = &cfg
}

// dumpExcludedSchemas are the server's system schemas, which are never dumped.
var dumpExcludedSchemas = map[string]struct{}{
	"mysql":              {},
	"sys":                {},
	"performance_schema": {},
	"information_schema": {},
}

// isDumpExcludedSchema reports whether a schema is never dumped: a server
// system schema, or the operator-owned heartbeat schema of this instance.
func (c *Controller) isDumpExcludedSchema(name string) bool {
	if _, ok := dumpExcludedSchemas[strings.ToLower(name)]; ok {
		return true
	}
	return strings.EqualFold(name, c.dump.HeartbeatSchema)
}

const (
	// maxDumpStderrTailBytes bounds the dump client's stderr kept for the error
	// message of a failed dump.
	maxDumpStderrTailBytes = 8 << 10
	// dumpReadBufferBytes is the line buffer of the stdout scan. Lines longer
	// than this (large extended INSERTs) pass through in pieces.
	dumpReadBufferBytes = 256 << 10
	// maxDumpCommentLineBytes and dumpCommentLines bound the comment lines kept
	// for the snapshot position: the first and the last few, which is where both
	// clients write it.
	maxDumpCommentLineBytes = 1 << 10
	dumpCommentLines        = 64
)

// StartDump checks that a dump can run, starts the dump client, and waits for
// its first output. Every failure up to that point is returned here, so the
// HTTP layer can still answer with a real status: a missing tool, a dump
// already running, a missing account, a bad database list, or a client that
// exits before writing anything (wrong password, unknown extra argument).
func (c *Controller) StartDump(ctx context.Context, req webserver.DumpRequest) (webserver.DumpSession, error) {
	if c.dump == nil {
		return nil, errors.New("logical dumps are not configured on this instance")
	}
	cfg := c.dump
	logical := cfg.Engine.Logical()

	binary := cfg.DumpPath
	if binary == "" {
		binary = logical.DumpBinary()
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return nil, fmt.Errorf("%w: %s is not in this instance image; move the cluster to an image tag "+
			"that ships it (see the logical backups documentation)", webserver.ErrDumpToolUnavailable, binary)
	}
	if strings.ContainsAny(req.Password, "\n\r") {
		return nil, fmt.Errorf("%w: the dump account password cannot contain a line break",
			webserver.ErrInvalidDumpRequest)
	}

	if !c.dumpRunning.CompareAndSwap(false, true) {
		return nil, webserver.ErrDumpInProgress
	}
	session := &dumpSession{controller: c}
	started := false
	defer func() {
		if !started {
			session.Close()
		}
	}()

	if err := c.checkDumpAccount(ctx); err != nil {
		return nil, err
	}
	databases, err := c.resolveDumpDatabases(ctx, req.Databases)
	if err != nil {
		return nil, err
	}

	session.dir, err = os.MkdirTemp(cfg.WorkDir, "cnmsql-dump-")
	if err != nil {
		return nil, fmt.Errorf("dump: creating work directory: %w", err)
	}
	defaults := filepath.Join(session.dir, "client.cnf")
	if err := os.WriteFile(defaults, dumpDefaultsFile(req.Password, cfg.Socket), 0o600); err != nil {
		return nil, fmt.Errorf("dump: writing credentials file: %w", err)
	}

	args, err := logical.DumpArgs(engine.DumpOpts{
		DefaultsFile:  defaults,
		Databases:     databases,
		ExtraArgs:     req.ExtraArgs,
		ServerVersion: c.version,
	})
	if err != nil {
		return nil, err
	}

	tool := filepath.Base(binary)
	log := logf.FromContext(ctx).WithName(tool).WithValues("instance", c.name)
	session.logical = logical
	session.tool = tool
	session.info = webserver.DumpInfo{
		Tool:          tool,
		Flavor:        string(cfg.Engine.Flavor()),
		ServerVersion: c.versionStr,
		Databases:     databases,
	}
	session.stderr = newTailWriter(maxDumpStderrTailBytes)
	session.cmd = exec.CommandContext(ctx, path, args...)
	session.cmd.Stderr = io.MultiWriter(newProcessLogWriter(log, "stderr"), session.stderr)
	stdout, err := session.cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	log.Info("Starting logical dump", "databases", databases)
	if err := session.cmd.Start(); err != nil {
		return nil, fmt.Errorf("dump: starting %s: %w", tool, err)
	}
	session.running = true
	session.out = bufio.NewReaderSize(stdout, dumpReadBufferBytes)

	// The client writes its header right after it connects, and it has read the
	// credentials file by then, so the file can go as soon as output appears.
	if _, err := session.out.Peek(1); err != nil {
		waitErr := session.wait()
		return nil, fmt.Errorf("dump: %s exited before writing any output: %v: %s",
			tool, errors.Join(waitErr, ignoreEOF(err)), strings.TrimSpace(session.stderr.String()))
	}
	session.removeDir()
	started = true
	return session, nil
}

// checkDumpAccount fails with ErrDumpAccountMissing when cnmsql_dump@localhost
// does not exist here yet, which happens on a replica that has not applied the
// operator's CREATE USER.
func (c *Controller) checkDumpAccount(ctx context.Context) error {
	var n int
	if err := c.conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = ?",
		engine.DumpAccountName, engine.DumpAccountHost,
	).Scan(&n); err != nil {
		return fmt.Errorf("dump: checking the dump account: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %s@%s does not exist on this instance yet",
			webserver.ErrDumpAccountMissing, engine.DumpAccountName, engine.DumpAccountHost)
	}
	return nil
}

// resolveDumpDatabases turns the request into an explicit, sorted schema list.
// An empty request means every application schema; a named schema must exist
// and must not be a system or operator schema.
func (c *Controller) resolveDumpDatabases(ctx context.Context, requested []string) ([]string, error) {
	rows, err := c.conn.QueryContext(ctx, "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA")
	if err != nil {
		return nil, fmt.Errorf("dump: listing schemas: %w", err)
	}
	defer func() { _ = rows.Close() }()
	existing := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("dump: scanning schema: %w", err)
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dump: listing schemas: %w", err)
	}

	var out []string
	if len(requested) == 0 {
		for name := range existing {
			if !c.isDumpExcludedSchema(name) {
				out = append(out, name)
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("%w: the instance has no application database to dump",
				webserver.ErrInvalidDumpRequest)
		}
	} else {
		for _, name := range requested {
			if c.isDumpExcludedSchema(name) {
				return nil, fmt.Errorf("%w: %q is a system or operator schema and cannot be dumped",
					webserver.ErrInvalidDumpRequest, name)
			}
			if _, ok := existing[name]; !ok {
				return nil, fmt.Errorf("%w: database %q does not exist", webserver.ErrInvalidDumpRequest, name)
			}
			if !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}

// dumpDefaultsFile renders the option file the dump client reads its
// credentials from. Values are double-quoted, where the option-file parser
// honours backslash escapes.
func dumpDefaultsFile(password, socket string) []byte {
	quote := func(v string) string {
		return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(v) + `"`
	}
	var b bytes.Buffer
	b.WriteString("[client]\n")
	b.WriteString("user=" + quote(engine.DumpAccountName) + "\n")
	b.WriteString("password=" + quote(password) + "\n")
	if socket != "" {
		b.WriteString("socket=" + quote(socket) + "\n")
	}
	return b.Bytes()
}

type dumpSession struct {
	controller *Controller
	logical    engine.LogicalTool
	tool       string
	info       webserver.DumpInfo
	cmd        *exec.Cmd
	out        *bufio.Reader
	stderr     *tailWriter
	dir        string
	running    bool
	closeOnce  sync.Once
}

func (s *dumpSession) Info() webserver.DumpInfo { return s.info }

// Stream copies the dump to w and waits for the client to exit. It keeps the
// comment lines the snapshot position is parsed from as they pass: a dump line
// that starts with "-- " is always a comment, because the clients escape line
// breaks inside values.
func (s *dumpSession) Stream(ctx context.Context, w io.Writer) (webserver.DumpResult, error) {
	comments := newCommentCapture(dumpCommentLines)
	atLineStart := true
	for {
		chunk, readErr := s.out.ReadSlice('\n')
		if len(chunk) > 0 {
			if atLineStart && bytes.HasPrefix(chunk, []byte("-- ")) && len(chunk) <= maxDumpCommentLineBytes {
				comments.add(chunk)
			}
			atLineStart = chunk[len(chunk)-1] == '\n'
			if _, err := w.Write(chunk); err != nil {
				_ = s.wait()
				return webserver.DumpResult{}, fmt.Errorf("dump: writing the stream: %w", err)
			}
		}
		if readErr == nil || errors.Is(readErr, bufio.ErrBufferFull) {
			continue
		}
		if !errors.Is(readErr, io.EOF) {
			_ = s.wait()
			return webserver.DumpResult{}, fmt.Errorf("dump: reading %s output: %w", s.tool, readErr)
		}
		break
	}
	if err := s.wait(); err != nil {
		return webserver.DumpResult{}, fmt.Errorf("dump: %s failed: %w: %s",
			s.tool, err, strings.TrimSpace(s.stderr.String()))
	}

	log := logf.FromContext(ctx).WithValues("instance", s.controller.name)
	info, err := s.logical.ParseSnapshotPosition(comments.String())
	if err != nil {
		// The position is for reference only; the dump itself is complete.
		log.Info("Could not read the snapshot position from the dump", "error", err.Error())
		return webserver.DumpResult{}, nil
	}
	result := webserver.DumpResult{
		SnapshotGTID:   info.GTIDSet,
		SnapshotBinlog: info.File + ":" + strconv.FormatInt(info.Position, 10),
	}
	log.Info("Logical dump finished", "snapshotBinlog", result.SnapshotBinlog, "snapshotGTID", result.SnapshotGTID)
	return result, nil
}

// Close stops a client that is still running, removes the credentials, and
// frees the instance's dump slot.
func (s *dumpSession) Close() {
	s.closeOnce.Do(func() {
		if s.running && s.cmd.Process != nil {
			_ = s.cmd.Process.Kill()
			_ = s.wait()
		}
		s.removeDir()
		s.controller.dumpRunning.Store(false)
	})
}

func (s *dumpSession) wait() error {
	if !s.running {
		return nil
	}
	s.running = false
	return s.cmd.Wait()
}

func (s *dumpSession) removeDir() {
	if s.dir != "" {
		_ = os.RemoveAll(s.dir)
		s.dir = ""
	}
}

func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// commentCapture keeps the first and the last n comment lines of a dump.
type commentCapture struct {
	n     int
	first []string
	last  []string
}

func newCommentCapture(n int) *commentCapture { return &commentCapture{n: n} }

func (c *commentCapture) add(line []byte) {
	if len(c.first) < c.n {
		c.first = append(c.first, string(line))
		return
	}
	c.last = append(c.last, string(line))
	if len(c.last) > c.n {
		c.last = c.last[1:]
	}
}

func (c *commentCapture) String() string {
	var b strings.Builder
	for _, l := range append(slices.Clone(c.first), c.last...) {
		b.WriteString(l)
		if !strings.HasSuffix(l, "\n") {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// Ensure Controller advertises the optional logical dump capability.
var _ webserver.DumpStreamer = (*Controller)(nil)
