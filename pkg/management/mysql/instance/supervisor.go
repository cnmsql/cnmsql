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
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// DefaultShutdownTimeout bounds a graceful mysqld shutdown before it is killed.
const DefaultShutdownTimeout = 30 * time.Second

// defaultMysqldBinary is the mysqld binary name assumed when none is configured.
const defaultMysqldBinary = "mysqld"

// defaultXtrabackupBinary is the xtrabackup binary name assumed when none is
// configured.
const defaultXtrabackupBinary = "xtrabackup"

// ProcessSupervisor runs and supervises a single child process (mysqld),
// forwarding its output and managing graceful shutdown and restart. It is the
// PID1 the instance pod runs.
type ProcessSupervisor struct {
	binary          string
	args            []string
	stdout          io.Writer
	stderr          io.Writer
	shutdownTimeout time.Duration

	mu     sync.Mutex
	cmd    *exec.Cmd
	waitCh chan error
}

// Option customises a ProcessSupervisor.
type Option func(*ProcessSupervisor)

// WithOutput sets the writers the child's stdout and stderr are forwarded to.
func WithOutput(stdout, stderr io.Writer) Option {
	return func(s *ProcessSupervisor) {
		s.stdout = stdout
		s.stderr = stderr
	}
}

// WithShutdownTimeout sets the graceful shutdown timeout.
func WithShutdownTimeout(d time.Duration) Option {
	return func(s *ProcessSupervisor) { s.shutdownTimeout = d }
}

// NewProcessSupervisor builds a supervisor for the given binary and arguments.
func NewProcessSupervisor(binary string, args []string, opts ...Option) *ProcessSupervisor {
	s := &ProcessSupervisor{
		binary:          binary,
		args:            args,
		stdout:          os.Stdout,
		stderr:          os.Stderr,
		shutdownTimeout: DefaultShutdownTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start launches the process. It returns an error if it is already running or
// fails to start.
func (s *ProcessSupervisor) Start(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil {
		return errors.New("supervisor: process already running")
	}

	cmd := exec.Command(s.binary, s.args...)
	cmd.Stdout = s.stdout
	cmd.Stderr = s.stderr
	// Run the child in its own process group so signals can be targeted.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("supervisor: starting %s: %w", s.binary, err)
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	s.cmd = cmd
	s.waitCh = waitCh
	return nil
}

// Wait blocks until the process exits and returns its exit error (nil on a
// clean exit). It returns an error if the process was never started.
func (s *ProcessSupervisor) Wait() error {
	s.mu.Lock()
	waitCh := s.waitCh
	s.mu.Unlock()
	if waitCh == nil {
		return errors.New("supervisor: process not started")
	}
	return <-waitCh
}

// Signal forwards a signal to the running process.
func (s *ProcessSupervisor) Signal(sig os.Signal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd == nil || s.cmd.Process == nil {
		return errors.New("supervisor: process not running")
	}
	return s.cmd.Process.Signal(sig)
}

// Shutdown gracefully stops the process: it sends SIGTERM (which mysqld handles
// as a clean shutdown) and waits up to the shutdown timeout before sending
// SIGKILL. An exit caused by these signals is not treated as an error.
func (s *ProcessSupervisor) Shutdown(_ context.Context) error {
	s.mu.Lock()
	cmd := s.cmd
	waitCh := s.waitCh
	timeout := s.shutdownTimeout
	s.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}

	_ = cmd.Process.Signal(syscall.SIGTERM)

	var waitErr error
	select {
	case waitErr = <-waitCh:
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		waitErr = <-waitCh
	}

	s.mu.Lock()
	s.cmd = nil
	s.waitCh = nil
	s.mu.Unlock()

	if isSignalExit(waitErr) {
		return nil
	}
	return waitErr
}

// Restart gracefully stops then starts the process. It implements the
// Supervisor interface consumed by the Controller.
func (s *ProcessSupervisor) Restart(ctx context.Context) error {
	if err := s.Shutdown(ctx); err != nil {
		return err
	}
	return s.Start(ctx)
}

// Running reports whether a process is currently supervised.
func (s *ProcessSupervisor) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cmd != nil
}

// isSignalExit reports whether the process exit was caused by SIGTERM or
// SIGKILL, which we consider an intentional shutdown rather than a failure.
func isSignalExit(err error) bool {
	if err == nil {
		return false
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		return false
	}
	if !status.Signaled() {
		return false
	}
	switch status.Signal() {
	case syscall.SIGTERM, syscall.SIGKILL:
		return true
	default:
		return false
	}
}
