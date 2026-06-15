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

// waitDelay bounds how long cmd.Wait blocks for the child's stdout/stderr pipes
// to close after the process itself has exited, so a lingering grandchild
// holding the pipe cannot wedge shutdown.
const waitDelay = 10 * time.Second

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

	mu      sync.Mutex
	cmd     *exec.Cmd
	done    chan struct{} // closed once the process has exited
	exitErr error         // the process exit error, valid after done is closed
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
	// Bound cmd.Wait: once the process exits, do not block forever waiting for
	// the stdout/stderr pipes to close if a lingering child still holds them.
	cmd.WaitDelay = waitDelay

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("supervisor: starting %s: %w", s.binary, err)
	}

	done := make(chan struct{})
	s.cmd = cmd
	s.done = done
	s.exitErr = nil
	// A single Wait goroutine records the exit and broadcasts via done, so any
	// number of waiters (the run loop's watcher and the shutdown path) observe
	// the exit instead of racing for a single-delivery channel.
	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		s.exitErr = err
		s.mu.Unlock()
		close(done)
	}()

	return nil
}

// Wait blocks until the process exits and returns its exit error (nil on a
// clean exit). It returns an error if the process was never started.
func (s *ProcessSupervisor) Wait() error {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done == nil {
		return errors.New("supervisor: process not started")
	}
	<-done
	return normalizeExit(s.exit())
}

// exit returns the recorded process exit error. Callers must only read it after
// the done channel has been closed.
func (s *ProcessSupervisor) exit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exitErr
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

// Shutdown gracefully stops the process: delegates to ShutdownWithTimeout using
// the configured shutdown timeout.
func (s *ProcessSupervisor) Shutdown(_ context.Context) error {
	return s.ShutdownWithTimeout(s.shutdownTimeout)
}

// ShutdownWithTimeout gracefully stops the process: it sends SIGTERM and waits
// up to the given timeout before sending SIGKILL. An exit caused by these
// signals is not treated as an error.
func (s *ProcessSupervisor) ShutdownWithTimeout(timeout time.Duration) error {
	s.mu.Lock()
	cmd := s.cmd
	done := s.done
	s.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}

	_ = cmd.Process.Signal(syscall.SIGTERM)

	select {
	case <-done:
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
	}

	s.clear()
	return normalizeExit(s.exit())
}

// clear forgets the current process so a subsequent Start may run a fresh one.
func (s *ProcessSupervisor) clear() {
	s.mu.Lock()
	s.cmd = nil
	s.mu.Unlock()
}

// ShutdownGraceful stops the process in two phases. It sends SIGTERM and waits
// up to smartTimeout for a clean (innodb_fast_shutdown) exit; if mysqld is still
// running it keeps waiting until hardTimeout (measured from the SIGTERM) before
// forcing an immediate shutdown with SIGKILL — mysqld recovers via crash
// recovery on the next start. killed reports whether SIGKILL was needed. An exit
// caused by these signals is not treated as an error.
func (s *ProcessSupervisor) ShutdownGraceful(smartTimeout, hardTimeout time.Duration) (killed bool, err error) {
	s.mu.Lock()
	cmd := s.cmd
	done := s.done
	s.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return false, nil
	}

	if hardTimeout < smartTimeout {
		hardTimeout = smartTimeout
	}

	_ = cmd.Process.Signal(syscall.SIGTERM)

	// Phase 1: smart budget — wait for a clean shutdown.
	select {
	case <-done:
		s.clear()
		return false, normalizeExit(s.exit())
	case <-time.After(smartTimeout):
	}

	// Phase 2: smart budget exceeded — keep waiting for the clean shutdown to
	// finish, up to the hard stop delay, before forcing the process down.
	select {
	case <-done:
		s.clear()
		return false, normalizeExit(s.exit())
	case <-time.After(hardTimeout - smartTimeout):
	}

	// Phase 3: forced immediate shutdown.
	_ = cmd.Process.Kill()
	<-done
	s.clear()
	return true, normalizeExit(s.exit())
}

// normalizeExit treats an exit caused by our SIGTERM/SIGKILL as a clean stop.
func normalizeExit(err error) error {
	if isSignalExit(err) {
		return nil
	}
	return err
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
