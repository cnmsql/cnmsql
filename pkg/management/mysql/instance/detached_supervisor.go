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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// DetachedSupervisor supervises the long-lived mysqld out-of-band: by PID rather
// than through an exec.Cmd handle, with output wired to inherited *os.File
// descriptors instead of pipes. Both properties let the manager re-exec itself
// (in-place upgrade) without disturbing mysqld: the file descriptors survive
// execve, and a re-exec'd image re-adopts the running process from its pidfile
// via AdoptProcess. It satisfies the same method set runner.go and the
// Controller rely on. The short-lived bootstrap servers keep ProcessSupervisor.
type DetachedSupervisor struct {
	binary          string
	args            []string
	stdout          *os.File
	stderr          *os.File
	pidFilePath     string
	shutdownTimeout time.Duration
	fifoLog         *FifoLog

	mu      sync.Mutex
	pid     int           // the supervised process PID, 0 when not running
	done    chan struct{} // closed once the process has exited
	exitErr error         // normalized exit error, valid after done is closed
}

// DetachedOption customises a DetachedSupervisor.
type DetachedOption func(*DetachedSupervisor)

// WithFileOutput sets the inheritable files the child's stdout and stderr are
// wired to. They must be real *os.File so exec.Cmd passes the descriptor
// directly (no pipe), keeping the output path execve-safe.
func WithFileOutput(stdout, stderr *os.File) DetachedOption {
	return func(s *DetachedSupervisor) {
		s.stdout = stdout
		s.stderr = stderr
	}
}

// WithDetachedShutdownTimeout sets the graceful shutdown timeout.
func WithDetachedShutdownTimeout(d time.Duration) DetachedOption {
	return func(s *DetachedSupervisor) { s.shutdownTimeout = d }
}

// WithPIDFile sets the path the supervisor writes the running mysqld PID to so a
// re-exec'd manager image can find and adopt it.
func WithPIDFile(path string) DetachedOption {
	return func(s *DetachedSupervisor) { s.pidFilePath = path }
}

// WithFIFO wires the supervisor's child output through a named FIFO whose read
// end survives execve (CLOEXEC cleared), so structured log wrapping is restored
// after a manager re-exec. The FifoLog must have been started before Start is
// called.
func WithFIFO(fl *FifoLog) DetachedOption {
	return func(s *DetachedSupervisor) {
		s.fifoLog = fl
		s.stdout = fl.WriteEnd()
		s.stderr = fl.WriteEnd()
	}
}

// NewDetachedSupervisor builds a detached supervisor for the given binary and
// arguments.
func NewDetachedSupervisor(binary string, args []string, opts ...DetachedOption) *DetachedSupervisor {
	s := &DetachedSupervisor{
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

// Start launches the process with inherited output descriptors, records its PID
// (and writes the pidfile), and supervises it via cmd.Wait. If a previous process
// has exited its leftover state is cleared first so the supervisor can restart.
func (s *DetachedSupervisor) Start(_ context.Context) error {
	s.mu.Lock()
	done := s.done
	if done != nil {
		select {
		case <-done:
			// Previous process exited; clear state (pidfile, fifo) outside the lock.
			s.mu.Unlock()
			s.clear()
		default:
			s.mu.Unlock()
			return errors.New("supervisor: process already running")
		}
	} else {
		s.mu.Unlock()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cmd := exec.Command(s.binary, s.args...)
	// Inherited *os.File targets: exec.Cmd passes the descriptor directly, so
	// there is no pipe to break when the manager later re-execs.
	cmd.Stdout = s.stdout
	cmd.Stderr = s.stderr
	// Own process group, so signals can be targeted and mysqld is insulated from
	// signals delivered to the manager's group.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("supervisor: starting %s: %w", s.binary, err)
	}

	pid := cmd.Process.Pid
	if err := s.writePIDFile(pid); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return fmt.Errorf("supervisor: writing pidfile: %w", err)
	}

	done = make(chan struct{})
	s.pid = pid
	s.done = done
	s.exitErr = nil
	// A single reaper goroutine records the exit and broadcasts via done so any
	// number of waiters observe it.
	go func() {
		err := cmd.Wait()
		s.finish(classifyCmdWait(err), done)
	}()

	return nil
}

// AdoptProcess supervises an already-running process by PID, started by a
// previous manager image before an in-place re-exec. It reaps the process via a
// raw syscall.Wait4 loop (exec.Cmd cannot wrap a process it did not start),
// feeding the same done/exitErr machinery as Start so Wait/Signal/Shutdown work
// identically for an adopted process.
func (s *DetachedSupervisor) AdoptProcess(pid int) error {
	s.mu.Lock()
	done := s.done
	if done != nil {
		select {
		case <-done:
			s.mu.Unlock()
			s.clear()
		default:
			s.mu.Unlock()
			return errors.New("supervisor: process already running")
		}
	} else {
		s.mu.Unlock()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if pid <= 0 {
		return fmt.Errorf("supervisor: invalid pid to adopt: %d", pid)
	}
	// The process must exist and be our child for Wait4 to reap it.
	if err := syscall.Kill(pid, 0); err != nil {
		return fmt.Errorf("supervisor: adopting pid %d: %w", pid, err)
	}

	if err := s.writePIDFile(pid); err != nil {
		return fmt.Errorf("supervisor: writing pidfile: %w", err)
	}

	done = make(chan struct{})
	s.pid = pid
	s.done = done
	s.exitErr = nil
	go func() {
		var ws syscall.WaitStatus
		for {
			wpid, err := syscall.Wait4(pid, &ws, 0, nil)
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if err != nil {
				s.finish(fmt.Errorf("supervisor: Wait4 on adopted pid %d: %w", pid, err), done)
				return
			}
			if wpid == pid {
				s.finish(classifyWaitStatus(ws), done)
				return
			}
		}
	}()

	return nil
}

// finish records the normalized exit error and closes done exactly once.
func (s *DetachedSupervisor) finish(exitErr error, done chan struct{}) {
	s.mu.Lock()
	s.exitErr = exitErr
	s.mu.Unlock()
	close(done)
}

// Wait blocks until the process exits and returns its exit error (nil on a clean
// exit or a stop caused by our SIGTERM/SIGKILL). It errors if nothing is running.
func (s *DetachedSupervisor) Wait() error {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done == nil {
		return errors.New("supervisor: process not started")
	}
	<-done
	return s.exit()
}

// exit returns the recorded exit error. Callers must only read it after done is
// closed.
func (s *DetachedSupervisor) exit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exitErr
}

// Pid returns the supervised process PID, or 0 when nothing is running. It lets
// the in-place upgrade path hand the PID to the re-exec'd image.
func (s *DetachedSupervisor) Pid() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pid
}

// Signal forwards a signal to the running process by PID.
func (s *DetachedSupervisor) Signal(sig os.Signal) error {
	s.mu.Lock()
	pid := s.pid
	running := s.done != nil
	s.mu.Unlock()
	if !running || pid <= 0 {
		return errors.New("supervisor: process not running")
	}
	sysSig, ok := sig.(syscall.Signal)
	if !ok {
		return fmt.Errorf("supervisor: unsupported signal type %T", sig)
	}
	return syscall.Kill(pid, sysSig)
}

// Shutdown gracefully stops the process using the configured shutdown timeout.
func (s *DetachedSupervisor) Shutdown(_ context.Context) error {
	return s.ShutdownWithTimeout(s.shutdownTimeout)
}

// ShutdownWithTimeout sends SIGTERM and waits up to timeout before SIGKILL. An
// exit caused by these signals is not treated as an error.
func (s *DetachedSupervisor) ShutdownWithTimeout(timeout time.Duration) error {
	s.mu.Lock()
	pid := s.pid
	done := s.done
	s.mu.Unlock()

	if done == nil || pid <= 0 {
		return nil
	}

	_ = syscall.Kill(pid, syscall.SIGTERM)

	select {
	case <-done:
	case <-time.After(timeout):
		_ = syscall.Kill(pid, syscall.SIGKILL)
		<-done
	}

	err := s.exit()
	s.clear()
	return err
}

// ShutdownGraceful stops the process in two phases: SIGTERM, then wait up to
// smartTimeout for a clean exit; if still running, keep waiting until hardTimeout
// (from the SIGTERM) before forcing SIGKILL. killed reports whether SIGKILL was
// needed. An exit caused by these signals is not treated as an error.
func (s *DetachedSupervisor) ShutdownGraceful(smartTimeout, hardTimeout time.Duration) (killed bool, err error) {
	s.mu.Lock()
	pid := s.pid
	done := s.done
	s.mu.Unlock()

	if done == nil || pid <= 0 {
		return false, nil
	}

	if hardTimeout < smartTimeout {
		hardTimeout = smartTimeout
	}

	_ = syscall.Kill(pid, syscall.SIGTERM)

	// Phase 1: smart budget — wait for a clean shutdown.
	select {
	case <-done:
		err := s.exit()
		s.clear()
		return false, err
	case <-time.After(smartTimeout):
	}

	// Phase 2: keep waiting for the clean shutdown up to the hard stop delay.
	select {
	case <-done:
		err := s.exit()
		s.clear()
		return false, err
	case <-time.After(hardTimeout - smartTimeout):
	}

	// Phase 3: forced immediate shutdown.
	_ = syscall.Kill(pid, syscall.SIGKILL)
	<-done
	err = s.exit()
	s.clear()
	return true, err
}

// Restart gracefully stops then starts the process.
func (s *DetachedSupervisor) Restart(ctx context.Context) error {
	if err := s.Shutdown(ctx); err != nil {
		return err
	}
	return s.Start(ctx)
}

// Running reports whether a process is currently supervised.
func (s *DetachedSupervisor) Running() bool {
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done == nil {
		return false
	}
	select {
	case <-done:
		return false
	default:
		return true
	}
}

// clear forgets the current process (and removes the pidfile) so a subsequent
// Start may run a fresh one.
func (s *DetachedSupervisor) clear() {
	s.mu.Lock()
	s.pid = 0
	s.done = nil
	path := s.pidFilePath
	s.mu.Unlock()
	if path != "" {
		_ = os.Remove(path)
	}
	if s.fifoLog != nil {
		s.fifoLog.Close()
	}
}

// writePIDFile atomically records pid at pidFilePath, along with FIFO metadata
// for re-adoption after a manager re-exec. A no-op when no path is set.
func (s *DetachedSupervisor) writePIDFile(pid int) error {
	if s.pidFilePath == "" {
		return nil
	}
	content := strconv.Itoa(pid) + "\n"
	if s.fifoLog != nil {
		content += fmt.Sprintf("fifo=%s\nfd=%d\n", s.fifoLog.Path(), s.fifoLog.ReadFD())
	}
	tmp := s.pidFilePath + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.pidFilePath)
}

// classifyCmdWait normalizes a cmd.Wait error into the same form as the Wait4
// path: nil for a clean exit or a stop caused by our SIGTERM/SIGKILL.
func classifyCmdWait(err error) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			return classifyWaitStatus(ws)
		}
	}
	return err
}

// classifyWaitStatus maps a reaped wait status to an error: nil for a clean exit
// (status 0) or a stop caused by our SIGTERM/SIGKILL; otherwise a descriptive
// error so an unexpected mysqld crash is fatal to the run loop.
func classifyWaitStatus(ws syscall.WaitStatus) error {
	switch {
	case ws.Exited() && ws.ExitStatus() == 0:
		return nil
	case ws.Signaled() && (ws.Signal() == syscall.SIGTERM || ws.Signal() == syscall.SIGKILL):
		return nil
	case ws.Exited():
		return fmt.Errorf("process exited with status %d", ws.ExitStatus())
	case ws.Signaled():
		return fmt.Errorf("process killed by signal %v", ws.Signal())
	default:
		return fmt.Errorf("process ended with wait status %#x", ws)
	}
}
