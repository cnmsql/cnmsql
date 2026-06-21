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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
)

// DetachedSupervisor must satisfy the Supervisor interface used by Controller.
var _ Supervisor = (*DetachedSupervisor)(nil)

// openOutFile creates a temp file usable as an inherited output descriptor.
func openOutFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestDetachedStartWritesInheritedOutput(t *testing.T) {
	out := openOutFile(t)
	s := NewDetachedSupervisor("/bin/sh", []string{"-c", "echo hello"}, WithFileOutput(out, out))

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	data, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if got := string(data); got != "hello\n" {
		t.Errorf("output = %q, want %q", got, "hello\n")
	}
}

func TestDetachedWritesAndRemovesPIDFile(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "mysqld.pid")
	s := NewDetachedSupervisor("/bin/sleep", []string{"60"},
		WithFileOutput(openOutFile(t), openOutFile(t)),
		WithPIDFile(pidFile),
		WithDetachedShutdownTimeout(2*time.Second))

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("pidfile not written: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("pidfile content %q not a pid: %v", string(data), err)
	}
	if pid != s.Pid() {
		t.Errorf("pidfile pid = %d, want %d", pid, s.Pid())
	}

	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Errorf("expected pidfile removed after shutdown, stat err = %v", err)
	}
}

func TestDetachedDoubleStartFails(t *testing.T) {
	s := NewDetachedSupervisor("/bin/sleep", []string{"30"}, WithFileOutput(openOutFile(t), openOutFile(t)))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })

	if err := s.Start(context.Background()); err == nil {
		t.Error("expected second Start to fail")
	}
}

func TestDetachedGracefulShutdown(t *testing.T) {
	s := NewDetachedSupervisor("/bin/sleep", []string{"60"},
		WithFileOutput(openOutFile(t), openOutFile(t)),
		WithDetachedShutdownTimeout(2*time.Second))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !s.Running() {
		t.Fatal("expected Running() true after Start")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if s.Running() {
		t.Error("expected Running() false after Shutdown")
	}
}

func TestDetachedKillsAfterTimeout(t *testing.T) {
	s := NewDetachedSupervisor("/bin/sh", []string{"-c", "trap '' TERM; sleep 60"},
		WithFileOutput(openOutFile(t), openOutFile(t)),
		WithDetachedShutdownTimeout(500*time.Millisecond))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let the trap install

	start := time.Now()
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("shutdown took too long: %s", elapsed)
	}
}

func TestDetachedShutdownGracefulForcesKill(t *testing.T) {
	s := NewDetachedSupervisor("/bin/sh", []string{"-c", "trap '' TERM; while true; do sleep 1; done"},
		WithFileOutput(openOutFile(t), openOutFile(t)))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	killed, err := s.ShutdownGraceful(200*time.Millisecond, 600*time.Millisecond)
	if err != nil {
		t.Fatalf("ShutdownGraceful: %v", err)
	}
	if !killed {
		t.Error("expected the process to be force-killed")
	}
	if elapsed := time.Since(start); elapsed < 600*time.Millisecond {
		t.Errorf("kill happened too early at %s; should wait the hard stop delay", elapsed)
	}
}

func TestDetachedRestart(t *testing.T) {
	s := NewDetachedSupervisor("/bin/sleep", []string{"60"},
		WithFileOutput(openOutFile(t), openOutFile(t)),
		WithDetachedShutdownTimeout(2*time.Second))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })

	if err := s.Restart(context.Background()); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !s.Running() {
		t.Error("expected Running() true after Restart")
	}
}

func TestDetachedShutdownWhenNotRunning(t *testing.T) {
	s := NewDetachedSupervisor("/bin/sleep", []string{"1"})
	if err := s.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on non-running supervisor should be a no-op, got %v", err)
	}
}

func TestDetachedWaitBeforeStart(t *testing.T) {
	s := NewDetachedSupervisor("/bin/sleep", []string{"1"})
	if err := s.Wait(); err == nil {
		t.Error("expected Wait before Start to error")
	}
}

// TestDetachedAdoptAndReap mirrors the in-place upgrade spike: a process started
// by someone else (standing in for mysqld surviving a manager re-exec) is
// adopted by PID and supervised — signalled and reaped via Wait4 — exactly as a
// re-exec'd manager image would do.
func TestDetachedAdoptAndReap(t *testing.T) {
	// Start a child of THIS process without Wait()ing it, so AdoptProcess's
	// Wait4 is the one that reaps it (as it would be for a re-adopted mysqld).
	child := exec.Command("/bin/sleep", "60")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.Start(); err != nil {
		t.Fatalf("starting child: %v", err)
	}
	pid := child.Process.Pid

	pidFile := filepath.Join(t.TempDir(), "mysqld.pid")
	s := NewDetachedSupervisor("/bin/sleep", nil, WithPIDFile(pidFile))
	if err := s.AdoptProcess(pid); err != nil {
		t.Fatalf("AdoptProcess: %v", err)
	}
	if !s.Running() {
		t.Fatal("expected Running() true after AdoptProcess")
	}
	if s.Pid() != pid {
		t.Errorf("Pid() = %d, want %d", s.Pid(), pid)
	}

	// SIGTERM stops sleep; the adopt reaper treats it as a clean stop.
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown of adopted process: %v", err)
	}
	if s.Running() {
		t.Error("expected Running() false after Shutdown")
	}
}

func TestDetachedAdoptInvalidPID(t *testing.T) {
	s := NewDetachedSupervisor("/bin/sleep", nil)
	if err := s.AdoptProcess(0); err == nil {
		t.Error("expected AdoptProcess(0) to fail")
	}
}

func TestDetachedStartWithFIFOWritesAndWrapsOutput(t *testing.T) {
	fifoPath := filepath.Join(t.TempDir(), "mysqld-stdout.fifo")
	var logLines []string
	logger := funcr.NewJSON(func(obj string) {
		logLines = append(logLines, obj)
	}, funcr.Options{})
	fl, err := NewFifoLog(fifoPath, logger)
	if err != nil {
		t.Fatalf("NewFifoLog: %v", err)
	}
	fl.Start(context.Background())
	t.Cleanup(func() { fl.Close() })

	pidFile := filepath.Join(t.TempDir(), "mysqld.pid")
	s := NewDetachedSupervisor("/bin/sh", []string{"-c", "echo hello-fifo; echo second-line"},
		WithFIFO(fl),
		WithPIDFile(pidFile),
		WithDetachedShutdownTimeout(2*time.Second))

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	// Give the FIFO reader goroutine time to flush.
	time.Sleep(200 * time.Millisecond)

	if len(logLines) != 2 {
		t.Fatalf("expected 2 log lines, got %d: %v", len(logLines), logLines)
	}
	if !strings.Contains(logLines[0], `"line":"hello-fifo"`) {
		t.Errorf("first log line = %s, wanted hello-fifo", logLines[0])
	}
	if !strings.Contains(logLines[1], `"line":"second-line"`) {
		t.Errorf("second log line = %s, wanted second-line", logLines[1])
	}

	// Verify the pidfile contains FIFO metadata.
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("ReadFile pidfile: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "fifo="+fifoPath) {
		t.Errorf("pidfile missing fifo path, got %q", content)
	}
	if !strings.Contains(content, "fd=") {
		t.Errorf("pidfile missing read fd, got %q", content)
	}
}
