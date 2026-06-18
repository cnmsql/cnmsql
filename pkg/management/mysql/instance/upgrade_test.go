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
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
)

func TestReexecEnvSetsAdoptPID(t *testing.T) {
	t.Setenv("CNMYSQL_UNRELATED", "keep-me")
	env := reexecEnv(4242)

	want := AdoptMysqldPIDEnv + "=4242"
	var found, kept bool
	for _, kv := range env {
		if kv == want {
			found = true
		}
		if kv == "CNMYSQL_UNRELATED=keep-me" {
			kept = true
		}
	}
	if !found {
		t.Errorf("reexecEnv missing %q in %v", want, env)
	}
	if !kept {
		t.Error("reexecEnv dropped an unrelated environment entry")
	}
}

func TestReexecEnvReplacesExistingAdoptPID(t *testing.T) {
	t.Setenv(AdoptMysqldPIDEnv, "1")
	env := reexecEnv(99)

	var count int
	for _, kv := range env {
		if strings.HasPrefix(kv, AdoptMysqldPIDEnv+"=") {
			count++
			if kv != AdoptMysqldPIDEnv+"=99" {
				t.Errorf("adopt pid = %q, want =99", kv)
			}
		}
	}
	if count != 1 {
		t.Errorf("expected exactly one %s entry, got %d", AdoptMysqldPIDEnv, count)
	}
}

func TestReExecForUpgradeRejectsInvalidPID(t *testing.T) {
	if err := ReExecForUpgrade(0); err == nil {
		t.Error("expected ReExecForUpgrade(0) to fail without exec'ing")
	}
}

func TestInPlaceUpgradingFlag(t *testing.T) {
	t.Cleanup(func() { inPlaceUpgrading.Store(false) })
	if IsInPlaceUpgrading() {
		t.Fatal("flag should start cleared")
	}
	SetInPlaceUpgrading()
	if !IsInPlaceUpgrading() {
		t.Error("flag should be set after SetInPlaceUpgrading")
	}
}

func TestAdoptRequest(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		t.Setenv(AdoptMysqldPIDEnv, "")
		if ok, pid := adoptRequest(); ok || pid != 0 {
			t.Errorf("adoptRequest() = %v,%d; want false,0", ok, pid)
		}
	})
	t.Run("valid", func(t *testing.T) {
		t.Setenv(AdoptMysqldPIDEnv, " 314 ")
		if ok, pid := adoptRequest(); !ok || pid != 314 {
			t.Errorf("adoptRequest() = %v,%d; want true,314", ok, pid)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		t.Setenv(AdoptMysqldPIDEnv, "not-a-pid")
		if ok, _ := adoptRequest(); ok {
			t.Error("adoptRequest() should reject a non-numeric pid")
		}
	})
}

func TestReadPIDFileFIFOFD(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "mysqld.pid")
	if err := os.WriteFile(pidFile, []byte("42\nfifo=/run/x.fifo\nfd=7\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fd, err := readPIDFileFIFOFD(pidFile)
	if err != nil {
		t.Fatalf("readPIDFileFIFOFD: %v", err)
	}
	if fd != 7 {
		t.Errorf("fd = %d, want 7", fd)
	}

	if _, err := readPIDFileFIFOFD(filepath.Join(dir, "missing.pid")); err == nil {
		t.Error("expected error for missing pidfile")
	}

	noFD := filepath.Join(dir, "nofd.pid")
	if err := os.WriteFile(noFD, []byte("42\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := readPIDFileFIFOFD(noFD); err == nil {
		t.Error("expected error when pidfile has no fd= entry")
	}
}

// TestFifoLogFromFDWrapsInheritedOutput mirrors the re-exec re-adoption path: a
// read end opened by a previous image is handed to FifoLogFromFD, which must
// resume wrapping mysqld output lines.
func TestFifoLogFromFDWrapsInheritedOutput(t *testing.T) {
	fifoPath := filepath.Join(t.TempDir(), "mysqld-stdout.fifo")
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	rfd, err := syscall.Open(fifoPath, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("open read end: %v", err)
	}
	// Clear CLOEXEC, as the original image does, so the fd is inheritable.
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(rfd), syscall.F_SETFD, 0); errno != 0 {
		t.Fatalf("clear CLOEXEC: %v", errno)
	}

	var logLines []string
	logger := funcr.NewJSON(func(obj string) { logLines = append(logLines, obj) }, funcr.Options{})

	fl, err := FifoLogFromFD(fifoPath, rfd, logger)
	if err != nil {
		t.Fatalf("FifoLogFromFD: %v", err)
	}
	fl.Start(context.Background())
	t.Cleanup(func() { fl.Close() })

	if _, err := fl.WriteEnd().Write([]byte("after-reexec\n")); err != nil {
		t.Fatalf("write to fifo: %v", err)
	}

	// Give the reader goroutine time to flush.
	time.Sleep(200 * time.Millisecond)
	if len(logLines) != 1 || !strings.Contains(logLines[0], `"line":"after-reexec"`) {
		t.Fatalf("expected wrapped line after-reexec, got %v", logLines)
	}
}

func TestFifoLogFromFDRejectsNegativeFD(t *testing.T) {
	if _, err := FifoLogFromFD("/run/x.fifo", -1, funcr.NewJSON(func(string) {}, funcr.Options{})); err == nil {
		t.Error("expected error for negative fd")
	}
}

func TestProcessSupervisorPid(t *testing.T) {
	s := NewProcessSupervisor("/bin/sleep", []string{"30"})
	if s.Pid() != 0 {
		t.Errorf("Pid before start = %d, want 0", s.Pid())
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := s.Pid()
	if pid <= 0 {
		t.Errorf("Pid after start = %d, want > 0", pid)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		t.Errorf("reported pid %d is not a live process: %v", pid, err)
	}
	_ = s.ShutdownWithTimeout(2 * time.Second)
	if s.Pid() != 0 {
		t.Errorf("Pid after shutdown = %d, want 0", s.Pid())
	}
}
