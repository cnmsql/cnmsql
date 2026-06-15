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
	"bytes"
	"context"
	"testing"
	"time"
)

func TestSupervisorStartAndCleanExit(t *testing.T) {
	var out bytes.Buffer
	s := NewProcessSupervisor("/bin/sh", []string{"-c", "echo hello"}, WithOutput(&out, &out))

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := out.String(); got != "hello\n" {
		t.Errorf("output = %q, want %q", got, "hello\n")
	}
}

func TestSupervisorDoubleStartFails(t *testing.T) {
	s := NewProcessSupervisor("/bin/sleep", []string{"30"})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })

	if err := s.Start(context.Background()); err == nil {
		t.Error("expected second Start to fail")
	}
}

func TestSupervisorGracefulShutdown(t *testing.T) {
	s := NewProcessSupervisor("/bin/sleep", []string{"60"}, WithShutdownTimeout(2*time.Second))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !s.Running() {
		t.Fatal("expected Running() true after Start")
	}

	// sleep exits on SIGTERM; that is treated as a clean shutdown.
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if s.Running() {
		t.Error("expected Running() false after Shutdown")
	}
}

func TestSupervisorKillsAfterTimeout(t *testing.T) {
	// A process that ignores SIGTERM must still be killed after the timeout.
	s := NewProcessSupervisor("/bin/sh", []string{"-c", "trap '' TERM; sleep 60"},
		WithShutdownTimeout(500*time.Millisecond))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	start := time.Now()
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("shutdown took too long: %s", elapsed)
	}
}

func TestSupervisorShutdownGracefulCleanExit(t *testing.T) {
	// sleep exits on SIGTERM within the smart budget, so SIGKILL is not needed.
	s := NewProcessSupervisor("/bin/sleep", []string{"60"})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	killed, err := s.ShutdownGraceful(2*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("ShutdownGraceful: %v", err)
	}
	if killed {
		t.Error("expected a graceful exit, but the process was killed")
	}
	if s.Running() {
		t.Error("expected Running() false after ShutdownGraceful")
	}
}

func TestSupervisorShutdownGracefulForcesKill(t *testing.T) {
	// A process that ignores SIGTERM must be killed once the hard stop delay
	// elapses, not at the smart budget.
	s := NewProcessSupervisor("/bin/sh", []string{"-c", "trap '' TERM; while true; do sleep 1; done"})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Let the shell install its SIGTERM trap before we signal it.
	time.Sleep(200 * time.Millisecond)

	start := time.Now()
	killed, err := s.ShutdownGraceful(200*time.Millisecond, 600*time.Millisecond)
	if err != nil {
		t.Fatalf("ShutdownGraceful: %v", err)
	}
	if !killed {
		t.Error("expected the process to be force-killed")
	}
	elapsed := time.Since(start)
	if elapsed < 600*time.Millisecond {
		t.Errorf("kill happened too early at %s; should wait the hard stop delay", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("shutdown took too long: %s", elapsed)
	}
}

func TestSupervisorShutdownGracefulWithConcurrentWaiter(t *testing.T) {
	// The run loop parks a goroutine on Wait() for the whole process lifetime.
	// The exit must be observable by both that watcher and the shutdown path;
	// a single-delivery channel would let one starve the other and wedge PID1.
	s := NewProcessSupervisor("/bin/sleep", []string{"60"})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waiterDone := make(chan error, 1)
	go func() { waiterDone <- s.Wait() }()

	shutdownDone := make(chan error, 1)
	go func() {
		_, err := s.ShutdownGraceful(2*time.Second, 5*time.Second)
		shutdownDone <- err
	}()

	// ShutdownGraceful treats the SIGTERM exit as clean (nil).
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Errorf("ShutdownGraceful: unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ShutdownGraceful never observed the process exit (single-delivery starvation)")
	}

	// The concurrent Wait() must also return; it surfaces the raw signal exit.
	select {
	case <-waiterDone:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Wait() never observed the process exit (single-delivery starvation)")
	}
}

func TestSupervisorShutdownGracefulWhenNotRunning(t *testing.T) {
	s := NewProcessSupervisor("/bin/sleep", []string{"1"})
	if killed, err := s.ShutdownGraceful(time.Second, 2*time.Second); err != nil || killed {
		t.Errorf("ShutdownGraceful on non-running supervisor should be a no-op, got killed=%v err=%v", killed, err)
	}
}

func TestSupervisorRestart(t *testing.T) {
	s := NewProcessSupervisor("/bin/sleep", []string{"60"}, WithShutdownTimeout(2*time.Second))
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

func TestSupervisorWaitBeforeStart(t *testing.T) {
	s := NewProcessSupervisor("/bin/sleep", []string{"1"})
	if err := s.Wait(); err == nil {
		t.Error("expected Wait before Start to error")
	}
}

func TestSupervisorShutdownWhenNotRunning(t *testing.T) {
	s := NewProcessSupervisor("/bin/sleep", []string{"1"})
	if err := s.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on non-running supervisor should be a no-op, got %v", err)
	}
}

// ProcessSupervisor must satisfy the Supervisor interface used by Controller.
var _ Supervisor = (*ProcessSupervisor)(nil)
