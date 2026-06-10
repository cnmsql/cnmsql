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
