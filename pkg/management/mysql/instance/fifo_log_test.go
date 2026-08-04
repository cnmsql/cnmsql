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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
)

// A crashed prior process can leave its FIFO behind on a persistent volume.
// NewFifoLog must remove that stale pipe and recreate it rather than failing
// with EEXIST, so a restarting manager comes back up.
func TestNewFifoLogReclaimsStaleFifo(t *testing.T) {
	t.Parallel()
	fifoPath := filepath.Join(t.TempDir(), "mysqld.pid.fifo")

	// Simulate the leftover named pipe from a previous, now-dead run.
	if err := syscall.Mkfifo(fifoPath, 0600); err != nil {
		t.Fatalf("seeding stale fifo: %v", err)
	}

	fl, err := NewFifoLog(fifoPath, testr.New(t))
	if err != nil {
		t.Fatalf("NewFifoLog over a stale fifo: %v", err)
	}
	defer fl.Close()

	info, err := os.Stat(fifoPath)
	if err != nil {
		t.Fatalf("stat fifo: %v", err)
	}
	if info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("expected a named pipe at %s, got mode %v", fifoPath, info.Mode())
	}
}

// A non-FIFO file at the path is unexpected — NewFifoLog must not clobber it and
// must surface the original error instead.
func TestNewFifoLogRefusesNonFifo(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "mysqld.pid.fifo")
	if err := os.WriteFile(path, []byte("not a pipe"), 0600); err != nil {
		t.Fatalf("seeding regular file: %v", err)
	}

	if _, err := NewFifoLog(path, testr.New(t)); err == nil {
		t.Fatal("NewFifoLog must refuse to clobber a non-FIFO file")
	}
	// The regular file must be left intact.
	if data, err := os.ReadFile(path); err != nil || string(data) != "not a pipe" {
		t.Fatalf("regular file was modified: data=%q err=%v", data, err)
	}
}

// The tail is what lets a failed start be diagnosed: by the time openControl
// gives up, mysqld's output has already been forwarded to the structured log, so
// FifoLog must have retained it.
func TestFifoLogRetainsTail(t *testing.T) {
	t.Parallel()
	fifoPath := filepath.Join(t.TempDir(), "mysqld.pid.fifo")

	fl, err := NewFifoLog(fifoPath, testr.New(t))
	if err != nil {
		t.Fatalf("NewFifoLog: %v", err)
	}
	defer fl.Close()

	fl.Start(t.Context())

	want := "[ERROR] [MY-012153] [InnoDB] Database page corruption on disk"
	for _, line := range []string{"InnoDB: Using Linux native AIO", want, "Aborting"} {
		if _, err := fl.WriteEnd().WriteString(line + "\n"); err != nil {
			t.Fatalf("writing to fifo: %v", err)
		}
	}

	var tail string
	for range 100 {
		if tail = fl.Tail(); strings.Contains(tail, want) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(tail, want) {
		t.Fatalf("Tail() = %q, want it to contain %q", tail, want)
	}
	if !indicatesInnoDBCorruption(tail) {
		t.Fatalf("the retained tail must be diagnosable as corruption, got %q", tail)
	}
}

// The tail is bounded so a long-running mysqld cannot grow it without limit.
func TestFifoLogTailIsBounded(t *testing.T) {
	t.Parallel()
	fl := &FifoLog{}
	for i := range tailLines * 3 {
		fl.recordTail(fmt.Sprintf("line %d", i))
	}
	if got := len(fl.tail); got != tailLines {
		t.Fatalf("retained %d lines, want %d", got, tailLines)
	}
	// The most recent lines are the ones that matter for a diagnosis.
	if !strings.Contains(fl.Tail(), fmt.Sprintf("line %d", tailLines*3-1)) {
		t.Fatal("tail must retain the most recent lines")
	}
	if strings.Contains(fl.Tail(), "line 0\n") {
		t.Fatal("tail must drop the oldest lines")
	}
}
