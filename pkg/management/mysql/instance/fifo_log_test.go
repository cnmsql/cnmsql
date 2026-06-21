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
	"os"
	"path/filepath"
	"syscall"
	"testing"

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
