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
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
)

func TestShouldAttemptForceRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		stderr string
		want   bool
	}{
		{
			name:   "page corruption",
			stderr: "InnoDB: Database page corruption on disk",
			want:   true,
		},
		{
			name:   "checksum mismatch",
			stderr: "InnoDB: Page checksum mismatch in file space",
			want:   true,
		},
		{
			name:   "clean shutdown message",
			stderr: "InnoDB: Normal shutdown",
			want:   false,
		},
		{
			name:   "empty output",
			stderr: "",
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldAttemptForceRecovery(tt.stderr); got != tt.want {
				t.Errorf("shouldAttemptForceRecovery(%q) = %v, want %v", tt.stderr, got, tt.want)
			}
		})
	}
}

func TestNextForceRecoveryLevel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		current int
		want    int
	}{
		{0, 1},
		{1, 2},
		{2, 3},
		{3, 0},
		{99, 0},
	}
	for _, tt := range tests {
		if got := nextForceRecoveryLevel(tt.current); got != tt.want {
			t.Errorf("nextForceRecoveryLevel(%d) = %d, want %d", tt.current, got, tt.want)
		}
	}
}

func TestForceRecoveryMarker(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	if _, exists := readForceRecoveryMarker(dir); exists {
		t.Fatal("marker should not exist in a fresh directory")
	}

	if err := writeForceRecoveryMarker(dir, 2); err != nil {
		t.Fatalf("writeForceRecoveryMarker: %v", err)
	}

	level, exists := readForceRecoveryMarker(dir)
	if !exists {
		t.Fatal("marker should exist after writing")
	}
	if level != 2 {
		t.Fatalf("level = %d, want 2", level)
	}

	clearForceRecoveryMarker(dir)
	if _, exists := readForceRecoveryMarker(dir); exists {
		t.Fatal("marker should not exist after clearing")
	}
}

func TestEscalateForceRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	escalateForceRecovery(logr.Discard(), dir, false, 0)

	level, exists := readForceRecoveryMarker(dir)
	if !exists {
		t.Fatal("marker should exist after first escalation")
	}
	if level != 1 {
		t.Fatalf("level = %d, want 1", level)
	}

	escalateForceRecovery(logr.Discard(), dir, true, 1)
	level, _ = readForceRecoveryMarker(dir)
	if level != 2 {
		t.Fatalf("level = %d, want 2", level)
	}

	escalateForceRecovery(logr.Discard(), dir, true, 2)
	level, _ = readForceRecoveryMarker(dir)
	if level != 3 {
		t.Fatalf("level = %d, want 3", level)
	}

	escalateForceRecovery(logr.Discard(), dir, true, 3)
	level, _ = readForceRecoveryMarker(dir)
	if level != 3 {
		t.Fatalf("level = %d, want 3 (should not escalate beyond 3)", level)
	}
}

func TestForceRecoveryMarkerPath(t *testing.T) {
	if got := markerPath("/var/lib/mysql"); got != "/var/lib/mysql/"+forceRecoveryMarker {
		t.Fatalf("markerPath = %q, want %q", got, "/var/lib/mysql/"+forceRecoveryMarker)
	}
	_ = filepath.Clean
	_ = os.Stat
}
