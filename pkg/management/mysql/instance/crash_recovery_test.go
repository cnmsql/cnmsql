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
	"strings"
	"testing"

	"github.com/go-logr/logr"
)

func TestIndicatesInnoDBCorruption(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		output string
		want   bool
	}{
		// The subsystem prefix differs by engine, so each real log format must
		// match. MySQL 8.0 tags the line "[InnoDB]"; 5.7 and MariaDB write
		// "InnoDB:".
		{
			name: "MySQL 8.0 page corruption",
			output: "2026-08-04T10:00:00.000000Z 1 [ERROR] [MY-012153] [InnoDB] " +
				"Database page corruption on disk or a failed file read of page [page id: space=4, page number=3]",
			want: true,
		},
		{
			name:   "MySQL 5.7 page corruption",
			output: "2026-08-04 10:00:00 0x7f [Note] InnoDB: Database page corruption on disk or a failed file read",
			want:   true,
		},
		{
			name: "MariaDB page corruption",
			output: "2026-08-04 10:00:00 0 [ERROR] InnoDB: Database page corruption on disk " +
				"or a failed read of file './ibdata1'",
			want: true,
		},
		{
			name: "MySQL 8.0 missing datafile",
			output: "2026-08-04T10:00:00.000000Z 1 [ERROR] [MY-012216] [InnoDB] " +
				"Cannot open datafile './app/t.ibd'",
			want: true,
		},
		{
			name:   "checksum mismatch",
			output: "[ERROR] [MY-012558] [InnoDB] Page checksum mismatch in file space",
			want:   true,
		},
		{
			name:   "corruption line among healthy noise",
			output: "InnoDB: Using Linux native AIO\nInnoDB: unable to read a page\nAborting",
			want:   true,
		},
		{
			name:   "MySQL 8.0 unable to read page wording",
			output: "[ERROR] [MY-011906] [InnoDB] Unable to read page [page id: space=0, page number=7]",
			want:   true,
		},
		{
			name:   "clean shutdown message",
			output: "InnoDB: Normal shutdown",
			want:   false,
		},
		{
			name:   "empty output",
			output: "",
			want:   false,
		},
		// The costly mistake is calling an environmental failure corruption: the
		// caller responds by having the operator discard the data volume. None of
		// these may match.
		{
			name:   "startup slower than the ready timeout",
			output: "InnoDB: Buffer pool(s) load completed\nmysqld: ready for connections",
			want:   false,
		},
		{
			name:   "rejected config value",
			output: "mysqld: unknown variable 'nonexistent_option=1'\nAborting",
			want:   false,
		},
		{
			name:   "out of memory",
			output: "InnoDB: Cannot allocate memory for the buffer pool\nAborting",
			want:   false,
		},
		{
			name:   "disk full",
			output: "InnoDB: Error while writing 16384 bytes: 28 (No space left on device)",
			want:   false,
		},
		{
			name:   "permission denied on the data directory",
			output: "mysqld: Can't create/write to file '/var/lib/mysql/x' (Errcode: 13 - Permission denied)",
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := indicatesInnoDBCorruption(tt.output); got != tt.want {
				t.Errorf("indicatesInnoDBCorruption(%q) = %v, want %v", tt.output, got, tt.want)
			}
		})
	}
}

func TestCorruptionEvidence(t *testing.T) {
	t.Parallel()

	out := "InnoDB: Using Linux native AIO\n" +
		"[ERROR] [MY-012153] [InnoDB] Database page corruption on disk\n" +
		"[ERROR] [MY-012558] [InnoDB] Page checksum mismatch in file space\n"
	got := corruptionEvidence(out)
	for _, want := range []string{"Database page corruption", "Page checksum mismatch"} {
		if !strings.Contains(got, want) {
			t.Errorf("corruptionEvidence = %q, want it to mention %q", got, want)
		}
	}
	if strings.Contains(got, "native AIO") {
		t.Errorf("corruptionEvidence = %q, want unrelated lines dropped", got)
	}

	if got := corruptionEvidence("nothing interesting"); got == "" {
		t.Error("corruptionEvidence must describe the absence of a match, not return empty")
	}
}

// corruptionEvidence caps how much it reports so the termination message stays
// within the size Kubernetes retains, even when InnoDB repeats the diagnosis for
// every damaged page.
func TestCorruptionEvidenceIsBounded(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	for range 500 {
		b.WriteString("InnoDB: Database page corruption on disk\n")
	}
	if got := corruptionEvidence(b.String()); len(got) > 512 {
		t.Errorf("corruptionEvidence length = %d, want it bounded", len(got))
	}
}

func TestCorruptionMarkerLifecycle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	if hasCorruptionMarker(dir) {
		t.Fatal("marker should not exist in a fresh directory")
	}

	reportCorruption(logr.Discard(), dir, "InnoDB: Database page corruption on disk")

	if !hasCorruptionMarker(dir) {
		t.Fatal("marker should exist after reporting corruption")
	}
	body, err := os.ReadFile(markerPath(dir))
	if err != nil {
		t.Fatalf("reading marker: %v", err)
	}
	if !strings.Contains(string(body), CorruptionSentinel) {
		t.Fatalf("marker = %q, want it to carry the sentinel", body)
	}

	clearCorruptionMarker(dir)
	if hasCorruptionMarker(dir) {
		t.Fatal("marker should not exist after clearing")
	}
}

// Recording the diagnosis is best-effort: an unwritable data directory must not
// panic or block the caller from returning the real startup error.
func TestReportCorruptionToleratesUnwritableDataDir(t *testing.T) {
	t.Parallel()
	reportCorruption(logr.Discard(), "/nonexistent-dir-for-test", "InnoDB: Database page corruption on disk")
}

func TestCorruptionMarkerPath(t *testing.T) {
	t.Parallel()
	want := "/var/lib/mysql/" + corruptionMarker
	if got := markerPath("/var/lib/mysql"); got != want {
		t.Fatalf("markerPath = %q, want %q", got, want)
	}
}
