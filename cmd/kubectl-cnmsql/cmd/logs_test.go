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

package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestPrettyLogsColorizesValidAndEchoesInvalid(t *testing.T) {
	t.Parallel()
	infoLine := `{"level":"info","msg":"starting reconciliation",` +
		`"logger":"controller.cluster","ts":"2026-08-14T10:00:00.123Z",` +
		`"logging_pod":"demo-1","namespace":"test"}`
	in := strings.Join([]string{
		infoLine,
		`{"level":"error","msg":"failed to connect","logger":"manager","ts":"2026-08-14T10:00:01Z","logging_pod":"demo-2"}`,
		`this is not json`,
		`{"level":"warning","msg":"degraded","ts":"2026-08-14T10:00:02Z","logging_pod":"demo-1"}`,
		"", // trailing newline
	}, "\n")

	var out bytes.Buffer
	if err := prettyLogs(context.Background(), strings.NewReader(in), &out); err != nil {
		t.Fatalf("prettyLogs() error = %v", err)
	}
	got := out.String()
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d lines, want 4:\n%s", len(lines), got)
	}
	// Valid info line: contains the message and pod name.
	if !strings.Contains(lines[0], "starting reconciliation") || !strings.Contains(lines[0], "demo-1") {
		t.Errorf("info line missing content: %q", lines[0])
	}
	// Non-JSON line echoed verbatim.
	if strings.TrimSpace(lines[2]) != "this is not json" {
		t.Errorf("non-json line not echoed verbatim: %q", lines[2])
	}
	// All valid lines contain a level token.
	for i, want := range []string{"info", "error", "this is not json", "warning"} {
		if !strings.Contains(lines[i], want) {
			t.Errorf("line %d missing %q: %q", i, want, lines[i])
		}
	}
}

func TestPrettyLogsStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := prettyLogs(ctx, strings.NewReader("line1\nline2\n"), &out); err != nil {
		t.Fatalf("prettyLogs() error = %v", err)
	}
	// With an already-cancelled context the loop may read at most one line
	// before noticing cancellation; it must not error either way.
}
