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

package slowlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeLog(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), FileName)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const entryHeader = `# Time: 2026-10-03T14:12:24.630320Z
# User@Host: app[app] @ localhost []  Id:     9
# Query_time: 0.500000  Lock_time: 0.000001 Rows_sent: 1  Rows_examined: 10
SET timestamp=1791036744;
`

// TestManyLineStatementParsesInLinearTime guards against building a statement
// by repeated string concatenation: a 200k-line IN list took 18 s that way,
// long enough for the startup drain to outlast the startup probe.
func TestManyLineStatementParsesInLinearTime(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	b.WriteString(entryHeader + "SELECT * FROM t WHERE id IN (\n")
	for i := range 200_000 {
		fmt.Fprintf(&b, "%d,\n", i)
	}
	b.WriteString("0);\n" + entryHeader + "SELECT 'after';\n")
	path := writeLog(t, b.String())

	start := time.Now()
	events := parseFixture(t, path)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("parsing took %s", elapsed)
	}
	if len(events) != 2 {
		t.Fatalf("%d records, want 2", len(events))
	}
	r := record(events[0])
	if q, _ := r["query"].(string); len(q) > MaxQueryBytes || r["query_truncated"] != true {
		t.Errorf("query len=%d truncated=%v, want capped and flagged", len(q), r["query_truncated"])
	}
	if record(events[1])["query"] != "SELECT 'after'" {
		t.Errorf("entry after the long statement = %v", record(events[1])["query"])
	}
}

// TestCommentLineStaysInStatement: a statement line that starts with "# " is
// part of the statement, not the start of a new entry.
func TestCommentLineStaysInStatement(t *testing.T) {
	t.Parallel()
	path := writeLog(t, entryHeader+"SELECT *\n  FROM t\n# Filter by status\n WHERE s = 1;\n"+
		entryHeader+"SELECT 'next';\n")
	events := parseFixture(t, path)
	if len(events) != 2 {
		t.Fatalf("%d records, want 2", len(events))
	}
	want := "SELECT *\n  FROM t\n# Filter by status\n WHERE s = 1"
	if q := record(events[0])["query"]; q != want {
		t.Errorf("query = %q, want %q", q, want)
	}
}
