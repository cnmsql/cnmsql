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
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestRecordLoggerDoesNotSample emits a burst of identical messages, which the
// manager's sampled logger would cut to 100 per second plus 1 in 100.
func TestRecordLoggerDoesNotSample(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := NewRecordLogger(&buf)
	for range 1000 {
		log.Info("Slow query", "query", "SELECT 1")
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1000 {
		t.Fatalf("%d records written, want 1000", len(lines))
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("record is not JSON: %v: %s", err, lines[0])
	}
	if rec["logger"] != "mysqld.slowlog" || rec["level"] != "info" || rec["msg"] != "Slow query" {
		t.Errorf("record = %v", rec)
	}
	if _, err := time.Parse(time.RFC3339, rec["ts"].(string)); err != nil {
		t.Errorf("ts = %v, want RFC 3339 like the manager's logger", rec["ts"])
	}
}
