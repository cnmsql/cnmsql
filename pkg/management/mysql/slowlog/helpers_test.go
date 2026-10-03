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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-logr/logr"
)

// recorder is a logr sink that keeps every record. When block is set, it
// blocks every "Slow query" record until block is closed, standing in for a
// backpressured stdout.
type recorder struct {
	mu      sync.Mutex
	records []map[string]any
	block   chan struct{}
}

func (r *recorder) Init(logr.RuntimeInfo) {}
func (r *recorder) Enabled(int) bool      { return true }
func (r *recorder) Info(_ int, msg string, kv ...any) {
	if msg == "Slow query" && r.block != nil {
		<-r.block
	}
	m := map[string]any{"msg": msg}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	r.mu.Lock()
	r.records = append(r.records, m)
	r.mu.Unlock()
}
func (r *recorder) Error(err error, msg string, kv ...any) {
	r.Info(0, msg, append(kv, "error", err)...)
}
func (r *recorder) WithValues(...any) logr.LogSink { return r }
func (r *recorder) WithName(string) logr.LogSink   { return r }

// queries returns the query of every "Slow query" record, in order.
func (r *recorder) queries() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, m := range r.records {
		if m["msg"] == "Slow query" {
			out = append(out, m["query"].(string))
		}
	}
	return out
}

// fakeMysqld writes slow log entries the way mysqld does: an O_APPEND
// descriptor that survives a rename until a flush reopens the path.
type fakeMysqld struct {
	t    *testing.T
	path string
	mu   sync.Mutex
	f    *os.File
}

func newFakeMysqld(t *testing.T, dir string) *fakeMysqld {
	t.Helper()
	m := &fakeMysqld{t: t, path: filepath.Join(dir, FileName)}
	m.reopen()
	t.Cleanup(func() { _ = m.f.Close() })
	return m
}

func (m *fakeMysqld) reopen() {
	if m.f != nil {
		_ = m.f.Close()
	}
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		m.t.Fatal(err)
	}
	m.f = f
}

const entryTemplate = `# Time: 2026-10-03T14:12:24.630320Z
# User@Host: app[app] @ localhost []  Id:     9
# Query_time: 0.500000  Lock_time: 0.000001 Rows_sent: 1  Rows_examined: 10
SET timestamp=1791036744;
SELECT '%s';
`

// slow writes one entry whose query is SELECT '<marker>'.
func (m *fakeMysqld) slow(marker string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := fmt.Fprintf(m.f, entryTemplate, marker); err != nil {
		m.t.Fatal(err)
	}
}

// flush is FLUSH LOCAL SLOW LOGS: mysqld closes its descriptor and reopens the
// path, creating it if a rotation renamed it away.
func (m *fakeMysqld) flush(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reopen()
	return nil
}

func newTestLog(t *testing.T, cfg Config) (*Log, *recorder) {
	t.Helper()
	rec := &recorder{}
	if cfg.Dir == "" {
		cfg.Dir = t.TempDir()
	}
	cfg.Logger = logr.New(rec)
	return New(cfg), rec
}

func sel(markers ...string) []string {
	out := make([]string, len(markers))
	for i, m := range markers {
		out[i] = "SELECT '" + m + "'"
	}
	return out
}
