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

// Package slowlog tails the mysqld slow query log, emits every entry as a
// structured log record, and keeps the log inside a fixed budget on the run
// volume (design 037).
package slowlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	mysqllog "github.com/percona/go-mysql/log"
)

const (
	// FileName is the slow log's name in the run directory, next to the socket.
	FileName = "mysqld-slow.log"
	// FlushStatement makes mysqld reopen its slow log after a rotation. LOCAL
	// keeps it out of the binary log: a plain FLUSH SLOW LOGS consumes a GTID,
	// which on a replica is an errant transaction.
	FlushStatement = "FLUSH LOCAL SLOW LOGS"
	// CursorEnv carries the tailer's position across an in-place re-exec.
	CursorEnv = "CNMSQL_SLOWLOG_CURSOR"

	// DefaultRotateBytes is the active file size at which the watchdog rotates.
	DefaultRotateBytes int64 = 4 << 20
	// DefaultHardCapBytes bounds all slow log files together. Above it the
	// watchdog truncates them, dropping what the tailer has not read.
	DefaultHardCapBytes int64 = 24 << 20
	// DefaultReserveBytes is the run volume space kept free for the sockets,
	// their lock files and the pidfile.
	DefaultReserveBytes int64 = 4 << 20

	defaultPollInterval  = 250 * time.Millisecond
	defaultCheckInterval = time.Second
	flushTimeout         = 5 * time.Second
	reExecStopTimeout    = 5 * time.Second
)

// Flusher reopens mysqld's slow log; in production it runs FlushStatement on
// the control connection.
type Flusher func(ctx context.Context) error

// Config configures a Log. Zero sizes and intervals take the defaults.
type Config struct {
	// Dir holds the slow log: the run directory, next to the socket.
	Dir           string
	Logger        logr.Logger
	RotateBytes   int64
	HardCapBytes  int64
	ReserveBytes  int64
	PollInterval  time.Duration
	CheckInterval time.Duration
	// Cursor is where a re-exec'd manager resumes (CursorFromEnv).
	Cursor Cursor
}

// Stats are the counters the metrics collector exports.
type Stats struct {
	Bytes        int64
	Entries      uint64
	DroppedBytes uint64
	Rotations    uint64
}

// Cursor is a position in a slow log file: the start of the first entry not
// yet emitted.
type Cursor struct {
	Inode  uint64
	Offset int64
}

func (c Cursor) String() string { return fmt.Sprintf("%d:%d", c.Inode, c.Offset) }

// ParseCursor parses the "<inode>:<offset>" form String produces.
func ParseCursor(s string) (Cursor, bool) {
	ino, off, ok := strings.Cut(s, ":")
	if !ok {
		return Cursor{}, false
	}
	i, err := strconv.ParseUint(ino, 10, 64)
	if err != nil {
		return Cursor{}, false
	}
	o, err := strconv.ParseInt(off, 10, 64)
	if err != nil || o < 0 {
		return Cursor{}, false
	}
	return Cursor{Inode: i, Offset: o}, true
}

// CursorFromEnv returns the cursor a previous manager image handed over, or
// the zero Cursor.
func CursorFromEnv() Cursor {
	c, _ := ParseCursor(os.Getenv(CursorEnv))
	return c
}

// Log tails one slow log and keeps it within its budget.
type Log struct {
	cfg    Config
	log    logr.Logger
	flush  Flusher
	parent context.Context

	mu      sync.Mutex
	flushed bool // the latest rotation's flush succeeded
	cursor  Cursor
	cancel  context.CancelFunc
	done    chan struct{}

	flushFailing bool // owned by the watchdog goroutine

	bytes     atomic.Int64
	entries   atomic.Uint64
	dropped   atomic.Uint64
	rotations atomic.Uint64
	// truncations counts the watchdog's truncations. A truncated file can be
	// refilled to its old size before the next poll, so the tailer cannot rely
	// on the size shrinking to notice.
	truncations atomic.Uint64
}

// New returns a Log for cfg. It does nothing until DrainLeftovers or Start.
func New(cfg Config) *Log {
	if cfg.RotateBytes <= 0 {
		cfg.RotateBytes = DefaultRotateBytes
	}
	if cfg.HardCapBytes <= 0 {
		cfg.HardCapBytes = DefaultHardCapBytes
	}
	if cfg.ReserveBytes <= 0 {
		cfg.ReserveBytes = DefaultReserveBytes
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = defaultCheckInterval
	}
	if cfg.Logger.GetSink() == nil {
		cfg.Logger = logr.Discard()
	}
	return &Log{cfg: cfg, log: cfg.Logger, flushed: true, cursor: cfg.Cursor}
}

func (l *Log) activePath() string  { return filepath.Join(l.cfg.Dir, FileName) }
func (l *Log) rotatedPath() string { return l.activePath() + ".1" }

func (l *Log) emit(e *mysqllog.Event) {
	l.log.Info("Slow query", keysAndValues(e)...)
	l.entries.Add(1)
}

// Cursor returns the tailer's position.
func (l *Log) Cursor() Cursor {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cursor
}

func (l *Log) setCursor(c Cursor) {
	l.mu.Lock()
	l.cursor = c
	l.mu.Unlock()
}

func (l *Log) isFlushed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flushed
}

func (l *Log) setFlushed(v bool) {
	l.mu.Lock()
	l.flushed = v
	l.mu.Unlock()
}

// Stats returns the current counters.
func (l *Log) Stats() Stats {
	return Stats{
		Bytes:        l.bytes.Load(),
		Entries:      l.entries.Load(),
		DroppedBytes: l.dropped.Load(),
		Rotations:    l.rotations.Load(),
	}
}
