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
	"os"
	"time"

	mysqllog "github.com/percona/go-mysql/log"
)

// tailer follows the slow log. It owns its fields; the Log only sees the
// cursor it publishes.
type tailer struct {
	l *Log
	f *os.File
	// ino is the inode of f.
	ino uint64
	// off is where the next parse starts: the first entry not yet emitted.
	off int64
	// lastSize is the size of f at the previous parse, -1 before the first.
	lastSize int64
	// held is the last entry of the previous parse. mysqld may still have been
	// writing it, so it is emitted once the file stops growing.
	held *mysqllog.Event
	// resume is the handed-over cursor, applied to the first file opened.
	resume Cursor
	// skipIno is a finished rotated file that could not be unlinked.
	skipIno uint64
	// truncations is the Log's truncation count the tailer last saw.
	truncations uint64
}

func newTailer(l *Log, resume Cursor) *tailer {
	return &tailer{l: l, lastSize: -1, resume: resume, truncations: l.truncations.Load()}
}

func (t *tailer) run(ctx context.Context) {
	defer t.close()
	tick := time.NewTicker(t.l.cfg.PollInterval)
	defer tick.Stop()
	for {
		t.poll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (t *tailer) poll(ctx context.Context) {
	if t.f == nil && !t.open() {
		return
	}
	// The order matters: the watchdog marks the flush pending before it
	// renames, so once the rename is visible, flushed describes that rotation.
	away := t.rotatedAway()
	flushed := t.l.isFlushed()
	st, err := statFile(t.f)
	if err != nil {
		t.l.log.Error(err, "Could not stat the slow log")
		t.close()
		return
	}
	truncations := t.l.truncations.Load()
	if truncations != t.truncations || st.size < t.off || (t.lastSize >= 0 && st.size < t.lastSize) {
		// The watchdog truncated a file; what was held is gone. When the
		// truncated file is not this one, re-reading from 0 only repeats work
		// the parse would redo anyway, so a single rule covers both files.
		t.truncations = truncations
		t.off, t.lastSize, t.held = 0, -1, nil
		t.publish()
	}
	if st.size != t.lastSize {
		t.parse(ctx, st.size)
		return
	}
	// The file stopped growing, so the held entry is complete.
	if t.held != nil {
		t.l.emit(t.held)
		t.held = nil
		t.off = st.size
		t.publish()
	}
	if away && flushed {
		t.finishRotated()
	}
}

// parse reads from off to the end of the file, emits every entry but the last
// and holds the last one.
func (t *tailer) parse(ctx context.Context, size int64) {
	var prev *mysqllog.Event
	err := parseFrom(t.f, t.off, func(e *mysqllog.Event) bool {
		if ctx.Err() != nil {
			return false
		}
		if prev != nil {
			t.l.emit(prev)
			t.off = int64(e.Offset)
			t.publish()
		}
		prev = e
		return true
	})
	t.lastSize = size
	t.held = prev
	if prev != nil {
		t.off = int64(prev.Offset)
		t.publish()
		return
	}
	if err != nil {
		// Nothing parsed: skip the region so it cannot stall the tailer.
		t.l.log.Error(err, "Could not parse the slow log, skipping to its end", "offset", t.off)
		t.l.dropped.Add(uint64(size - t.off))
		t.off = size
		t.publish()
	}
}

// open opens the rotated file if one is pending, else the active file.
func (t *tailer) open() bool {
	for _, path := range []string{t.l.rotatedPath(), t.l.activePath()} {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		st, err := statFile(f)
		if err != nil || st.ino == t.skipIno {
			_ = f.Close()
			continue
		}
		t.f, t.ino, t.off, t.lastSize, t.held = f, st.ino, 0, -1, nil
		if t.resume.Inode == st.ino && t.resume.Offset <= st.size {
			t.off = t.resume.Offset
		}
		t.resume = Cursor{}
		t.publish()
		return true
	}
	return false
}

// rotatedAway reports whether the active path no longer names the open file.
func (t *tailer) rotatedAway() bool {
	st, ok := statPath(t.l.activePath())
	return !ok || st.ino != t.ino
}

// finishRotated closes a rotated file mysqld no longer writes and unlinks it.
func (t *tailer) finishRotated() {
	if st, ok := statPath(t.l.rotatedPath()); ok && st.ino == t.ino {
		if err := os.Remove(t.l.rotatedPath()); err != nil {
			t.l.log.Error(err, "Could not delete the rotated slow log")
			t.skipIno = t.ino
		}
	}
	t.close()
	t.l.setCursor(Cursor{})
}

func (t *tailer) publish() { t.l.setCursor(Cursor{Inode: t.ino, Offset: t.off}) }

func (t *tailer) close() {
	if t.f != nil {
		_ = t.f.Close()
		t.f = nil
	}
}
