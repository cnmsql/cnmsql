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
)

// watch enforces the soft threshold and the hard cap. It never waits on the
// tailer, so the bound holds even when emitting records blocks.
func (l *Log) watch(ctx context.Context) {
	tick := time.NewTicker(l.cfg.CheckInterval)
	defer tick.Stop()
	for {
		l.check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (l *Log) check(ctx context.Context) {
	// The cap comes first: a flush can wait up to flushTimeout on a slow
	// control connection, and the volume's headroom is sized for one check.
	l.enforceHardCap()
	_, pending := statPath(l.rotatedPath())
	active, _ := statPath(l.activePath())
	switch {
	case pending && !l.isFlushed():
		l.tryFlush(ctx)
	case !pending && active.size >= l.cfg.RotateBytes:
		l.rotate(ctx)
	}
	l.bytes.Store(l.totalBytes())
}

func (l *Log) rotate(ctx context.Context) {
	// Mark the flush pending before the rename, so a tailer that sees the
	// rename also sees that mysqld may still be writing the renamed file.
	l.setFlushed(false)
	if err := os.Rename(l.activePath(), l.rotatedPath()); err != nil {
		l.setFlushed(true)
		l.log.Error(err, "Could not rotate the slow log")
		return
	}
	l.tryFlush(ctx)
}

func (l *Log) tryFlush(ctx context.Context) {
	if l.flush == nil {
		return
	}
	fctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()
	if err := l.flush(fctx); err != nil {
		if !l.flushFailing {
			l.log.Error(err, "Could not flush the slow log, retrying")
			l.flushFailing = true
		}
		return
	}
	if l.flushFailing {
		l.log.Info("Flushed the slow log after earlier failures")
		l.flushFailing = false
	}
	l.setFlushed(true)
	l.rotations.Add(1)
}

// enforceHardCap truncates the rotated file, then the active one, until all
// slow log files together are under the hard cap. mysqld opens the log with
// O_APPEND, so it keeps writing at the new end of a truncated file.
func (l *Log) enforceHardCap() {
	for _, path := range []string{l.rotatedPath(), l.activePath()} {
		if l.totalBytes() < l.cfg.HardCapBytes {
			return
		}
		st, ok := statPath(path)
		if !ok || st.size == 0 {
			continue
		}
		if err := os.Truncate(path, 0); err != nil {
			l.log.Error(err, "Could not truncate the slow log", "file", path)
			continue
		}
		l.truncations.Add(1)
		unread := l.unread(st)
		l.dropped.Add(uint64(unread))
		l.log.Info("Truncated the slow log to stay under its size cap", "file", path, "droppedBytes", unread)
	}
}

func (l *Log) totalBytes() int64 {
	active, _ := statPath(l.activePath())
	rotated, _ := statPath(l.rotatedPath())
	return active.size + rotated.size
}

// unread is how much of a file the tailer has not emitted yet.
func (l *Log) unread(st fileStat) int64 {
	if c := l.Cursor(); c.Inode == st.ino && c.Offset <= st.size {
		return st.size - c.Offset
	}
	return st.size
}
