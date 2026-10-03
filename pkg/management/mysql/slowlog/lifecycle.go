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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/slowlog/internal/slowparse"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/diskusage"
)

// Start runs the tailer and the watchdog until ctx ends. flush reopens
// mysqld's slow log after a rotation.
func (l *Log) Start(ctx context.Context, flush Flusher) {
	l.mu.Lock()
	l.parent, l.flush = ctx, flush
	l.mu.Unlock()
	l.run(func() Cursor { return l.cfg.Cursor }, nil)
}

// run starts a tailer and a watchdog once after is closed, resuming at the
// cursor resume returns then. The run's context is registered right away, so
// a Stop issued while it waits still cancels it.
func (l *Log) run(resume func() Cursor, after <-chan struct{}) {
	ctx, cancel := context.WithCancel(l.parent)
	done := make(chan struct{})
	l.mu.Lock()
	l.cancel, l.done = cancel, done
	l.mu.Unlock()

	go func() {
		defer close(done)
		// A previous run that outlived Stop's timeout must finish first: two
		// tailers would emit the same entries and two watchdogs would race.
		if after != nil {
			<-after
		}
		if ctx.Err() != nil {
			return
		}
		// A rotated file left by a previous image may not have been flushed
		// yet; flushing again is harmless.
		_, pending := statPath(l.rotatedPath())
		l.setFlushed(!pending)

		t := newTailer(l, resume())
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); t.run(ctx) }()
		go func() { defer wg.Done(); l.watch(ctx) }()
		wg.Wait()
	}()
}

// Stop stops the tailer and the watchdog and returns the tailer's cursor. It
// waits at most timeout: a tailer blocked emitting a record hands over the
// cursor of that record, which is then emitted again.
func (l *Log) Stop(timeout time.Duration) Cursor {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.cancel = nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
		select {
		case <-done:
		case <-time.After(timeout):
			l.log.Info("Slow log tailer did not stop in time, handing over its last position")
		}
	}
	return l.Cursor()
}

// PrepareReExec stops tailing before an in-place re-exec and returns the
// environment entry that hands the cursor to the new image.
func (l *Log) PrepareReExec() []string {
	return []string{CursorEnv + "=" + l.Stop(reExecStopTimeout).String()}
}

// ReExecFailed resumes tailing when the re-exec did not happen, once the
// stopped run has finished, from where it left off.
func (l *Log) ReExecFailed() {
	l.mu.Lock()
	previous := l.done
	l.mu.Unlock()
	l.run(l.Cursor, previous)
}

// DrainLeftovers emits and deletes the slow log files a previous run left on
// the volume, which survives a container restart. Call it before mysqld
// starts. Above the hard cap the files are deleted unread.
func (l *Log) DrainLeftovers() {
	total := l.totalBytes()
	for _, path := range []string{l.rotatedPath(), l.activePath()} {
		st, ok := statPath(path)
		if !ok {
			continue
		}
		if total > l.cfg.HardCapBytes {
			l.dropped.Add(uint64(st.size))
			l.log.Info("Deleted an unread slow log over the size cap", "file", path, "droppedBytes", st.size)
		} else {
			l.drainFile(path)
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			l.log.Error(err, "Could not delete a leftover slow log", "file", path)
		}
	}
	l.checkReserve()
}

func (l *Log) drainFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		l.log.Error(err, "Could not open a leftover slow log", "file", path)
		return
	}
	defer func() { _ = f.Close() }()
	if err := parseFrom(f, 0, func(e *slowparse.Event) bool {
		l.emit(e)
		return true
	}); err != nil {
		l.log.Error(err, "Could not parse a leftover slow log", "file", path)
	}
}

// checkReserve logs what fills the run volume when less than the reserve is
// free. MySQL cannot start without room for its socket lock file.
func (l *Log) checkReserve() {
	usage, err := diskusage.Of(l.cfg.Dir)
	if err != nil || usage.AvailableBytes >= l.cfg.ReserveBytes {
		return
	}
	entries, _ := os.ReadDir(l.cfg.Dir)
	files := make([]string, 0, len(entries))
	for _, de := range entries {
		if info, err := de.Info(); err == nil {
			files = append(files, fmt.Sprintf("%s=%d", de.Name(), info.Size()))
		}
	}
	l.log.Info("Run volume has less free space than its reserve",
		"dir", l.cfg.Dir, "availableBytes", usage.AvailableBytes,
		"reserveBytes", l.cfg.ReserveBytes, "files", files)
}
