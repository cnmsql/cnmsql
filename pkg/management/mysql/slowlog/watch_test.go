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
	"os"
	"testing"
)

func writeUntil(m *fakeMysqld, path string, size int64) {
	for {
		if st, ok := statPath(path); ok && st.size >= size {
			return
		}
		m.slow("fill")
	}
}

func TestCheckRotatesAtThreshold(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, Config{RotateBytes: 512})
	m := newFakeMysqld(t, l.cfg.Dir)
	l.flush = m.flush
	writeUntil(m, l.activePath(), 512)

	l.check(context.Background())
	if _, ok := statPath(l.rotatedPath()); !ok {
		t.Fatal("no rotated file")
	}
	if st, ok := statPath(l.activePath()); !ok || st.size != 0 {
		t.Fatalf("active file after the flush = %+v, %v; want a fresh empty file", st, ok)
	}
	if !l.isFlushed() || l.Stats().Rotations != 1 {
		t.Fatalf("flushed=%v rotations=%d", l.isFlushed(), l.Stats().Rotations)
	}
	l.check(context.Background()) // .1 pending, flushed: nothing to do
	if l.Stats().Rotations != 1 {
		t.Fatal("rotated again while .1 is still being drained")
	}
}

func TestCheckRetriesAFailedFlush(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, Config{RotateBytes: 512})
	m := newFakeMysqld(t, l.cfg.Dir)
	fail := true
	l.flush = func(ctx context.Context) error {
		if fail {
			return errors.New("mysqld is starting")
		}
		return m.flush(ctx)
	}
	writeUntil(m, l.activePath(), 512)

	l.check(context.Background())
	if l.isFlushed() || l.Stats().Rotations != 0 {
		t.Fatal("a failed flush was counted as done")
	}
	fail = false
	l.check(context.Background())
	if !l.isFlushed() || l.Stats().Rotations != 1 {
		t.Fatal("the flush was not retried")
	}
}

func TestCheckEnforcesHardCap(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, Config{RotateBytes: 1 << 30, HardCapBytes: 2048})
	m := newFakeMysqld(t, l.cfg.Dir)
	writeUntil(m, l.activePath(), 2048)
	st, _ := statPath(l.activePath())

	l.check(context.Background())
	if now, _ := statPath(l.activePath()); now.size != 0 {
		t.Fatalf("active file is %d bytes after the hard cap", now.size)
	}
	if got := l.Stats().DroppedBytes; got != uint64(st.size) {
		t.Fatalf("dropped = %d, want %d", got, st.size)
	}
}

func TestCheckTruncatesRotatedFileFirst(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, Config{RotateBytes: 1 << 30, HardCapBytes: 2048})
	m := newFakeMysqld(t, l.cfg.Dir)
	writeUntil(m, l.activePath(), 2048)
	if err := os.Rename(l.activePath(), l.rotatedPath()); err != nil {
		t.Fatal(err)
	}
	_ = m.flush(context.Background())
	m.slow("kept")

	l.check(context.Background())
	if st, _ := statPath(l.rotatedPath()); st.size != 0 {
		t.Fatal("rotated file not truncated")
	}
	if st, _ := statPath(l.activePath()); st.size == 0 {
		t.Fatal("active file truncated although the total was under the cap")
	}
}

// TestHardCapHoldsWhileFlushHangs: a slow control connection must not delay
// the hard cap, which the run volume's headroom is sized for at one check.
func TestHardCapHoldsWhileFlushHangs(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, Config{RotateBytes: 1 << 30, HardCapBytes: 2048})
	m := newFakeMysqld(t, l.cfg.Dir)
	writeUntil(m, l.activePath(), 2048)
	if err := os.Rename(l.activePath(), l.rotatedPath()); err != nil {
		t.Fatal(err)
	}
	l.setFlushed(false) // a rotation whose flush has not happened yet

	entered, release := make(chan struct{}), make(chan struct{})
	l.flush = func(ctx context.Context) error {
		close(entered)
		<-release
		return errors.New("timed out")
	}
	done := make(chan struct{})
	go func() { l.check(context.Background()); close(done) }()
	<-entered
	st, _ := statPath(l.rotatedPath())
	close(release)
	<-done
	if st.size != 0 {
		t.Fatalf("rotated file was %d bytes while the flush hung, want it truncated first", st.size)
	}
}
