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
	"slices"
	"strings"
	"testing"
	"time"
)

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func fast(cfg Config) Config {
	cfg.PollInterval = 5 * time.Millisecond
	cfg.CheckInterval = 5 * time.Millisecond
	return cfg
}

func TestRotationUnderLoadIsLossless(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, fast(Config{RotateBytes: 2048}))
	m := newFakeMysqld(t, l.cfg.Dir)
	ctx := t.Context()
	l.Start(ctx, m.flush)

	const n = 300
	for i := range n {
		m.slow(fmt.Sprintf("q%03d", i))
		if i%10 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	eventually(t, func() bool { return len(rec.queries()) >= n }, "not every entry was emitted")
	got := rec.queries()
	for i := range n {
		if c := countOf(got, fmt.Sprintf("SELECT 'q%03d'", i)); c != 1 {
			t.Fatalf("q%03d emitted %d times", i, c)
		}
	}
	if l.Stats().Rotations == 0 {
		t.Fatal("no rotation happened")
	}
	if l.Stats().DroppedBytes != 0 {
		t.Fatalf("dropped %d bytes", l.Stats().DroppedBytes)
	}
}

func countOf(s []string, v string) int {
	n := 0
	for _, x := range s {
		if x == v {
			n++
		}
	}
	return n
}

func TestStalledEmitterStaysUnderHardCap(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, fast(Config{RotateBytes: 2048, HardCapBytes: 8192}))
	rec.block = make(chan struct{})
	m := newFakeMysqld(t, l.cfg.Dir)
	ctx := t.Context()
	l.Start(ctx, m.flush)

	for i := range 400 {
		m.slow(fmt.Sprintf("s%03d", i))
		if i%20 == 0 {
			time.Sleep(2 * time.Millisecond)
		}
	}
	eventually(t, func() bool { return l.totalBytes() < 8192 }, "slow log stayed over the hard cap")
	eventually(t, func() bool { return l.Stats().DroppedBytes > 0 }, "no drop was counted")
	close(rec.block)
}

func TestDrainLeftovers(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, Config{})
	m := newFakeMysqld(t, l.cfg.Dir)
	m.slow("a")
	m.slow("b")
	if err := os.Rename(l.activePath(), l.rotatedPath()); err != nil {
		t.Fatal(err)
	}
	_ = m.flush(context.Background())
	m.slow("c")

	l.DrainLeftovers()
	if got := rec.queries(); !slices.Equal(got, sel("a", "b", "c")) {
		t.Fatalf("queries = %q", got)
	}
	for _, p := range []string{l.activePath(), l.rotatedPath()} {
		if _, ok := statPath(p); ok {
			t.Errorf("%s left behind", p)
		}
	}
}

func TestDrainLeftoversOverHardCapDeletesUnread(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, Config{HardCapBytes: 100})
	m := newFakeMysqld(t, l.cfg.Dir)
	m.slow("a")
	st, _ := statPath(l.activePath())

	l.DrainLeftovers()
	if len(rec.queries()) != 0 {
		t.Fatal("emitted entries from a log over the hard cap")
	}
	if _, ok := statPath(l.activePath()); ok {
		t.Fatal("leftover not deleted")
	}
	if l.Stats().DroppedBytes != uint64(st.size) {
		t.Fatalf("dropped = %d, want %d", l.Stats().DroppedBytes, st.size)
	}
}

func TestReExecHandoff(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, fast(Config{}))
	m := newFakeMysqld(t, l.cfg.Dir)
	ctx := t.Context()
	l.Start(ctx, m.flush)

	m.slow("a")
	eventually(t, func() bool { return len(rec.queries()) == 1 }, "a not emitted")
	env := l.PrepareReExec()
	if len(env) != 1 || !strings.HasPrefix(env[0], CursorEnv+"=") {
		t.Fatalf("PrepareReExec = %q", env)
	}

	// The exec fails: the tailer resumes where it stopped.
	m.slow("b")
	time.Sleep(50 * time.Millisecond)
	if len(rec.queries()) != 1 {
		t.Fatal("tailer kept running after PrepareReExec")
	}
	l.ReExecFailed()
	eventually(t, func() bool { return len(rec.queries()) == 2 }, "b not emitted after ReExecFailed")

	// The exec succeeds: a new image resumes from the handed-over cursor.
	env = l.PrepareReExec()
	c, ok := ParseCursor(strings.TrimPrefix(env[0], CursorEnv+"="))
	if !ok {
		t.Fatalf("bad cursor %q", env[0])
	}
	m.slow("c")
	l2, rec2 := newTestLog(t, fast(Config{Dir: l.cfg.Dir, Cursor: c}))
	l2.Start(ctx, m.flush)
	eventually(t, func() bool { return len(rec2.queries()) == 1 }, "c not emitted by the new image")
	time.Sleep(50 * time.Millisecond)
	if got := rec2.queries(); !slices.Equal(got, sel("c")) {
		t.Fatalf("new image emitted %q, want only c", got)
	}
}

// An upgrade can happen between the rename and the flush: the new image drains
// .1, re-issues the flush and carries on with the active file.
func TestResumeFinishesAPendingRotation(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, fast(Config{}))
	m := newFakeMysqld(t, l.cfg.Dir)
	m.slow("a")
	if err := os.Rename(l.activePath(), l.rotatedPath()); err != nil {
		t.Fatal(err)
	}
	m.slow("b") // mysqld still writes the renamed file: no flush yet

	ctx := t.Context()
	l.Start(ctx, m.flush)
	eventually(t, func() bool { _, ok := statPath(l.rotatedPath()); return !ok }, ".1 never finished")
	m.slow("c")
	eventually(t, func() bool { return len(rec.queries()) == 3 }, "entries missing")
	if got := rec.queries(); !slices.Equal(got, sel("a", "b", "c")) {
		t.Fatalf("queries = %q", got)
	}
}

// A tailer stuck writing a record outlives Stop's timeout. If the exec then
// fails, the resumed tailer must not run next to the stuck one: both would
// emit the stuck record, and both watchdogs would race on their state.
func TestReExecFailedWaitsForTheStuckTailer(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, fast(Config{}))
	rec.block = make(chan struct{})
	m := newFakeMysqld(t, l.cfg.Dir)
	l.Start(t.Context(), m.flush)

	m.slow("a")
	m.slow("b")
	time.Sleep(50 * time.Millisecond) // the tailer is now blocked emitting a
	l.Stop(10 * time.Millisecond)
	l.ReExecFailed()
	time.Sleep(50 * time.Millisecond)
	close(rec.block)

	eventually(t, func() bool { return len(rec.queries()) >= 2 }, "entries not emitted")
	time.Sleep(100 * time.Millisecond)
	if got := rec.queries(); !slices.Equal(got, sel("a", "b")) {
		t.Fatalf("queries = %q, want each entry once", got)
	}
}
