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
	"slices"
	"strings"
	"testing"
)

func expectQueries(t *testing.T, rec *recorder, want ...string) {
	t.Helper()
	if got := rec.queries(); !slices.Equal(got, sel(want...)) {
		t.Fatalf("queries = %q, want %q", got, sel(want...))
	}
}

// The tailer holds the last entry of a parse, because mysqld may still be
// writing it, and emits it once the file stops growing.
func TestTailerHoldsLastEntryUntilFileStopsGrowing(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, Config{})
	m := newFakeMysqld(t, l.cfg.Dir)
	tl := newTailer(l, Cursor{})
	ctx := context.Background()

	m.slow("a")
	m.slow("b")
	tl.poll(ctx)
	expectQueries(t, rec, "a")
	tl.poll(ctx)
	expectQueries(t, rec, "a", "b")
	m.slow("c")
	tl.poll(ctx)
	expectQueries(t, rec, "a", "b")
	tl.poll(ctx)
	expectQueries(t, rec, "a", "b", "c")
}

func TestTailerWaitsForTheFile(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, Config{})
	tl := newTailer(l, Cursor{})
	ctx := context.Background()
	tl.poll(ctx)
	expectQueries(t, rec)

	m := newFakeMysqld(t, l.cfg.Dir)
	m.slow("a")
	tl.poll(ctx)
	tl.poll(ctx)
	expectQueries(t, rec, "a")
}

func TestTailerRestartsAfterTruncation(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, Config{})
	m := newFakeMysqld(t, l.cfg.Dir)
	tl := newTailer(l, Cursor{})
	ctx := context.Background()

	m.slow("a")
	tl.poll(ctx)
	tl.poll(ctx)
	// What the watchdog does: truncate, then record the truncation. "b" refills
	// the file to exactly its old size, so only the counter reveals it.
	if err := os.Truncate(l.activePath(), 0); err != nil {
		t.Fatal(err)
	}
	st, _ := statPath(l.activePath())
	l.noteTruncation(st.ino)
	m.slow("b") // O_APPEND: lands at the new end, offset 0
	tl.poll(ctx)
	tl.poll(ctx)
	expectQueries(t, rec, "a", "b")
}

// A rotation: the tailer keeps reading the renamed inode while mysqld still
// writes it, and only unlinks it once the flush is done and it stopped growing.
func TestTailerFinishesARotation(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, Config{})
	m := newFakeMysqld(t, l.cfg.Dir)
	tl := newTailer(l, Cursor{})
	ctx := context.Background()

	m.slow("a")
	tl.poll(ctx) // a held
	l.setFlushed(false)
	if err := os.Rename(l.activePath(), l.rotatedPath()); err != nil {
		t.Fatal(err)
	}
	m.slow("b") // still the renamed inode: no flush yet
	tl.poll(ctx)
	tl.poll(ctx)
	expectQueries(t, rec, "a", "b")
	if _, ok := statPath(l.rotatedPath()); !ok {
		t.Fatal("rotated file unlinked before the flush")
	}

	_ = m.flush(ctx)
	l.setFlushed(true)
	m.slow("c")
	tl.poll(ctx) // .1 did not grow: finished and unlinked
	if _, ok := statPath(l.rotatedPath()); ok {
		t.Fatal("rotated file still present after the flush")
	}
	tl.poll(ctx)
	tl.poll(ctx)
	expectQueries(t, rec, "a", "b", "c")
}

// The cursor points at the first entry not yet emitted, so a handoff never
// skips the held entry.
func TestTailerCursorPointsAtUnemittedEntry(t *testing.T) {
	t.Parallel()
	l, _ := newTestLog(t, Config{})
	m := newFakeMysqld(t, l.cfg.Dir)
	tl := newTailer(l, Cursor{})
	m.slow("a")
	m.slow("b")
	tl.poll(context.Background()) // a emitted, b held

	raw, _ := os.ReadFile(l.activePath())
	second := strings.Index(string(raw)[1:], "# Time:") + 1
	if got := l.Cursor().Offset; got != int64(second) {
		t.Fatalf("cursor offset = %d, want %d (start of the held entry)", got, second)
	}
}

func TestTailerResumesAtCursor(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, Config{})
	m := newFakeMysqld(t, l.cfg.Dir)
	ctx := context.Background()
	tl := newTailer(l, Cursor{})
	m.slow("a")
	m.slow("b")
	tl.poll(ctx)
	tl.poll(ctx)
	expectQueries(t, rec, "a", "b")

	l2, rec2 := newTestLog(t, Config{Dir: l.cfg.Dir})
	tl2 := newTailer(l2, l.Cursor())
	m.slow("c")
	tl2.poll(ctx)
	tl2.poll(ctx)
	expectQueries(t, rec2, "c")
}

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	c := Cursor{Inode: 1234, Offset: 5678}
	got, ok := ParseCursor(c.String())
	if !ok || got != c {
		t.Fatalf("ParseCursor(%q) = %v, %v", c.String(), got, ok)
	}
	for _, bad := range []string{"", "1", "a:1", "1:b", "1:-1"} {
		if _, ok := ParseCursor(bad); ok {
			t.Errorf("ParseCursor(%q) accepted", bad)
		}
	}
}

// A rotated file the tailer never opened, next to the active file it reads, is
// what the open race leaves behind: the tailer missed .1 and opened the file
// the flush created. It must still be drained and deleted, or it blocks every
// later rotation.
func TestTailerDrainsARotatedFileItNeverOpened(t *testing.T) {
	t.Parallel()
	l, rec := newTestLog(t, Config{})
	m := newFakeMysqld(t, l.cfg.Dir)
	tl := newTailer(l, Cursor{})
	ctx := context.Background()

	m.slow("a")
	tl.poll(ctx)
	tl.poll(ctx)
	expectQueries(t, rec, "a")

	stray := "# User@Host: app[app] @ localhost []\n# Query_time: 0.5  Lock_time: 0.0 Rows_sent: 1  Rows_examined: 1\n" +
		"SET timestamp=1791036744;\nSELECT 'missed';\n"
	if err := os.WriteFile(l.rotatedPath(), []byte(stray), 0o600); err != nil {
		t.Fatal(err)
	}
	tl.poll(ctx)
	tl.poll(ctx)
	expectQueries(t, rec, "a", "missed")
	if _, ok := statPath(l.rotatedPath()); ok {
		t.Fatal("the missed rotated file was not deleted")
	}
	m.slow("b")
	tl.poll(ctx)
	tl.poll(ctx)
	expectQueries(t, rec, "a", "missed", "b")
}
