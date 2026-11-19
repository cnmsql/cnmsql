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

package binlog

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

// longSegment is a single-segment plan of n ten-transaction files, the base
// backup holding the first covered of them. Its reader counts manifest reads.
type longSegment struct {
	plan  ReplayPlan
	spans map[ReplayFile]ReplayFileSpan
	reads int
}

func newLongSegment(n, covered int) *longSegment {
	seg := ReplaySegment{ServerUUID: orderPrimary}
	spans := map[ReplayFile]ReplayFileSpan{}
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("binlog.%06d", i)
		seg.Files = append(seg.Files, name)
		spans[ReplayFile{orderPrimary, name}] = span((i-1)*10+1, i*10, i)
	}
	anchor := fmt.Sprintf("%s:1-%d", orderPrimary, covered*10)
	return &longSegment{
		plan:  ReplayPlan{AnchorGTID: anchor, ExcludeGTIDs: anchor, Segments: []ReplaySegment{seg}},
		spans: spans,
	}
}

func (l *longSegment) read(_ context.Context, f ReplayFile) (ReplayFileSpan, error) {
	l.reads++
	s, ok := l.spans[f]
	if !ok {
		return ReplayFileSpan{}, errors.New("no manifest")
	}
	return s, nil
}

func fileNames(files []ReplayFile) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, f.Name)
	}
	return out
}

// A long-lived primary's segment is narrowed to the files the base backup
// does not hold with a handful of manifest reads, not one per file.
func TestNarrowReplayFilesFindsTheStartByBinarySearch(t *testing.T) {
	t.Parallel()
	l := newLongSegment(3000, 2985)
	got, _, err := NarrowReplayFiles(context.Background(), l.plan, l.read)
	if err != nil {
		t.Fatal(err)
	}
	want := l.plan.Segments[0].Files[2985:]
	if names := fileNames(got.Files()); !slices.Equal(names, want) {
		t.Fatalf("files = %v, want the last 15", names)
	}
	if l.reads > 16 {
		t.Fatalf("read %d manifests for 3000 files, want a binary search", l.reads)
	}
	if len(l.plan.Segments[0].Files) != 3000 {
		t.Fatal("the caller's plan was modified")
	}
}

// A file the backup holds only in part is the first one downloaded.
func TestNarrowReplayFilesKeepsAPartlyCoveredFile(t *testing.T) {
	t.Parallel()
	l := newLongSegment(100, 50)
	l.plan.AnchorGTID = orderPrimary + ":1-495"
	l.plan.ExcludeGTIDs = l.plan.AnchorGTID
	got, _, err := NarrowReplayFiles(context.Background(), l.plan, l.read)
	if err != nil {
		t.Fatal(err)
	}
	if names := fileNames(got.Files()); names[0] != "binlog.000050" || len(names) != 51 {
		t.Fatalf("files = %v, want binlog.000050 onwards", names)
	}
}

// With a time target the end is found the same way: a file whose first event
// is at or past the target is never reached by mysqlbinlog.
func TestNarrowReplayFilesFindsTheEndByBinarySearch(t *testing.T) {
	t.Parallel()
	l := newLongSegment(3000, 1000)
	l.plan.TargetTime = time.Date(2026, 10, 20, 6, 1200, 0, 0, time.UTC) // binlog.001200's first event
	got, _, err := NarrowReplayFiles(context.Background(), l.plan, l.read)
	if err != nil {
		t.Fatal(err)
	}
	want := l.plan.Segments[0].Files[1000:1199]
	if names := fileNames(got.Files()); !slices.Equal(names, want) {
		t.Fatalf("files = %d from %v to %v, want 199 from binlog.001001 to binlog.001199",
			len(names), names[0], names[len(names)-1])
	}
	if l.reads > 30 {
		t.Fatalf("read %d manifests, want two binary searches", l.reads)
	}
}

// A file with no transactions among the covered ones is neither needed nor
// in the way: the search still lands on the first needed file.
func TestNarrowReplayFilesIgnoresAnEmptyFileInTheCoveredPrefix(t *testing.T) {
	t.Parallel()
	l := newLongSegment(64, 40)
	l.spans[ReplayFile{orderPrimary, "binlog.000020"}] = ReplayFileSpan{}
	l.spans[ReplayFile{orderPrimary, "binlog.000039"}] = ReplayFileSpan{}
	got, _, err := NarrowReplayFiles(context.Background(), l.plan, l.read)
	if err != nil {
		t.Fatal(err)
	}
	names := fileNames(got.Files())
	if slices.Index(names, "binlog.000041") < 0 || slices.Contains(names, "binlog.000020") {
		t.Fatalf("files = %v, want binlog.000041 kept and the empty binlog.000020 left out", names)
	}
}

// The spans read during the search are returned, so the caller does not read
// them again to order the files.
func TestNarrowReplayFilesReturnsWhatItRead(t *testing.T) {
	t.Parallel()
	l := newLongSegment(8, 4)
	_, spans, err := NarrowReplayFiles(context.Background(), l.plan, l.read)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != l.reads || len(spans) == 0 {
		t.Fatalf("returned %d spans after %d reads", len(spans), l.reads)
	}
}

// An unreadable manifest fails the narrowing rather than guess.
func TestNarrowReplayFilesReportsAnUnreadableManifest(t *testing.T) {
	t.Parallel()
	l := newLongSegment(8, 4)
	delete(l.spans, ReplayFile{orderPrimary, "binlog.000004"})
	_, _, err := NarrowReplayFiles(context.Background(), l.plan, l.read)
	if err == nil {
		t.Fatal("expected an error for a missing manifest")
	}
}

// A plan whose anchor does not parse is replayed as planned, reading nothing.
func TestNarrowReplayFilesLeavesAnUnparsablePlanAlone(t *testing.T) {
	t.Parallel()
	l := newLongSegment(8, 4)
	l.plan.AnchorGTID = "not-a-gtid-set"
	got, _, err := NarrowReplayFiles(context.Background(), l.plan, l.read)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Files()) != 8 || l.reads != 0 {
		t.Fatalf("files = %d, reads = %d; want all eight and no read", len(got.Files()), l.reads)
	}
}

// Disowned transactions join the exclude set but sit in the middle of a
// segment (a dead branch, before the old primary re-logged its successor), so
// "fully excluded" is not a prefix. The search judges files by the base
// backup alone, and the files before the dead branch, which hold the CREATE
// TABLE every later file needs, are kept.
func TestNarrowReplayFilesIsNotMisledByADisownedStretch(t *testing.T) {
	t.Parallel()
	l := newLongSegment(3, 0)
	// binlog.000001 is needed, binlog.000002 (11-20) is disowned, binlog.000003 needed.
	l.plan.AnchorGTID = ""
	l.plan.DisownedGTIDs = orderPrimary + ":11-20"
	l.plan.ExcludeGTIDs = orderPrimary + ":11-20"
	got, _, err := NarrowReplayFiles(context.Background(), l.plan, l.read)
	if err != nil {
		t.Fatal(err)
	}
	if names := fileNames(got.Files()); !slices.Equal(names, []string{"binlog.000001", "binlog.000002", "binlog.000003"}) {
		t.Fatalf("files = %v, want all three", names)
	}
}
