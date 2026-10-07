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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// txns builds one file's boundaries: each GTID d-s-n starts 100 bytes after the
// previous one, the first at offset 4.
func txns(server uint32, seqs ...uint64) []TxnBoundary {
	out := make([]TxnBoundary, len(seqs))
	for i, seq := range seqs {
		out[i] = TxnBoundary{Domain: 0, Server: server, Seq: seq, StartPos: int64(4 + 100*i)}
	}
	return out
}

func seqRange(from, to uint64) []uint64 {
	var out []uint64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

// forkedMariaDBFiles is the lagged-promotion archive as downloaded: the old
// primary (server 1) archived 0-1-201..0-1-225 over two files and the dead
// branch starts at 219; the successor (server 2) re-logged 0-1-201..218 and
// wrote 0-2-219..0-2-230.
func forkedMariaDBFiles() ([]PositionalFile, []ReplaySegment) {
	segs := []ReplaySegment{
		{ServerUUID: "old", Files: []string{"binlog.000001", "binlog.000002"}, GTIDSet: "0-1-225",
			Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 218}}},
		{ServerUUID: "new", Files: []string{"binlog.000001"}, GTIDSet: "0-2-230"},
	}
	newTxns := append(txns(1, seqRange(201, 218)...), txns(2, seqRange(219, 230)...)...)
	for i := range newTxns {
		newTxns[i].StartPos = int64(4 + 100*i)
	}
	files := []PositionalFile{
		{Path: "old_binlog.000001", Segment: "old", Boundaries: txns(1, seqRange(201, 215)...)},
		{Path: "old_binlog.000002", Segment: "old", Boundaries: txns(1, seqRange(216, 225)...)},
		{Path: "new_binlog.000001", Segment: "new", Boundaries: newTxns},
	}
	return files, segs
}

// A fork marks the end of its segment in the domain. The file holding the first
// disowned transaction is capped at it, and the segment's later files are
// dropped, so a whole-file replay can never carry the dead tail.
func TestApplyMariadbForksCutsTheForkedSegment(t *testing.T) {
	t.Parallel()
	files, segs := forkedMariaDBFiles()
	// Put the dead branch's first transaction mid-file: 216..225 has 219 at index 3.
	got, err := ApplyMariadbForks(files, segs, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("files = %+v", got)
	}
	capped := got[1]
	if capped.Path != "old_binlog.000002" || capped.EndPos != 304 || len(capped.Boundaries) != 3 ||
		capped.Boundaries[2].Seq != 218 {
		t.Fatalf("capped file = %+v, want 216..218 ending where 219 starts (304)", capped)
	}
	if !reflect.DeepEqual(got[0], files[0]) || !reflect.DeepEqual(got[2], files[2]) {
		t.Fatal("files outside the dead branch must be untouched")
	}
}

func TestApplyMariadbForksDropsLaterFilesOfTheSegment(t *testing.T) {
	t.Parallel()
	files, segs := forkedMariaDBFiles()
	segs[0].Fork.AfterSeq[0] = 210 // dead branch starts in the first file
	got, err := ApplyMariadbForks(files, segs, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 0, len(got))
	for _, f := range got {
		paths = append(paths, f.Path)
	}
	if !reflect.DeepEqual(paths, []string{"old_binlog.000001", "new_binlog.000001"}) {
		t.Fatalf("paths = %v, want the forked segment's later file dropped", paths)
	}
	if got[0].EndPos != 4+100*10 || got[0].Boundaries[len(got[0].Boundaries)-1].Seq != 210 {
		t.Fatalf("capped file = %+v", got[0])
	}
}

// A file whose first transaction is already disowned contributes nothing.
func TestApplyMariadbForksDropsAFileThatStartsDead(t *testing.T) {
	t.Parallel()
	files, segs := forkedMariaDBFiles()
	segs[0].Fork.AfterSeq[0] = 215
	got, err := ApplyMariadbForks(files, segs, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Path != "old_binlog.000001" || got[0].EndPos != 0 {
		t.Fatalf("files = %+v, want the first file whole and the second dropped", got)
	}
}

// A target on the dead branch selects it: the holding segment is used up to
// the target, every other segment is cut at the fork, so the surviving
// branch's reuse of 219 onwards never replays.
func TestApplyMariadbForksSelectsTheTargetBranch(t *testing.T) {
	t.Parallel()
	files, segs := forkedMariaDBFiles()
	target := engine.MariaDBGTID{Domain: 0, Server: 1, Seq: 222}
	got, err := ApplyMariadbForks(files, segs, 0, &target)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].EndPos != 0 || len(got[1].Boundaries) != 10 {
		t.Fatalf("the holding segment must be kept whole: %+v", got[1])
	}
	survivor := got[2]
	if survivor.EndPos != 4+100*18 || survivor.Boundaries[len(survivor.Boundaries)-1].Seq != 218 {
		t.Fatalf("the surviving segment must be cut at the fork: %+v", survivor)
	}
	if err := CheckMariadbAuthors(got, 0, 200); err != nil {
		t.Fatalf("a selected branch must not conflict: %v", err)
	}
	chunks, err := PlanMariadbPositionalFiles(got, 0, 200, 222)
	if err != nil {
		t.Fatal(err)
	}
	if last := chunks[len(chunks)-1]; last.Files[0] != "old_binlog.000002" || last.StopPosition != 4+100*7 {
		t.Fatalf("last chunk = %+v, want old_binlog.000002 stopped before 223", last)
	}
}

// A target on the surviving branch is unaffected by branch selection.
func TestApplyMariadbForksSurvivingTargetKeepsTheCut(t *testing.T) {
	t.Parallel()
	files, segs := forkedMariaDBFiles()
	target := engine.MariaDBGTID{Domain: 0, Server: 2, Seq: 225}
	got, err := ApplyMariadbForks(files, segs, 0, &target)
	if err != nil {
		t.Fatal(err)
	}
	if got[1].EndPos != 304 || got[2].EndPos != 0 {
		t.Fatalf("files = %+v", got)
	}
}

// Without the fork record the downloaded files carry two authors at the same
// sequence: the backstop refuses to splice them.
func TestCheckMariadbAuthorsRefusesAnUnexplainedFork(t *testing.T) {
	t.Parallel()
	files, _ := forkedMariaDBFiles()
	if err := CheckMariadbAuthors(files, 0, 200); !errors.Is(err, ErrForkedTimeline) {
		t.Fatalf("err = %v, want ErrForkedTimeline", err)
	}
	// Below the anchor nothing replays, so a conflict there does not matter.
	if err := CheckMariadbAuthors(files, 0, 225); err != nil {
		t.Fatalf("conflicts at or below the anchor must be ignored: %v", err)
	}
}

// The failover re-log puts the same (seq, server) in two segments: not a fork.
func TestCheckMariadbAuthorsAcceptsTheFailoverRelog(t *testing.T) {
	t.Parallel()
	files := []PositionalFile{
		{Path: "a", Boundaries: txns(1, seqRange(1, 20)...)},
		{Path: "b", Boundaries: append(txns(1, seqRange(15, 20)...), txns(2, 21, 22)...)},
	}
	if err := CheckMariadbAuthors(files, 0, 0); err != nil {
		t.Fatal(err)
	}
}

// A capped file is never coalesced into a whole-file chunk: it is its own chunk
// with a stop offset, even when it is not the target file.
func TestPlanMariadbPositionalFilesHonoursCaps(t *testing.T) {
	t.Parallel()
	files, segs := forkedMariaDBFiles()
	// A successor provisioned by clone re-logs nothing: its binlog starts with
	// its own 0-2-219, so the capped file is the only source of 216..218.
	files[2].Boundaries = txns(2, seqRange(219, 230)...)
	cut, err := ApplyMariadbForks(files, segs, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckMariadbAuthors(cut, 0, 200); err != nil {
		t.Fatal(err)
	}
	target, ok := HighestMariadbSeq(cut, 0)
	if !ok || target != 230 {
		t.Fatalf("highest = %d, %v", target, ok)
	}
	chunks, err := PlanMariadbPositionalFiles(cut, 0, 200, target)
	if err != nil {
		t.Fatal(err)
	}
	want := []ReplayChunk{
		{Files: []string{"old_binlog.000001"}, StartPosition: 4},
		{Files: []string{"old_binlog.000002"}, StartPosition: 4, StopPosition: 304},
		{Files: []string{"new_binlog.000001"}},
	}
	if !reflect.DeepEqual(chunks, want) {
		t.Fatalf("chunks = %+v\nwant     %+v", chunks, want)
	}
}

// PlanMariadbPositional keeps its behavior: it is PlanMariadbPositionalFiles
// over uncapped files.
func TestPlanMariadbPositionalMatchesTheFilesVariant(t *testing.T) {
	t.Parallel()
	b := [][]TxnBoundary{txns(1, 1, 2, 3), txns(1, 4, 5, 6)}
	legacy, err := PlanMariadbPositional([]string{"x", "y"}, b, 0, 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	pf := []PositionalFile{{Path: "x", Boundaries: b[0]}, {Path: "y", Boundaries: b[1]}}
	files, err := PlanMariadbPositionalFiles(pf, 0, 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacy, files) {
		t.Fatalf("legacy %+v != files %+v", legacy, files)
	}
}

func mariadbForkIndex() *objectstore.ArchiveIndex {
	return &objectstore.ArchiveIndex{
		Segments: []objectstore.ArchiveSegment{
			{ServerUUID: "old", Binlogs: []string{"binlog.000001", "binlog.000002"}, StartGTIDSet: "0-1-201", GTIDSet: "0-1-225",
				Fork: &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 218}, DetectedAt: time.Unix(1700000000, 0).UTC()}},
			{ServerUUID: "new", Binlogs: []string{"binlog.000001"}, StartGTIDSet: "0-1-201", GTIDSet: "0-2-230"},
		},
		ForkCheck: &objectstore.ArchiveForkCheck{CheckedAt: time.Unix(1700000000, 0).UTC()},
	}
}

// Time and latest recoveries on a single-domain MariaDB archive now run
// positionally, which is what lets them skip a fork at all.
func TestPrepareMariadbPositionalTimeAndLatest(t *testing.T) {
	t.Parallel()
	when := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	for name, target := range map[string]RecoveryTarget{"latest": {}, "time": {Time: &when}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, err := PrepareMariadbPositional(mariadbForkIndex(), "0-1-200", target)
			if err != nil {
				t.Fatal(err)
			}
			if !p.Enabled || p.Domain != 0 || p.AnchorSeq != 200 || p.TargetSeq != 0 || p.Target != nil {
				t.Fatalf("setup = %+v", p)
			}
			if len(p.Segments) == 0 {
				t.Fatal("segments must be selected")
			}
		})
	}
}

func TestPrepareMariadbPositionalTargetGTID(t *testing.T) {
	t.Parallel()
	p, err := PrepareMariadbPositional(mariadbForkIndex(), "0-1-200", RecoveryTarget{GTID: "0-1-222"})
	if err != nil {
		t.Fatal(err)
	}
	if !p.Enabled || p.TargetSeq != 222 || p.Target == nil || p.Target.Server != 1 {
		t.Fatalf("setup = %+v", p)
	}
	// The dead branch lives only in the old segment: it must be downloaded,
	// even though the successor's segment reaches further.
	if len(p.Segments) != 1 || p.Segments[0].ServerUUID != "old" {
		t.Fatalf("segments = %+v, want the holding segment", p.Segments)
	}
	survivor, err := PrepareMariadbPositional(mariadbForkIndex(), "0-1-200", RecoveryTarget{GTID: "0-2-225"})
	if err != nil {
		t.Fatal(err)
	}
	if len(survivor.Segments) != 1 || survivor.Segments[0].ServerUUID != "new" {
		t.Fatalf("segments = %+v, want the surviving segment", survivor.Segments)
	}
}

// PlanMariadbReplay chains the fork cut, the author backstop and the chunk
// plan; a latest target is the highest sequence the cut leaves.
func TestPlanMariadbReplay(t *testing.T) {
	t.Parallel()
	files, segs := forkedMariaDBFiles()
	plan := ReplayPlan{Segments: segs, MariaDBDomain: 0}
	chunks, err := PlanMariadbReplay(plan, files, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) == 0 || chunks[len(chunks)-1].Files[0] != "new_binlog.000001" {
		t.Fatalf("chunks = %+v", chunks)
	}
	if chunks, err := PlanMariadbReplay(plan, files, 230); err != nil || len(chunks) != 0 {
		t.Fatalf("an anchor at the latest point replays nothing: %+v %v", chunks, err)
	}
	unrecorded := ReplayPlan{Segments: []ReplaySegment{{ServerUUID: "old"}, {ServerUUID: "new"}}}
	if _, err := PlanMariadbReplay(unrecorded, files, 200); !errors.Is(err, ErrForkedTimeline) {
		t.Fatalf("an unrecorded fork must fail closed, err = %v", err)
	}
}

func TestPrepareMariadbPositionalRefusesADeadBranchBackup(t *testing.T) {
	t.Parallel()
	_, err := PrepareMariadbPositional(mariadbForkIndex(), "0-1-220", RecoveryTarget{})
	if !errors.Is(err, ErrBackupOnDeadBranch) {
		t.Fatalf("err = %v, want ErrBackupOnDeadBranch", err)
	}
	// The successor's own 220 is canonical.
	if _, err := PrepareMariadbPositional(mariadbForkIndex(), "0-2-220", RecoveryTarget{}); err != nil {
		t.Fatalf("a backup on the surviving branch must proceed: %v", err)
	}
	// An explicit target containing the anchor proceeds.
	if _, err := PrepareMariadbPositional(mariadbForkIndex(), "0-1-220", RecoveryTarget{GTID: "0-1-222"}); err != nil {
		t.Fatalf("targetGTID must proceed: %v", err)
	}
}

func TestPrepareMariadbPositionalMultiDomain(t *testing.T) {
	t.Parallel()
	idx := mariadbForkIndex()
	idx.Segments[1].GTIDSet = "0-2-230,1-2-5"
	if _, err := PrepareMariadbPositional(idx, "0-1-200", RecoveryTarget{}); !errors.Is(err, ErrForkedTimeline) {
		t.Fatalf("a multi-domain archive with a fork must fail closed, err = %v", err)
	}
	idx.Segments[0].Fork = nil
	p, err := PrepareMariadbPositional(idx, "0-1-200", RecoveryTarget{})
	if err != nil || p.Enabled {
		t.Fatalf("a multi-domain archive without a fork keeps the concatenation path: %+v %v", p, err)
	}
}

// A GTID-less archive (old 10.11 backups) has no ranges to plan on.
func TestPrepareMariadbPositionalGTIDLessArchive(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{
		{ServerUUID: "a", Binlogs: []string{"binlog.000001"}},
	}}
	p, err := PrepareMariadbPositional(idx, "", RecoveryTarget{})
	if err != nil || p.Enabled {
		t.Fatalf("setup = %+v, err = %v", p, err)
	}
}

// A dead branch the archive recorded at index level refuses the backup even
// once retention dropped the segment that held it.
func TestPrepareMariadbPositionalRefusesABackupOnARecordedDeadBranch(t *testing.T) {
	t.Parallel()
	idx := mariadbForkIndex()
	for i := range idx.Segments {
		idx.Segments[i].Fork = nil
	}
	idx.Disowned = &objectstore.ArchiveDisowned{Ranges: []objectstore.ArchiveDisownedRange{{Domain: 0, Server: 1, After: 218, Through: 225}}}
	if _, err := PrepareMariadbPositional(idx, "0-1-220", RecoveryTarget{}); !errors.Is(err, ErrBackupOnDeadBranch) {
		t.Fatalf("err = %v, want ErrBackupOnDeadBranch", err)
	}
	if _, err := PrepareMariadbPositional(idx, "0-2-220", RecoveryTarget{}); errors.Is(err, ErrBackupOnDeadBranch) {
		t.Fatalf("a backup on the surviving branch was refused: %v", err)
	}
}

// A time target becomes a sequence bound: a transaction stamped past the
// target stops replay there, even when a later file (another primary's,
// whose clock lags) carries earlier stamps.
func TestMariaDBTimeTargetIsAPrefix(t *testing.T) {
	t.Parallel()
	at := func(sec int) time.Time { return time.Date(2026, 10, 7, 12, 0, sec, 0, time.UTC) }
	files := []PositionalFile{
		{Path: "a", Segment: "a", Boundaries: []TxnBoundary{
			{Seq: 11, Server: 1, StartPos: 4, Time: at(1)},
			{Seq: 12, Server: 1, StartPos: 100, Time: at(30)},
		}},
		{Path: "b", Segment: "b", Boundaries: []TxnBoundary{
			{Seq: 13, Server: 2, StartPos: 4, Time: at(5)},
		}},
	}
	plan := ReplayPlan{Segments: []ReplaySegment{{ServerUUID: "a"}, {ServerUUID: "b"}}, TargetTime: at(10)}
	chunks, err := PlanMariadbReplay(plan, files, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0].Files[0] != "a" || chunks[0].StopPosition != 100 {
		t.Fatalf("chunks = %+v, want file a stopped before seq 12", chunks)
	}
}
