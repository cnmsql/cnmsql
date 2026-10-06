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
	"fmt"
	"slices"
	"sort"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// PositionalFile is one downloaded binlog of a MariaDB positional replay: its
// local path, the archive segment it came from, its transaction boundaries,
// and EndPos, the offset replay must stop at (0 = end of file) when a fork cut
// the file short.
type PositionalFile struct {
	Path       string
	Segment    string
	Boundaries []TxnBoundary
	EndPos     int64
}

// segmentAuthor is the server that authored a segment's last transaction in
// domain, read from its position.
func segmentAuthor(seg ReplaySegment, domain uint32) (uint32, uint64, bool) {
	gtids, err := engine.ParseMariaDBPosition(seg.GTIDSet)
	if err != nil {
		return 0, 0, false
	}
	for _, g := range gtids {
		if g.Domain == domain {
			return g.Server, g.Seq, true
		}
	}
	return 0, 0, false
}

// forkCut is the sequence a segment's fork record cuts it at in domain.
func forkCut(seg ReplaySegment, domain uint32) (uint64, bool) {
	if seg.Fork == nil {
		return 0, false
	}
	cut, ok := seg.Fork.AfterSeq[domain]
	return cut, ok
}

// branchHolder returns the segment whose fork record holds target, or "" when
// the target is on the surviving timeline.
func branchHolder(segs []ReplaySegment, domain uint32, target *engine.MariaDBGTID) (string, uint64) {
	if target == nil {
		return "", 0
	}
	for _, seg := range segs {
		cut, ok := forkCut(seg, domain)
		if !ok {
			continue
		}
		server, end, ok := segmentAuthor(seg, domain)
		if ok && server == target.Server && target.Seq > cut && target.Seq <= end {
			return seg.ServerUUID, cut
		}
	}
	return "", 0
}

// ApplyMariadbForks applies the planned segments' fork records to the
// downloaded files. A fork marks the end of its segment in the domain: the
// file holding the first disowned transaction is cut at it (EndPos) and the
// segment's later files are dropped. Skipping disowned boundaries like
// already-applied ones would not do, because files replay whole.
//
// A target whose (server, seq) falls in a fork selects that branch instead: the
// holding segment is kept up to the target and every other segment is cut at
// the fork, so the surviving branch's reuse of those sequences never replays.
// A target on the surviving branch changes nothing.
func ApplyMariadbForks(
	files []PositionalFile, segs []ReplaySegment, domain uint32, target *engine.MariaDBGTID,
) ([]PositionalFile, error) {
	cuts := map[string]uint64{}
	holder, branchCut := branchHolder(segs, domain, target)
	for _, seg := range segs {
		if holder != "" {
			if seg.ServerUUID != holder {
				cuts[seg.ServerUUID] = branchCut
				if own, ok := forkCut(seg, domain); ok && own < branchCut {
					cuts[seg.ServerUUID] = own
				}
			}
			continue
		}
		if cut, ok := forkCut(seg, domain); ok {
			cuts[seg.ServerUUID] = cut
		}
	}

	exhausted := map[string]bool{}
	out := make([]PositionalFile, 0, len(files))
	for _, f := range files {
		cut, ok := cuts[f.Segment]
		if !ok {
			out = append(out, f)
			continue
		}
		if exhausted[f.Segment] {
			continue
		}
		first := -1
		for i, b := range f.Boundaries {
			if b.Domain == domain && b.Seq > cut {
				first = i
				break
			}
		}
		if first < 0 {
			out = append(out, f)
			continue
		}
		exhausted[f.Segment] = true
		kept := f.Boundaries[:first]
		if _, _, any := domainFileStats(kept, domain); !any {
			// The file starts on the dead branch: it contributes nothing.
			continue
		}
		// Stop where the first disowned transaction starts, which excludes it
		// and everything after it in the file.
		f.EndPos = f.Boundaries[first].StartPos
		f.Boundaries = kept
		out = append(out, f)
	}
	return out, nil
}

// CheckMariadbAuthors is the restore backstop for forks the archive check had
// no verdict for. Restore runs without the source Cluster, so it cannot read
// the timeline; but two different servers at the same (domain, seq) in what is
// about to replay can only be a fork. It fails with ErrForkedTimeline rather
// than splice the branches. Sequences at or below anchorSeq never replay and
// are not checked. The failover re-log (same server, same seq) is fine.
func CheckMariadbAuthors(files []PositionalFile, domain uint32, anchorSeq uint64) error {
	authors := map[uint64]uint32{}
	for _, f := range files {
		for _, b := range f.Boundaries {
			if b.Domain != domain || b.Seq <= anchorSeq {
				continue
			}
			if server, ok := authors[b.Seq]; ok && server != b.Server {
				return fmt.Errorf("%w: servers %d and %d both wrote %d-*-%d and no fork record explains it",
					ErrForkedTimeline, server, b.Server, domain, b.Seq)
			}
			authors[b.Seq] = b.Server
		}
	}
	return nil
}

// HighestMariadbSeq returns the highest sequence the files carry in domain.
func HighestMariadbSeq(files []PositionalFile, domain uint32) (uint64, bool) {
	var highest uint64
	var found bool
	for _, f := range files {
		if _, maxSeq, ok := domainFileStats(f.Boundaries, domain); ok && (!found || maxSeq > highest) {
			highest, found = maxSeq, true
		}
	}
	return highest, found
}

// PlanMariadbPositionalFiles turns transaction boundaries into the ordered,
// positionally bounded chunks that replay the domain's transactions with
// sequence in (anchorSeq, targetSeq] exactly once. files are in timeline order.
//
// mariadb-binlog cannot filter by GTID, so replay is bounded by byte offsets: a
// chunk's first file starts at the offset of the first transaction with seq past
// the highest sequence already applied, and the target file stops at the offset of
// the first transaction with seq > targetSeq (so the target is included and nothing
// after it is).
//
// The wrinkle is failover. With log_slave_updates the promoted server re-logs the
// transactions it replicated under their original server_id before appending its
// own, so across a failover two segments carry overlapping domain sequence ranges
// (e.g. server 1 archives 0-1-15..0-1-26 and server 2's binlog re-logs 0-1-15..26
// then continues 0-2-27..). Concatenating whole segments would feed mariadb-binlog
// a non-monotonic stream ("Found out of order GTID"). To avoid that we walk the
// files tracking the highest sequence applied so far and, for each file:
//   - skip it entirely when all its transactions are already applied (the re-logged
//     overlap prefix, or files at/below the anchor);
//   - start a fresh start-position-bounded chunk when it overlaps (its first new
//     transaction is mid-file), because --start-position only applies to a chunk's
//     first file;
//   - otherwise append it to the current chunk (replayed whole, in one invocation).
//
// A file a fork cut short (EndPos) is always its own chunk bounded by
// --stop-position, so it is never replayed whole. The file carrying the target is
// always its own chunk bounded by --stop-position (mysqlbinlog requires a single
// file for a stop offset), unless the target is the last archived transaction, in
// which case it is replayed to EOF with no stop bound.
func PlanMariadbPositionalFiles(
	files []PositionalFile, domain uint32, anchorSeq, targetSeq uint64,
) ([]ReplayChunk, error) {
	maxSeq, hasAny := HighestMariadbSeq(files, domain)
	if !hasAny || maxSeq < targetSeq {
		return nil, ErrTargetBeyondArchive
	}
	if targetSeq < anchorSeq {
		// The target predates the base backup: unreachable by forward replay.
		return nil, ErrTargetBeforeBackup
	}

	// Replay must proceed in ascending sequence order, but the caller passes files
	// grouped by segment (server). Across a failover — and especially a re-init clone,
	// where one server's segment covers a gap in another's — segment order is not
	// sequence order. Sort a working copy by each file's first domain sequence so
	// contiguous runs from different segments stitch together; files with no
	// transactions in this domain sort last (the loop skips them anyway). For inputs
	// already in sequence order (the single-server case) this is a no-op.
	files = sortFilesByDomainSeq(files, domain)

	applied := anchorSeq
	firstReplayed := true
	var chunks []ReplayChunk
	var cur *ReplayChunk
	flush := func() {
		if cur != nil {
			chunks = append(chunks, *cur)
			cur = nil
		}
	}

	for _, f := range files {
		minSeq, fileMax, found := domainFileStats(f.Boundaries, domain)
		if !found || fileMax <= applied {
			// No transactions this replay hasn't already applied: the anchor covers
			// them, or an earlier segment re-logs the same sequences. Skipping keeps
			// the stream monotonic without breaking coalescing of the files around it.
			continue
		}

		overlap := minSeq <= applied // re-logs sequences already applied (failover re-log)

		// A hole between runs: this file's first new sequence is more than one past
		// what we've applied, and it is not the leading edge (nothing replayed yet,
		// where starting above the anchor just means the archive begins later). No
		// downloaded file supplies the missing sequences, so replay cannot proceed
		// monotonically. Segment selection should have caught this before download;
		// fail closed here as a backstop.
		if !firstReplayed && !overlap && minSeq > applied+1 {
			return nil, ErrForkedTimeline
		}

		if fileMax >= targetSeq {
			// This file carries the target. It is its own chunk: a stop offset needs a
			// single file, and even without a stop bound an overlapping target file
			// needs its own start offset.
			stopPos, stopHere := firstAfterInFile(f.Boundaries, domain, targetSeq)
			if !stopHere && f.EndPos > 0 {
				stopPos, stopHere = f.EndPos, true
			}
			if !stopHere && cur != nil && !overlap {
				// Target is this file's last transaction and it continues the current
				// coalescing chunk with no overlap: replay it whole in the same invocation.
				cur.Files = append(cur.Files, f.Path)
				flush()
				return chunks, nil
			}
			flush()
			c := ReplayChunk{Files: []string{f.Path}}
			if overlap || firstReplayed {
				c.StartPosition, _ = firstAfterInFile(f.Boundaries, domain, applied)
			}
			if stopHere {
				c.StopPosition = stopPos
			}
			chunks = append(chunks, c)
			return chunks, nil
		}

		switch {
		case f.EndPos > 0:
			// Cut short by a fork: replay up to the cut only, in a chunk of its own.
			flush()
			startPos, _ := firstAfterInFile(f.Boundaries, domain, applied)
			chunks = append(chunks, ReplayChunk{Files: []string{f.Path}, StartPosition: startPos, StopPosition: f.EndPos})
		case cur != nil && !overlap:
			// Whole file is new transactions below the target: replay it fully.
			cur.Files = append(cur.Files, f.Path)
		default:
			flush()
			startPos, _ := firstAfterInFile(f.Boundaries, domain, applied)
			cur = &ReplayChunk{Files: []string{f.Path}, StartPosition: startPos}
		}
		applied = fileMax
		firstReplayed = false
	}

	// The target's file was never reached even though maxSeq >= targetSeq: the only
	// way here is that every file was skipped as already-applied, i.e. the base
	// backup already covers the target.
	flush()
	return chunks, nil
}

// sortFilesByDomainSeq returns a copy of files reordered by each file's first
// (minimum) sequence in the domain, stably. Files carrying no transaction in
// the domain sort last.
func sortFilesByDomainSeq(files []PositionalFile, domain uint32) []PositionalFile {
	out := append([]PositionalFile(nil), files...)
	sort.SliceStable(out, func(a, b int) bool {
		minA, _, okA := domainFileStats(out[a].Boundaries, domain)
		minB, _, okB := domainFileStats(out[b].Boundaries, domain)
		if okA != okB {
			return okA
		}
		if !okA {
			return false
		}
		return minA < minB
	})
	return out
}

// PlanMariadbReplay plans a MariaDB positional replay from the downloaded
// files: it cuts the planned segments at their fork records (or selects the
// target's branch), refuses an unexplained fork, resolves a time or latest
// target to the highest sequence left, and returns the chunks. No chunk means
// the base backup is already at the target.
func PlanMariadbReplay(plan ReplayPlan, files []PositionalFile, anchorSeq uint64) ([]ReplayChunk, error) {
	cut, err := ApplyMariadbForks(files, plan.Segments, plan.MariaDBDomain, plan.MariaDBTarget)
	if err != nil {
		return nil, err
	}
	if err := CheckMariadbAuthors(cut, plan.MariaDBDomain, anchorSeq); err != nil {
		return nil, err
	}
	target := plan.MariaDBTargetSeq
	if target == 0 {
		highest, ok := HighestMariadbSeq(cut, plan.MariaDBDomain)
		if !ok || highest <= anchorSeq {
			return nil, nil
		}
		target = highest
	}
	return PlanMariadbPositionalFiles(cut, plan.MariaDBDomain, anchorSeq, target)
}

// MariadbPositional is how a MariaDB recovery replays: positionally (bounded by
// byte offsets computed from transaction boundaries) or, when disabled, by
// concatenating files from the anchor.
type MariadbPositional struct {
	// Enabled selects the positional replay.
	Enabled bool
	// Domain is the single replication domain replayed.
	Domain uint32
	// AnchorSeq is the sequence the base backup already holds (0 when its
	// position carries no GTID; the executor then derives it from the binlog).
	AnchorSeq uint64
	// TargetSeq is the targetGTID's sequence, or 0 for a time or latest target.
	TargetSeq uint64
	// Target is the targetGTID as a transaction, nil for time or latest.
	Target *engine.MariaDBGTID
	// Selected reports whether Segments replace the planned segments: the
	// index carries per-segment ranges to select on.
	Selected bool
	// Segments is the minimal set of segments to download.
	Segments []ReplaySegment
}

// PrepareMariadbPositional decides how a MariaDB recovery replays.
//
// A targetGTID always replays positionally, since mariadb-binlog cannot filter
// by GTID. A time or latest target replays positionally on a single-domain
// archive, which is what lets it leave out a recorded fork at all; a
// multi-domain archive keeps the concatenation path when no segment carries a
// fork and fails closed with ErrForkedTimeline when one does. For time and
// latest, a base backup whose anchor falls in a fork was taken on the dead
// branch and fails with ErrBackupOnDeadBranch.
//
// When the index carries per-segment ranges the download is pruned to the
// segments covering (anchor, target], with forked segments capped at their cut
// (or, for a target inside a fork, every segment but the one holding it).
func PrepareMariadbPositional(
	idx *objectstore.ArchiveIndex, anchorGTID string, target RecoveryTarget,
) (MariadbPositional, error) {
	segs := make([]ReplaySegment, len(idx.Segments))
	for i := range idx.Segments {
		segs[i] = replaySegment(&idx.Segments[i])
	}
	domains := archiveDomains(idx)
	var p MariadbPositional

	if target.GTID != "" {
		domain, seq, ok, err := SingleDomainMariaGTID(target.GTID)
		if err != nil {
			return MariadbPositional{}, err
		}
		if !ok {
			return p, nil
		}
		g, err := engine.ParseMariaDBGTID(target.GTID)
		if err != nil {
			return MariadbPositional{}, err
		}
		p = MariadbPositional{Enabled: true, Domain: domain, TargetSeq: seq, Target: &g,
			AnchorSeq: MariaSeqForDomain(anchorGTID, domain)}
	} else {
		switch len(domains) {
		case 0:
			// A GTID-less archive has nothing to plan positionally on.
			return p, nil
		case 1:
			p = MariadbPositional{Enabled: true, Domain: domains[0], AnchorSeq: MariaSeqForDomain(anchorGTID, domains[0])}
		default:
			for _, seg := range segs {
				if seg.Fork != nil {
					return MariadbPositional{}, fmt.Errorf(
						"%w: segment %s of a multi-domain archive holds a recorded fork, which positional replay "+
							"cannot leave out across domains", ErrForkedTimeline, seg.ServerUUID)
				}
			}
			return p, nil
		}
		if err := checkMariadbAnchor(segs, anchorGTID, p.Domain); err != nil {
			return MariadbPositional{}, err
		}
	}

	if !segmentsHaveRanges(idx.Segments) {
		return p, nil
	}
	capped, selectTarget := capSegments(idx.Segments, segs, p)
	if selectTarget <= p.AnchorSeq {
		p.Selected = true
		return p, nil
	}
	selected, err := SelectMariadbSegments(capped, p.Domain, p.AnchorSeq, selectTarget)
	if err != nil {
		return MariadbPositional{}, err
	}
	// Selection ran on capped copies; hand back the real segments (with their
	// fork records) the executor cuts with.
	byUUID := map[string]ReplaySegment{}
	for _, seg := range segs {
		byUUID[seg.ServerUUID] = seg
	}
	for _, s := range selected {
		p.Segments = append(p.Segments, byUUID[s.ServerUUID])
	}
	p.Selected = true
	return p, nil
}

// capSegments returns copies of the index segments whose range end in the
// domain stops at the fork cut, and the sequence segment selection has to
// reach: the target, or for time and latest the highest sequence left.
func capSegments(
	index []objectstore.ArchiveSegment, segs []ReplaySegment, p MariadbPositional,
) ([]objectstore.ArchiveSegment, uint64) {
	holder, branchCut := branchHolder(segs, p.Domain, p.Target)
	capped := make([]objectstore.ArchiveSegment, len(index))
	var highest uint64
	for i, seg := range index {
		capped[i] = seg
		server, end, ok := segmentAuthor(segs[i], p.Domain)
		if !ok {
			continue
		}
		limit, hasLimit := forkCut(segs[i], p.Domain)
		if holder != "" {
			limit, hasLimit = branchCut, seg.ServerUUID != holder
		}
		if hasLimit && limit < end {
			end = limit
			capped[i].GTIDSet = engine.MariaDBGTID{Domain: p.Domain, Server: server, Seq: end}.String()
		}
		if end > highest {
			highest = end
		}
	}
	if p.TargetSeq != 0 {
		return capped, p.TargetSeq
	}
	return capped, highest
}

// checkMariadbAnchor refuses a time or latest recovery from a base backup whose
// anchor (server, seq) falls in a recorded fork: the backup holds a disowned
// transaction, and replay cannot remove it.
func checkMariadbAnchor(segs []ReplaySegment, anchorGTID string, domain uint32) error {
	gtids, err := engine.ParseMariaDBPosition(anchorGTID)
	if err != nil || len(gtids) == 0 {
		return nil
	}
	for _, a := range gtids {
		if a.Domain != domain {
			continue
		}
		for _, seg := range segs {
			cut, ok := forkCut(seg, domain)
			if !ok {
				continue
			}
			if server, _, ok := segmentAuthor(seg, domain); ok && server == a.Server && a.Seq > cut {
				return fmt.Errorf("%w: its position %s is past the fork recorded on segment %s (after %d-%d-%d)",
					ErrBackupOnDeadBranch, a, seg.ServerUUID, domain, server, cut)
			}
		}
	}
	return nil
}

// archiveDomains lists the replication domains the index's segments cover.
func archiveDomains(idx *objectstore.ArchiveIndex) []uint32 {
	seen := map[uint32]bool{}
	var out []uint32
	for _, seg := range idx.Segments {
		gtids, err := engine.ParseMariaDBPosition(seg.GTIDSet)
		if err != nil {
			continue
		}
		for _, g := range gtids {
			if !seen[g.Domain] {
				seen[g.Domain] = true
				out = append(out, g.Domain)
			}
		}
	}
	slices.Sort(out)
	return out
}

// segmentsHaveRanges reports whether at least one segment carries a GTID
// position to select on.
func segmentsHaveRanges(segments []objectstore.ArchiveSegment) bool {
	for i := range segments {
		if segments[i].GTIDSet != "" {
			return true
		}
	}
	return false
}
