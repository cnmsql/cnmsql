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
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// ForkJudge decides, for one archive segment, which of the transactions it
// archived the surviving timeline does not hold. It is a snapshot of the
// authority taken once per check, so every segment of an index is compared
// against the same view.
type ForkJudge interface {
	// Judge returns the disowned part of seg as a fork record holding only
	// GTIDSet (MySQL) or AfterSeq (MariaDB), or nil when the surviving
	// timeline holds everything or cannot tell.
	Judge(seg objectstore.ArchiveSegment) (*objectstore.ArchiveFork, error)
	// Authority is what the judge compares against, recorded for audit.
	Authority() string
}

// ForkSource takes the authority snapshot a fork check compares against. It
// returns a nil judge (and no error) when no authority is available right now,
// which skips the check.
type ForkSource func(ctx context.Context) (ForkJudge, error)

// mysqlForkJudge judges against the primary's gtid_executed. It names its
// authors, so whatever a segment holds outside it is exactly what some past
// primary committed and the surviving chain never received.
type mysqlForkJudge struct {
	raw      string
	executed replication.GTIDSet
}

// NewMySQLForkJudge builds a judge from the primary's @@GLOBAL.gtid_executed.
func NewMySQLForkJudge(executed string) (ForkJudge, error) {
	set, err := replication.ParseGTIDSet(executed)
	if err != nil {
		return nil, fmt.Errorf("binlog: parsing gtid_executed: %w", err)
	}
	return &mysqlForkJudge{raw: executed, executed: set}, nil
}

func (j *mysqlForkJudge) Judge(seg objectstore.ArchiveSegment) (*objectstore.ArchiveFork, error) {
	set, err := replication.ParseGTIDSet(seg.GTIDSet)
	if err != nil {
		return nil, fmt.Errorf("binlog: parsing gtid set of segment %s: %w", seg.ServerUUID, err)
	}
	disowned := set.Difference(j.executed)
	if disowned.IsEmpty() {
		return nil, nil
	}
	return &objectstore.ArchiveFork{GTIDSet: disowned.String()}, nil
}

func (j *mysqlForkJudge) Authority() string { return j.raw }

// Unheld implements forkRetractor.
func (j *mysqlForkJudge) Unheld(gtidSet string) (string, error) {
	set, err := replication.ParseGTIDSet(gtidSet)
	if err != nil {
		return "", fmt.Errorf("binlog: parsing disowned set: %w", err)
	}
	return set.Difference(j.executed).String(), nil
}

// forkRetractor is a judge that can tell which recorded disowned transactions
// the surviving timeline does not hold: MySQL names each transaction's author,
// so a set the authority holds part of again (a former primary that held a
// dead transaction was promoted after all) is narrowed to what it still lacks.
// The fencing of the index writes is what makes this safe: only the newest
// primary judges, and its executed set only grows.
type forkRetractor interface {
	// Unheld returns the part of gtidSet the authority does not hold.
	Unheld(gtidSet string) (string, error)
}

// retractFork narrows a MySQL fork record to what the authority still lacks. It
// returns nil when nothing is left, and whether the record shrank.
func retractFork(
	fork *objectstore.ArchiveFork, retractor forkRetractor, authority string,
) (*objectstore.ArchiveFork, bool, error) {
	if fork == nil || fork.GTIDSet == "" {
		return fork, false, nil
	}
	unheld, err := retractor.Unheld(fork.GTIDSet)
	if err != nil {
		return nil, false, err
	}
	if unheld == canonicalGTIDSet(fork.GTIDSet) {
		return fork, false, nil
	}
	if unheld == "" && len(fork.AfterSeq) == 0 {
		return nil, true, nil
	}
	kept := *fork
	kept.GTIDSet = unheld
	kept.AuthorityGTIDSet = authority
	return &kept, true, nil
}

// foldDisowned folds the segments' fork records into the index-level disowned
// record, narrowing its MySQL set to what the authority still lacks when the
// judge can tell. forks are the segments' records after this check.
func foldDisowned(
	existing *objectstore.ArchiveDisowned, segs []objectstore.ArchiveSegment,
	forks []*objectstore.ArchiveFork, retractor forkRetractor,
) (*objectstore.ArchiveDisowned, bool, error) {
	out := &objectstore.ArchiveDisowned{}
	before := ""
	if existing != nil {
		out.GTIDSet = existing.GTIDSet
		out.Ranges = slices.Clone(existing.Ranges)
		before = canonicalGTIDSet(existing.GTIDSet)
	}
	changed := false
	sets := []string{out.GTIDSet}
	for i, fork := range forks {
		if fork == nil {
			continue
		}
		if fork.GTIDSet != "" {
			sets = append(sets, fork.GTIDSet)
		}
		for domain, after := range fork.AfterSeq {
			gtids, err := engine.ParseMariaDBPosition(segs[i].GTIDSet)
			if err != nil {
				return nil, false, fmt.Errorf("binlog: parsing position of segment %s: %w", segs[i].ServerUUID, err)
			}
			for _, g := range gtids {
				if g.Domain == domain && out.AddRange(objectstore.ArchiveDisownedRange{
					Domain: domain, Server: g.Server, After: after, Through: g.Seq,
				}) {
					changed = true
				}
			}
		}
	}
	union, err := replication.UnionGTIDStrings(sets...)
	if err != nil {
		return nil, false, fmt.Errorf("binlog: merging disowned sets: %w", err)
	}
	if retractor != nil && union != "" {
		if union, err = retractor.Unheld(union); err != nil {
			return nil, false, err
		}
	}
	out.GTIDSet = union
	if union != before {
		changed = true
	}
	if out.GTIDSet == "" && len(out.Ranges) == 0 {
		return nil, changed, nil
	}
	return out, changed, nil
}

// mariadbForkJudge judges against the primary timeline: a MariaDB position
// names only the author of its last transaction, and the timeline says who
// authored each stretch of sequence numbers.
type mariadbForkJudge struct {
	timeline engine.MariaDBTimeline
	position string
}

// NewMariaDBForkJudge builds a judge from the cluster's primary timeline and
// the checking primary's position (recorded for audit).
func NewMariaDBForkJudge(timeline engine.MariaDBTimeline, position string) ForkJudge {
	return &mariadbForkJudge{timeline: timeline, position: position}
}

func (j *mariadbForkJudge) Judge(seg objectstore.ArchiveSegment) (*objectstore.ArchiveFork, error) {
	gtids, err := engine.ParseMariaDBPosition(seg.GTIDSet)
	if err != nil {
		return nil, fmt.Errorf("binlog: parsing position of segment %s: %w", seg.ServerUUID, err)
	}
	var after map[uint32]uint64
	for _, g := range gtids {
		// Within a domain a segment's disowned transactions are a suffix, so its
		// last GTID settles it: off the timeline means everything its author
		// holds past the handoff that ended its authorship is dead.
		seq, ok := j.timeline.DeadAfter(g)
		if !ok {
			continue
		}
		if after == nil {
			after = map[uint32]uint64{}
		}
		after[g.Domain] = seq
	}
	if after == nil {
		return nil, nil
	}
	return &objectstore.ArchiveFork{AfterSeq: after}, nil
}

func (j *mariadbForkJudge) Authority() string { return j.position }

// mergeFork folds a freshly judged delta into a segment's existing record and
// returns the result, never modifying existing. A record only ever grows: a
// MySQL set by union, a MariaDB cut by keeping the lower sequence per domain.
// DetectedAt and DetectedBy keep the first detection; the authority is updated
// whenever the record grows.
func mergeFork(
	existing, delta *objectstore.ArchiveFork, authority string, now time.Time, by string,
) (*objectstore.ArchiveFork, bool, error) {
	if delta == nil || (delta.GTIDSet == "" && len(delta.AfterSeq) == 0) {
		return existing, false, nil
	}
	if existing == nil {
		if delta.GTIDSet != "" {
			if _, err := replication.ParseGTIDSet(delta.GTIDSet); err != nil {
				return nil, false, fmt.Errorf("binlog: parsing fork gtid set: %w", err)
			}
		}
		return &objectstore.ArchiveFork{
			GTIDSet:          delta.GTIDSet,
			AfterSeq:         maps.Clone(delta.AfterSeq),
			AuthorityGTIDSet: authority,
			DetectedAt:       now,
			DetectedBy:       by,
		}, true, nil
	}

	merged := *existing
	merged.AfterSeq = maps.Clone(existing.AfterSeq)
	grew := false
	if delta.GTIDSet != "" {
		union, err := replication.UnionGTIDStrings(existing.GTIDSet, delta.GTIDSet)
		if err != nil {
			return nil, false, fmt.Errorf("binlog: merging fork gtid set: %w", err)
		}
		if union != existing.GTIDSet {
			merged.GTIDSet = union
			grew = true
		}
	}
	for domain, seq := range delta.AfterSeq {
		if cur, ok := merged.AfterSeq[domain]; ok && cur <= seq {
			continue
		}
		if merged.AfterSeq == nil {
			merged.AfterSeq = map[uint32]uint64{}
		}
		merged.AfterSeq[domain] = seq
		grew = true
	}
	if !grew {
		return existing, false, nil
	}
	merged.AuthorityGTIDSet = authority
	return &merged, true, nil
}

// RenderFork renders a segment's fork record for status: the MySQL disowned
// set, or for MariaDB one "d-s-first..d-s-last" range per domain built from
// AfterSeq and the segment's position. Empty when nothing is disowned.
func RenderFork(seg objectstore.ArchiveSegment) string {
	if seg.Fork == nil {
		return ""
	}
	if seg.Fork.GTIDSet != "" {
		return seg.Fork.GTIDSet
	}
	gtids, err := engine.ParseMariaDBPosition(seg.GTIDSet)
	if err != nil {
		return ""
	}
	var parts []string
	for _, g := range gtids {
		after, ok := seg.Fork.AfterSeq[g.Domain]
		if !ok || g.Seq <= after {
			continue
		}
		first := engine.MariaDBGTID{Domain: g.Domain, Server: g.Server, Seq: after + 1}
		if first.Seq == g.Seq {
			parts = append(parts, g.String())
			continue
		}
		parts = append(parts, first.String()+".."+g.String())
	}
	return strings.Join(parts, ",")
}

// SegmentFork is a fork record as status reports it.
type SegmentFork struct {
	// ServerUUID and InstanceName identify the segment holding the dead branch.
	ServerUUID   string
	InstanceName string
	// GTIDs is the rendered disowned set (see RenderFork).
	GTIDs string
	// DetectedAt is when the record was first written.
	DetectedAt time.Time
}

// SegmentForks lists the fork records an index carries, in timeline order.
func SegmentForks(idx *objectstore.ArchiveIndex) []SegmentFork {
	if idx == nil {
		return nil
	}
	var out []SegmentFork
	for _, seg := range idx.Segments {
		gtids := RenderFork(seg)
		if gtids == "" {
			continue
		}
		out = append(out, SegmentFork{
			ServerUUID:   seg.ServerUUID,
			InstanceName: seg.InstanceName,
			GTIDs:        gtids,
			DetectedAt:   seg.Fork.DetectedAt,
		})
	}
	return out
}

// OldestSegmentPosition returns, per domain, the lowest position any segment
// of a MariaDB archive has reached: the oldest position the fork check still
// has to judge, which pins the operator's timeline pruning. Segments whose set
// is not a MariaDB position (a MySQL archive) are ignored.
func OldestSegmentPosition(segs []objectstore.ArchiveSegment) string {
	oldest := map[uint32]engine.MariaDBGTID{}
	for _, seg := range segs {
		gtids, err := engine.ParseMariaDBPosition(seg.GTIDSet)
		if err != nil {
			continue
		}
		for _, g := range gtids {
			if cur, ok := oldest[g.Domain]; !ok || g.Seq < cur.Seq {
				oldest[g.Domain] = g
			}
		}
	}
	domains := slices.Sorted(maps.Keys(oldest))
	parts := make([]string, 0, len(domains))
	for _, d := range domains {
		parts = append(parts, oldest[d].String())
	}
	return strings.Join(parts, ",")
}

// forkCheckIdentity is how DetectedBy and CheckedBy name a primary.
func forkCheckIdentity(instance, identity string) string {
	return instance + "/" + identity
}

// ArchiveGaps lists the stretches of the timeline the archive is missing
// between transactions it holds: recovery from a base backup taken before one
// of them cannot cross it. On MySQL they are the holes of the covered set, per
// UUID; on MariaDB the gaps between the segments' sequence ranges, per domain,
// with each segment's range ending at its fork cut. A stretch before the first
// archived transaction is not a gap: the archive simply starts later.
func ArchiveGaps(idx *objectstore.ArchiveIndex) []string {
	if idx == nil || len(idx.Segments) == 0 {
		return nil
	}
	if covered, err := replication.ParseGTIDSet(idx.CoveredGTIDSet); err == nil && !mariadbArchive(idx) {
		if holes := covered.Holes(); !holes.IsEmpty() {
			return []string{holes.String()}
		}
		return nil
	}
	type interval struct{ start, end uint64 }
	byDomain := map[uint32][]interval{}
	for _, seg := range idx.Segments {
		gtids, err := engine.ParseMariaDBPosition(seg.GTIDSet)
		if err != nil {
			continue
		}
		for _, g := range gtids {
			start := MariaSeqForDomain(seg.StartGTIDSet, g.Domain)
			if start == 0 {
				start = 1
			}
			end := g.Seq
			if seg.Fork != nil {
				if cut, ok := seg.Fork.AfterSeq[g.Domain]; ok && cut < end {
					end = cut
				}
			}
			if end >= start {
				byDomain[g.Domain] = append(byDomain[g.Domain], interval{start, end})
			}
		}
	}
	var out []string
	for _, domain := range slices.Sorted(maps.Keys(byDomain)) {
		ivs := byDomain[domain]
		slices.SortFunc(ivs, func(a, b interval) int { return cmp.Compare(a.start, b.start) })
		reached := ivs[0].end
		for _, iv := range ivs[1:] {
			if iv.start > reached+1 {
				out = append(out, fmt.Sprintf("%d-%d..%d", domain, reached+1, iv.start-1))
			}
			reached = max(reached, iv.end)
		}
	}
	return out
}

// mariadbArchive reports whether the index's segments carry MariaDB positions.
func mariadbArchive(idx *objectstore.ArchiveIndex) bool {
	for _, seg := range idx.Segments {
		if seg.GTIDSet == "" {
			continue
		}
		_, err := engine.ParseMariaDBPosition(seg.GTIDSet)
		return err == nil
	}
	return false
}
