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
