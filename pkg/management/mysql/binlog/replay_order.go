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
	"strconv"
	"strings"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// ReplayFile names one archived binlog of a replay plan.
type ReplayFile struct {
	// ServerUUID is the segment the file belongs to.
	ServerUUID string
	// Name is the binlog basename.
	Name string
}

// ReplayFileSpan is what a file's manifest says about its place on the
// timeline.
type ReplayFileSpan struct {
	// FirstGTID is the file's first transaction.
	FirstGTID string
	// GTIDSet is everything the file holds.
	GTIDSet string
	// FirstEventTime is the commit time of its first event.
	FirstEventTime time.Time
}

// Files lists the plan's files in replay order: Order when set, otherwise each
// segment's files, one segment after the other.
func (p ReplayPlan) Files() []ReplayFile {
	if len(p.Order) > 0 {
		return p.Order
	}
	var out []ReplayFile
	for _, seg := range p.Segments {
		for _, name := range seg.Files {
			out = append(out, ReplayFile{ServerUUID: seg.ServerUUID, Name: name})
		}
	}
	return out
}

// OrderReplayFiles orders a MySQL plan's files along the timeline instead of
// segment by segment.
//
// Segments usually follow one another: a successor's files re-log the end of
// its predecessor's and carry on. But one segment can also hold a single file
// that fills a hole in another, e.g. a former primary's copy of a binlog whose
// index entry the primary lost. Replayed after (or before) the whole of the
// other segment, that file's transactions would be applied out of order.
//
// A file is ready once its first transaction, or the one before it, is
// applied; each segment's files stay in their own order. The next file is the
// first ready one, segments taken in plan order, so successive segments replay
// exactly as before. When none is ready, the file with the earliest first event
// goes next: a file with no transactions, which has no place by GTID, or a
// hole nothing fills, which the check of the recovered set reports. A file
// whose manifest carries no time goes after those that do. A file missing from
// spans counts as ready.
//
// The order matters beyond the transactions: with a time target mysqlbinlog
// stops at the first event stamped past it, in whatever file that is, so a
// later file replayed early ends the replay before the earlier ones.
func OrderReplayFiles(plan ReplayPlan, spans map[ReplayFile]ReplayFileSpan) []ReplayFile {
	applied, err := replication.ParseGTIDSet(plan.AnchorGTID)
	if err != nil {
		return plan.Files()
	}
	next := make([]int, len(plan.Segments))
	var out []ReplayFile
	for {
		pick, fallback := -1, -1
		var fallbackAt time.Time
		for i, seg := range plan.Segments {
			if next[i] >= len(seg.Files) {
				continue
			}
			span, known := spans[ReplayFile{ServerUUID: seg.ServerUUID, Name: seg.Files[next[i]]}]
			if !known || followsApplied(applied, span.FirstGTID) {
				pick = i
				break
			}
			if fallback < 0 || earlier(span.FirstEventTime, fallbackAt) {
				fallback, fallbackAt = i, span.FirstEventTime
			}
		}
		if pick < 0 {
			pick = fallback
		}
		if pick < 0 {
			return out
		}
		file := ReplayFile{ServerUUID: plan.Segments[pick].ServerUUID, Name: plan.Segments[pick].Files[next[pick]]}
		next[pick]++
		out = append(out, file)
		if set, err := replication.ParseGTIDSet(spans[file].GTIDSet); err == nil {
			applied.Union(set)
		}
	}
}

// earlier reports whether a comes before b, an unknown (zero) time coming
// after every known one.
func earlier(a, b time.Time) bool {
	if a.IsZero() || b.IsZero() {
		return !a.IsZero()
	}
	return a.Before(b)
}

// followsApplied reports whether gtid, a file's first transaction, or the one
// before it is applied. A file with no transactions follows nothing: it is
// placed by time.
func followsApplied(applied replication.GTIDSet, gtid string) bool {
	at := strings.LastIndex(gtid, ":")
	if at < 0 {
		return false
	}
	n, err := strconv.ParseUint(gtid[at+1:], 10, 64)
	if err != nil {
		return false
	}
	if first, err := replication.ParseGTIDSet(gtid); err == nil && applied.Contains(first) {
		return true
	}
	if n <= 1 {
		// What precedes a server's first transaction is not in its GTID: the
		// file waits until nothing else is ready.
		return false
	}
	before, err := replication.ParseGTIDSet(gtid[:at+1] + strconv.FormatUint(n-1, 10))
	if err != nil {
		return false
	}
	return applied.Contains(before)
}
