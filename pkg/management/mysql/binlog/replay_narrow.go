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
	"sort"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// SpanReader reads one file's manifest.
type SpanReader func(ctx context.Context, file ReplayFile) (ReplayFileSpan, error)

// NarrowReplayFiles drops from a MySQL plan the files the replay would
// download and then throw away, finding the boundaries by binary search over
// each segment's manifests rather than reading one per file.
//
// The planner keeps a segment whole as soon as the base backup lacks any of
// it, so a long-lived primary's segment is downloaded from its first file even
// when the backup holds all but the last few hours. Within a segment the files
// are in sequence order and one server's history only grows, so the files
// whose every transaction the base backup holds form a prefix, and with a time
// target the files whose first event is at or past the target form a suffix:
// mysqlbinlog drops the former whole and never reaches the latter. Each
// boundary takes a dozen reads of a 3000-file segment instead of 3000.
//
// The search judges files by the base backup's anchor alone, not the whole
// exclude set. The disowned transactions in it sit in the middle of a segment
// (a dead branch, before the old primary re-logged its successor), so a file
// they fully cover is not a prefix boundary; mysqlbinlog still drops them.
//
// A base backup taken from a replica applying in parallel can have holes in
// its executed set, which breaks the prefix. A file skipped that way leaves a
// hole in the recovered set that neither the backup nor the disowned set
// explains, and VerifyReplayedGTIDs fails the recovery rather than finish
// with a state that never existed.
//
// A file with no transactions (FLUSH BINARY LOGS on an idle server) holds
// nothing to judge, so it takes the verdict of the nearest file before it that
// holds some: judged as not needed, one after the needed files would break the
// prefix and let the search skip them. A file with no time stamp is not in the
// way, so where the end search lands it is kept or left out harmlessly. The
// spans read are returned so the caller need not read them again; the caller's
// plan is not modified. A plan whose anchor does not parse is returned as is.
func NarrowReplayFiles(
	ctx context.Context, plan ReplayPlan, read SpanReader,
) (ReplayPlan, map[ReplayFile]ReplayFileSpan, error) {
	spans := map[ReplayFile]ReplayFileSpan{}
	anchor, err := replication.ParseGTIDSet(plan.AnchorGTID)
	if err != nil {
		return plan, spans, nil
	}
	var readErr error
	span := func(seg ReplaySegment, i int) ReplayFileSpan {
		file := ReplayFile{ServerUUID: seg.ServerUUID, Name: seg.Files[i]}
		if s, ok := spans[file]; ok {
			return s
		}
		s, err := read(ctx, file)
		if err != nil && readErr == nil {
			readErr = fmt.Errorf("binlog: reading manifest of %s/%s: %w", file.ServerUUID, file.Name, err)
		}
		spans[file] = s
		return s
	}
	needed := func(seg ReplaySegment, i int) bool {
		for ; i >= 0; i-- {
			if s := span(seg, i); s.GTIDSet != "" {
				return holdsNeeded(s, anchor)
			}
		}
		return false
	}

	out := plan
	out.Segments = make([]ReplaySegment, len(plan.Segments))
	for i, seg := range plan.Segments {
		n := len(seg.Files)
		// First file that holds something the base backup lacks.
		start := sort.Search(n, func(k int) bool { return needed(seg, k) })
		end := n
		if !plan.TargetTime.IsZero() {
			// First file whose first event is at or past the target.
			end = start + sort.Search(n-start, func(k int) bool {
				s := span(seg, start+k)
				return !s.FirstEventTime.IsZero() && !s.FirstEventTime.Before(plan.TargetTime)
			})
		}
		if readErr != nil {
			return ReplayPlan{}, nil, readErr
		}
		seg.Files = append([]string(nil), seg.Files[start:end]...)
		out.Segments[i] = seg
	}
	return out, spans, nil
}

// holdsNeeded reports whether a file holds a transaction the base backup
// lacks. A file with no transactions holds none.
func holdsNeeded(span ReplayFileSpan, anchor replication.GTIDSet) bool {
	if span.GTIDSet == "" {
		return false
	}
	set, err := replication.ParseGTIDSet(span.GTIDSet)
	if err != nil {
		return true
	}
	return !anchor.Contains(set)
}
