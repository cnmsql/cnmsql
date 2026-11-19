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
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// PruneReplayFiles drops from a MySQL plan the files the replay would download
// and then throw away, judging each by its manifest.
//
// The planner keeps a segment whole as soon as the base backup lacks any of
// it, so a long-lived primary's segment is downloaded from its first file even
// when the backup holds all but the last few hours. A file whose every
// transaction is in the exclude set (the base backup's, plus what the archive
// disowned) contributes nothing: mysqlbinlog drops all of it. With a time
// target, a file whose first event is at or past the target is never reached:
// mysqlbinlog stops at the first event stamped past the target.
//
// A file with no manifest, no transactions or no time stamp cannot be judged
// and is kept, as is every file of a plan whose exclude set does not parse.
// The segments' coverage is left as it is: the gap check after replay judges
// what the plan covered, which pruning does not change. The caller's plan is
// not modified.
func PruneReplayFiles(plan ReplayPlan, spans map[ReplayFile]ReplayFileSpan) ReplayPlan {
	excluded, err := replication.ParseGTIDSet(plan.ExcludeGTIDs)
	if err != nil {
		return plan
	}
	out := plan
	out.Segments = make([]ReplaySegment, len(plan.Segments))
	for i, seg := range plan.Segments {
		kept := make([]string, 0, len(seg.Files))
		for _, name := range seg.Files {
			span, known := spans[ReplayFile{ServerUUID: seg.ServerUUID, Name: name}]
			if known && !neededFile(span, excluded, plan) {
				continue
			}
			kept = append(kept, name)
		}
		seg.Files = kept
		out.Segments[i] = seg
	}
	return out
}

// neededFile reports whether a file may hold something the replay applies.
func neededFile(span ReplayFileSpan, excluded replication.GTIDSet, plan ReplayPlan) bool {
	if !plan.TargetTime.IsZero() && !span.FirstEventTime.IsZero() && !span.FirstEventTime.Before(plan.TargetTime) {
		return false
	}
	if span.GTIDSet == "" {
		return true
	}
	set, err := replication.ParseGTIDSet(span.GTIDSet)
	if err != nil {
		return true
	}
	return !excluded.Contains(set)
}
