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
	"reflect"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

var archivedEpochs = []objectstore.ArchiveEpoch{
	{Instance: "demo-1", ServerID: 1},
	{Instance: "demo-2", ServerID: 2, Handoff: "0-1-218"},
}

// The MariaDB primary's check writes the Cluster's timeline into the index,
// back to what the oldest segment still needs.
func TestMariaDBCheckPersistsTheTimeline(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{
		mariadbSegment("token-oldest", "0-1-100"),
		mariadbSegment("token-old", "0-1-219"), mariadbSegment("token-new", "0-2-300"),
	}})
	a := mariadbForkArchiver(t, store, nil, "")
	a.forks = func(context.Context) (ForkJudge, error) {
		return NewMariaDBArchiveJudge(archivedEpochs, "0-2-300"), nil
	}
	report, err := a.CheckForks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	idx := readIndex(t, store)
	if !reflect.DeepEqual(idx.MariaDBTimeline, archivedEpochs) || !reflect.DeepEqual(report.Timeline, archivedEpochs) {
		t.Fatalf("timeline = %+v, report = %+v", idx.MariaDBTimeline, report.Timeline)
	}
}

// A Cluster whose timeline restarted (its status was lost) does not erase
// the older history the archive holds: the new epochs are appended.
func TestMergeArchiveTimelineKeepsOlderHistory(t *testing.T) {
	t.Parallel()
	segs := []objectstore.ArchiveSegment{mariadbSegment("a", "0-1-10")}
	restarted := []objectstore.ArchiveEpoch{{Instance: "demo-2", ServerID: 2, Handoff: "0-2-300"}}
	got := mergeArchiveTimeline(archivedEpochs, restarted, segs)
	want := append(append([]objectstore.ArchiveEpoch{}, archivedEpochs...), restarted...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merged = %+v, want %+v", got, want)
	}
	// The Cluster still holding the stored history replaces it as is.
	grown := append(append([]objectstore.ArchiveEpoch{}, archivedEpochs...),
		objectstore.ArchiveEpoch{Instance: "demo-3", ServerID: 3, Handoff: "0-2-400"})
	if got := mergeArchiveTimeline(archivedEpochs, grown, segs); !reflect.DeepEqual(got, grown) {
		t.Fatalf("merged = %+v, want %+v", got, grown)
	}
}

// Restore judges the segments against the archived timeline, so a fork no
// live check recorded is cut, and a backup on it refused.
func TestApplyArchiveTimelineRecordsUnrecordedForks(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{
		MariaDBTimeline: archivedEpochs,
		Segments: []objectstore.ArchiveSegment{
			{ServerUUID: "token-old", Binlogs: []string{"binlog.000001"}, StartGTIDSet: "0-1-1", GTIDSet: "0-1-219"},
			{ServerUUID: "token-new", Binlogs: []string{"binlog.000001"}, StartGTIDSet: "0-1-200", GTIDSet: "0-2-300"},
		},
	}
	if err := ApplyArchiveTimeline(idx); err != nil {
		t.Fatal(err)
	}
	if fork := idx.Segments[0].Fork; fork == nil || fork.AfterSeq[0] != 218 {
		t.Fatalf("fork = %+v, want a cut after 218", fork)
	}
	if idx.Segments[1].Fork != nil {
		t.Fatalf("surviving segment got fork %+v", idx.Segments[1].Fork)
	}
	if _, err := PrepareMariadbPositional(idx, "0-1-219", RecoveryTarget{}); !errors.Is(err, ErrBackupOnDeadBranch) {
		t.Fatalf("err = %v, want ErrBackupOnDeadBranch", err)
	}
}
