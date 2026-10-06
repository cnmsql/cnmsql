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
	"reflect"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// mariadbForkArchiver is the current MariaDB primary (archive token "token-new")
// judging the archive against timeline from its position.
func mariadbForkArchiver(t *testing.T, store Store, timeline engine.MariaDBTimeline, position string) *Archiver {
	t.Helper()
	model := engine.MustForFlavor(engine.FlavorMariaDB).GTID()
	a, err := NewArchiver(ArchiverOptions{
		Store: store, ObjectStore: testObjectStore, ClusterName: "demo", InstanceName: "demo-2",
		ServerUUID: "token-new", BinlogDir: t.TempDir(), Scan: staticScan(nil),
		Now:    func() time.Time { return time.Unix(1700000000, 0).UTC() },
		NewSet: func() GTIDOps { return NewMariadbGTIDSet(model) },
		Forks: func(context.Context) (ForkJudge, error) {
			if timeline == nil {
				return nil, nil
			}
			return NewMariaDBForkJudge(timeline, position), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mariadbSegment(token, position string) objectstore.ArchiveSegment {
	return objectstore.ArchiveSegment{ServerUUID: token, Binlogs: []string{"binlog.000001"}, GTIDSet: position}
}

func checkMariaDB(
	t *testing.T, timeline engine.MariaDBTimeline, segs ...objectstore.ArchiveSegment,
) objectstore.ArchiveIndex {
	t.Helper()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{Segments: segs})
	if _, err := mariadbForkArchiver(t, store, timeline, "0-2-300").CheckForks(context.Background()); err != nil {
		t.Fatal(err)
	}
	return readIndex(t, store)
}

// The motivating MariaDB fork: server 1 uploaded 0-1-219, the lagged successor
// (server 2) inherited 0-1-218 and has since written 0-2-219 onwards. Position
// containment alone sees nothing; the timeline names the dead branch.
func TestMariaDBForkCheckDetectsTheDeadBranch(t *testing.T) {
	t.Parallel()
	idx := checkMariaDB(t, engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-218"}},
		mariadbSegment("token-old", "0-1-219"), mariadbSegment("token-new", "0-2-300"))
	fork := segmentByUUID(t, idx, "token-old").Fork
	if fork == nil || !reflect.DeepEqual(fork.AfterSeq, map[uint32]uint64{0: 218}) || fork.AuthorityGTIDSet != "0-2-300" {
		t.Fatalf("fork = %+v, want afterSeq {0: 218}", fork)
	}
	if got := RenderFork(segmentByUUID(t, idx, "token-old")); got != "0-1-219" {
		t.Fatalf("rendered fork = %q", got)
	}
}

// Case 11: the successor S died before it ever wrote the index, and R was
// promoted from it. The timeline still records S's epoch, so R finds the fork
// on the old primary's segment.
func TestMariaDBForkCheckByALaterPrimary(t *testing.T) {
	t.Parallel()
	timeline := engine.MariaDBTimeline{
		{ServerID: 1},
		{ServerID: 2, Handoff: "0-1-218"},
		{ServerID: 3, Handoff: "0-2-305"},
	}
	idx := checkMariaDB(t, timeline, mariadbSegment("token-old", "0-1-219"), mariadbSegment("token-new", "0-3-400"))
	if fork := segmentByUUID(t, idx, "token-old").Fork; fork == nil || fork.AfterSeq[0] != 218 {
		t.Fatalf("fork = %+v, want afterSeq 218", fork)
	}
}

// A former primary that rejoined, re-logged the current primary's transactions
// and drained them carries a segment whose last GTID is the current primary's:
// on the timeline, not a fork.
func TestMariaDBForkCheckIgnoresARelogOfTheSurvivingTimeline(t *testing.T) {
	t.Parallel()
	idx := checkMariaDB(t, engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-218"}},
		mariadbSegment("token-old", "0-2-290"), mariadbSegment("token-new", "0-2-300"))
	if fork := segmentByUUID(t, idx, "token-old").Fork; fork != nil {
		t.Fatalf("a re-log of the surviving timeline was recorded as a fork: %+v", fork)
	}
	if idx.ForkCheck == nil {
		t.Fatal("the index must be stamped as checked")
	}
}

func TestMariaDBForkCheckKeepsTheLowerAfterSeq(t *testing.T) {
	t.Parallel()
	old := mariadbSegment("token-old", "0-1-219")
	old.Fork = &objectstore.ArchiveFork{AfterSeq: map[uint32]uint64{0: 210}, DetectedBy: "demo-3/x"}
	idx := checkMariaDB(t, engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-218"}},
		old, mariadbSegment("token-new", "0-2-300"))
	if fork := segmentByUUID(t, idx, "token-old").Fork; fork.AfterSeq[0] != 210 || fork.DetectedBy != "demo-3/x" {
		t.Fatalf("fork = %+v, want the lower cut kept", fork)
	}
}

// History below the timeline's floor gets no verdict and records nothing; a
// dead branch above it is still recorded after the operator pruned the epoch
// its author held, cut at the floor.
func TestMariaDBForkCheckOnAPrunedTimeline(t *testing.T) {
	t.Parallel()
	pruned := engine.MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}
	idx := checkMariaDB(t, pruned, mariadbSegment("token-below", "0-1-100"), mariadbSegment("token-old", "0-1-219"),
		mariadbSegment("token-new", "0-2-300"))
	if fork := segmentByUUID(t, idx, "token-below").Fork; fork != nil {
		t.Fatalf("no verdict must record nothing, got %+v", fork)
	}
	if fork := segmentByUUID(t, idx, "token-old").Fork; fork == nil || fork.AfterSeq[0] != 218 {
		t.Fatalf("fork = %+v, want the dead branch cut at the floor 218", fork)
	}
}

// Before the operator has recorded a timeline there is no MariaDB authority:
// the check is skipped and the index is not stamped.
func TestMariaDBForkCheckWaitsForATimeline(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	seedIndex(t, store, objectstore.ArchiveIndex{
		Segments: []objectstore.ArchiveSegment{mariadbSegment("token-old", "0-1-219")},
	})
	report, err := mariadbForkArchiver(t, store, nil, "0-2-300").CheckForks(context.Background())
	if err != nil || report.Checked {
		t.Fatalf("report = %+v, err = %v", report, err)
	}
	if readIndex(t, store).ForkCheck != nil {
		t.Fatal("no timeline must not stamp the index")
	}
}
