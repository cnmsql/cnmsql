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
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// cloneHoleIndex is the clone-point hole: the old primary archived up to 100,
// its successor was cloned at 150 and archived from 151 onward with its own
// writes; 101-150 survive only in the old primary's lost active binlog.
func cloneHoleIndex() *objectstore.ArchiveIndex {
	return &objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{
		{ServerUUID: testUUID, Binlogs: []string{"binlog.000001"}, GTIDSet: testUUID + ":1-100"},
		{ServerUUID: otherUUID, Binlogs: []string{"binlog.000001"}, GTIDSet: testUUID + ":151-160," + otherUUID + ":1-30"},
	}}
}

func TestLatestRecoveryRefusesToCrossAnArchiveGap(t *testing.T) {
	t.Parallel()
	_, err := PlanReplay(cloneHoleIndex(), testUUID+":1-50", RecoveryTarget{})
	if !errors.Is(err, ErrArchiveGap) {
		t.Fatalf("err = %v, want ErrArchiveGap", err)
	}
	// A backup taken past the hole recovers to latest.
	if _, err := PlanReplay(cloneHoleIndex(), testUUID+":1-150", RecoveryTarget{}); err != nil {
		t.Fatalf("a backup past the hole must recover: %v", err)
	}
	// A time target may stop before the hole: planning leaves it to the check
	// after replay.
	at := time.Unix(1700000000, 0)
	if _, err := PlanReplay(cloneHoleIndex(), testUUID+":1-50", RecoveryTarget{Time: &at}); err != nil {
		t.Fatalf("a time target must plan: %v", err)
	}
}

func TestRecordedForksAreNotGaps(t *testing.T) {
	t.Parallel()
	idx := &objectstore.ArchiveIndex{Segments: []objectstore.ArchiveSegment{
		{ServerUUID: testUUID, Binlogs: []string{"binlog.000001"}, GTIDSet: testUUID + ":1-100",
			Fork: &objectstore.ArchiveFork{GTIDSet: testUUID + ":100"}},
		{ServerUUID: otherUUID, Binlogs: []string{"binlog.000001"}, GTIDSet: testUUID + ":90-99," + otherUUID + ":1-30"},
	}}
	plan, err := PlanReplay(idx, testUUID+":1-50", RecoveryTarget{})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyReplayedGTIDs(plan, testUUID+":1-50", testUUID+":1-99,"+otherUUID+":1-30"); err != nil {
		t.Fatalf("a recovery that left out the recorded fork must verify: %v", err)
	}
}

func TestVerifyReplayedGTIDs(t *testing.T) {
	t.Parallel()
	plan := ReplayPlan{}
	cases := []struct {
		name, anchor, executed string
		gap                    bool
	}{
		{"contiguous", testUUID + ":1-50", testUUID + ":1-160," + otherUUID + ":1-30", false},
		{"stopped early", testUUID + ":1-50", testUUID + ":1-70", false},
		{"crossed a hole", testUUID + ":1-50", testUUID + ":1-100:151-160," + otherUUID + ":1-30", true},
		{"hole the backup already had", testUUID + ":1-10:20-50", testUUID + ":1-10:20-160", false},
		{"new uuid missing its start", testUUID + ":1-50", testUUID + ":1-60," + otherUUID + ":5-30", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := VerifyReplayedGTIDs(plan, tc.anchor, tc.executed)
			if got := errors.Is(err, ErrArchiveGap); got != tc.gap {
				t.Fatalf("err = %v, want gap=%v", err, tc.gap)
			}
		})
	}
	target := ReplayPlan{IncludeGTIDs: testUUID + ":1-80"}
	if err := VerifyReplayedGTIDs(target, testUUID+":1-50", testUUID+":1-70"); !errors.Is(err, ErrArchiveGap) {
		t.Fatalf("a targetGTID the recovery fell short of must fail, err = %v", err)
	}
}

// A targetTime past what the archive proves it holds fails instead of
// recovering less than asked.
func TestTimeTargetBeyondArchivedThrough(t *testing.T) {
	t.Parallel()
	through := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	idx := &objectstore.ArchiveIndex{ArchivedThrough: through, Segments: []objectstore.ArchiveSegment{
		{ServerUUID: testUUID, Binlogs: []string{"binlog.000001"}, GTIDSet: testUUID + ":1-100"},
	}}
	late := through.Add(time.Minute)
	if _, err := PlanReplay(idx, testUUID+":1-50", RecoveryTarget{Time: &late}); !errors.Is(err, ErrTargetBeyondArchive) {
		t.Fatalf("err = %v, want ErrTargetBeyondArchive", err)
	}
	early := through.Add(-time.Minute)
	if _, err := PlanReplay(idx, testUUID+":1-50", RecoveryTarget{Time: &early}); err != nil {
		t.Fatalf("a target the archive covers must plan: %v", err)
	}
	idx.ArchivedThrough = time.Time{}
	if _, err := PlanReplay(idx, testUUID+":1-50", RecoveryTarget{Time: &late}); err != nil {
		t.Fatalf("an archive without the stamp is not judged: %v", err)
	}
}
