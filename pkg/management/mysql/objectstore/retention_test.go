/*
Copyright 2026 The cloudnative-mysql Authors.

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

package objectstore

import (
	"testing"
	"time"
)

func backup(prefix string, started, completed time.Time) BackupEntry {
	return BackupEntry{
		Prefix: prefix,
		Meta:   BackupMetadata{StartedAt: started, CompletedAt: completed},
	}
}

func binlog(uuid, name string, last time.Time) BinlogEntry {
	return BinlogEntry{
		Keys: BinlogKeys{BinlogKey: uuid + "/" + name, ManifestKey: uuid + "/" + name + ".json"},
		Meta: BinlogMetadata{ServerUUID: uuid, BinlogName: name, LastEventTime: last},
	}
}

func TestPlanRetention(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	cutoff := now.Add(-7 * day) // keep last 7 days

	const uuid = "11111111-1111-1111-1111-111111111111"

	t.Run("no backups leaves binlogs untouched", func(t *testing.T) {
		t.Parallel()
		plan := PlanRetention(nil, []BinlogEntry{binlog(uuid, "binlog.000001", now.Add(-30*day))}, nil, cutoff)
		if !plan.Empty() {
			t.Fatalf("expected empty plan, got %+v", plan)
		}
	})

	t.Run("nothing expired keeps all", func(t *testing.T) {
		t.Parallel()
		backups := []BackupEntry{
			backup("c/b1/", now.Add(-3*day), now.Add(-3*day)),
			backup("c/b2/", now.Add(-1*day), now.Add(-1*day)),
		}
		plan := PlanRetention(backups, nil, nil, cutoff)
		if !plan.Empty() {
			t.Fatalf("expected empty plan, got %+v", plan)
		}
	})

	t.Run("expired deleted, newest kept as floor", func(t *testing.T) {
		t.Parallel()
		// All three are older than the window; newest (b3) must survive.
		backups := []BackupEntry{
			backup("c/b1/", now.Add(-30*day), now.Add(-30*day)),
			backup("c/b3/", now.Add(-10*day), now.Add(-10*day)),
			backup("c/b2/", now.Add(-20*day), now.Add(-20*day)),
		}
		plan := PlanRetention(backups, nil, nil, cutoff)
		if got := plan.DeleteBackupPrefixes; len(got) != 2 {
			t.Fatalf("expected 2 deleted, got %v", got)
		}
		for _, p := range plan.DeleteBackupPrefixes {
			if p == "c/b3/" {
				t.Fatalf("newest backup b3 must be retained, got deleted")
			}
		}
		// Horizon = oldest retained (b3) start.
		if !plan.Horizon.Equal(now.Add(-10 * day)) {
			t.Fatalf("horizon = %v, want %v", plan.Horizon, now.Add(-10*day))
		}
	})

	t.Run("binlog GC by anchor horizon and index rewrite", func(t *testing.T) {
		t.Parallel()
		// b_old expires; b_new retained with start at -2d → horizon -2d.
		backups := []BackupEntry{
			backup("c/b_old/", now.Add(-20*day), now.Add(-20*day)),
			backup("c/b_new/", now.Add(-2*day), now.Add(-2*day)),
		}
		binlogs := []BinlogEntry{
			binlog(uuid, "binlog.000001", now.Add(-19*day)), // before horizon → delete
			binlog(uuid, "binlog.000002", now.Add(-1*day)),  // after horizon → keep
			binlog(uuid, "binlog.000003", time.Time{}),      // unknown time → keep
		}
		index := &ArchiveIndex{
			ClusterName: "c",
			Segments: []ArchiveSegment{{
				ServerUUID: uuid,
				Binlogs:    []string{"binlog.000001", "binlog.000002", "binlog.000003"},
			}},
		}
		plan := PlanRetention(backups, binlogs, index, cutoff)

		if len(plan.DeleteBackupPrefixes) != 1 || plan.DeleteBackupPrefixes[0] != "c/b_old/" {
			t.Fatalf("deleted backups = %v", plan.DeleteBackupPrefixes)
		}
		// One binlog deleted = its file + manifest = 2 keys.
		if len(plan.DeleteBinlogKeys) != 2 {
			t.Fatalf("deleted binlog keys = %v", plan.DeleteBinlogKeys)
		}
		if plan.NewIndex == nil {
			t.Fatal("expected rewritten index")
		}
		got := plan.NewIndex.Segments[0].Binlogs
		want := []string{"binlog.000002", "binlog.000003"}
		if len(got) != len(want) {
			t.Fatalf("rewritten binlogs = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("rewritten binlogs = %v, want %v", got, want)
			}
		}
	})

	t.Run("segment emptied is dropped from index", func(t *testing.T) {
		t.Parallel()
		backups := []BackupEntry{
			backup("c/b_old/", now.Add(-20*day), now.Add(-20*day)),
			backup("c/b_new/", now.Add(-2*day), now.Add(-2*day)),
		}
		oldUUID := "22222222-2222-2222-2222-222222222222"
		binlogs := []BinlogEntry{
			binlog(oldUUID, "binlog.000001", now.Add(-19*day)),
			binlog(uuid, "binlog.000007", now.Add(-1*day)),
		}
		index := &ArchiveIndex{
			ClusterName: "c",
			Segments: []ArchiveSegment{
				{ServerUUID: oldUUID, Binlogs: []string{"binlog.000001"}},
				{ServerUUID: uuid, Binlogs: []string{"binlog.000007"}},
			},
		}
		plan := PlanRetention(backups, binlogs, index, cutoff)
		if plan.NewIndex == nil || len(plan.NewIndex.Segments) != 1 {
			t.Fatalf("expected one segment left, got %+v", plan.NewIndex)
		}
		if plan.NewIndex.Segments[0].ServerUUID != uuid {
			t.Fatalf("wrong segment retained: %s", plan.NewIndex.Segments[0].ServerUUID)
		}
	})
}
