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

package objectstore

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
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

	t.Run("missing index parks binlog GC", func(t *testing.T) {
		t.Parallel()
		// Same shape as the case above, but the archive index is gone. The expired
		// base backup still goes; every binlog must survive, however far it sits
		// behind the horizon, because nothing is left to record its removal.
		backups := []BackupEntry{
			backup("c/b_old/", now.Add(-20*day), now.Add(-20*day)),
			backup("c/b_new/", now.Add(-2*day), now.Add(-2*day)),
		}
		binlogs := []BinlogEntry{
			binlog(uuid, "binlog.000001", now.Add(-19*day)),
			binlog(uuid, "binlog.000002", now.Add(-1*day)),
		}
		plan := PlanRetention(backups, binlogs, nil, cutoff)

		if len(plan.DeleteBackupPrefixes) != 1 || plan.DeleteBackupPrefixes[0] != "c/b_old/" {
			t.Fatalf("deleted backups = %v, want [c/b_old/]", plan.DeleteBackupPrefixes)
		}
		if len(plan.DeleteBinlogKeys) != 0 {
			t.Fatalf("deleted binlog keys = %v, want none without an index", plan.DeleteBinlogKeys)
		}
		if plan.NewIndex != nil {
			t.Fatalf("expected no index rewrite, got %+v", plan.NewIndex)
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

// recordingS3 accepts every request and records "METHOD /path".
func recordingS3(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path)
		mu.Unlock()
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(seen) }
}

func testClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	c, err := NewClient(Config{Endpoint: endpoint, ForcePathStyle: true, AccessKeyID: "k", SecretAccessKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestApplyExpirySplitsStores(t *testing.T) {
	baseSrv, baseSeen := recordingS3(t)
	logSrv, logSeen := recordingS3(t)
	baseStore := mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "base"}
	logStore := mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"}
	plan := RetentionPlan{
		DeleteBinlogKeys: []string{"archive/demo/binlogs/u/binlog.000001", "archive/demo/binlogs/u/binlog.000001.json"},
		NewIndex:         &ArchiveIndex{},
	}

	if err := ApplyBackupExpiry(context.Background(), testClient(t, baseSrv.URL), baseStore, plan); err != nil {
		t.Fatal(err)
	}
	if err := ApplyBinlogExpiry(context.Background(), testClient(t, logSrv.URL), logStore, "demo", plan); err != nil {
		t.Fatal(err)
	}

	if got := baseSeen(); len(got) != 0 {
		t.Fatalf("base store must not see binlog requests, got %v", got)
	}
	got := logSeen()
	for _, want := range []string{
		"DELETE /binlogs/archive/demo/binlogs/u/binlog.000001",
		"DELETE /binlogs/archive/demo/binlogs/u/binlog.000001.json",
		"PUT /binlogs/archive/demo/binlogs/_index.json",
	} {
		if !slices.Contains(got, want) {
			t.Fatalf("binlog store requests %v missing %q", got, want)
		}
	}
}
