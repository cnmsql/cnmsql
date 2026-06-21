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
	"testing"
	"time"
)

func backupID(prefix, id string, completed time.Time) BackupEntry {
	return BackupEntry{
		Prefix: prefix,
		Meta:   BackupMetadata{BackupID: id, CompletedAt: completed},
	}
}

func TestSelectLatestBackup(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	entries := []BackupEntry{
		backupID("c/old/", "old", now.Add(-2*day)),
		backupID("c/new/", "new", now),
		backupID("c/mid/", "mid", now.Add(-1*day)),
	}
	got, err := SelectLatestBackup(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Meta.BackupID != "new" {
		t.Fatalf("expected latest backup id %q, got %q", "new", got.Meta.BackupID)
	}

	if _, err := SelectLatestBackup(nil); err == nil {
		t.Fatal("expected error on empty entries")
	}
}

func TestFindBackupByID(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	entries := []BackupEntry{
		backupID("c/a/", "a", now),
		backupID("c/b/", "b", now),
	}
	got, err := FindBackupByID(entries, "b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Prefix != "c/b/" {
		t.Fatalf("expected prefix %q, got %q", "c/b/", got.Prefix)
	}

	if _, err := FindBackupByID(entries, "missing"); err == nil {
		t.Fatal("expected error for missing backup id")
	}
}
