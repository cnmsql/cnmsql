/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
