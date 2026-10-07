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

package controller

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

func backupEntry(id, anchor string, completed time.Time) objectstore.BackupEntry {
	return objectstore.BackupEntry{Prefix: id + "/", Meta: objectstore.BackupMetadata{
		BackupID: id, AnchorGTID: anchor, CompletedAt: completed,
	}}
}

func TestSelectRecoveryBackup(t *testing.T) {
	base := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	entries := []objectstore.BackupEntry{
		backupEntry("monday", gapUUID+":1-100", base.Add(-48*time.Hour)),
		backupEntry("tuesday", gapUUID+":1-200", base.Add(-24*time.Hour)),
		backupEntry("today", gapUUID+":1-219", base),
	}
	pick := func(target *mysqlv1alpha1.RecoveryTarget, index *objectstore.ArchiveIndex) string {
		t.Helper()
		entry, err := selectRecoveryBackup(entries, target, index, false)
		if err != nil {
			return "error: " + err.Error()
		}
		return entry.Meta.BackupID
	}

	if got := pick(nil, nil); got != "today" {
		t.Fatalf("no target picked %q, want the newest", got)
	}
	at := base.Add(-30 * time.Hour).Format(time.RFC3339)
	if got := pick(&mysqlv1alpha1.RecoveryTarget{TargetTime: at}, nil); got != "monday" {
		t.Fatalf("targetTime before tuesday picked %q, want monday", got)
	}
	if got := pick(&mysqlv1alpha1.RecoveryTarget{TargetGTID: gapUUID + ":1-150"}, nil); got != "monday" {
		t.Fatalf("targetGTID picked %q, want the newest anchor it contains", got)
	}
	forked := &objectstore.ArchiveIndex{Disowned: &objectstore.ArchiveDisowned{GTIDSet: gapUUID + ":219"}}
	later := base.Add(time.Hour).Format(time.RFC3339)
	if got := pick(&mysqlv1alpha1.RecoveryTarget{TargetTime: later}, forked); got != "tuesday" {
		t.Fatalf("picked %q, want the newest backup off the dead branch", got)
	}
	if got := pick(nil, forked); got != "today" {
		t.Fatalf("a restore without replay picked %q, want the newest", got)
	}
	if got := pick(&mysqlv1alpha1.RecoveryTarget{TargetTime: base.Add(-72 * time.Hour).Format(time.RFC3339)}, nil); !strings.HasPrefix(got, "error") {
		t.Fatalf("a target before every backup picked %q", got)
	}
}

func TestSelectRecoveryBackupMariaDBDeadBranch(t *testing.T) {
	base := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	entries := []objectstore.BackupEntry{
		backupEntry("old", "0-1-100", base.Add(-time.Hour)),
		backupEntry("dead", "0-1-220", base),
	}
	index := &objectstore.ArchiveIndex{Disowned: &objectstore.ArchiveDisowned{
		Ranges: []objectstore.ArchiveDisownedRange{{Domain: 0, Server: 1, After: 218, Through: 225}},
	}}
	later := base.Add(time.Hour).Format(time.RFC3339)
	entry, err := selectRecoveryBackup(entries, &mysqlv1alpha1.RecoveryTarget{TargetTime: later}, index, true)
	if err != nil || entry.Meta.BackupID != "old" {
		t.Fatalf("picked %q (%v), want old", entry.Meta.BackupID, err)
	}
}

func TestCheckNamedBackupTarget(t *testing.T) {
	stopped := metav1.NewTime(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC))
	backup := &mysqlv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "nightly"}}
	backup.Status.StoppedAt = &stopped
	before := &mysqlv1alpha1.RecoveryTarget{TargetTime: "2026-10-07T11:00:00Z"}
	if err := checkNamedBackupTarget(backup, before); err == nil {
		t.Fatal("a targetTime before the backup completed must be refused")
	}
	after := &mysqlv1alpha1.RecoveryTarget{TargetTime: "2026-10-07T13:00:00Z"}
	if err := checkNamedBackupTarget(backup, after); err != nil {
		t.Fatal(err)
	}
}
