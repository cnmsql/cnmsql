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
	"sort"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// gcBackup builds a Backup with a name, phase, and age (how long ago it was
// created) for the retention planner tests.
func gcBackup(name string, phase mysqlv1alpha1.BackupPhase, ageDays int) mysqlv1alpha1.Backup {
	return mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Duration(ageDays) * 24 * time.Hour)),
		},
		Status: mysqlv1alpha1.BackupStatus{Phase: phase},
	}
}

func deletedNames(entries []mysqlv1alpha1.Backup) []string {
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	sort.Strings(names)
	return names
}

func TestPlanBackupGC(t *testing.T) {
	t.Parallel()

	const (
		day  = 24 * time.Hour
		week = 7 * day
	)
	now := time.Now()

	tests := []struct {
		name      string
		children  []mysqlv1alpha1.Backup
		succLimit *int32
		failLimit *int32
		window    time.Duration
		wantDel   []string
	}{
		{
			name: "no knobs set is a no-op",
			children: []mysqlv1alpha1.Backup{
				gcBackup("c1", mysqlv1alpha1.BackupPhaseCompleted, 100),
				gcBackup("c2", mysqlv1alpha1.BackupPhaseCompleted, 90),
			},
			wantDel: nil,
		},
		{
			name: "count keeps newest N completed",
			children: []mysqlv1alpha1.Backup{
				gcBackup("c-old", mysqlv1alpha1.BackupPhaseCompleted, 3),
				gcBackup("c-mid", mysqlv1alpha1.BackupPhaseCompleted, 2),
				gcBackup("c-new", mysqlv1alpha1.BackupPhaseCompleted, 1),
			},
			succLimit: ptr.To(int32(2)),
			wantDel:   []string{"c-old"},
		},
		{
			name: "failed count is independent of completed",
			children: []mysqlv1alpha1.Backup{
				gcBackup("c-new", mysqlv1alpha1.BackupPhaseCompleted, 1),
				gcBackup("f-old", mysqlv1alpha1.BackupPhaseFailed, 3),
				gcBackup("f-new", mysqlv1alpha1.BackupPhaseFailed, 1),
			},
			succLimit: ptr.To(int32(5)),
			failLimit: ptr.To(int32(1)),
			wantDel:   []string{"f-old"},
		},
		{
			name: "time window deletes aged terminal backups",
			children: []mysqlv1alpha1.Backup{
				gcBackup("c-new", mysqlv1alpha1.BackupPhaseCompleted, 1),
				gcBackup("c-aged", mysqlv1alpha1.BackupPhaseCompleted, 40),
				gcBackup("f-aged", mysqlv1alpha1.BackupPhaseFailed, 40),
			},
			window:  30 * day,
			wantDel: []string{"c-aged", "f-aged"},
		},
		{
			name: "newest completed is always kept even if aged past window",
			children: []mysqlv1alpha1.Backup{
				gcBackup("c-older", mysqlv1alpha1.BackupPhaseCompleted, 100),
				gcBackup("c-newer", mysqlv1alpha1.BackupPhaseCompleted, 90),
			},
			window:  30 * day,
			wantDel: []string{"c-older"}, // c-newer (smaller age) is the floor
		},
		{
			name: "pending and running are never deleted",
			children: []mysqlv1alpha1.Backup{
				gcBackup("p", mysqlv1alpha1.BackupPhasePending, 100),
				gcBackup("r", mysqlv1alpha1.BackupPhaseRunning, 100),
				gcBackup("c-new", mysqlv1alpha1.BackupPhaseCompleted, 1),
			},
			succLimit: ptr.To(int32(1)),
			window:    1 * day,
			wantDel:   nil,
		},
		{
			name: "count OR window union",
			children: []mysqlv1alpha1.Backup{
				gcBackup("c-new", mysqlv1alpha1.BackupPhaseCompleted, 1),   // kept (floor + within limit)
				gcBackup("c-2", mysqlv1alpha1.BackupPhaseCompleted, 2),     // kept (within count of 2)
				gcBackup("c-3", mysqlv1alpha1.BackupPhaseCompleted, 3),     // over count
				gcBackup("c-aged", mysqlv1alpha1.BackupPhaseCompleted, 40), // over count AND over window
			},
			succLimit: ptr.To(int32(2)),
			window:    30 * day,
			wantDel:   []string{"c-3", "c-aged"},
		},
		{
			name: "zero successful limit prunes all completed but the floor",
			children: []mysqlv1alpha1.Backup{
				gcBackup("c-new", mysqlv1alpha1.BackupPhaseCompleted, 1),
				gcBackup("c-old", mysqlv1alpha1.BackupPhaseCompleted, 2),
			},
			succLimit: ptr.To(int32(0)),
			wantDel:   []string{"c-old"}, // c-new kept as the mandatory floor
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := planBackupGC(tc.children, tc.succLimit, tc.failLimit, tc.window, now)
			gotNames := deletedNames(got)
			want := append([]string(nil), tc.wantDel...)
			sort.Strings(want)
			if len(gotNames) != len(want) {
				t.Fatalf("delete set = %v, want %v", gotNames, want)
			}
			for i := range want {
				if gotNames[i] != want[i] {
					t.Fatalf("delete set = %v, want %v", gotNames, want)
				}
			}
		})
	}
}

// TestScheduledBackupReconcileGCDeletesExpired drives the reconciler end to end
// through the fake client and asserts GC deletes exactly the expired completed
// Backups while keeping the newest and never touching a running one.
func TestScheduledBackupReconcileGCDeletesExpired(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	sb.Spec.Suspend = ptr.To(true) // isolate GC from the scheduling path
	sb.Spec.SuccessfulBackupsHistoryLimit = ptr.To(int32(1))

	child := func(name string, phase mysqlv1alpha1.BackupPhase, ageMin int) *mysqlv1alpha1.Backup {
		return &mysqlv1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         "default",
				CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Duration(ageMin) * time.Minute)),
				Labels:            map[string]string{parentScheduledBackupLabel: "nightly"},
			},
			Status: mysqlv1alpha1.BackupStatus{Phase: phase},
		}
	}
	objs := []client.Object{
		sb,
		child("c-new", mysqlv1alpha1.BackupPhaseCompleted, 1),
		child("c-old", mysqlv1alpha1.BackupPhaseCompleted, 10),
		child("r", mysqlv1alpha1.BackupPhaseRunning, 5),
	}
	r := newScheduledBackupReconciler(t, scheme, objs...)

	reconcileScheduled(t, r)

	remaining := deletedNames(listScheduledChildBackups(t, r))
	want := []string{"c-new", "r"}
	if len(remaining) != len(want) {
		t.Fatalf("remaining backups = %v, want %v", remaining, want)
	}
	for i := range want {
		if remaining[i] != want[i] {
			t.Fatalf("remaining backups = %v, want %v", remaining, want)
		}
	}
}
