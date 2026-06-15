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

package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

const scheduledTestCluster = "demo"

func baseScheduledBackup() *mysqlv1alpha1.ScheduledBackup {
	return &mysqlv1alpha1.ScheduledBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nightly",
			Namespace: "default",
		},
		Spec: mysqlv1alpha1.ScheduledBackupSpec{
			Schedule: "* * * * * *",
			Cluster:  mysqlv1alpha1.LocalObjectReference{Name: scheduledTestCluster},
		},
	}
}

func newScheduledBackupReconciler(t *testing.T, scheme *runtime.Scheme, objs ...client.Object) *ScheduledBackupReconciler {
	t.Helper()
	return &ScheduledBackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.ScheduledBackup{}, &mysqlv1alpha1.Backup{}).
			WithIndex(&mysqlv1alpha1.Backup{}, backupParentScheduledBackupIndex, func(rawObj client.Object) []string {
				backup := rawObj.(*mysqlv1alpha1.Backup)
				if parent, ok := backup.Labels[parentScheduledBackupLabel]; ok {
					return []string{parent}
				}
				return nil
			}).
			WithObjects(objs...).
			Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
}

func reconcileScheduled(t *testing.T, r *ScheduledBackupReconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "nightly",
	}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func listScheduledChildBackups(t *testing.T, r *ScheduledBackupReconciler) []mysqlv1alpha1.Backup {
	t.Helper()
	var list mysqlv1alpha1.BackupList
	if err := r.List(context.Background(), &list, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	return list.Items
}

func getScheduledBackup(t *testing.T, r *ScheduledBackupReconciler) *mysqlv1alpha1.ScheduledBackup {
	t.Helper()
	sb := &mysqlv1alpha1.ScheduledBackup{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "nightly"}, sb); err != nil {
		t.Fatal(err)
	}
	return sb
}

func TestScheduledBackupSuspendedIsNoOp(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	sb.Spec.Suspend = ptr.To(true)
	r := newScheduledBackupReconciler(t, scheme, sb)

	result := reconcileScheduled(t, r)
	if result.RequeueAfter != 0 {
		t.Fatalf("requeueAfter = %s, want 0", result.RequeueAfter)
	}
	if backups := listScheduledChildBackups(t, r); len(backups) != 0 {
		t.Fatalf("created %d backups, want 0", len(backups))
	}
}

func TestScheduledBackupImmediateCreatesOneBackup(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	sb.Spec.Immediate = ptr.To(true)
	r := newScheduledBackupReconciler(t, scheme, sb)

	reconcileScheduled(t, r)

	backups := listScheduledChildBackups(t, r)
	if len(backups) != 1 {
		t.Fatalf("created %d backups, want 1", len(backups))
	}
	backup := backups[0]
	if backup.Labels[parentScheduledBackupLabel] != "nightly" {
		t.Fatalf("parent label = %q", backup.Labels[parentScheduledBackupLabel])
	}
	if backup.Labels[immediateBackupLabel] != "true" {
		t.Fatalf("immediate label = %q", backup.Labels[immediateBackupLabel])
	}
	if backup.Labels[clusterLabel] != scheduledTestCluster {
		t.Fatalf("cluster label = %q", backup.Labels[clusterLabel])
	}
	// Default ownerReference mode is "self": the SB owns the Backup.
	if len(backup.OwnerReferences) != 1 || backup.OwnerReferences[0].Kind != "ScheduledBackup" {
		t.Fatalf("owner references = %#v", backup.OwnerReferences)
	}

	updated := getScheduledBackup(t, r)
	if updated.Status.LastScheduleTime == nil {
		t.Fatal("lastScheduleTime not set")
	}
	if updated.Status.LastCheckTime == nil {
		t.Fatal("lastCheckTime not set")
	}
}

func TestScheduledBackupImmediateAdoptsExistingOnRetry(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	sb.Spec.Immediate = ptr.To(true)
	// Simulate a prior reconcile that created the immediate Backup but failed to
	// land the status patch (LastCheckTime still nil).
	existing := &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "nightly-20260613010203",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
			Labels: map[string]string{
				parentScheduledBackupLabel: "nightly",
				immediateBackupLabel:       "true",
			},
		},
		// Already finished: the concurrency guard lets the adopt path run.
		Status: mysqlv1alpha1.BackupStatus{Phase: mysqlv1alpha1.BackupPhaseCompleted},
	}
	r := newScheduledBackupReconciler(t, scheme, sb, existing)

	reconcileScheduled(t, r)

	if backups := listScheduledChildBackups(t, r); len(backups) != 1 {
		t.Fatalf("have %d backups, want 1 (no duplicate)", len(backups))
	}
	updated := getScheduledBackup(t, r)
	if updated.Status.LastScheduleTime == nil {
		t.Fatal("lastScheduleTime not set after adoption")
	}
}

func TestScheduledBackupFirstCheckStampsLastCheckTime(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	// Far-future schedule so no slot is due; not immediate.
	sb.Spec.Schedule = "0 0 0 1 1 *"
	r := newScheduledBackupReconciler(t, scheme, sb)

	result := reconcileScheduled(t, r)
	if result.RequeueAfter <= 0 {
		t.Fatalf("requeueAfter = %s, want > 0", result.RequeueAfter)
	}
	if backups := listScheduledChildBackups(t, r); len(backups) != 0 {
		t.Fatalf("created %d backups, want 0 on first check", len(backups))
	}
	updated := getScheduledBackup(t, r)
	if updated.Status.LastCheckTime == nil {
		t.Fatal("lastCheckTime not stamped")
	}
	if updated.Status.LastScheduleTime != nil {
		t.Fatal("lastScheduleTime should not be set on first check")
	}
}

func TestScheduledBackupDueSlotCreatesBackup(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	last := time.Now().Truncate(time.Second).Add(-2 * time.Second)
	sb.Status.LastCheckTime = &metav1.Time{Time: last}
	r := newScheduledBackupReconciler(t, scheme, sb)

	schedule, err := mysqlv1alpha1.ParseSchedule(sb.Spec.Schedule)
	if err != nil {
		t.Fatal(err)
	}
	expectedName := sb.BackupName(schedule.Next(last))

	reconcileScheduled(t, r)

	backup := &mysqlv1alpha1.Backup{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: expectedName}, backup); err != nil {
		t.Fatalf("expected backup %q: %v", expectedName, err)
	}
	if backup.Labels[parentScheduledBackupLabel] != "nightly" {
		t.Fatalf("parent label = %q", backup.Labels[parentScheduledBackupLabel])
	}
	if backup.Labels[immediateBackupLabel] != "false" {
		t.Fatalf("immediate label = %q, want false", backup.Labels[immediateBackupLabel])
	}
	updated := getScheduledBackup(t, r)
	if updated.Status.LastScheduleTime == nil {
		t.Fatal("lastScheduleTime not set")
	}
}

func TestScheduledBackupClusterOwnerReference(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	sb.Spec.Immediate = ptr.To(true)
	sb.Spec.BackupOwnerReference = "cluster"
	cluster := baseCluster()
	r := newScheduledBackupReconciler(t, scheme, sb, cluster)

	reconcileScheduled(t, r)

	backups := listScheduledChildBackups(t, r)
	if len(backups) != 1 {
		t.Fatalf("created %d backups, want 1", len(backups))
	}
	refs := backups[0].OwnerReferences
	if len(refs) != 1 || refs[0].Kind != "Cluster" {
		t.Fatalf("owner references = %#v, want a Cluster owner", refs)
	}
}

func TestScheduledBackupNoneOwnerReference(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	sb.Spec.Immediate = ptr.To(true)
	sb.Spec.BackupOwnerReference = "none"
	r := newScheduledBackupReconciler(t, scheme, sb)

	reconcileScheduled(t, r)

	backups := listScheduledChildBackups(t, r)
	if len(backups) != 1 {
		t.Fatalf("created %d backups, want 1", len(backups))
	}
	if len(backups[0].OwnerReferences) != 0 {
		t.Fatalf("owner references = %#v, want none", backups[0].OwnerReferences)
	}
}

func TestScheduledBackupConcurrencyGuardRequeues(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	sb.Status.LastCheckTime = &metav1.Time{Time: time.Now().Add(-time.Minute)}
	running := &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nightly-running",
			Namespace: "default",
			Labels:    map[string]string{parentScheduledBackupLabel: "nightly"},
		},
		Status: mysqlv1alpha1.BackupStatus{Phase: mysqlv1alpha1.BackupPhaseRunning},
	}
	r := newScheduledBackupReconciler(t, scheme, sb, running)

	result := reconcileScheduled(t, r)
	if result.RequeueAfter != time.Minute {
		t.Fatalf("requeueAfter = %s, want %s", result.RequeueAfter, time.Minute)
	}
	// No new backup beyond the running one.
	if backups := listScheduledChildBackups(t, r); len(backups) != 1 {
		t.Fatalf("have %d backups, want 1", len(backups))
	}
}

func TestScheduledBackupSkipsOnNameCollision(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	sb := baseScheduledBackup()
	last := time.Now().Truncate(time.Second).Add(-2 * time.Second)
	sb.Status.LastCheckTime = &metav1.Time{Time: last}

	schedule, err := mysqlv1alpha1.ParseSchedule(sb.Spec.Schedule)
	if err != nil {
		t.Fatal(err)
	}
	collidingName := sb.BackupName(schedule.Next(last))
	// A Backup occupies this slot's deterministic name but is not ours.
	colliding := &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      collidingName,
			Namespace: "default",
			Labels:    map[string]string{parentScheduledBackupLabel: "someone-else"},
		},
		Status: mysqlv1alpha1.BackupStatus{Phase: mysqlv1alpha1.BackupPhaseCompleted},
	}
	r := newScheduledBackupReconciler(t, scheme, sb, colliding)

	result := reconcileScheduled(t, r)
	if result.RequeueAfter <= 0 {
		t.Fatalf("requeueAfter = %s, want > 0", result.RequeueAfter)
	}
	// We did not create a new backup and did not adopt the collision.
	if backups := listScheduledChildBackups(t, r); len(backups) != 1 {
		t.Fatalf("have %d backups, want 1 (collision untouched)", len(backups))
	}
	updated := getScheduledBackup(t, r)
	if updated.Status.LastScheduleTime != nil {
		t.Fatal("lastScheduleTime should remain unset when skipping a collision")
	}
}
