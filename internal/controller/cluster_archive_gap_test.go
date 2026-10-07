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
	"context"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

const gapUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func gapCluster(gaps ...string) *mysqlv1alpha1.Cluster {
	cluster := archivingCluster()
	cluster.Status.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true, Gaps: gaps}
	return cluster
}

func completedBackup(name, anchor string, at time.Time) *mysqlv1alpha1.Backup {
	stopped := metav1.NewTime(at)
	return &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       mysqlv1alpha1.BackupSpec{Cluster: mysqlv1alpha1.LocalObjectReference{Name: "demo"}},
		Status: mysqlv1alpha1.BackupStatus{
			Phase: mysqlv1alpha1.BackupPhaseCompleted, EndGTID: anchor, StoppedAt: &stopped,
		},
	}
}

func gapReconciler(t *testing.T, objects ...client.Object) *ClusterReconciler {
	t.Helper()
	scheme := testScheme(t)
	return &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(8),
	}
}

func TestAnchorCoversGap(t *testing.T) {
	cases := []struct {
		anchor, gap string
		want        bool
	}{
		{"", gapUUID + ":101-150", false},
		{gapUUID + ":1-160", gapUUID + ":101-150", true},
		{gapUUID + ":1-120", gapUUID + ":101-150", false},
		{"0-2-300", "0-101..150", true},
		{"0-1-100", "0-101..150", false},
		{"1-2-300", "0-101..150", false},
	}
	for _, tc := range cases {
		if got := anchorCoversGap(tc.anchor, tc.gap); got != tc.want {
			t.Errorf("anchorCoversGap(%q, %q) = %v, want %v", tc.anchor, tc.gap, got, tc.want)
		}
	}
}

func TestArchiveGapCondition(t *testing.T) {
	ctx := context.Background()
	gap := gapUUID + ":101-150"
	now := time.Now()

	cluster := gapCluster(gap)
	r := gapReconciler(t, completedBackup("old", gapUUID+":1-50", now.Add(-time.Hour)))
	r.applyArchiveGapCondition(ctx, cluster)
	cond := apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionArchiveGap)
	if cond == nil || cond.Status != metav1.ConditionTrue || !strings.Contains(cond.Message, gap) {
		t.Fatalf("condition = %+v, want True naming the gap", cond)
	}

	// A newer backup past the gap anchors past it.
	r = gapReconciler(t,
		completedBackup("old", gapUUID+":1-50", now.Add(-time.Hour)),
		completedBackup("new", gapUUID+":1-170", now))
	r.applyArchiveGapCondition(ctx, cluster)
	cond = apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionArchiveGap)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "GapBehindNewestBackup" {
		t.Fatalf("condition = %+v, want False behind the newest backup", cond)
	}

	clean := gapCluster()
	r.applyArchiveGapCondition(ctx, clean)
	cond = apimeta.FindStatusCondition(clean.Status.Conditions, mysqlv1alpha1.ConditionArchiveGap)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "NoGap" {
		t.Fatalf("condition = %+v, want NoGap", cond)
	}
}

// On the transition to True the operator warns and takes a base backup, once.
func TestArchiveGapTakesABackup(t *testing.T) {
	ctx := context.Background()
	cluster := gapCluster(gapUUID + ":101-150")
	r := gapReconciler(t, cluster)
	r.applyArchiveGapCondition(ctx, cluster)
	r.onArchiveGap(ctx, cluster, false)
	r.onArchiveGap(ctx, cluster, false)

	var backups mysqlv1alpha1.BackupList
	if err := r.List(ctx, &backups); err != nil {
		t.Fatal(err)
	}
	if len(backups.Items) != 1 {
		t.Fatalf("backups = %d, want exactly one", len(backups.Items))
	}
	b := backups.Items[0]
	if b.Spec.Cluster.Name != cluster.Name || b.Labels[archiveGapBackupLabel] != "true" || len(b.OwnerReferences) != 1 {
		t.Fatalf("backup = %+v", b.ObjectMeta)
	}
	select {
	case ev := <-r.Recorder.(*record.FakeRecorder).Events:
		if !strings.Contains(ev, "Warning ArchiveGap") {
			t.Fatalf("event = %q", ev)
		}
	default:
		t.Fatal("expected a warning event")
	}

	// Already gapped: no new event, no new backup.
	r.onArchiveGap(ctx, cluster, true)
	if err := r.List(ctx, &backups); err != nil || len(backups.Items) != 1 {
		t.Fatalf("backups = %d, err = %v", len(backups.Items), err)
	}
}

// A fresh gap waits for the former primary's drain before it counts.
func TestArchiveGapWaitsForTheGrace(t *testing.T) {
	ctx := context.Background()
	cluster := gapCluster(gapUUID + ":101-150")
	recent := metav1.NewTime(time.Now().Add(-30 * time.Second))
	cluster.Status.ContinuousArchiving.GapsSince = &recent
	r := gapReconciler(t)
	r.applyArchiveGapCondition(ctx, cluster)
	cond := apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionArchiveGap)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "GapPendingRepair" {
		t.Fatalf("condition = %+v, want False pending repair", cond)
	}
	old := metav1.NewTime(time.Now().Add(-archiveGapGrace(cluster) - time.Second))
	cluster.Status.ContinuousArchiving.GapsSince = &old
	r.applyArchiveGapCondition(ctx, cluster)
	if !apimeta.IsStatusConditionTrue(cluster.Status.Conditions, mysqlv1alpha1.ConditionArchiveGap) {
		t.Fatal("a gap past the grace must raise the condition")
	}
}

func TestAggregateArchivingKeepsGapsSince(t *testing.T) {
	since := metav1.NewTime(time.Now().Add(-time.Hour))
	prior := &mysqlv1alpha1.ContinuousArchivingStatus{Gaps: []string{"u:5"}, GapsSince: &since}
	observed := forkedPrimaryStatus("2026-10-06T12:05:00Z")
	observed.StatusByInstance["demo-2"].Archiving.Gaps = []string{"u:5"}
	if got := aggregateArchiving(observed, prior); got.GapsSince == nil || !got.GapsSince.Equal(&since) {
		t.Fatalf("gapsSince = %v, want the prior stamp kept", got.GapsSince)
	}
	observed.StatusByInstance["demo-2"].Archiving.Gaps = []string{"u:5", "u:9"}
	if got := aggregateArchiving(observed, prior); got.GapsSince == nil || got.GapsSince.Equal(&since) {
		t.Fatalf("gapsSince = %v, want a new stamp for new gaps", got.GapsSince)
	}
}
