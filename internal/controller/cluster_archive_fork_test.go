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
	"slices"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func forkedPrimaryStatus(checkedAt string, forks ...webserver.ArchiveForkStatus) observedCluster {
	return observedCluster{
		PrimaryName: "demo-2",
		StatusByInstance: map[string]*webserver.Status{
			"demo-2": {Archiving: &webserver.ArchivingStatus{
				Active: true, Forks: forks, ForkCheckedAt: checkedAt, OldestSegmentPosition: "0-1-219",
				DisownedGTIDs: "u1:219",
			}},
		},
	}
}

func TestAggregateArchivingMirrorsForks(t *testing.T) {
	observed := forkedPrimaryStatus("2026-10-06T12:05:00Z",
		webserver.ArchiveForkStatus{Segment: "old", GTIDs: "u1:219", DetectedAt: "2026-10-06T12:00:00Z"},
		webserver.ArchiveForkStatus{Segment: "older", GTIDs: "u0:5-6", DetectedAt: "2026-10-06T11:00:00Z"},
	)
	got := aggregateArchiving(observed, nil)
	if !slices.Equal(got.ForkGTIDs, []string{"u1:219", "u0:5-6"}) {
		t.Fatalf("forkGTIDs = %v", got.ForkGTIDs)
	}
	if got.ForkDetectedAt == nil || !got.ForkDetectedAt.Time.Equal(time.Date(2026, 10, 6, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("forkDetectedAt = %v, want the earliest detection", got.ForkDetectedAt)
	}
	if got.OldestSegmentPosition != "0-1-219" {
		t.Fatalf("oldestSegmentPosition = %q", got.OldestSegmentPosition)
	}
	if got.DisownedGTIDs != "u1:219" {
		t.Fatalf("disownedGTIDs = %q", got.DisownedGTIDs)
	}
}

// Between the old primary going away and the new primary's first fork check,
// nothing the cluster sees says the archive changed: the recorded forks are
// kept, so the condition does not flap.
func TestAggregateArchivingKeepsForksUntilTheNewPrimaryChecks(t *testing.T) {
	detected := metav1.NewTime(time.Date(2026, 10, 6, 11, 0, 0, 0, time.UTC))
	prior := &mysqlv1alpha1.ContinuousArchivingStatus{
		Enabled: true, ForkGTIDs: []string{"u1:219"}, ForkDetectedAt: &detected, OldestSegmentPosition: "0-1-3",
		DisownedGTIDs: "u1:219",
	}
	for name, observed := range map[string]observedCluster{
		"primary unreachable":          {PrimaryName: "demo-2", StatusByInstance: map[string]*webserver.Status{}},
		"primary has not checked yet":  forkedPrimaryStatus(""),
		"primary reports no archiving": {PrimaryName: "demo-2", StatusByInstance: map[string]*webserver.Status{"demo-2": {}}},
	} {
		t.Run(name, func(t *testing.T) {
			got := aggregateArchiving(observed, prior)
			if !slices.Equal(got.ForkGTIDs, prior.ForkGTIDs) || got.ForkDetectedAt == nil ||
				got.OldestSegmentPosition != "0-1-3" || got.DisownedGTIDs != "u1:219" {
				t.Fatalf("aggregated = %+v, want the prior fork fields", got)
			}
		})
	}
	// Once the primary has checked, its answer wins, empty included.
	if got := aggregateArchiving(forkedPrimaryStatus("2026-10-06T12:05:00Z"), prior); len(got.ForkGTIDs) != 0 || got.ForkDetectedAt != nil {
		t.Fatalf("a checked primary reporting no forks must clear them: %+v", got)
	}
}

func forkCluster(forks ...string) *mysqlv1alpha1.Cluster {
	cluster := archivingCluster()
	cluster.Status.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true, ForkGTIDs: forks}
	return cluster
}

func TestArchiveForkedCondition(t *testing.T) {
	cluster := forkCluster("u1:219", "0-1-219..0-1-225")
	applyArchiveForkedCondition(cluster)
	cond := apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionArchiveForked)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "DisownedTransactionsArchived" {
		t.Fatalf("condition = %+v", cond)
	}
	if !strings.Contains(cond.Message, "u1:219") || !strings.Contains(cond.Message, "0-1-219..0-1-225") {
		t.Fatalf("message should name the disowned transactions: %q", cond.Message)
	}

	cluster.Status.ContinuousArchiving.ForkGTIDs = nil
	applyArchiveForkedCondition(cluster)
	cond = apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionArchiveForked)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "NoFork" {
		t.Fatalf("condition = %+v, want False once retention dropped the forked segments", cond)
	}

	off := forkCluster("u1:219")
	applyArchiveForkedCondition(off)
	off.Spec.Backup.ContinuousArchiving.Enabled = false
	applyArchiveForkedCondition(off)
	if apimeta.FindStatusCondition(off.Status.Conditions, mysqlv1alpha1.ConditionArchiveForked) != nil {
		t.Fatal("the condition must be absent with archiving off")
	}
}

func TestArchiveForkedEventOnlyOnTransition(t *testing.T) {
	cluster := forkCluster("u1:219")
	applyArchiveForkedCondition(cluster)
	recorder := record.NewFakeRecorder(4)
	r := &ClusterReconciler{Recorder: recorder}

	r.recordArchiveForkedEvent(cluster, true)
	if len(recorder.Events) != 0 {
		t.Fatal("no event while the fork was already reported")
	}
	r.recordArchiveForkedEvent(cluster, false)
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "Warning ArchiveForked") || !strings.Contains(ev, "u1:219") {
			t.Fatalf("event = %q", ev)
		}
	default:
		t.Fatal("expected a warning event on the transition")
	}

	clean := forkCluster()
	applyArchiveForkedCondition(clean)
	r.recordArchiveForkedEvent(clean, false)
	if len(recorder.Events) != 0 {
		t.Fatal("no event without a fork")
	}
}

// A status patch made without observing the instances (an early return of the
// reconcile) carries no archiving status. It must leave the condition as it
// was, not report the archive unforked and re-fire the event later.
func TestArchiveForkedConditionKeptWithoutAnObservation(t *testing.T) {
	cluster := forkCluster("u1:219")
	applyArchiveForkedCondition(cluster)
	cluster.Status.ContinuousArchiving = nil
	applyArchiveForkedCondition(cluster)
	cond := apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionArchiveForked)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("condition = %+v, want it kept True", cond)
	}
}

// A pass that did not observe the instances keeps the last reported archiving
// status instead of wiping it; turning archiving off clears it.
func TestApplyArchivingStatus(t *testing.T) {
	detected := metav1.NewTime(time.Date(2026, 10, 6, 11, 0, 0, 0, time.UTC))
	reported := &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true, ForkGTIDs: []string{"u1:219"}, ForkDetectedAt: &detected}

	cluster := forkCluster()
	cluster.Status.ContinuousArchiving = reported
	applyArchivingStatus(cluster, observedCluster{})
	if cluster.Status.ContinuousArchiving != reported {
		t.Fatalf("status = %+v, want the last report kept", cluster.Status.ContinuousArchiving)
	}

	fresh := &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true}
	applyArchivingStatus(cluster, observedCluster{ContinuousArchiving: fresh})
	if cluster.Status.ContinuousArchiving != fresh {
		t.Fatal("a full observation must replace the status")
	}

	cluster.Spec.Backup.ContinuousArchiving.Enabled = false
	applyArchivingStatus(cluster, observedCluster{})
	if cluster.Status.ContinuousArchiving != nil {
		t.Fatal("archiving off must clear the status")
	}
}
