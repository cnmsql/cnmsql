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
	got := aggregateArchiving(observed, nil, 0)
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
			got := aggregateArchiving(observed, prior, 0)
			if !slices.Equal(got.ForkGTIDs, prior.ForkGTIDs) || got.ForkDetectedAt == nil ||
				got.OldestSegmentPosition != "0-1-3" || got.DisownedGTIDs != "u1:219" {
				t.Fatalf("aggregated = %+v, want the prior fork fields", got)
			}
		})
	}
	// Once the primary has checked, its answer wins, empty included.
	if got := aggregateArchiving(forkedPrimaryStatus("2026-10-06T12:05:00Z"), prior, 0); len(got.ForkGTIDs) != 0 || got.ForkDetectedAt != nil {
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

// returningPrimaryReport is the report of the incident: a former primary that
// was unreachable while its successor recorded a fork, back writable for the
// seconds before it is demoted. Its preserved state comes from its last fork
// check, under generation 3, and does not carry its successor's fork. The
// check's wall clock is later than the fork's detection (clock skew, or a
// record that grew after its first detection), so only the generation can
// tell the report is stale.
func returningPrimaryReport() observedCluster {
	return observedCluster{
		PrimaryName: "demo-0",
		StatusByInstance: map[string]*webserver.Status{
			"demo-0": {Archiving: &webserver.ArchivingStatus{
				Active: true, ForkCheckedAt: "2026-10-06T12:30:00Z", ForkCheckGeneration: 3,
				LastArchivedBinlog: "mysql-bin.000042", DisownedGTIDs: "", CoveredGTIDSet: "u1:1-218",
				OldestSegmentPosition: "0-1-1", Gaps: []string{"u1:100-110"},
				MariaDBTimeline: []webserver.ArchiveEpochStatus{{Instance: "demo-0", ServerID: 1}},
			}},
		},
	}
}

// A report fork-checked under a superseded primary generation must not replace
// anything read from the archive index: not the fork records its successor
// wrote, and not the frontier (coverage, gaps, oldest segment) of the same
// stale read.
func TestAggregateArchivingKeepsTheRecordOverASupersededReport(t *testing.T) {
	detected := metav1.NewTime(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC))
	since := metav1.NewTime(time.Date(2026, 10, 6, 11, 0, 0, 0, time.UTC))
	for name, prior := range map[string]*mysqlv1alpha1.ContinuousArchivingStatus{
		"fork recorded": {
			Enabled: true, ForkGTIDs: []string{"u1:219"}, ForkDetectedAt: &detected, DisownedGTIDs: "u1:219",
			CoveredGTIDSet: "u1:1-219", OldestSegmentPosition: "0-1-3",
		},
		// Retention dropped the forked segment: no record, but the index's
		// disowned set still names its transactions.
		"segment dropped by retention": {
			Enabled: true, DisownedGTIDs: "u1:219", CoveredGTIDSet: "u1:1-219", OldestSegmentPosition: "0-1-3",
			Gaps: []string{"u1:50"}, GapsSince: &since,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := aggregateArchiving(returningPrimaryReport(), prior, 4)
			if got.DisownedGTIDs != "u1:219" || !slices.Equal(got.ForkGTIDs, prior.ForkGTIDs) ||
				got.ForkDetectedAt != prior.ForkDetectedAt {
				t.Errorf("fork fields = %q %v %v, want the recorded ones", got.DisownedGTIDs, got.ForkGTIDs, got.ForkDetectedAt)
			}
			if got.CoveredGTIDSet != "u1:1-219" || got.OldestSegmentPosition != "0-1-3" ||
				!slices.Equal(got.Gaps, prior.Gaps) || got.GapsSince != prior.GapsSince {
				t.Errorf("index frontier = %q %q %v %v, want the recorded one",
					got.CoveredGTIDSet, got.OldestSegmentPosition, got.Gaps, got.GapsSince)
			}
			// What the instance itself shipped is not read from the index.
			if got.LastArchivedBinlog != "mysql-bin.000042" {
				t.Errorf("LastArchivedBinlog = %q, want the report's", got.LastArchivedBinlog)
			}
		})
	}
}

// A report checked under the current generation is the archive's answer,
// empty included: a retraction or a retention clear must reach the status.
func TestAggregateArchivingTakesACurrentReport(t *testing.T) {
	detected := metav1.NewTime(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC))
	prior := &mysqlv1alpha1.ContinuousArchivingStatus{
		Enabled: true, ForkGTIDs: []string{"u1:219"}, ForkDetectedAt: &detected, DisownedGTIDs: "u1:219",
	}
	// The cached Cluster can lag the instance that just promoted itself, so a
	// newer generation than the controller's is current too.
	for _, generation := range []int64{3, 2} {
		got := aggregateArchiving(returningPrimaryReport(), prior, generation)
		if got.DisownedGTIDs != "" || len(got.ForkGTIDs) != 0 || got.ForkDetectedAt != nil {
			t.Errorf("generation %d: fork fields = %q %v %v, want the report's empty answer",
				generation, got.DisownedGTIDs, got.ForkGTIDs, got.ForkDetectedAt)
		}
		if got.CoveredGTIDSet != "u1:1-218" || got.OldestSegmentPosition != "0-1-1" {
			t.Errorf("generation %d: frontier = %q %q, want the report's", generation, got.CoveredGTIDSet, got.OldestSegmentPosition)
		}
	}
}

// An instance manager older than ForkCheckGeneration reports none. Once the
// Cluster has a primary generation its report cannot be ordered, so the record
// waits for a primary that reports one; a Cluster that never had one takes it.
func TestAggregateArchivingReportWithoutAGeneration(t *testing.T) {
	prior := &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true, DisownedGTIDs: "u1:219"}
	observed := forkedPrimaryStatus("2026-10-06T12:05:00Z")
	observed.StatusByInstance["demo-2"].Archiving.DisownedGTIDs = ""
	if got := aggregateArchiving(observed, prior, 1); got.DisownedGTIDs != "u1:219" {
		t.Errorf("generation 1: disownedGTIDs = %q, want the record kept", got.DisownedGTIDs)
	}
	if got := aggregateArchiving(observed, prior, 0); got.DisownedGTIDs != "" {
		t.Errorf("generation 0: disownedGTIDs = %q, want the report's", got.DisownedGTIDs)
	}
}

// The readers that need the archive's fields before or outside the aggregation
// apply the same rule: a superseded report falls back to status.
func TestArchiveIndexReadersIgnoreASupersededReport(t *testing.T) {
	cluster := archivingCluster()
	cluster.Status.CurrentPrimaryGeneration = 4
	cluster.Status.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingStatus{
		Enabled: true, DisownedGTIDs: "u1:219", OldestSegmentPosition: "0-1-3",
	}
	observed := returningPrimaryReport()
	if got := disownedGTIDs(observed, cluster); got != "u1:219" {
		t.Errorf("disownedGTIDs = %q, want the recorded u1:219", got)
	}
	if got := oldestArchivedPosition(cluster, observed); got != "0-1-3" {
		t.Errorf("oldestArchivedPosition = %q, want the recorded 0-1-3", got)
	}
	if got := archivedTimeline(observed, cluster, time.Now()); got != nil {
		t.Errorf("archivedTimeline = %v, want nothing seeded from a superseded report", got)
	}

	cluster.Status.CurrentPrimaryGeneration = 3
	if got := disownedGTIDs(observed, cluster); got != "" {
		t.Errorf("current: disownedGTIDs = %q, want the report's empty set", got)
	}
	if got := oldestArchivedPosition(cluster, observed); got != "0-1-1" {
		t.Errorf("current: oldestArchivedPosition = %q, want the report's 0-1-1", got)
	}
	if got := archivedTimeline(observed, cluster, time.Now()); len(got) != 1 || got[0].Instance != "demo-0" {
		t.Errorf("current: archivedTimeline = %v, want the report's epoch", got)
	}
}
