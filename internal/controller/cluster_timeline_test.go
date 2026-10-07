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
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

var timelineNow = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func mariadbTimelineCluster(epochs ...mysqlv1alpha1.MariaDBEpoch) *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Flavor = mysqlv1alpha1.FlavorMariaDB
	cluster.Spec.Instances = 3
	cluster.Status.MariaDBTimeline = epochs
	return cluster
}

func epoch(instance string, server uint32, handoff string) mysqlv1alpha1.MariaDBEpoch {
	return mysqlv1alpha1.MariaDBEpoch{Instance: instance, ServerID: server, Handoff: handoff,
		Since: metav1.NewTime(timelineNow.Add(-time.Hour))}
}

// observedPrimary is an observation where primary reports writable with the
// given server id and gtid_slave_pos.
func observedPrimary(primary string, server uint32, slavePos string, positions map[string]string) observedCluster {
	o := observedCluster{
		PrimaryName:      primary,
		InstanceNames:    []string{"demo-1", "demo-2", "demo-3"},
		StatusByInstance: map[string]*webserver.Status{},
		GTIDByInstance:   positions,
	}
	o.StatusByInstance[primary] = &webserver.Status{
		Role: webserver.RolePrimary, ServerID: server, GTIDSlavePos: slavePos,
	}
	return o
}

func timelineInstances(tl []mysqlv1alpha1.MariaDBEpoch) []string {
	out := make([]string, 0, len(tl))
	for _, e := range tl {
		out = append(out, fmt.Sprintf("%s/%d/%s", e.Instance, e.ServerID, e.Handoff))
	}
	return out
}

func TestTimelineRecordsTheFirstPrimary(t *testing.T) {
	update := nextMariaDBTimeline(mariadbTimelineCluster(), observedPrimary("demo-1", 1, "", nil), timelineNow)
	if !update.Observed || len(update.Timeline) != 1 {
		t.Fatalf("update = %+v", update)
	}
	got := update.Timeline[0]
	if got.Instance != "demo-1" || got.ServerID != 1 || got.Handoff != "" || !got.Since.Time.Equal(timelineNow) {
		t.Fatalf("epoch = %+v", got)
	}
}

func TestTimelineAppendsOnChangeOfPrimary(t *testing.T) {
	cluster := mariadbTimelineCluster(epoch("demo-1", 1, ""))
	// Failover to demo-2, which inherited 0-1-218.
	update := nextMariaDBTimeline(cluster, observedPrimary("demo-2", 2, "0-1-218", map[string]string{
		"demo-1": "0-1-100", "demo-2": "0-2-300",
	}), timelineNow)
	if want := []string{"demo-1/1/", "demo-2/2/0-1-218"}; !slices.Equal(timelineInstances(update.Timeline), want) {
		t.Fatalf("timeline = %v, want %v", timelineInstances(update.Timeline), want)
	}

	// Failback to demo-1, which inherited 0-2-300.
	cluster.Status.MariaDBTimeline = update.Timeline
	update = nextMariaDBTimeline(cluster, observedPrimary("demo-1", 1, "0-2-300", map[string]string{
		"demo-1": "0-1-100", "demo-2": "0-2-300",
	}), timelineNow)
	if want := []string{"demo-1/1/", "demo-2/2/0-1-218", "demo-1/1/0-2-300"}; !slices.Equal(timelineInstances(update.Timeline), want) {
		t.Fatalf("timeline = %v, want %v", timelineInstances(update.Timeline), want)
	}
}

// A primary that restarts (or is observed again) is the same epoch.
func TestTimelineDoesNotDuplicateTheCurrentPrimary(t *testing.T) {
	cluster := mariadbTimelineCluster(epoch("demo-1", 1, ""), epoch("demo-2", 2, "0-1-218"))
	update := nextMariaDBTimeline(cluster, observedPrimary("demo-2", 2, "0-1-250", map[string]string{
		"demo-1": "0-1-100", "demo-2": "0-2-300",
	}), timelineNow)
	if len(update.Timeline) != 2 || update.Timeline[1].Handoff != "0-1-218" {
		t.Fatalf("timeline = %v", timelineInstances(update.Timeline))
	}
}

func TestTimelineIgnoresANonWritablePrimary(t *testing.T) {
	cluster := mariadbTimelineCluster(epoch("demo-1", 1, ""))
	readOnly := observedPrimary("demo-2", 2, "0-1-218", nil)
	readOnly.StatusByInstance["demo-2"].ReadOnly = true
	noServerID := observedPrimary("demo-2", 0, "0-1-218", nil)
	replica := observedPrimary("demo-2", 2, "0-1-218", nil)
	replica.StatusByInstance["demo-2"].Role = webserver.RoleReplica
	unreachable := observedPrimary("demo-2", 2, "0-1-218", nil)
	delete(unreachable.StatusByInstance, "demo-2")
	for name, o := range map[string]observedCluster{
		"read only": readOnly, "old instance manager": noServerID, "replica role": replica, "unreachable": unreachable,
	} {
		t.Run(name, func(t *testing.T) {
			update := nextMariaDBTimeline(cluster, o, timelineNow)
			if len(update.Timeline) != 1 || update.Timeline[0].Instance != "demo-1" {
				t.Fatalf("timeline = %v", timelineInstances(update.Timeline))
			}
		})
	}
}

func TestTimelineAbsentOnMySQLAndReplicaClusters(t *testing.T) {
	mysql := baseCluster()
	mysql.Status.MariaDBTimeline = []mysqlv1alpha1.MariaDBEpoch{epoch("demo-1", 1, "")}
	if update := nextMariaDBTimeline(mysql, observedPrimary("demo-2", 2, "", nil), timelineNow); !update.Observed || update.Timeline != nil {
		t.Fatalf("mysql update = %+v", update)
	}
	replica := mariadbTimelineCluster(epoch("demo-1", 1, ""))
	replica.Spec.Replica = &mysqlv1alpha1.ReplicaClusterConfiguration{}
	if update := nextMariaDBTimeline(replica, observedPrimary("demo-2", 2, "", nil), timelineNow); update.Timeline != nil {
		t.Fatalf("replica cluster update = %+v", update)
	}
}

// The oldest entry stays while any position the operator still judges sits at
// or below the next handoff: an unreachable or diverged instance's recorded
// position included.
func TestTimelinePruningKeepsWhatAnInstanceStillNeeds(t *testing.T) {
	cluster := mariadbTimelineCluster(epoch("demo-1", 1, ""), epoch("demo-2", 2, "0-1-218"))
	cluster.Status.DivergedInstances = []string{"demo-1"}
	// demo-1 is unreachable this pass; its recorded position is all we have.
	cluster.Status.GTIDExecutedByInstance = map[string]string{"demo-1": "0-1-219"}
	o := observedPrimary("demo-3", 3, "0-2-300", map[string]string{"demo-2": "0-2-310", "demo-3": "0-3-400"})
	update := nextMariaDBTimeline(cluster, o, timelineNow)
	// demo-1's dead 0-1-219 sits above the first handoff, so the genesis entry
	// can go, but the entry that says server 2 authored 219 must stay.
	if want := []string{"demo-2/2/0-1-218", "demo-3/3/0-2-300"}; !slices.Equal(timelineInstances(update.Timeline), want) {
		t.Fatalf("timeline = %v, want %v", timelineInstances(update.Timeline), want)
	}
	if on, known, _ := update.engineTimeline().Judge("0-1-219"); on || !known {
		t.Fatal("the diverged instance's position must still be judged off the timeline after pruning")
	}

	// A lagging instance at or below the first handoff pins the genesis entry.
	cluster.Status.GTIDExecutedByInstance["demo-1"] = "0-1-200"
	update = nextMariaDBTimeline(cluster, o, timelineNow)
	if want := []string{"demo-1/1/", "demo-2/2/0-1-218", "demo-3/3/0-2-300"}; !slices.Equal(timelineInstances(update.Timeline), want) {
		t.Fatalf("timeline = %v, want the lagging instance's history kept", timelineInstances(update.Timeline))
	}

	// Once demo-1 is re-cloned past the handoffs, nothing pins the old entries.
	o.GTIDByInstance["demo-1"] = "0-3-400"
	cluster.Status.MariaDBTimeline = update.Timeline
	update = nextMariaDBTimeline(cluster, o, timelineNow)
	if want := []string{"demo-3/3/0-2-300"}; !slices.Equal(timelineInstances(update.Timeline), want) {
		t.Fatalf("timeline = %v, want %v", timelineInstances(update.Timeline), want)
	}
}

func TestTimelinePruningKeepsWhatTheArchiveStillNeeds(t *testing.T) {
	cluster := mariadbTimelineCluster(epoch("demo-1", 1, ""), epoch("demo-2", 2, "0-1-218"))
	cluster.Spec.Backup = archivingCluster().Spec.Backup
	cluster.Status.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true, OldestSegmentPosition: "0-1-200"}
	o := observedPrimary("demo-2", 2, "0-1-218", map[string]string{"demo-1": "0-2-300", "demo-2": "0-2-300", "demo-3": "0-2-300"})
	if update := nextMariaDBTimeline(cluster, o, timelineNow); len(update.Timeline) != 2 {
		t.Fatalf("timeline = %v, want the archive's oldest segment to pin the first entry", timelineInstances(update.Timeline))
	}
	// The primary's fresh report wins over the last recorded one.
	o.StatusByInstance["demo-2"].Archiving = &webserver.ArchivingStatus{
		ForkCheckedAt: "2026-10-06T12:00:00Z", OldestSegmentPosition: "0-2-250",
	}
	if update := nextMariaDBTimeline(cluster, o, timelineNow); len(update.Timeline) != 1 {
		t.Fatalf("timeline = %v, want pruned once retention moved the oldest segment", timelineInstances(update.Timeline))
	}
	// Archiving off: the archive reference does not apply.
	cluster.Spec.Backup.ContinuousArchiving.Enabled = false
	o.StatusByInstance["demo-2"].Archiving = nil
	if update := nextMariaDBTimeline(cluster, o, timelineNow); len(update.Timeline) != 1 {
		t.Fatalf("timeline = %v, want the stale archive reference ignored", timelineInstances(update.Timeline))
	}
}

// An instance with no recorded position has never been compared, and pins
// nothing.
func TestTimelinePruningIgnoresUnknownPositions(t *testing.T) {
	cluster := mariadbTimelineCluster(epoch("demo-1", 1, ""), epoch("demo-2", 2, "0-1-218"))
	o := observedPrimary("demo-2", 2, "0-1-218", map[string]string{"demo-2": "0-2-300"})
	if update := nextMariaDBTimeline(cluster, o, timelineNow); len(update.Timeline) != 1 {
		t.Fatalf("timeline = %v", timelineInstances(update.Timeline))
	}
}

func TestTimelineCeilingTruncatesAndNamesWhatItPinned(t *testing.T) {
	epochs := make([]mysqlv1alpha1.MariaDBEpoch, 0, engine.MariaDBTimelineCeiling)
	for i := range engine.MariaDBTimelineCeiling {
		epochs = append(epochs, epoch(fmt.Sprintf("demo-%d", i%2+1), uint32(i%2+1), fmt.Sprintf("0-%d-%d", (i+1)%2+1, i*10)))
	}
	cluster := mariadbTimelineCluster(epochs...)
	cluster.Status.GTIDExecutedByInstance = map[string]string{"demo-3": "0-1-5"}
	o := observedPrimary("demo-3", 3, fmt.Sprintf("0-2-%d", engine.MariaDBTimelineCeiling*10), map[string]string{
		"demo-1": "0-3-99999", "demo-2": "0-3-99999",
	})
	update := nextMariaDBTimeline(cluster, o, timelineNow)
	if len(update.Timeline) != engine.MariaDBTimelineCeiling || !update.Truncated {
		t.Fatalf("len = %d truncated = %v", len(update.Timeline), update.Truncated)
	}
	if !slices.Equal(update.PinnedBy, []string{"demo-3"}) {
		t.Fatalf("pinnedBy = %v", update.PinnedBy)
	}
	if update.Timeline[len(update.Timeline)-1].Instance != "demo-3" {
		t.Fatal("the new epoch must survive truncation")
	}

	recorder := record.NewFakeRecorder(2)
	r := &ClusterReconciler{Recorder: recorder}
	r.recordTimelineTruncatedEvent(cluster, update)
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "Warning MariaDBTimelineTruncated") || !strings.Contains(ev, "demo-3") {
			t.Fatalf("event = %q", ev)
		}
	default:
		t.Fatal("expected a truncation event")
	}
	r.recordTimelineTruncatedEvent(cluster, timelineUpdate{Observed: true})
	if len(recorder.Events) != 0 {
		t.Fatal("no event without truncation")
	}
}

// A first primary did not necessarily author the history it holds: a cluster
// bootstrapped from a backup holds the source cluster's transactions, under
// another server id, and a cluster upgraded mid-life holds history no epoch saw.
// The first epoch therefore starts at the primary's current position, so that
// history gets no verdict instead of being attributed to the first primary.
func TestTimelineFirstEpochStartsAtThePrimaryPosition(t *testing.T) {
	o := observedPrimary("demo-1", 7, "", map[string]string{"demo-1": "0-1-500", "demo-2": "0-1-500"})
	o.StatusByInstance["demo-1"].GTIDExecuted = "0-1-500"
	update := nextMariaDBTimeline(mariadbTimelineCluster(), o, timelineNow)
	if len(update.Timeline) != 1 || update.Timeline[0].Handoff != "0-1-500" {
		t.Fatalf("timeline = %v, want the first epoch to start at 0-1-500", timelineInstances(update.Timeline))
	}
	// The restored replica, cloned at the source's position, is not judged off.
	if _, known, _ := update.engineTimeline().Judge("0-1-500"); known {
		t.Fatal("history from before the first epoch must get no verdict")
	}
	if on, known, _ := update.engineTimeline().Judge("0-7-501"); !on || !known {
		t.Fatal("the first primary's own writes are on the timeline")
	}

	// Later epochs keep the inherited position.
	cluster := mariadbTimelineCluster(update.Timeline...)
	next := observedPrimary("demo-2", 8, "0-7-600", map[string]string{"demo-1": "0-7-600", "demo-2": "0-8-700"})
	next.StatusByInstance["demo-2"].GTIDExecuted = "0-8-700"
	update = nextMariaDBTimeline(cluster, next, timelineNow)
	if got := update.Timeline[len(update.Timeline)-1].Handoff; got != "0-7-600" {
		t.Fatalf("a successor's handoff = %q, want its gtid_slave_pos 0-7-600", got)
	}
}

// A Cluster whose status lost its timeline seeds it from the archive the
// primary reports, instead of restarting history at the current position.
func TestTimelineSeedsFromTheArchive(t *testing.T) {
	cluster := mariadbTimelineCluster()
	// demo-3 still sits before the handoff, which pins the first epoch.
	observed := observedPrimary("demo-2", 2, "0-1-218", map[string]string{"demo-2": "0-2-300", "demo-3": "0-1-100"})
	observed.StatusByInstance["demo-2"].Archiving = &webserver.ArchivingStatus{
		ForkCheckedAt: "2026-10-06T11:00:00Z",
		MariaDBTimeline: []webserver.ArchiveEpochStatus{
			{Instance: "demo-1", ServerID: 1},
			{Instance: "demo-2", ServerID: 2, Handoff: "0-1-218"},
		},
	}
	got := timelineInstances(nextMariaDBTimeline(cluster, observed, timelineNow).Timeline)
	want := []string{"demo-1/1/", "demo-2/2/0-1-218"}
	if !slices.Equal(got, want) {
		t.Fatalf("timeline = %v, want %v (seeded, current primary not duplicated)", got, want)
	}

	// An archive the primary has not checked yet seeds nothing.
	observed.StatusByInstance["demo-2"].Archiving.ForkCheckedAt = ""
	got = timelineInstances(nextMariaDBTimeline(cluster, observed, timelineNow).Timeline)
	if !slices.Equal(got, []string{"demo-2/2/"}) {
		t.Fatalf("timeline = %v, want a fresh first epoch", got)
	}
}
