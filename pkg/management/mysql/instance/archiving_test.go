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

package instance

import (
	"maps"
	"slices"
	"testing"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/binlog"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func TestClusterFloorFailsClosedBeforeObservingACluster(t *testing.T) {
	t.Parallel()
	if _, _, ok := newClusterFloor("demo-1").Positions(); ok {
		t.Fatal("a floor that never saw the Cluster must not allow a purge")
	}
}

func TestClusterFloorPositions(t *testing.T) {
	t.Parallel()
	floor := newClusterFloor("demo-1")
	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Status.InstanceNames = []string{"demo-1", "demo-2", "demo-3", "demo-4", "demo-5"}
	cluster.Status.DivergedInstances = []string{"demo-3"}
	cluster.Status.GTIDExecutedByInstance = map[string]string{
		"demo-1": "primary:1-100",
		"demo-2": "uuid:1-90",
		"demo-3": "uuid:1-50",
		"demo-5": "uuid:1-80",
		// A scaled-down instance whose entry lingered: not expected, not counted.
		"demo-9": "uuid:1",
	}
	floor.Observe(cluster)
	// The floor keeps its own copy: later changes to the object do not leak in.
	cluster.Status.GTIDExecutedByInstance["demo-2"] = "changed"

	positions, unknown, ok := floor.Positions()
	if !ok {
		t.Fatal("an observed Cluster must be usable")
	}
	// Itself (demo-1) and the diverged demo-3 are skipped; demo-4 has no position.
	want := map[string]string{"demo-2": "uuid:1-90", "demo-5": "uuid:1-80"}
	if !maps.Equal(positions, want) {
		t.Fatalf("positions = %v, want %v", positions, want)
	}
	if !slices.Equal(unknown, []string{"demo-4"}) {
		t.Fatalf("unknown = %v, want [demo-4]", unknown)
	}
}

// The drain gate reads the current primary's recorded position, the divergence
// list and, on MariaDB, the primary timeline from the same Cluster view.
func TestClusterFloorClusterView(t *testing.T) {
	t.Parallel()
	floor := newClusterFloor("demo-1")
	if _, _, ok := floor.Primary(); ok {
		t.Fatal("an unobserved floor knows no primary")
	}
	if _, ok := floor.Timeline(); ok {
		t.Fatal("an unobserved floor has no timeline")
	}

	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Spec.Flavor = mysqlv1alpha1.FlavorMariaDB
	cluster.Status.CurrentPrimary = "demo-2"
	cluster.Status.DivergedInstances = []string{"demo-3"}
	cluster.Status.GTIDExecutedByInstance = map[string]string{"demo-2": "0-2-300"}
	cluster.Status.MariaDBTimeline = []mysqlv1alpha1.MariaDBEpoch{
		{Instance: "demo-1", ServerID: 1},
		{Instance: "demo-2", ServerID: 2, Handoff: "0-1-218"},
	}
	floor.Observe(cluster)
	cluster.Status.MariaDBTimeline[1].Handoff = "changed"

	name, pos, ok := floor.Primary()
	if !ok || name != "demo-2" || pos != "0-2-300" {
		t.Fatalf("Primary() = %q %q %v", name, pos, ok)
	}
	if !floor.Diverged("demo-3") || floor.Diverged("demo-2") {
		t.Fatal("Diverged must follow status.divergedInstances")
	}
	tl, ok := floor.Timeline()
	if !ok || len(tl) != 2 || tl[1].ServerID != 2 || tl[1].Handoff != "0-1-218" {
		t.Fatalf("Timeline() = %+v %v", tl, ok)
	}

	cluster.Status.CurrentPrimary = "demo-4"
	cluster.Status.MariaDBTimeline = nil
	floor.Observe(cluster)
	if _, _, ok := floor.Primary(); ok {
		t.Fatal("a primary without a recorded position is unknown")
	}
	if _, ok := floor.Timeline(); ok {
		t.Fatal("an empty timeline is no timeline")
	}
}

func TestArchivingStatusProviderReportsForks(t *testing.T) {
	t.Parallel()
	detected := time.Date(2026, 10, 6, 1, 2, 3, 0, time.UTC)
	got := archivingStatus(binlog.State{
		Active: true,
		Forks: []binlog.SegmentFork{
			{ServerUUID: "old", InstanceName: "demo-0", GTIDs: "u:219", DetectedAt: detected},
		},
		ForkCheckedAt:         detected.Add(time.Minute),
		OldestSegmentPosition: "0-1-5",
		DeferredFile:          "binlog.000009",
	})
	if len(got.Forks) != 1 || got.Forks[0] != (webserver.ArchiveForkStatus{
		Segment: "old", InstanceName: "demo-0", GTIDs: "u:219", DetectedAt: "2026-10-06T01:02:03Z",
	}) {
		t.Fatalf("forks = %+v", got.Forks)
	}
	if got.ForkCheckedAt != "2026-10-06T01:03:03Z" || got.OldestSegmentPosition != "0-1-5" ||
		got.DeferredFile != "binlog.000009" {
		t.Fatalf("status = %+v", got)
	}
	if empty := archivingStatus(binlog.State{}); empty.ForkCheckedAt != "" || empty.Forks != nil {
		t.Fatalf("an unchecked state must report no check time: %+v", empty)
	}
}

func TestClusterFloorAuthority(t *testing.T) {
	t.Parallel()
	floor := newClusterFloor("demo-1")
	if _, ok := floor.Authority(); ok {
		t.Fatal("a floor that never saw the Cluster must not claim authority")
	}
	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Status.CurrentPrimary = "demo-0"
	cluster.Status.CurrentPrimaryGeneration = 3
	floor.Observe(cluster)
	if _, ok := floor.Authority(); ok {
		t.Fatal("an instance the Cluster does not name primary must not claim authority")
	}
	cluster.Status.CurrentPrimary = "demo-1"
	cluster.Status.CurrentPrimaryGeneration = 4
	floor.Observe(cluster)
	if gen, ok := floor.Authority(); !ok || gen != 4 {
		t.Fatalf("Authority() = %d, %v, want 4, true", gen, ok)
	}
}
