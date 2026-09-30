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

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
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
