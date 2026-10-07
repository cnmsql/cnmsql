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

package async

import (
	"slices"
	"testing"

	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// laggedTimeline: demo-1 (server 1) was primary, demo-2 (server 2) took over
// having inherited 0-1-218.
var laggedTimeline = engine.MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-218"}}

func mariadbInput(positions map[string]string, prior []string) topology.ObservationInput {
	return topology.ObservationInput{
		PrimaryName:            "demo-2",
		InstanceNames:          []string{"demo-1", "demo-2", "demo-3"},
		StatusByInstance:       map[string]*webserver.Status{},
		GTIDByInstance:         positions,
		EngineFlavor:           "mariadb",
		PriorDivergedInstances: prior,
		MariaDBTimeline:        laggedTimeline,
	}
}

// The forked former primary's dead 0-1-219 compares as contained in the
// successor's 0-2-300 (MariaDB positions compare by sequence alone), so
// containment alone never marks it. The timeline does, on first contact,
// before it has tried to replicate.
func TestMariaDBForkedFormerPrimaryIsDivergedOnFirstContact(t *testing.T) {
	t.Parallel()
	in := mariadbInput(map[string]string{"demo-1": "0-1-219", "demo-2": "0-2-300", "demo-3": "0-2-290"}, nil)
	if got := detectDivergedReplicas(in); !slices.Equal(got, []string{"demo-1"}) {
		t.Fatalf("diverged = %v, want [demo-1]", got)
	}
	in.MariaDBTimeline = nil
	if got := detectDivergedReplicas(in); len(got) != 0 {
		t.Fatalf("without a timeline only containment applies, got %v", got)
	}
}

func TestMariaDBCanonicalReplicasAreNotDiverged(t *testing.T) {
	t.Parallel()
	in := mariadbInput(map[string]string{"demo-1": "0-1-200", "demo-2": "0-2-300", "demo-3": "0-2-300"}, nil)
	if got := detectDivergedReplicas(in); len(got) != 0 {
		t.Fatalf("diverged = %v, want none", got)
	}
}

// When the timeline has no verdict, the 1236 refusal from the current primary
// marks the replica: the source would not accept its GTID position.
func TestMariaDB1236FromThePrimaryMarksDiverged(t *testing.T) {
	t.Parallel()
	in := mariadbInput(map[string]string{"demo-1": "0-1-10", "demo-2": "0-2-300", "demo-3": "0-2-300"}, nil)
	in.MariaDBTimeline = engine.MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}} // no verdict below 218
	in.StatusByInstance["demo-1"] = &webserver.Status{Replication: &webserver.ReplicationStatus{
		SourceHost: "demo-2.demo-rw.default.svc", LastIOErrno: 1236, LastError: "Got fatal error 1236",
	}}
	in.StatusByInstance["demo-3"] = &webserver.Status{Replication: &webserver.ReplicationStatus{
		SourceHost: "elsewhere.svc", LastIOErrno: 1236,
	}}
	got := detectDivergedReplicas(in)
	if !slices.Equal(got, []string{"demo-1"}) {
		t.Fatalf("diverged = %v, want only the replica refused by the current primary", got)
	}
}

func TestMySQLIgnores1236Backstop(t *testing.T) {
	t.Parallel()
	in := topology.ObservationInput{
		PrimaryName:   "demo-2",
		InstanceNames: []string{"demo-1", "demo-2"},
		StatusByInstance: map[string]*webserver.Status{"demo-1": {Replication: &webserver.ReplicationStatus{
			SourceHost: "demo-2.svc", LastIOErrno: 1236,
		}}},
		GTIDByInstance: map[string]string{"demo-1": "u:1-5", "demo-2": "u:1-9"},
		EngineFlavor:   "mysql",
	}
	if got := detectDivergedReplicas(in); len(got) != 0 {
		t.Fatalf("diverged = %v, MySQL keeps set containment only", got)
	}
}

// A diverged mark clears only on positive proof. With a timeline in place, a
// position that merely compares as contained but gets no verdict keeps the
// mark; a re-cloned instance on the timeline clears it.
func TestMariaDBDivergedMarkClearsOnlyOnProof(t *testing.T) {
	t.Parallel()
	floor := engine.MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}
	in := mariadbInput(map[string]string{"demo-1": "0-1-100", "demo-2": "0-2-300", "demo-3": "0-2-300"}, []string{"demo-1"})
	in.MariaDBTimeline = floor
	if got := detectDivergedReplicas(in); !slices.Equal(got, []string{"demo-1"}) {
		t.Fatalf("diverged = %v, a no-verdict position must keep the mark", got)
	}

	in.GTIDByInstance["demo-1"] = "0-2-300" // re-cloned from the primary
	if got := detectDivergedReplicas(in); len(got) != 0 {
		t.Fatalf("diverged = %v, a re-cloned instance on the timeline must clear", got)
	}

	// No timeline at all (history predating it): today's containment rule.
	in.MariaDBTimeline = nil
	in.GTIDByInstance["demo-1"] = "0-1-100"
	if got := detectDivergedReplicas(in); len(got) != 0 {
		t.Fatalf("diverged = %v, without a timeline containment clears as before", got)
	}
}
