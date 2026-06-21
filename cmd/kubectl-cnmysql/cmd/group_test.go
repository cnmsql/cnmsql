/*
Copyright 2026 The CloudNative MySQL Authors.

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

package cmd

import (
	"testing"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func grTestCluster(gr *mysqlv1alpha1.GroupReplicationStatus) *mysqlv1alpha1.Cluster {
	c := &mysqlv1alpha1.Cluster{}
	c.Name = "demo"
	c.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{
		Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
	}
	c.Status.GroupReplication = gr
	return c
}

func TestGroupMemberRowsAreSortedAndFormatted(t *testing.T) {
	gr := &mysqlv1alpha1.GroupReplicationStatus{
		Members: []mysqlv1alpha1.GroupMember{
			{Instance: "demo-3", State: "RECOVERING", Role: "SECONDARY", Reachable: true},
			{Instance: "demo-1", State: "ONLINE", Role: "PRIMARY", Reachable: true},
			{Instance: "demo-2", State: "UNREACHABLE", Role: "SECONDARY", Reachable: false},
		},
	}
	rows := groupMemberRows(gr)
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	if rows[0][0] != "demo-1" || rows[1][0] != "demo-2" || rows[2][0] != "demo-3" {
		t.Errorf("rows not sorted by instance: %v", rows)
	}
	// REACHABLE column renders the bool as yes/no.
	if rows[0][3] != readyYes || rows[2-1][3] != readyNo {
		t.Errorf("reachable not rendered as yes/no: %v", rows)
	}
	if rows[0][1] != "ONLINE" || rows[0][2] != "PRIMARY" {
		t.Errorf("state/role not preserved: %v", rows[0])
	}
}

func TestGroupMemberRowsNilStatus(t *testing.T) {
	if rows := groupMemberRows(nil); rows != nil {
		t.Errorf("nil status should yield nil rows, got %v", rows)
	}
}

func TestCheckRecoverableAllowsQuorumLossOnBootstrappedGroup(t *testing.T) {
	cluster := grTestCluster(&mysqlv1alpha1.GroupReplicationStatus{
		Bootstrapped: true,
		HasQuorum:    false,
	})
	if err := checkRecoverable(cluster); err != nil {
		t.Errorf("quorum-lost bootstrapped group should be recoverable, got %v", err)
	}
}

func TestCheckRecoverableRejects(t *testing.T) {
	async := &mysqlv1alpha1.Cluster{}
	async.Name = "async"

	cases := map[string]*mysqlv1alpha1.Cluster{
		"async cluster":      async,
		"no group status":    grTestCluster(nil),
		"never bootstrapped": grTestCluster(&mysqlv1alpha1.GroupReplicationStatus{Bootstrapped: false}),
		"still has quorum":   grTestCluster(&mysqlv1alpha1.GroupReplicationStatus{Bootstrapped: true, HasQuorum: true}),
	}
	for name, cluster := range cases {
		if err := checkRecoverable(cluster); err == nil {
			t.Errorf("%s: expected checkRecoverable to refuse, got nil", name)
		}
	}
}

func TestCountOnline(t *testing.T) {
	gr := &mysqlv1alpha1.GroupReplicationStatus{
		Members: []mysqlv1alpha1.GroupMember{
			{State: "ONLINE"}, {State: "ONLINE"}, {State: "RECOVERING"}, {State: "ERROR"},
		},
	}
	if got := countOnline(gr); got != 2 {
		t.Errorf("countOnline = %d, want 2", got)
	}
}
