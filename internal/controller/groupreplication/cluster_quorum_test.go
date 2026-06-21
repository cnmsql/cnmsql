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

package groupreplication

import (
	"testing"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	mysqlgr "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
)

func onlineMember(name, role string) mysqlv1alpha1.GroupMember {
	return mysqlv1alpha1.GroupMember{Instance: name, State: mysqlgr.MemberStateOnline, Role: role}
}

// GTID sets where demo-2 strictly dominates demo-1 (contains its single
// transaction plus more), and demo-3 has a divergent transaction.
const (
	gtidShort     = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10"
	gtidLong      = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20"
	gtidDivergent = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5,AAAAAAAA-71CA-11E1-9E33-C80AA9429562:1-3"
)

func TestSelectQuorumSurvivor(t *testing.T) {
	tests := []struct {
		name   string
		online []mysqlv1alpha1.GroupMember
		gtids  map[string]string
		want   string
		wantOK bool
	}{
		{
			name:   "no online members is unrecoverable",
			online: nil,
			wantOK: false,
		},
		{
			name:   "lone survivor is taken as-is",
			online: []mysqlv1alpha1.GroupMember{onlineMember("demo-1", mysqlgr.MemberRoleSecondary)},
			gtids:  map[string]string{},
			want:   "demo-1",
			wantOK: true,
		},
		{
			name: "surviving primary is authoritative regardless of GTID",
			online: []mysqlv1alpha1.GroupMember{
				onlineMember("demo-1", mysqlgr.MemberRoleSecondary),
				onlineMember("demo-2", mysqlgr.MemberRolePrimary),
			},
			gtids:  map[string]string{"demo-1": gtidLong, "demo-2": gtidShort},
			want:   "demo-2",
			wantOK: true,
		},
		{
			name: "most-advanced secondary wins by GTID dominance",
			online: []mysqlv1alpha1.GroupMember{
				onlineMember("demo-1", mysqlgr.MemberRoleSecondary),
				onlineMember("demo-2", mysqlgr.MemberRoleSecondary),
			},
			gtids:  map[string]string{"demo-1": gtidShort, "demo-2": gtidLong},
			want:   "demo-2",
			wantOK: true,
		},
		{
			name: "lexically-first but stale member is NOT chosen",
			online: []mysqlv1alpha1.GroupMember{
				onlineMember("demo-1", mysqlgr.MemberRoleSecondary),
				onlineMember("demo-9", mysqlgr.MemberRoleSecondary),
			},
			gtids:  map[string]string{"demo-1": gtidShort, "demo-9": gtidLong},
			want:   "demo-9",
			wantOK: true,
		},
		{
			name: "incomparable GTID sets are unrecoverable",
			online: []mysqlv1alpha1.GroupMember{
				onlineMember("demo-1", mysqlgr.MemberRoleSecondary),
				onlineMember("demo-2", mysqlgr.MemberRoleSecondary),
			},
			gtids:  map[string]string{"demo-1": gtidLong, "demo-2": gtidDivergent},
			wantOK: false,
		},
		{
			name: "missing GTID for candidates is unrecoverable",
			online: []mysqlv1alpha1.GroupMember{
				onlineMember("demo-1", mysqlgr.MemberRoleSecondary),
				onlineMember("demo-2", mysqlgr.MemberRoleSecondary),
			},
			gtids:  map[string]string{},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := selectQuorumSurvivor(tt.online, tt.gtids)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got.Instance != tt.want {
				t.Fatalf("survivor = %q, want %q", got.Instance, tt.want)
			}
		})
	}
}

func TestComputeForceQuorumRecoveryAddressIsFQDN(t *testing.T) {
	r := &Reconciler{}
	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Name = "demo"
	cluster.Namespace = "prod"
	cluster.Status.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{
		HasQuorum: false,
		Members: []mysqlv1alpha1.GroupMember{
			onlineMember("demo-1", mysqlgr.MemberRolePrimary),
		},
	}

	recovery := r.ComputeForceQuorumRecovery(cluster, map[string]string{"demo-1": gtidLong})
	if recovery == nil {
		t.Fatal("expected a recovery plan, got nil")
	}
	if recovery.Survivor != "demo-1" {
		t.Fatalf("survivor = %q, want demo-1", recovery.Survivor)
	}
	// The force_members address must match group_replication_local_address, i.e.
	// the XCom FQDN, not the bare Pod name.
	want := "demo-1.prod.svc:33061"
	if recovery.ForceMembers != want {
		t.Fatalf("forceMembers = %q, want %q", recovery.ForceMembers, want)
	}
}

func TestComputeForceQuorumRecoverySkippedWhenQuorate(t *testing.T) {
	r := &Reconciler{}
	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Status.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{HasQuorum: true}
	if got := r.ComputeForceQuorumRecovery(cluster, nil); got != nil {
		t.Fatalf("expected nil recovery for a quorate group, got %+v", got)
	}
}

func TestSelectRebootstrapSurvivor(t *testing.T) {
	tests := []struct {
		name       string
		gtids      map[string]string
		configured int
		want       string
		wantOK     bool
	}{
		{
			name:       "most-advanced reachable member wins",
			gtids:      map[string]string{"demo-0": gtidShort, "demo-1": gtidLong, "demo-2": gtidShort},
			configured: 3,
			want:       "demo-1",
			wantOK:     true,
		},
		{
			name:       "an unreachable member makes re-bootstrap unsafe",
			gtids:      map[string]string{"demo-0": gtidShort, "demo-1": gtidLong},
			configured: 3,
			wantOK:     false,
		},
		{
			name:       "divergent histories are unrecoverable",
			gtids:      map[string]string{"demo-0": gtidLong, "demo-1": gtidDivergent},
			configured: 2,
			wantOK:     false,
		},
		{
			name:       "identical GTID sets pick deterministically (lexically first)",
			gtids:      map[string]string{"demo-2": gtidLong, "demo-0": gtidLong, "demo-1": gtidLong},
			configured: 3,
			want:       "demo-0",
			wantOK:     true,
		},
		{
			name:       "no instances is unrecoverable",
			gtids:      map[string]string{},
			configured: 3,
			wantOK:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := selectRebootstrapSurvivor(tt.gtids, tt.configured)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Fatalf("survivor = %q, want %q", got, tt.want)
			}
		})
	}
}

// In a total outage no member is ONLINE, so the group view (and gr.Members) is
// empty; recovery must fall through to re-bootstrap selection by GTID.
func TestComputeForceQuorumRecoveryRebootstrapsOnTotalOutage(t *testing.T) {
	r := &Reconciler{}
	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Name = "demo"
	cluster.Namespace = "prod"
	cluster.Spec.Instances = 3
	cluster.Status.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{
		Bootstrapped: true,
		HasQuorum:    false,
		Members:      nil, // no ONLINE view survived
	}

	gtids := map[string]string{"demo-0": gtidShort, "demo-1": gtidLong, "demo-2": gtidShort}
	recovery := r.ComputeForceQuorumRecovery(cluster, gtids)
	if recovery == nil {
		t.Fatal("expected a re-bootstrap recovery plan, got nil")
	}
	if recovery.Action != topology.QuorumRecoveryRebootstrap {
		t.Fatalf("action = %q, want %q", recovery.Action, topology.QuorumRecoveryRebootstrap)
	}
	if recovery.Survivor != "demo-1" {
		t.Fatalf("survivor = %q, want demo-1", recovery.Survivor)
	}
	if recovery.ForceMembers != "" {
		t.Fatalf("ForceMembers = %q, want empty for re-bootstrap", recovery.ForceMembers)
	}
}

// A total outage where one member is unreachable cannot be proven safe; the
// cluster stays Blocked rather than re-bootstrapping from a possibly-behind member.
func TestComputeForceQuorumRecoveryRebootstrapBlockedWhenMemberMissing(t *testing.T) {
	r := &Reconciler{}
	cluster := &mysqlv1alpha1.Cluster{}
	cluster.Spec.Instances = 3
	cluster.Status.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{
		Bootstrapped: true,
		HasQuorum:    false,
	}
	gtids := map[string]string{"demo-0": gtidShort, "demo-1": gtidLong}
	if got := r.ComputeForceQuorumRecovery(cluster, gtids); got != nil {
		t.Fatalf("expected nil recovery when a member is unreachable, got %+v", got)
	}
}
