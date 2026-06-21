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

package controller

import (
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	mysqlgr "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
)

func grSampleReader(t *testing.T, objs ...*mysqlv1alpha1.Cluster) client.Reader {
	t.Helper()
	builder := fake.NewClientBuilder().WithScheme(testScheme(t))
	for _, o := range objs {
		builder = builder.WithObjects(o)
	}
	return builder.Build()
}

func TestCollectGRSamplesReportsGroupStatus(t *testing.T) {
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{
		Bootstrapped:    true,
		HasQuorum:       true,
		ObservedViewMax: 3,
		Members: []mysqlv1alpha1.GroupMember{
			{Instance: "demo-1", State: mysqlgr.MemberStateOnline, Role: mysqlgr.MemberRolePrimary},
			{Instance: "demo-2", State: mysqlgr.MemberStateOnline, Role: mysqlgr.MemberRoleSecondary},
			{Instance: "demo-3", State: mysqlgr.MemberStateRecovering, Role: mysqlgr.MemberRoleSecondary},
		},
	})

	samples := collectGRSamples(grSampleReader(t, cluster))
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want 1", len(samples))
	}
	s := samples[0]
	if s.namespace != "default" || s.name != "demo" {
		t.Errorf("identity = %s/%s, want default/demo", s.namespace, s.name)
	}
	if !s.hasQuorum || !s.bootstrapped {
		t.Errorf("hasQuorum=%v bootstrapped=%v, want both true", s.hasQuorum, s.bootstrapped)
	}
	if s.viewSize != 3 {
		t.Errorf("viewSize = %d, want 3", s.viewSize)
	}
	if got := s.membersByState[mysqlgr.MemberStateOnline]; got != 2 {
		t.Errorf("ONLINE members = %d, want 2", got)
	}
	if got := s.membersByState[mysqlgr.MemberStateRecovering]; got != 1 {
		t.Errorf("RECOVERING members = %d, want 1", got)
	}
	// A state with no members is still present as zero, so absence is not
	// confused with "unscraped".
	got, ok := s.membersByState[mysqlgr.MemberStateError]
	if !ok || got != 0 {
		t.Errorf("ERROR members = %d (present=%v), want 0 and present", got, ok)
	}
}

func TestCollectGRSamplesSkipsNonGroupReplicationClusters(t *testing.T) {
	async := baseCluster() // default replication mode, not GR
	if samples := collectGRSamples(grSampleReader(t, async)); len(samples) != 0 {
		t.Errorf("got %d samples for an async cluster, want 0", len(samples))
	}
}

func TestCollectGRSamplesSkipsClusterWithoutGroupStatus(t *testing.T) {
	cluster := grCluster(nil) // GR cluster that has not reported status yet
	if samples := collectGRSamples(grSampleReader(t, cluster)); len(samples) != 0 {
		t.Errorf("got %d samples before status.groupReplication is set, want 0", len(samples))
	}
}
