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
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// grCluster returns a Group Replication cluster with the given group status.
func grCluster(group *mysqlv1alpha1.GroupReplicationStatus) *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{
		Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
	}
	cluster.Status.GroupReplication = group
	return cluster
}

// onlineMemberStatus is a control-API status for a member ONLINE in the group.
func onlineMemberStatus(instance, uuid, role string) *webserver.Status {
	st := &webserver.Status{InstanceName: instance, IsReady: true, Role: webserver.RolePrimary}
	st.GroupReplication = &webserver.GroupReplicationMemberStatus{
		MemberID: uuid,
		State:    groupreplication.MemberStateOnline,
		Role:     role,
		ViewID:   "view-1",
		Members: []webserver.GroupReplicationMember{{
			MemberID: uuid,
			Host:     instance + ".default.svc",
			Port:     3306,
			State:    groupreplication.MemberStateOnline,
			Role:     role,
		}},
	}
	return st
}

func TestObserveGroupReplicationAggregatesPrimary(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		InstanceNames: []string{testPrimary},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: onlineMemberStatus(testPrimary, "uuid-1", groupreplication.MemberRolePrimary),
		},
	}
	status, primary := observeGroupReplication(observed)
	if primary != testPrimary {
		t.Fatalf("primary = %q, want %q", primary, testPrimary)
	}
	if status == nil || status.PrimaryMember != testPrimary {
		t.Fatalf("status.PrimaryMember = %+v, want %q", status, testPrimary)
	}
	if !status.HasQuorum {
		t.Fatal("a single ONLINE member is a quorum of one")
	}
	if len(status.Members) != 1 || status.Members[0].Instance != testPrimary {
		t.Fatalf("members = %+v, want one mapped to %q", status.Members, testPrimary)
	}
	if status.ViewID != "view-1" {
		t.Fatalf("viewID = %q, want view-1", status.ViewID)
	}
}

func TestObserveGroupReplicationNilUntilOnline(t *testing.T) {
	t.Parallel()
	recovering := onlineMemberStatus(testPrimary, "uuid-1", groupreplication.MemberRolePrimary)
	recovering.GroupReplication.State = groupreplication.MemberStateRecovering
	recovering.GroupReplication.Members[0].State = groupreplication.MemberStateRecovering
	observed := observedCluster{
		InstanceNames:    []string{testPrimary},
		StatusByInstance: map[string]*webserver.Status{testPrimary: recovering},
	}
	status, primary := observeGroupReplication(observed)
	if status != nil || primary != "" {
		t.Fatalf("expected (nil, \"\") before any member is ONLINE, got (%+v, %q)", status, primary)
	}
}

func TestMergeGroupReplicationSetsCurrentPrimaryAndBootstrapped(t *testing.T) {
	t.Parallel()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "group-uuid"})
	observed := observedCluster{
		GroupReplication: &mysqlv1alpha1.GroupReplicationStatus{
			PrimaryMember: testPrimary,
			HasQuorum:     true,
		},
	}
	mergeGroupReplicationStatus(cluster, observed)
	if cluster.Status.CurrentPrimary != testPrimary {
		t.Fatalf("currentPrimary = %q, want %q", cluster.Status.CurrentPrimary, testPrimary)
	}
	if !cluster.Status.GroupReplication.Bootstrapped {
		t.Fatal("observing an ONLINE PRIMARY must set bootstrapped")
	}
	if cluster.Status.GroupReplication.GroupName != "group-uuid" {
		t.Fatal("merge must preserve the sticky group name")
	}
}

func TestMergeGroupReplicationKeepsBootstrappedSticky(t *testing.T) {
	t.Parallel()
	// Already bootstrapped, but the current observation sees no ONLINE primary
	// (e.g. mid-election). bootstrapped must not be cleared.
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{
		GroupName:    "group-uuid",
		Bootstrapped: true,
	})
	mergeGroupReplicationStatus(cluster, observedCluster{GroupReplication: nil})
	if !cluster.Status.GroupReplication.Bootstrapped {
		t.Fatal("bootstrapped is monotonic and must never be cleared")
	}
}

func TestEnsureGroupNameGeneratesAndIsSticky(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := testScheme(t)
	cluster := grCluster(nil)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme: scheme,
	}
	if err := r.ensureGroupName(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	name := pinnedGroupName(cluster)
	if name == "" {
		t.Fatal("a group name should have been generated and pinned")
	}
	// Idempotent: a second call must not change the pinned name.
	if err := r.ensureGroupName(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if pinnedGroupName(cluster) != name {
		t.Fatalf("group name changed on re-pin: %q -> %q", name, pinnedGroupName(cluster))
	}
}

func TestEnsureGroupNameRespectsUserPinned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := testScheme(t)
	cluster := grCluster(nil)
	cluster.Spec.Replication.GroupReplication = &mysqlv1alpha1.GroupReplicationConfiguration{
		GroupName: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme: scheme,
	}
	if err := r.ensureGroupName(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if got := pinnedGroupName(cluster); got != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" {
		t.Fatalf("pinned group name = %q, want the user-pinned value", got)
	}
}

func TestEnsureGroupNameNoOpForAsync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := testScheme(t)
	cluster := baseCluster() // async
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme: scheme,
	}
	if err := r.ensureGroupName(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.GroupReplication != nil {
		t.Fatal("async clusters must not get a groupReplication status block")
	}
}

func TestRenderMyCnfGroupReplicationBlock(t *testing.T) {
	t.Parallel()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "group-uuid-123"})
	plan := testPlan()
	plan.Instances = 1
	inst := plan.instanceFor(cluster, 1)

	rendered, err := renderMyCnf(cluster, plan, inst)
	if err != nil {
		t.Fatal(err)
	}
	wants := []string{
		"plugin_load_add = group_replication.so",
		"group_replication_group_name = group-uuid-123",
		"group_replication_local_address = demo-1.default.svc:33061",
		"group_replication_group_seeds = demo-1.default.svc:33061",
		"group_replication_single_primary_mode = ON",
	}
	for _, w := range wants {
		if !strings.Contains(rendered, w) {
			t.Fatalf("rendered my.cnf missing %q:\n%s", w, rendered)
		}
	}
	// Async-only semi-sync settings must not appear under GR.
	if strings.Contains(rendered, "rpl_semi_sync") {
		t.Fatalf("GR config must not render semi-sync settings:\n%s", rendered)
	}
}

func TestRunArgsGroupReplicationFlag(t *testing.T) {
	t.Parallel()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "g"})
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	args := runArgs(cluster, plan, inst)
	found := false
	for _, a := range args {
		if a == "--group-replication" {
			found = true
		}
	}
	if !found {
		t.Fatalf("runArgs for a GR cluster must include --group-replication, got %v", args)
	}
	// Async cluster must not carry the flag.
	asyncArgs := runArgs(baseCluster(), plan, inst)
	for _, a := range asyncArgs {
		if a == "--group-replication" {
			t.Fatal("async cluster must not carry --group-replication")
		}
	}
}
