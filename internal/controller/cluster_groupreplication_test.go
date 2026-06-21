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
	controllergr "github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
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

func observeGroupReplicationForTest(observed observedCluster) (*mysqlv1alpha1.GroupReplicationStatus, string) {
	result := controllergr.NewReconciler(nil, nil).Observe(topologyObservationInput(observed, nil))
	return result.GroupReplication, result.PrimaryName
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

func groupViewStatus(instance, memberID, primaryID string, members []webserver.GroupReplicationMember) *webserver.Status {
	role := groupreplication.MemberRoleSecondary
	if memberID == primaryID {
		role = groupreplication.MemberRolePrimary
	}
	return &webserver.Status{
		InstanceName: instance,
		IsReady:      true,
		Role:         webserver.RoleReplica,
		GroupReplication: &webserver.GroupReplicationMemberStatus{
			MemberID: memberID,
			State:    groupreplication.MemberStateOnline,
			Role:     role,
			ViewID:   "view-2",
			Members:  members,
		},
	}
}

func TestObserveGroupReplicationAggregatesPrimary(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		InstanceNames: []string{testPrimary},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: onlineMemberStatus(testPrimary, "uuid-1", groupreplication.MemberRolePrimary),
		},
	}
	status, primary := observeGroupReplicationForTest(observed)
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
	status, primary := observeGroupReplicationForTest(observed)
	if status != nil || primary != "" {
		t.Fatalf("expected (nil, \"\") before any member is ONLINE, got (%+v, %q)", status, primary)
	}
}

func TestObserveGroupReplicationRequiresMajorityPrimaryVerdict(t *testing.T) {
	t.Parallel()
	members := []webserver.GroupReplicationMember{
		{MemberID: "uuid-1", State: groupreplication.MemberStateUnreachable, Role: groupreplication.MemberRoleSecondary},
		{MemberID: "uuid-2", State: groupreplication.MemberStateOnline, Role: groupreplication.MemberRolePrimary},
		{MemberID: "uuid-3", State: groupreplication.MemberStateOnline, Role: groupreplication.MemberRoleSecondary},
	}
	observed := observedCluster{
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: groupViewStatus(testReplica2, "uuid-2", "uuid-2", members),
			testReplica3: groupViewStatus(testReplica3, "uuid-3", "uuid-2", members),
		},
	}

	status, primary := observeGroupReplicationForTest(observed)
	if primary != testReplica2 {
		t.Fatalf("primary = %q, want majority-observed %q", primary, testReplica2)
	}
	if status == nil || status.PrimaryMember != testReplica2 || !status.HasQuorum {
		t.Fatalf("status = %+v, want quorate group with primary %q", status, testReplica2)
	}
}

func TestObserveGroupReplicationRejectsSplitPrimaryVerdict(t *testing.T) {
	t.Parallel()
	viewFor := func(primaryID string) []webserver.GroupReplicationMember {
		members := []webserver.GroupReplicationMember{
			{MemberID: "uuid-1", State: groupreplication.MemberStateUnreachable, Role: groupreplication.MemberRoleSecondary},
			{MemberID: "uuid-2", State: groupreplication.MemberStateOnline, Role: groupreplication.MemberRoleSecondary},
			{MemberID: "uuid-3", State: groupreplication.MemberStateOnline, Role: groupreplication.MemberRoleSecondary},
		}
		for i := range members {
			if members[i].MemberID == primaryID {
				members[i].Role = groupreplication.MemberRolePrimary
			}
		}
		return members
	}
	observed := observedCluster{
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: groupViewStatus(testReplica2, "uuid-2", "uuid-2", viewFor("uuid-2")),
			testReplica3: groupViewStatus(testReplica3, "uuid-3", "uuid-3", viewFor("uuid-3")),
		},
	}

	status, primary := observeGroupReplicationForTest(observed)
	if primary != "" {
		t.Fatalf("primary = %q, want empty without a majority verdict", primary)
	}
	if status == nil || status.PrimaryMember != "" {
		t.Fatalf("status = %+v, want group view without an authoritative primary", status)
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
	controllergr.NewReconciler(nil, nil).MergeStatus(cluster, topology.Observation{
		GroupReplication: observed.GroupReplication,
	})
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
	controllergr.NewReconciler(nil, nil).MergeStatus(cluster, topology.Observation{})
	if !cluster.Status.GroupReplication.Bootstrapped {
		t.Fatal("bootstrapped is monotonic and must never be cleared")
	}
}

func TestMergeGroupReplicationClampsObservedMaxOnScaleDown(t *testing.T) {
	t.Parallel()
	// A 5-member group that has been scaled down to 3: the sticky ObservedViewMax
	// (5, the largest view ever seen) must be clamped to spec.Instances so the
	// quorum denominator tracks the smaller group. Otherwise losing one of the
	// three would falsely read as quorum-lost (2*2=4 <= 5).
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{
		GroupName:         "group-uuid",
		Bootstrapped:      true,
		ObservedViewMax:   5,
		ObservedOnlineMax: 5,
	})
	cluster.Spec.Instances = 3
	online := func(i string) mysqlv1alpha1.GroupMember {
		return mysqlv1alpha1.GroupMember{Instance: i, State: groupreplication.MemberStateOnline, Role: groupreplication.MemberRoleSecondary}
	}
	controllergr.NewReconciler(nil, nil).MergeStatus(cluster, topology.Observation{
		GroupReplication: &mysqlv1alpha1.GroupReplicationStatus{
			Members:           []mysqlv1alpha1.GroupMember{online("demo-1"), online("demo-2"), online("demo-3")},
			ObservedViewMax:   3,
			ObservedOnlineMax: 3,
		},
	})
	gr := cluster.Status.GroupReplication
	if gr.ObservedViewMax != 3 {
		t.Fatalf("ObservedViewMax = %d, want clamped to 3", gr.ObservedViewMax)
	}
	if !gr.HasQuorum {
		t.Fatal("a full 3-member group must be quorate after scale-down clamp")
	}
}

// Restore into a fresh GR group: the bootstrap primary restores the physical
// backup into its data dir (then bootstraps a fresh single-member group via the
// in-Pod role strategy), while secondaries initialise an empty GR server and
// provision via distributed recovery from that primary — never an async clone.
func TestBootstrapArgsGroupReplicationRecovery(t *testing.T) {
	t.Parallel()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "g"})
	cluster.Spec.Instances = 3
	plan := testPlan()
	plan.Instances = 3
	plan.Recovery = &recoveryPlan{Bucket: "bkt", ArchiveKey: "clusters/demo/bk/", MetadataKey: "clusters/demo/bk/meta"}
	r := &ClusterReconciler{}

	primaryArgs := strings.Join(r.bootstrapArgs(cluster, plan, plan.instanceFor(cluster, 1)), " ")
	if !strings.Contains(primaryArgs, "instance restore") || !strings.Contains(primaryArgs, "--bucket=bkt") {
		t.Fatalf("GR recovery primary must restore from the object store, got: %s", primaryArgs)
	}

	secondaryArgs := strings.Join(r.bootstrapArgs(cluster, plan, plan.instanceFor(cluster, 2)), " ")
	if !strings.Contains(secondaryArgs, "instance initdb") || !strings.Contains(secondaryArgs, "--group-replication") {
		t.Fatalf("GR secondary must initialise an empty server for distributed recovery, got: %s", secondaryArgs)
	}
	if strings.Contains(secondaryArgs, "instance join") || strings.Contains(secondaryArgs, "instance restore") {
		t.Fatalf("GR secondary must not async-clone or restore; it joins via distributed recovery, got: %s", secondaryArgs)
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
	if err := r.topologyReconciler(cluster).EnsureConfigured(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	name := cluster.PinnedGroupName()
	if name == "" {
		t.Fatal("a group name should have been generated and pinned")
	}
	// Idempotent: a second call must not change the pinned name.
	if err := r.topologyReconciler(cluster).EnsureConfigured(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.PinnedGroupName() != name {
		t.Fatalf("group name changed on re-pin: %q -> %q", name, cluster.PinnedGroupName())
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
	if err := r.topologyReconciler(cluster).EnsureConfigured(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if got := cluster.PinnedGroupName(); got != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" {
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
	if err := r.topologyReconciler(cluster).EnsureConfigured(ctx, cluster); err != nil {
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

	rendered, err := (&ClusterReconciler{}).renderMyCnf(cluster, plan, inst, plan.instanceNames(cluster))
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

// TestScaleDoesNotRollExistingMembers verifies that growing the group (3→5)
// leaves an existing member's Pod template hash unchanged, so a scale-up no
// longer rolls the healthy members. The seed list does change in the member's
// actual rendered config (configHash), proving the seeds were normalised out of
// the roll-triggering template hash only, not dropped from the real config.
func TestScaleDoesNotRollExistingMembers(t *testing.T) {
	t.Parallel()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "group-uuid-123"})
	reconciler := &ClusterReconciler{}

	annotationsFor := func(instances int) map[string]string {
		plan := testPlan()
		plan.Instances = instances
		plan.PrimaryName = instanceName(cluster, 1)
		inst := plan.instanceFor(cluster, 1)
		labels := labelsFor(cluster, inst.Name, roleOf(inst))
		spec := reconciler.podSpec(cluster, plan, inst)
		annotations, err := reconciler.podAnnotations(cluster, plan, inst, labels, spec)
		if err != nil {
			t.Fatal(err)
		}
		return annotations
	}

	three := annotationsFor(3)
	five := annotationsFor(5)

	if three[podTemplateHashAnnotation] != five[podTemplateHashAnnotation] {
		t.Fatalf("template hash changed on scale 3->5: %q vs %q (existing member would needlessly roll)",
			three[podTemplateHashAnnotation], five[podTemplateHashAnnotation])
	}
	if three[configHashAnnotation] == five[configHashAnnotation] {
		t.Fatal("config hash unchanged on scale 3->5: the seed list should differ in the actual config")
	}
}

func TestRunArgsGroupReplicationFlag(t *testing.T) {
	t.Parallel()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{GroupName: "g"})
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	args := (&ClusterReconciler{}).runArgs(cluster, plan, inst)
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
	asyncArgs := (&ClusterReconciler{}).runArgs(baseCluster(), plan, inst)
	for _, a := range asyncArgs {
		if a == "--group-replication" {
			t.Fatal("async cluster must not carry --group-replication")
		}
	}
}
