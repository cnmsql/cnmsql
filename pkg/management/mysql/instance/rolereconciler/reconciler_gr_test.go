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

package rolereconciler

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// newGRReconciler builds a Reconciler whose owning Cluster runs Group
// Replication, with the given status.
func newGRReconciler(
	t *testing.T,
	instanceName string,
	status mysqlv1alpha1.ClusterStatus,
	local *fakeLocal,
) *Reconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := mysqlv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &mysqlv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: mysqlv1alpha1.ClusterSpec{
			Instances: 1,
			Replication: &mysqlv1alpha1.ReplicationConfiguration{
				Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
			},
		},
		Status: status,
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster).
		Build()
	return &Reconciler{
		Client:         c,
		ClusterKey:     types.NamespacedName{Namespace: "default", Name: "demo"},
		InstanceName:   instanceName,
		ServiceDomain:  "default.svc",
		SourceTemplate: replication.SourceOptions{User: "repl", Port: 3306, SSL: true},
		Local:          local,
	}
}

// grStatus is a webserver status whose member is in the given GR state/role.
func grStatus(state, role string) *webserver.Status {
	return &webserver.Status{
		Role: webserver.RolePrimary,
		GroupReplication: &webserver.GroupReplicationMemberStatus{
			MemberID: "uuid-1",
			State:    state,
			Role:     role,
		},
	}
}

func TestGroupRoleBootstrapMemberBootstrapsFreshGroup(t *testing.T) {
	t.Parallel()
	// Designated bootstrap member (targetPrimary), group never bootstrapped, GR not
	// running locally (no GroupReplication block) → it bootstraps, does not Start.
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	r := newGRReconciler(t, "demo-1", mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-1"}, local)
	reconcile(t, r)
	if !local.grBootstrapped {
		t.Fatal("bootstrap member should bootstrap the group")
	}
	if local.grStarted {
		t.Fatal("bootstrap member must not also START GROUP_REPLICATION")
	}
}

func TestGroupRoleBootstrappedGroupJoinsNotBootstraps(t *testing.T) {
	t.Parallel()
	// Same member, but the group is already bootstrapped → it joins (START), it must
	// never bootstrap a second time (split-brain guard).
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	r := newGRReconciler(t, "demo-1", mysqlv1alpha1.ClusterStatus{
		TargetPrimary:    "demo-1",
		GroupReplication: &mysqlv1alpha1.GroupReplicationStatus{Bootstrapped: true},
	}, local)
	reconcile(t, r)
	if local.grBootstrapped {
		t.Fatal("must not bootstrap an already-bootstrapped group")
	}
	if !local.grStarted {
		t.Fatal("member should START GROUP_REPLICATION to join")
	}
}

func TestGroupRoleNonTargetMemberJoins(t *testing.T) {
	t.Parallel()
	// A non-bootstrap member never bootstraps, even before the group is bootstrapped.
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RoleReplica}}
	r := newGRReconciler(t, "demo-2", mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-1"}, local)
	reconcile(t, r)
	if local.grBootstrapped {
		t.Fatal("non-target member must never bootstrap")
	}
	if !local.grStarted {
		t.Fatal("non-target member should join via START GROUP_REPLICATION")
	}
}

func TestGroupRoleOnlineMemberIsSteadyAndWritesNothing(t *testing.T) {
	t.Parallel()
	// An ONLINE member is steady: no start/bootstrap, no promote/demote, and crucially
	// it must not write currentPrimary (the operator is the sole writer under GR).
	local := &fakeLocal{status: grStatus(groupreplication.MemberStateOnline, groupreplication.MemberRolePrimary)}
	r := newGRReconciler(t, "demo-1", mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-1"}, local)
	reconcile(t, r)
	if local.grStarted || local.grBootstrapped {
		t.Fatal("an ONLINE member must be left alone")
	}
	if local.promoted || local.demoted {
		t.Fatal("GR strategy must never promote or demote")
	}
	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(context.Background(), r.ClusterKey, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.CurrentPrimary != "" {
		t.Fatalf("GR in-Pod strategy must not write currentPrimary, got %q", cluster.Status.CurrentPrimary)
	}
}

func TestGroupRoleRecoveringMemberWaits(t *testing.T) {
	t.Parallel()
	// A member that has started GR but is not yet ONLINE waits; it must not call
	// START again (which would error on an already-started member).
	local := &fakeLocal{status: grStatus(groupreplication.MemberStateRecovering, groupreplication.MemberRoleSecondary)}
	r := newGRReconciler(t, "demo-2", mysqlv1alpha1.ClusterStatus{
		TargetPrimary:    "demo-1",
		GroupReplication: &mysqlv1alpha1.GroupReplicationStatus{Bootstrapped: true},
	}, local)
	res := reconcile(t, r)
	if local.grStarted || local.grBootstrapped {
		t.Fatal("a member already in the group must not be re-started")
	}
	if res.RequeueAfter != waitRequeue {
		t.Fatalf("RequeueAfter = %s, want %s", res.RequeueAfter, waitRequeue)
	}
}
