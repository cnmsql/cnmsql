/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package rolereconciler

import (
	"context"
	"errors"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// instDemo1 is the conventional primary instance name used across these tests.
const instDemo1 = "demo-1"

type fakeLocal struct {
	status         *webserver.Status
	statusErr      error
	promoted       bool
	demoted        bool
	demoteErr      error
	configured     *replication.SourceOptions
	shutdownCalled bool
}

func (f *fakeLocal) Status(context.Context) (*webserver.Status, error) { return f.status, f.statusErr }
func (f *fakeLocal) Promote(context.Context) error {
	f.promoted = true
	f.status.Role = webserver.RolePrimary
	return nil
}
func (f *fakeLocal) Demote(context.Context) error { f.demoted = true; return f.demoteErr }
func (f *fakeLocal) EnsureReplicaConfigured(_ context.Context, s replication.SourceOptions) error {
	f.configured = &s
	return nil
}
func (f *fakeLocal) Shutdown(context.Context) error { f.shutdownCalled = true; return nil }

func newReconciler(
	t *testing.T,
	instanceName string,
	status *mysqlv1alpha1.ClusterStatus,
	local *fakeLocal,
	objects ...client.Object,
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
		Spec:       mysqlv1alpha1.ClusterSpec{Instances: 3},
	}
	if status != nil {
		cluster.Status = *status
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(append([]client.Object{cluster}, objects...)...).
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

func TestAcquireOrRenewLeaseCreatesLease(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RoleReplica}}
	r := newReconciler(t, "demo-2", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2"}, local)
	r.primaryLeaseEnabled = true
	if err := r.acquireOrRenewLease(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease := &coordinationv1.Lease{}
	if err := r.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: "demo-primary"}, lease); err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "demo-2" {
		t.Fatalf("holder = %v, want demo-2", lease.Spec.HolderIdentity)
	}
	if lease.Spec.AcquireTime == nil || lease.Spec.RenewTime == nil {
		t.Fatal("lease acquireTime and renewTime must be set")
	}
	if lease.Spec.LeaseTransitions == nil || *lease.Spec.LeaseTransitions != 1 {
		t.Fatalf("leaseTransitions = %v, want 1", lease.Spec.LeaseTransitions)
	}
}

func TestAcquireOrRenewLeaseRenewsOwnLease(t *testing.T) {
	t.Parallel()
	holder := instDemo1
	transitions := int32(4)
	duration := int32(15)
	acquired := metav1.MicroTime{Time: time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)}
	renewed := metav1.MicroTime{Time: time.Date(2026, 6, 14, 12, 0, 5, 0, time.UTC)}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-primary", Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			AcquireTime:          &acquired,
			RenewTime:            &renewed,
			LeaseDurationSeconds: &duration,
			LeaseTransitions:     &transitions,
		},
	}
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	r := newReconciler(t, instDemo1, &mysqlv1alpha1.ClusterStatus{TargetPrimary: instDemo1}, local, lease)
	r.primaryLeaseEnabled = true
	if err := r.acquireOrRenewLease(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := &coordinationv1.Lease{}
	if err := r.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: "demo-primary"}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Spec.AcquireTime.Time.Equal(acquired.Time) {
		t.Fatalf("acquireTime changed: %s", got.Spec.AcquireTime.Time)
	}
	if got.Spec.LeaseTransitions == nil || *got.Spec.LeaseTransitions != transitions {
		t.Fatalf("leaseTransitions = %v, want %d", got.Spec.LeaseTransitions, transitions)
	}
	if !got.Spec.RenewTime.After(renewed.Time) {
		t.Fatalf("renewTime = %s, want after %s", got.Spec.RenewTime.Time, renewed.Time)
	}
}

func TestAcquireOrRenewLeaseWaitsWhenAnotherHolderIsCurrent(t *testing.T) {
	t.Parallel()
	holder := instDemo1
	duration := int32(15)
	renewed := metav1.MicroTime{Time: time.Now()}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-primary", Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			RenewTime:            &renewed,
			LeaseDurationSeconds: &duration,
		},
	}
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RoleReplica}}
	r := newReconciler(t, "demo-2", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2"}, local, lease)
	r.primaryLeaseEnabled = true
	if err := r.acquireOrRenewLease(context.Background()); !errors.Is(err, errPrimaryLeaseHeld) {
		t.Fatalf("error = %v, want errPrimaryLeaseHeld", err)
	}
}

func TestReleaseLeaseDeletesOnlyOwnLease(t *testing.T) {
	t.Parallel()
	holder := instDemo1
	duration := int32(15)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-primary", Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &duration,
		},
	}
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	r := newReconciler(t, instDemo1, &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2"}, local, lease)
	r.primaryLeaseEnabled = true
	if err := r.releaseLease(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := &coordinationv1.Lease{}
	if err := r.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: "demo-primary"}, got); err == nil {
		t.Fatal("lease still exists after release")
	}
}

func reconcile(t *testing.T, r *Reconciler) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func TestClusterCacheOptionsSelectsSingleClusterByName(t *testing.T) {
	t.Parallel()
	opts := clusterCacheOptions(StartOptions{Namespace: "default", ClusterName: "demo"})
	if len(opts.ByObject) != 2 {
		t.Fatalf("ByObject entries = %d, want 2", len(opts.ByObject))
	}
	var foundCluster, foundLease bool
	for obj, cfg := range opts.ByObject {
		switch obj.(type) {
		case *mysqlv1alpha1.Cluster:
			foundCluster = true
			if _, ok := cfg.Namespaces["default"]; !ok {
				t.Fatalf("cluster namespaces = %#v, want default", cfg.Namespaces)
			}
			if got := cfg.Field.String(); got != "metadata.name=demo" {
				t.Fatalf("cluster field selector = %q, want metadata.name=demo", got)
			}
		case *coordinationv1.Lease:
			foundLease = true
			if _, ok := cfg.Namespaces["default"]; !ok {
				t.Fatalf("lease namespaces = %#v, want default", cfg.Namespaces)
			}
			if got := cfg.Field.String(); got != "metadata.name=demo-primary" {
				t.Fatalf("lease field selector = %q, want metadata.name=demo-primary", got)
			}
		default:
			t.Fatalf("ByObject key = %T, want *Cluster or *Lease", obj)
		}
	}
	if !foundCluster {
		t.Fatal("cluster cache config not found")
	}
	if !foundLease {
		t.Fatal("lease cache config not found")
	}
}

func TestTargetPrimaryAlreadyPrimarySetsCurrentPrimary(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary, SuperReadOnly: true}}
	r := newReconciler(t, instDemo1, &mysqlv1alpha1.ClusterStatus{TargetPrimary: instDemo1}, local)
	reconcile(t, r)
	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(context.Background(), r.ClusterKey, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.CurrentPrimary != instDemo1 {
		t.Fatalf("currentPrimary = %q, want demo-1", cluster.Status.CurrentPrimary)
	}
	if !local.promoted {
		t.Fatal("read-only primary should be promoted to clear read-only flags")
	}
}

func TestTargetPrimaryReplicaCaughtUpPromotes(t *testing.T) {
	t.Parallel()
	behind := int64(0)
	local := &fakeLocal{status: &webserver.Status{
		Role:         webserver.RoleReplica,
		GTIDExecuted: "a:1-10",
		Replication: &webserver.ReplicationStatus{
			SQLRunning:          true,
			RetrievedGTIDSet:    "a:1-10",
			SecondsBehindSource: &behind,
		},
	}}
	r := newReconciler(t, "demo-2",
		&mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: instDemo1}, local)
	reconcile(t, r)
	if !local.promoted {
		t.Fatal("caught-up target should promote")
	}
	cluster := &mysqlv1alpha1.Cluster{}
	_ = r.Get(context.Background(), r.ClusterKey, cluster)
	if cluster.Status.CurrentPrimary != "demo-2" {
		t.Fatalf("currentPrimary = %q, want demo-2", cluster.Status.CurrentPrimary)
	}
}

func TestTargetPrimaryReplicaNotCaughtUpWaits(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{
		Role:         webserver.RoleReplica,
		GTIDExecuted: "a:1-5",
		Replication: &webserver.ReplicationStatus{
			SQLRunning:       true,
			RetrievedGTIDSet: "a:1-10",
		},
	}}
	r := newReconciler(t, "demo-2",
		&mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: instDemo1}, local)
	res := reconcile(t, r)
	if local.promoted {
		t.Fatal("must not promote before draining the relay log")
	}
	if res.RequeueAfter == 0 {
		t.Fatal("expected a requeue while waiting to catch up")
	}
}

func TestReplicaFollowsCurrentPrimary(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{
		Role:        webserver.RoleReplica,
		Replication: &webserver.ReplicationStatus{SourceHost: "demo-1.default.svc", SQLRunning: true, IORunning: true},
	}}
	r := newReconciler(t, "demo-3", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: "demo-2"}, local)
	reconcile(t, r)
	if local.configured == nil || local.configured.Host != "demo-2.default.svc" {
		t.Fatalf("configured = %#v, want host demo-2.default.svc", local.configured)
	}
	if local.configured.User != "repl" {
		t.Fatalf("source user = %q, want repl (from template)", local.configured.User)
	}
}

func TestFormerPrimaryDemotesThenFollows(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	r := newReconciler(t, instDemo1,
		&mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: "demo-2"}, local)
	reconcile(t, r)
	if !local.demoted {
		t.Fatal("former primary must be demoted before following")
	}
	if local.configured == nil || local.configured.Host != "demo-2.default.svc" {
		t.Fatalf("former primary did not follow new primary: %#v", local.configured)
	}
}

func TestFormerPrimaryShutsDownWhenDemoteFails(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}, demoteErr: errors.New("boom")}
	r := newReconciler(t, instDemo1,
		&mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: "demo-2"}, local)
	reconcile(t, r)
	if !local.shutdownCalled {
		t.Fatal("failed live demotion should fall back to shutdown")
	}
	if local.configured != nil {
		t.Fatal("must not configure replication after a failed demotion")
	}
}

func TestDivergedInstanceStaysReadOnly(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	r := newReconciler(t, instDemo1, &mysqlv1alpha1.ClusterStatus{
		TargetPrimary:     "demo-2",
		CurrentPrimary:    "demo-2",
		DivergedInstances: []string{instDemo1},
	}, local)
	reconcile(t, r)
	if local.configured != nil {
		t.Fatal("diverged instance must not self-configure as a replica")
	}
	if !local.demoted {
		t.Fatal("diverged former primary should be demoted read-only")
	}
}

func TestFencedInstanceStaysReadOnlyAndDoesNotPromote(t *testing.T) {
	t.Parallel()
	// Even though demo-1 is the target primary, being fenced keeps it read-only:
	// it must not promote, and a fenced primary is demoted to stop writes (which
	// also stands the continuous archiver down).
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	r := newReconciler(t, instDemo1, &mysqlv1alpha1.ClusterStatus{
		TargetPrimary:   instDemo1,
		FencedInstances: []string{instDemo1},
	}, local)
	reconcile(t, r)
	if local.promoted {
		t.Fatal("a fenced instance must never promote")
	}
	if !local.demoted {
		t.Fatal("a fenced primary should be demoted to stop writes")
	}
	if local.configured != nil {
		t.Fatal("a fenced instance must not configure replication")
	}
}

func TestOldPrimaryAwaitingPromotionDemotesAndWaits(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	// targetPrimary moved to demo-2 but it has not promoted yet, so currentPrimary
	// is still me (demo-1).
	r := newReconciler(t, instDemo1,
		&mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: instDemo1}, local)
	res := reconcile(t, r)
	if !local.demoted {
		t.Fatal("old primary should stop writes (demote) while awaiting the new primary")
	}
	if local.configured != nil {
		t.Fatal("old primary must not try to follow itself")
	}
	if res.RequeueAfter == 0 {
		t.Fatal("expected a requeue while awaiting promotion")
	}
}
