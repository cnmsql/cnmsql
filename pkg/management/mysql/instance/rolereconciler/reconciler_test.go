/*
Copyright 2026 The CNMySQL Authors.

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
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

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
	if len(opts.ByObject) != 1 {
		t.Fatalf("ByObject entries = %d, want 1", len(opts.ByObject))
	}
	var found bool
	for obj, cfg := range opts.ByObject {
		if _, ok := obj.(*mysqlv1alpha1.Cluster); !ok {
			t.Fatalf("ByObject key = %T, want *Cluster", obj)
		}
		found = true
		if _, ok := cfg.Namespaces["default"]; !ok {
			t.Fatalf("namespaces = %#v, want default", cfg.Namespaces)
		}
		if got := cfg.Field.String(); got != "metadata.name=demo" {
			t.Fatalf("field selector = %q, want metadata.name=demo", got)
		}
	}
	if !found {
		t.Fatal("cluster cache config not found")
	}
}

func TestTargetPrimaryAlreadyPrimarySetsCurrentPrimary(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary, SuperReadOnly: true}}
	r := newReconciler(t, "demo-1", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-1"}, local)
	reconcile(t, r)
	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(context.Background(), r.ClusterKey, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.CurrentPrimary != "demo-1" {
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
	r := newReconciler(t, "demo-2", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: "demo-1"}, local)
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
	r := newReconciler(t, "demo-2", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: "demo-1"}, local)
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
	r := newReconciler(t, "demo-1", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: "demo-2"}, local)
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
	r := newReconciler(t, "demo-1", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: "demo-2"}, local)
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
	r := newReconciler(t, "demo-1", &mysqlv1alpha1.ClusterStatus{
		TargetPrimary:     "demo-2",
		CurrentPrimary:    "demo-2",
		DivergedInstances: []string{"demo-1"},
	}, local)
	reconcile(t, r)
	if local.configured != nil {
		t.Fatal("diverged instance must not self-configure as a replica")
	}
	if !local.demoted {
		t.Fatal("diverged former primary should be demoted read-only")
	}
}

func TestOldPrimaryAwaitingPromotionDemotesAndWaits(t *testing.T) {
	t.Parallel()
	local := &fakeLocal{status: &webserver.Status{Role: webserver.RolePrimary}}
	// targetPrimary moved to demo-2 but it has not promoted yet, so currentPrimary
	// is still me (demo-1).
	r := newReconciler(t, "demo-1", &mysqlv1alpha1.ClusterStatus{TargetPrimary: "demo-2", CurrentPrimary: "demo-1"}, local)
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
