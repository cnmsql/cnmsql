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
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
)

const (
	drainPrimary = "demo-1"
	drainReplica = "demo-2"
)

// drainState returns a FailoverState with a healthy primary being drained and a
// healthy replica candidate ready to take over.
func drainState() topology.FailoverState {
	return topology.FailoverState{
		PrimaryName:   drainPrimary,
		InstanceNames: []string{drainPrimary, drainReplica},
		Terminating:   []string{drainPrimary},
		Instances: map[string]topology.FailoverInstance{
			drainPrimary: {Ready: true, Primary: true, Role: "primary", GTID: "uuid:1-10"},
			drainReplica: {Ready: true, Replica: true, Role: "replica", SQLRunning: true, IORunning: true, GTID: "uuid:1-10"},
		},
	}
}

func drainCluster() *mysqlv1alpha1.Cluster {
	cluster := testCluster()
	cluster.Status.CurrentPrimary = drainPrimary
	cluster.Status.TargetPrimary = drainPrimary
	return cluster
}

func newDrainReconciler(t *testing.T, cluster *mysqlv1alpha1.Cluster) (*Reconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(8)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		Build()
	return NewReconciler(client, scheme, nil, recorder, ""), recorder
}

func TestReconcileDrainSwitchoverPromotesSafeReplica(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := drainCluster()
	r, recorder := newDrainReconciler(t, cluster)

	result, err := r.ReconcileDrainSwitchover(ctx, cluster, drainState())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Handled {
		t.Fatal("expected the drain switchover to be handled")
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.TargetPrimary != drainReplica {
		t.Fatalf("TargetPrimary = %q, want demo-2", got.Status.TargetPrimary)
	}
	if got.Status.Phase != topology.PhaseSwitchover {
		t.Fatalf("Phase = %q, want %q", got.Status.Phase, topology.PhaseSwitchover)
	}
	select {
	case <-recorder.Events:
	default:
		t.Fatal("expected a switchover event to be recorded")
	}
}

func TestReconcileDrainSwitchoverDisabledIsNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := drainCluster()
	cluster.Spec.EnableSwitchoverOnDrain = ptrBool(false)
	r, _ := newDrainReconciler(t, cluster)

	result, err := r.ReconcileDrainSwitchover(ctx, cluster, drainState())
	if err != nil {
		t.Fatal(err)
	}
	if result != (topology.FailoverResult{}) {
		t.Fatalf("expected an empty result when disabled, got %#v", result)
	}
	assertTargetUnchanged(t, r, cluster)
}

func TestReconcileDrainSwitchoverPrimaryNotTerminatingIsNoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := drainCluster()
	r, _ := newDrainReconciler(t, cluster)

	state := drainState()
	state.Terminating = nil // primary is healthy and staying put

	result, err := r.ReconcileDrainSwitchover(ctx, cluster, state)
	if err != nil {
		t.Fatal(err)
	}
	if result.Handled {
		t.Fatal("expected no handoff when the primary is not terminating")
	}
	assertTargetUnchanged(t, r, cluster)
}

func TestReconcileDrainSwitchoverNoSafeCandidateDefersToFailover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := drainCluster()
	r, _ := newDrainReconciler(t, cluster)

	state := drainState()
	// The only replica has diverged from the primary: not a safe target.
	state.Diverged = []string{drainReplica}

	result, err := r.ReconcileDrainSwitchover(ctx, cluster, state)
	if err != nil {
		t.Fatal(err)
	}
	if result.Handled {
		t.Fatal("expected no handoff when no safe candidate exists")
	}
	assertTargetUnchanged(t, r, cluster)
}

func TestReconcileDrainSwitchoverUnreachablePrimaryDefersToFailover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := drainCluster()
	r, _ := newDrainReconciler(t, cluster)

	state := drainState()
	// Primary already gone (not ready): this is a failure, not a planned drain.
	primary := state.Instances[drainPrimary]
	primary.Ready = false
	state.Instances[drainPrimary] = primary

	result, err := r.ReconcileDrainSwitchover(ctx, cluster, state)
	if err != nil {
		t.Fatal(err)
	}
	if result.Handled {
		t.Fatal("expected no handoff when the primary is already unreachable")
	}
	assertTargetUnchanged(t, r, cluster)
}

func assertTargetUnchanged(t *testing.T, r *Reconciler, cluster *mysqlv1alpha1.Cluster) {
	t.Helper()
	got := &mysqlv1alpha1.Cluster{}
	if err := r.client.Get(context.Background(),
		types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.TargetPrimary != drainPrimary {
		t.Fatalf("TargetPrimary = %q, want it unchanged (demo-1)", got.Status.TargetPrimary)
	}
}

func ptrBool(b bool) *bool { return &b }
