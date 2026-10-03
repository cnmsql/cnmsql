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

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
)

// TestPrimaryUnhealthyReason pins the trigger reasons the failover log line
// reports. The action ("Failing over primary from=X to=Y") is useless without
// the cause: a Pod that is not ready, an instance manager that stopped
// answering, or an instance that no longer acts as primary are different
// incidents with different runbooks.
func TestPrimaryUnhealthyReason(t *testing.T) {
	t.Parallel()

	primary := drainPrimary
	for _, tc := range []struct {
		name     string
		observed topology.FailoverState
		want     string
	}{
		{
			name: "status endpoint unreachable",
			observed: topology.FailoverState{
				PrimaryName:   primary,
				InstanceNames: []string{primary, drainReplica},
				Instances: map[string]topology.FailoverInstance{
					drainReplica: {Ready: true, Replica: true},
				},
			},
			want: "instance manager status is unreachable",
		},
		{
			name: "pod not ready",
			observed: topology.FailoverState{
				PrimaryName:   primary,
				InstanceNames: []string{primary, drainReplica},
				Instances: map[string]topology.FailoverInstance{
					primary:      {Ready: false, Primary: true, Role: "primary"},
					drainReplica: {Ready: true, Replica: true},
				},
			},
			want: "pod is not ready",
		},
		{
			name: "pod terminating",
			observed: topology.FailoverState{
				PrimaryName:   primary,
				InstanceNames: []string{primary, drainReplica},
				Terminating:   []string{primary},
				Instances: map[string]topology.FailoverInstance{
					primary:      {Ready: true, Primary: true, Role: "primary"},
					drainReplica: {Ready: true, Replica: true},
				},
			},
			want: "pod is terminating",
		},
		{
			name: "role no longer primary",
			observed: topology.FailoverState{
				PrimaryName:   primary,
				InstanceNames: []string{primary, drainReplica},
				Instances: map[string]topology.FailoverInstance{
					primary:      {Ready: true, Primary: false, Role: "replica"},
					drainReplica: {Ready: true, Replica: true},
				},
			},
			want: "instance reports role replica, not primary",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := primaryUnhealthyReason(tc.observed)
			if got != tc.want {
				t.Fatalf("primaryUnhealthyReason = %q, want %q", got, tc.want)
			}
		})
	}
}

// switchoverState returns a FailoverState for the demote step of an in-flight
// switchover: the old primary has been demoted (read-only, lease released) while
// status.currentPrimary still names it, and the target is a healthy replica
// about to promote itself. This is the state a node drain produces between the
// demotion and the promotion, and it must not read as a dead primary.
const drainThird = "demo-3"

func switchoverState() topology.FailoverState {
	return topology.FailoverState{
		PrimaryName:   drainPrimary,
		InstanceNames: []string{drainPrimary, drainReplica},
		Terminating:   []string{drainPrimary},
		Instances: map[string]topology.FailoverInstance{
			drainPrimary: {Ready: true, Primary: false, Role: "replica"},
			drainReplica: {Ready: true, Replica: true, Role: "replica", SQLRunning: true, IORunning: true, GTID: "uuid:1-10"},
		},
	}
}

// switchoverCluster returns a Cluster with a planned switchover already in
// flight, as ReconcileDrainSwitchover leaves it.
func switchoverCluster() *mysqlv1alpha1.Cluster {
	cluster := testCluster()
	cluster.Status.CurrentPrimary = drainPrimary
	cluster.Status.TargetPrimary = drainReplica
	cluster.Status.Phase = topology.PhaseSwitchover
	return cluster
}

// TestReconcileFailoverDefersToInFlightSwitchover pins the race between the
// planned switchover path and the reactive failover path: once the switchover
// demotes the old primary, it reports a replica role with its lease released,
// which is indistinguishable from a failed primary. Failing over in that window
// stamps an emergency FailingOver over a planned handoff; the promotion
// machinery already owns the change.
func TestReconcileFailoverDefersToInFlightSwitchover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := switchoverCluster()
	r, recorder := newDrainReconciler(t, cluster)

	result, err := r.ReconcileFailover(ctx, cluster, topology.FailoverRequest{
		Instances: 2,
		Observed:  switchoverState(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Handled {
		t.Fatal("expected failover to defer to the in-flight switchover")
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != topology.PhaseSwitchover {
		t.Fatalf("Phase = %q, want the in-flight switchover to keep the phase", got.Status.Phase)
	}
	if got.Status.PrimaryFailingSince != nil {
		t.Fatal("failover recorded the demoted primary as failing")
	}
	if got.Status.LastFailoverTimestamp != nil {
		t.Fatal("failover recorded a promotion during an in-flight switchover")
	}
	select {
	case ev := <-recorder.Events:
		t.Fatalf("expected no failover event during an in-flight switchover, got %q", ev)
	default:
	}
}

// TestReconcileFailoverDefersWhenSwitchoverTargetAlreadyPromoted covers the
// tail of the same window: the target has promoted itself but
// status.currentPrimary still names the old primary. The switchover path waits
// for that catch-up; failover must not turn it into a Blocked incident.
func TestReconcileFailoverDefersWhenSwitchoverTargetAlreadyPromoted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := switchoverCluster()
	state := switchoverState()
	target := state.Instances[drainReplica]
	target.Replica = false
	target.Primary = true
	target.Role = "primary"
	state.Instances[drainReplica] = target
	r, _ := newDrainReconciler(t, cluster)

	result, err := r.ReconcileFailover(ctx, cluster, topology.FailoverRequest{
		Instances: 2,
		Observed:  state,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Handled || result.Phase != nil {
		t.Fatalf("expected a clean deferral, got handled=%v phase=%v", result.Handled, result.Phase)
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.PrimaryFailingSince != nil {
		t.Fatal("failover recorded the demoted primary as failing")
	}
}

// TestReconcileFailoverFiresWhenSwitchoverTargetUnfit makes sure the deferral
// is bounded: when the switchover target itself is gone, the switchover cannot
// complete and failover must stay free to elect another replica.
func TestReconcileFailoverFiresWhenSwitchoverTargetUnfit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := switchoverCluster()
	state := switchoverState()
	state.InstanceNames = append(state.InstanceNames, drainThird)
	// The switchover target is gone; another replica is healthy and caught up.
	state.Instances[drainReplica] = topology.FailoverInstance{Ready: false}
	state.Instances[drainThird] = topology.FailoverInstance{
		Ready: true, Replica: true, Role: "replica", SQLRunning: true, IORunning: true, GTID: "uuid:1-10",
	}
	r, recorder := newDrainReconciler(t, cluster)

	result, err := r.ReconcileFailover(ctx, cluster, topology.FailoverRequest{
		Instances: 3,
		Observed:  state,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Handled {
		t.Fatal("expected failover to proceed when the switchover target is unfit")
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != topology.PhaseFailingOver {
		t.Fatalf("Phase = %q, want %q", got.Status.Phase, topology.PhaseFailingOver)
	}
	if got.Status.TargetPrimary != drainThird {
		t.Fatalf("TargetPrimary = %q, want the election to pick %s", got.Status.TargetPrimary, drainThird)
	}
	select {
	case <-recorder.Events:
	default:
		t.Fatal("expected a failover event to be recorded")
	}
}

// TestReconcileFailoverFiresWhenSwitchoverTargetDiverged pins the other escape
// hatch: a diverged target never promotes (the in-Pod reconciler refuses), so
// deferring on it would strand the cluster with no writable primary. The
// failover path must keep ownership and refuse loudly instead.
func TestReconcileFailoverFiresWhenSwitchoverTargetDiverged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := switchoverCluster()
	state := switchoverState()
	state.Diverged = []string{drainReplica}
	r, _ := newDrainReconciler(t, cluster)

	result, err := r.ReconcileFailover(ctx, cluster, topology.FailoverRequest{
		Instances: 2,
		Observed:  state,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Handled {
		t.Fatal("expected failover to keep ownership of a diverged target")
	}
	if result.Phase == nil || result.Phase.Phase != topology.PhaseBlocked {
		t.Fatalf("Phase = %v, want %q", result.Phase, topology.PhaseBlocked)
	}
}
