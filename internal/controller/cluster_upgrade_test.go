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
	"io"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

const oldHash, newHash = "old", "new"

func TestUpgradeCandidates(t *testing.T) {
	t.Parallel()

	const target = "target-hash"

	tests := []struct {
		name     string
		observed observedCluster
		want     []string
	}{
		{
			name: "all up to date",
			observed: observedCluster{
				InstanceNames: []string{"c-1", "c-2", "c-3"},
				PrimaryName:   "c-1",
				ExecutableHashByInstance: map[string]string{
					"c-1": target, "c-2": target, "c-3": target,
				},
			},
			want: nil,
		},
		{
			name: "stale replicas come before the stale primary",
			observed: observedCluster{
				InstanceNames: []string{"c-1", "c-2", "c-3"},
				PrimaryName:   "c-1",
				ExecutableHashByInstance: map[string]string{
					"c-1": "old", "c-2": "old", "c-3": "old",
				},
			},
			want: []string{"c-2", "c-3", "c-1"},
		},
		{
			name: "fenced instances are excluded",
			observed: observedCluster{
				InstanceNames:   []string{"c-1", "c-2", "c-3"},
				PrimaryName:     "c-1",
				FencedInstances: []string{"c-2"},
				ExecutableHashByInstance: map[string]string{
					"c-1": "old", "c-2": "old", "c-3": "old",
				},
			},
			want: []string{"c-3", "c-1"},
		},
		{
			name: "instances that report no hash are skipped",
			observed: observedCluster{
				InstanceNames: []string{"c-1", "c-2", "c-3"},
				PrimaryName:   "c-1",
				ExecutableHashByInstance: map[string]string{
					"c-1": "old",
					"c-3": "old",
					// c-2 unreachable: no reported hash.
				},
			},
			want: []string{"c-3", "c-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := upgradeCandidates(tt.observed, target)
			var names []string
			for _, c := range got {
				names = append(names, c.Name)
			}
			if !equalStrings(names, tt.want) {
				t.Fatalf("upgradeCandidates order = %v, want %v", names, tt.want)
			}
		})
	}
}

func TestHealthyReplicaForSwitchover(t *testing.T) {
	t.Parallel()

	ready := func(names ...string) map[string]*webserver.Status {
		m := map[string]*webserver.Status{}
		for _, n := range names {
			m[n] = &webserver.Status{IsReady: true}
		}
		return m
	}

	tests := []struct {
		name     string
		observed observedCluster
		want     string
	}{
		{
			name: "picks first ready replica, never the primary",
			observed: observedCluster{
				InstanceNames:    []string{"c-1", "c-2", "c-3"},
				PrimaryName:      "c-1",
				StatusByInstance: ready("c-1", "c-2", "c-3"),
			},
			want: "c-2",
		},
		{
			name: "skips fenced, diverged and replication-broken replicas",
			observed: observedCluster{
				InstanceNames:     []string{"c-1", "c-2", "c-3"},
				PrimaryName:       "c-1",
				FencedInstances:   []string{"c-2"},
				DivergedInstances: []string{"c-3"},
				StatusByInstance:  ready("c-1", "c-2", "c-3"),
			},
			want: "",
		},
		{
			name: "skips non-ready replicas",
			observed: observedCluster{
				InstanceNames:    []string{"c-1", "c-2", "c-3"},
				PrimaryName:      "c-1",
				StatusByInstance: ready("c-1", "c-3"),
			},
			want: "c-3",
		},
		{
			name: "no eligible replica",
			observed: observedCluster{
				InstanceNames:    []string{"c-1"},
				PrimaryName:      "c-1",
				StatusByInstance: ready("c-1"),
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := healthyReplicaForSwitchover(tt.observed); got != tt.want {
				t.Fatalf("healthyReplicaForSwitchover = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestReconcileUpgradeRollsInstancesInOrder drives reconcileUpgrade in a loop,
// faking what a real reconcile would do between calls (recreate a rolled Pod
// with the up-to-date hash, complete a requested switchover), and asserts the
// end-to-end ordering: replicas first, the primary handled last via switchover,
// and the demoted old primary rolled afterwards. One instance changes per call.
func TestReconcileUpgradeRollsInstancesInOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	// Defaults are unsupervised + switchover, but be explicit.
	cluster.Spec.PrimaryUpdateStrategy = mysqlv1alpha1.PrimaryUpdateStrategyUnsupervised
	cluster.Spec.PrimaryUpdateMethod = mysqlv1alpha1.PrimaryUpdateMethodSwitchover

	scheme := testScheme(t)
	pods := []*corev1.Pod{
		readyPod(cluster, testPrimary, rolePrimary),
		readyPod(cluster, testReplica2, roleReplica),
		readyPod(cluster, testReplica3, roleReplica),
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, pods[0], pods[1], pods[2]).
			Build(),
		Scheme:                 scheme,
		OperatorExecutableHash: newHash,
	}

	plan := testPlan()
	plan.Instances = 3
	names := []string{testPrimary, testReplica2, testReplica3}

	// Every instance starts stale (reporting the old hash) and ready.
	observed := observedCluster{
		Plan:                     plan,
		PrimaryName:              testPrimary,
		InstanceNames:            names,
		ReadyInstances:           3,
		ExecutableHashByInstance: map[string]string{},
		StatusByInstance:         map[string]*webserver.Status{},
		GTIDByInstance:           map[string]string{},
	}
	for _, n := range names {
		observed.ExecutableHashByInstance[n] = oldHash
		observed.StatusByInstance[n] = &webserver.Status{InstanceName: n, IsReady: true}
	}

	podExists := func(name string) bool {
		err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, &corev1.Pod{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		return err == nil
	}

	var actions []string
	for i := range 10 {
		handled, _, err := reconciler.reconcileUpgrade(ctx, cluster, plan, observed)
		if err != nil {
			t.Fatal(err)
		}
		if !handled {
			break
		}

		// Find which Pod, if any, was rolled (deleted) this iteration.
		var rolled string
		for _, n := range names {
			if !podExists(n) {
				if rolled != "" {
					t.Fatalf("more than one Pod deleted in a single reconcile (%s and %s)", rolled, n)
				}
				rolled = n
			}
		}

		if rolled != "" {
			actions = append(actions, "roll:"+rolled)
			// Fake the next reconcile recreating the Pod with the new manager.
			role := roleReplica
			if rolled == observed.PrimaryName {
				role = rolePrimary
			}
			if err := reconciler.Create(ctx, readyPod(cluster, rolled, role)); err != nil {
				t.Fatal(err)
			}
			observed.ExecutableHashByInstance[rolled] = newHash
			continue
		}

		// No Pod was rolled: this must be a switchover request on the primary.
		got := &mysqlv1alpha1.Cluster{}
		if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
			t.Fatal(err)
		}
		target := got.Status.TargetPrimary
		if target == "" {
			t.Fatalf("reconcileUpgrade reported handled but neither rolled a Pod nor set a switchover target (iteration %d)", i)
		}
		actions = append(actions, "switchover:"+target)
		// Fake the switchover completing: the target becomes primary, the old
		// primary is demoted to a (still stale) replica.
		observed.PrimaryName = target
		if err := reconciler.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
			s.CurrentPrimary = target
			s.TargetPrimary = ""
		}); err != nil {
			t.Fatal(err)
		}
	}

	want := []string{
		"roll:" + testReplica2,
		"roll:" + testReplica3,
		"switchover:" + testReplica2,
		"roll:" + testPrimary,
	}
	if !equalStrings(actions, want) {
		t.Fatalf("upgrade action order = %v, want %v", actions, want)
	}
}

// Under Group Replication the operator never promotes (the group elects) and the
// GR ReconcileSwitchover is a no-op, so an operator-binary upgrade of a stale GR
// primary must roll it directly — delete its Pod and let the group re-elect —
// rather than setting a switchover target that would move nothing and deadlock.
func TestReconcileUpgradeGroupReplicationRollsPrimaryWithoutSwitchover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, observed := upgradeFixture(t, 3, mysqlv1alpha1.PrimaryUpdateStrategyUnsupervised)
	cluster.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{
		Mode: mysqlv1alpha1.ReplicationModeGroupReplication,
	}

	// Replicas are already on the new manager; only the primary is stale.
	observed.ExecutableHashByInstance[testReplica2] = upgradeNewHash
	observed.ExecutableHashByInstance[testReplica3] = upgradeNewHash
	observed.ExecutableHashByInstance[testPrimary] = oldHash

	handled, _, err := reconciler.reconcileUpgrade(ctx, cluster, observed.Plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("expected the stale GR primary upgrade to be handled")
	}

	// The primary Pod must have been rolled directly.
	if missing := anyPodMissing(t, reconciler, cluster, []string{testPrimary}); missing != testPrimary {
		t.Fatal("GR primary Pod should have been deleted for a direct roll")
	}
	// No switchover target must have been set.
	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.TargetPrimary != "" {
		t.Fatalf("GR primary upgrade must not set a switchover target, got %q", got.Status.TargetPrimary)
	}
}

const upgradeNewHash = "new"

// upgradeFixture builds a cluster of the given size with all Pods ready, plus an
// observedCluster reporting every instance ready. Instance executable hashes are
// left empty for the caller to set (empty = no reported hash = not a candidate).
// The reconciler's target hash is upgradeNewHash.
func upgradeFixture(
	t *testing.T,
	instances int,
	strategy mysqlv1alpha1.PrimaryUpdateStrategy,
) (*mysqlv1alpha1.Cluster, *ClusterReconciler, observedCluster) {
	t.Helper()

	names := []string{testPrimary, testReplica2, testReplica3}[:instances]
	cluster := baseCluster()
	cluster.Spec.Instances = instances
	cluster.Spec.PrimaryUpdateStrategy = strategy
	cluster.Spec.PrimaryUpdateMethod = mysqlv1alpha1.PrimaryUpdateMethodSwitchover

	scheme := testScheme(t)
	objs := make([]client.Object, 1, 1+len(names))
	objs[0] = cluster
	for i, n := range names {
		role := roleReplica
		if i == 0 {
			role = rolePrimary
		}
		objs = append(objs, readyPod(cluster, n, role))
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(objs...).
			Build(),
		Scheme:                 scheme,
		OperatorExecutableHash: upgradeNewHash,
	}

	plan := testPlan()
	plan.Instances = instances
	observed := observedCluster{
		Plan:                     plan,
		PrimaryName:              testPrimary,
		InstanceNames:            names,
		ReadyInstances:           instances,
		ExecutableHashByInstance: map[string]string{},
		StatusByInstance:         map[string]*webserver.Status{},
		GTIDByInstance:           map[string]string{},
	}
	for _, n := range names {
		observed.StatusByInstance[n] = &webserver.Status{InstanceName: n, IsReady: true}
	}
	return cluster, reconciler, observed
}

func anyPodMissing(t *testing.T, r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster, names []string) string {
	t.Helper()
	for _, n := range names {
		err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: n}, &corev1.Pod{})
		if apierrors.IsNotFound(err) {
			return n
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return ""
}

func clusterPhase(t *testing.T, r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster) string {
	t.Helper()
	got := &mysqlv1alpha1.Cluster{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	return got.Status.Phase
}

// Supervised + stale primary on a multi-instance cluster: the operator must stop
// and wait for the user instead of rolling anything.
func TestReconcileUpgradeSupervisedWaitsForUserOnStalePrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, observed := upgradeFixture(t, 3, mysqlv1alpha1.PrimaryUpdateStrategySupervised)
	for _, n := range observed.InstanceNames {
		observed.ExecutableHashByInstance[n] = oldHash
	}

	handled, _, err := reconciler.reconcileUpgrade(ctx, cluster, observed.Plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("supervised stale primary should be reported as handled")
	}
	if missing := anyPodMissing(t, reconciler, cluster, observed.InstanceNames); missing != "" {
		t.Fatalf("supervised upgrade rolled Pod %q, want no Pod deleted", missing)
	}
	if phase := clusterPhase(t, reconciler, cluster); phase != topology.PhaseWaitingForUser {
		t.Fatalf("phase = %q, want %q", phase, topology.PhaseWaitingForUser)
	}
}

// Supervised but the primary is already current: stale replicas must still roll;
// the wait-for-user gate only applies when the primary itself is stale.
func TestReconcileUpgradeSupervisedRollsReplicasWhilePrimaryCurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, observed := upgradeFixture(t, 3, mysqlv1alpha1.PrimaryUpdateStrategySupervised)
	observed.ExecutableHashByInstance[testPrimary] = upgradeNewHash
	observed.ExecutableHashByInstance[testReplica2] = oldHash
	observed.ExecutableHashByInstance[testReplica3] = oldHash

	handled, _, err := reconciler.reconcileUpgrade(ctx, cluster, observed.Plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("stale replica should be rolled even under supervised")
	}
	if phase := clusterPhase(t, reconciler, cluster); phase == topology.PhaseWaitingForUser {
		t.Fatal("should not wait for user when only replicas are stale")
	}
	if missing := anyPodMissing(t, reconciler, cluster, observed.InstanceNames); missing != testReplica2 {
		t.Fatalf("rolled Pod = %q, want %q (replicas first, primary untouched)", missing, testReplica2)
	}
}

// A single-instance primary cannot be switched over, so supervised must not block
// it: the operator restarts it in place (CNPG does the same).
func TestReconcileUpgradeSingleInstanceSupervisedRestartsInPlace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, observed := upgradeFixture(t, 1, mysqlv1alpha1.PrimaryUpdateStrategySupervised)
	observed.ExecutableHashByInstance[testPrimary] = oldHash

	handled, _, err := reconciler.reconcileUpgrade(ctx, cluster, observed.Plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("single-instance supervised primary should be rolled in place")
	}
	if phase := clusterPhase(t, reconciler, cluster); phase == topology.PhaseWaitingForUser {
		t.Fatal("single-instance supervised primary must not wait for user")
	}
	if missing := anyPodMissing(t, reconciler, cluster, observed.InstanceNames); missing != testPrimary {
		t.Fatalf("rolled Pod = %q, want %q (in-place restart)", missing, testPrimary)
	}
}

// TestReconcileUpgradeInPlaceStreamsBinaryReplicasFirst drives the in-place
// path: each reconcile must stream the operator binary to exactly one stale
// instance (replicas before the primary), never delete a Pod, and never request
// a switchover. Between calls the test fakes the manager re-execing by marking
// that instance's reported hash current.
func TestReconcileUpgradeInPlaceStreamsBinaryReplicasFirst(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, observed := upgradeFixture(t, 3, mysqlv1alpha1.PrimaryUpdateStrategyUnsupervised)
	cluster.Spec.InPlaceInstanceManagerUpdates = true
	reconciler.openOperatorBinary = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("operator-binary")), nil
	}
	rec := &recordingControlClient{statuses: map[string]*webserver.Status{}}
	reconciler.ControlClient = rec

	for _, n := range observed.InstanceNames {
		observed.ExecutableHashByInstance[n] = oldHash
	}

	for range 10 {
		handled, _, err := reconciler.reconcileUpgrade(ctx, cluster, observed.Plan, observed)
		if err != nil {
			t.Fatal(err)
		}
		if !handled {
			break
		}
		// No Pod may ever be deleted on the in-place path.
		if missing := anyPodMissing(t, reconciler, cluster, observed.InstanceNames); missing != "" {
			t.Fatalf("in-place upgrade deleted Pod %q; it must stream the binary instead", missing)
		}
		// No switchover may ever be requested.
		got := &mysqlv1alpha1.Cluster{}
		if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
			t.Fatal(err)
		}
		if got.Status.TargetPrimary != "" && got.Status.TargetPrimary != observed.PrimaryName {
			t.Fatalf("in-place upgrade requested a switchover to %q", got.Status.TargetPrimary)
		}
		// Mark the most recently upgraded instance current for the next pass.
		if n := len(rec.upgraded); n > 0 {
			observed.ExecutableHashByInstance[rec.upgraded[n-1]] = upgradeNewHash
		}
	}

	want := []string{testReplica2, testReplica3, testPrimary}
	if !equalStrings(rec.upgraded, want) {
		t.Fatalf("in-place upgrade order = %v, want %v", rec.upgraded, want)
	}
	for _, n := range want {
		if rec.upgradeHash[n] != upgradeNewHash {
			t.Errorf("instance %s upgraded with hash %q, want %q", n, rec.upgradeHash[n], upgradeNewHash)
		}
		if string(rec.upgradeBytes[n]) != "operator-binary" {
			t.Errorf("instance %s got binary %q", n, rec.upgradeBytes[n])
		}
	}
}

// Under the in-place path, a supervised stale primary must NOT wait for the
// user: an in-place swap needs no switchover, so it proceeds like any replica.
func TestReconcileUpgradeInPlaceIgnoresSupervisedGate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, observed := upgradeFixture(t, 3, mysqlv1alpha1.PrimaryUpdateStrategySupervised)
	cluster.Spec.InPlaceInstanceManagerUpdates = true
	reconciler.openOperatorBinary = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("operator-binary")), nil
	}
	rec := &recordingControlClient{statuses: map[string]*webserver.Status{}}
	reconciler.ControlClient = rec
	// Only the primary is stale, so the first action targets the primary.
	observed.ExecutableHashByInstance[testPrimary] = oldHash
	observed.ExecutableHashByInstance[testReplica2] = upgradeNewHash
	observed.ExecutableHashByInstance[testReplica3] = upgradeNewHash

	handled, _, err := reconciler.reconcileUpgrade(ctx, cluster, observed.Plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("in-place stale primary should be handled, not left waiting")
	}
	if phase := clusterPhase(t, reconciler, cluster); phase == topology.PhaseWaitingForUser {
		t.Fatal("in-place upgrade must not wait for user on a supervised primary")
	}
	if !equalStrings(rec.upgraded, []string{testPrimary}) {
		t.Fatalf("upgraded = %v, want [%s]", rec.upgraded, testPrimary)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
