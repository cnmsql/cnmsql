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

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	controllerasync "github.com/cnmsql/cnmsql/internal/controller/async"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

var (
	mysqlGTIDModel     = engine.MustForFlavor(engine.FlavorMySQL).GTID()
	mysqlFlavorCluster = &mysqlv1alpha1.Cluster{Spec: mysqlv1alpha1.ClusterSpec{Flavor: "mysql"}}
)

const (
	testPrimary  = "demo-1"
	testReplica2 = "demo-2"
	testReplica3 = "demo-3"
	testGTID     = "uuid:1-10"
	appName      = "app"
)

func healthyReplicaStatus(name, gtid string) *webserver.Status {
	return &webserver.Status{
		InstanceName: name,
		Role:         webserver.RoleReplica,
		IsReady:      true,
		GTIDExecuted: gtid,
		Replication:  &webserver.ReplicationStatus{IORunning: true, SQLRunning: true},
	}
}

// selectCandidate elects with no promotion bound, the behaviour of a cluster
// that sets no failoverPolicy.
func selectCandidate(observed topology.FailoverState, knownDiverged []string) (string, string) {
	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:      observed,
		KnownDiverged: knownDiverged,
		GTID:          mysqlGTIDModel,
	})
	return elected.Name, elected.Reason
}

func TestSelectFailoverCandidatePrefersMostCompleteThenOrdinal(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testReplica2: "uuid:1-8",
			testReplica3: testGTID,
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, "uuid:1-8"),
			testReplica3: healthyReplicaStatus(testReplica3, testGTID),
		},
	}
	got, reason := selectCandidate(topologyFailoverState(observed), nil)
	if got != testReplica3 {
		t.Fatalf("candidate = %q (reason %q), want demo-3", got, reason)
	}

	// Equal GTID: lowest ordinal wins.
	observed.GTIDByInstance[testReplica2] = testGTID
	observed.StatusByInstance[testReplica2] = healthyReplicaStatus(testReplica2, testGTID)
	if got, _ := selectCandidate(topologyFailoverState(observed), nil); got != testReplica2 {
		t.Fatalf("candidate = %q, want demo-2 on equal GTID", got)
	}
}

func TestSelectFailoverCandidateBlocksOnDivergedGTID(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testReplica2: "uuid:1-8",
			testReplica3: "other:1-4",
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, "uuid:1-8"),
			testReplica3: healthyReplicaStatus(testReplica3, "other:1-4"),
		},
	}
	got, reason := selectCandidate(topologyFailoverState(observed), nil)
	if got != "" {
		t.Fatalf("candidate = %q, want empty (blocked)", got)
	}
	if reason == "" {
		t.Fatal("expected a block reason")
	}
}

func TestSelectFailoverCandidateExcludesKnownDivergedReplica(t *testing.T) {
	t.Parallel()
	// A diverged replica's GTID set is a superset of the clean replica's, so the
	// dominance check would otherwise pick it and make its errant transactions
	// canonical. It must be excluded by the known-diverged set instead.
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testReplica2: "a:1-15",
			testReplica3: "a:1-15,b:1-3", // errant transactions b:1-3
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, "a:1-15"),
			testReplica3: healthyReplicaStatus(testReplica3, "a:1-15,b:1-3"),
		},
	}
	// Sanity: without the guard the diverged superset would be chosen.
	if got, _ := selectCandidate(topologyFailoverState(observed), nil); got != testReplica3 {
		t.Fatalf("precondition: candidate = %q, want the diverged superset demo-3 to dominate", got)
	}
	// With it flagged diverged, the clean replica wins instead.
	got, reason := selectCandidate(topologyFailoverState(observed), []string{testReplica3})
	if got != testReplica2 {
		t.Fatalf("candidate = %q (reason %q), want the clean replica demo-2", got, reason)
	}
}

func TestSelectFailoverCandidateBlocksWhenOnlyCandidateDiverged(t *testing.T) {
	t.Parallel()
	// The sole surviving replica is known-diverged: promoting it would canonicalise
	// errant transactions, so failover blocks for manual recovery rather than
	// silently corrupting the cluster.
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{
			testReplica2: "a:1-15,b:1-3",
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, "a:1-15,b:1-3"),
		},
	}
	got, reason := selectCandidate(topologyFailoverState(observed), []string{testReplica2})
	if got != "" {
		t.Fatalf("candidate = %q, want empty (blocked)", got)
	}
	if !strings.Contains(reason, "diverged") {
		t.Fatalf("reason = %q, want it to explain the divergence", reason)
	}
}

func TestSelectFailoverCandidateSkipsUnhealthyReplicas(t *testing.T) {
	t.Parallel()
	broken := healthyReplicaStatus(testReplica2, "uuid:1-9")
	broken.Replication.SQLRunning = false
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testReplica2: "uuid:1-9",
			testReplica3: "uuid:1-7",
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: broken,
			testReplica3: healthyReplicaStatus(testReplica3, "uuid:1-7"),
		},
	}
	if got, _ := selectCandidate(topologyFailoverState(observed), nil); got != testReplica3 {
		t.Fatalf("candidate = %q, want demo-3 (demo-2 has stalled SQL thread)", got)
	}
}

func TestSelectFailoverCandidateAllowsStoppedIOThread(t *testing.T) {
	t.Parallel()
	replica2 := healthyReplicaStatus(testReplica2, testGTID)
	replica2.IsReady = false
	replica2.Replication.IORunning = false
	replica2.Replication.LastError = "connecting to source failed"
	replica3 := healthyReplicaStatus(testReplica3, testGTID)
	replica3.IsReady = false
	replica3.Replication.IORunning = false
	replica3.Replication.LastError = "connecting to source failed"
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testReplica2: testGTID,
			testReplica3: testGTID,
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: replica2,
			testReplica3: replica3,
		},
	}

	got, reason := selectCandidate(topologyFailoverState(observed), nil)
	if got != testReplica2 {
		t.Fatalf("candidate = %q (reason %q), want demo-2", got, reason)
	}
}

func TestSelectFailoverCandidateSkipsReplicasWithoutGTID(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, ""),
		},
	}

	got, reason := selectCandidate(topologyFailoverState(observed), nil)
	if got != "" {
		t.Fatalf("candidate = %q (reason %q), want empty without GTID status", got, reason)
	}
}

// lagRelativeToPrimary builds the failure #76 describes: the primary committed
// through uuid:1-100 before dying, and the only reachable replica stopped
// receiving at uuid:1-40. Its relay log is fully drained, so the replica reports
// itself caught up (Seconds_Behind_Source 0) while missing 60 transactions.
func lagRelativeToPrimary() (topology.FailoverState, string) {
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{
			testReplica2: "uuid:1-40",
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, "uuid:1-40"),
		},
	}
	return topologyFailoverState(observed), "uuid:1-100"
}

func TestSelectFailoverCandidateBlocksWhenLagExceedsBound(t *testing.T) {
	t.Parallel()
	observed, primaryGTID := lagRelativeToPrimary()
	bound := int64(10)

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:              observed,
		GTID:                  mysqlGTIDModel,
		MaxTransactionsBehind: &bound,
		ReferenceGTID:         primaryGTID,
	})
	// It is the only replica, but it is 60 transactions behind a bound of 10.
	// Promoting it would silently discard those 60, so the election must refuse
	// and say by how much it missed.
	if elected.Name != "" {
		t.Fatalf("candidate = %q, want no election: the only replica is 60 transactions behind a bound of 10", elected.Name)
	}
	if elected.TransactionsBehind != 60 {
		t.Errorf("TransactionsBehind = %d, want 60", elected.TransactionsBehind)
	}
	for _, want := range []string{testReplica2, "60 transactions behind", "maxTransactionsBehind (10)"} {
		if !strings.Contains(elected.Reason, want) {
			t.Errorf("Reason = %q, want it to mention %q", elected.Reason, want)
		}
	}
}

func TestSelectFailoverCandidatePrefersReplicaWithinBound(t *testing.T) {
	t.Parallel()
	// demo-2 is 60 transactions behind the dead primary; demo-3 has everything.
	// The bound must exclude demo-2 rather than let ordinal order pick it.
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testReplica2: "uuid:1-40",
			testReplica3: "uuid:1-100",
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, "uuid:1-40"),
			testReplica3: healthyReplicaStatus(testReplica3, "uuid:1-100"),
		},
	}
	bound := int64(10)

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:              topologyFailoverState(observed),
		GTID:                  mysqlGTIDModel,
		MaxTransactionsBehind: &bound,
		ReferenceGTID:         "uuid:1-100",
	})
	if elected.Name != testReplica3 {
		t.Fatalf("candidate = %q (reason %q), want demo-3", elected.Name, elected.Reason)
	}
	if elected.TransactionsBehind != 0 {
		t.Errorf("elected = %+v, want a candidate within the bound at zero lag", elected)
	}
}

func TestSelectFailoverCandidateCountsUnappliedRelayLogAsHeld(t *testing.T) {
	t.Parallel()
	// demo-2 has received every transaction the primary had but applied only 40 of
	// them. That is applier delay, not data loss: the relay log drains before
	// promotion, so it must stay within the bound.
	status := healthyReplicaStatus(testReplica2, "uuid:1-40")
	status.Replication.RetrievedGTIDSet = "uuid:1-100"
	observed := observedCluster{
		PrimaryName:      testPrimary,
		InstanceNames:    []string{testPrimary, testReplica2},
		GTIDByInstance:   map[string]string{testReplica2: "uuid:1-40"},
		StatusByInstance: map[string]*webserver.Status{testReplica2: status},
	}
	bound := int64(0)

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:              topologyFailoverState(observed),
		GTID:                  mysqlGTIDModel,
		MaxTransactionsBehind: &bound,
		ReferenceGTID:         "uuid:1-100",
	})
	if elected.Name != testReplica2 {
		t.Fatalf("candidate = %q, want demo-2", elected.Name)
	}
	if elected.TransactionsBehind != 0 {
		t.Errorf("elected = %+v, want zero lag: the transactions are in the relay log", elected)
	}
}

func TestSelectFailoverCandidateUnboundedIgnoresLag(t *testing.T) {
	t.Parallel()
	// Without a failoverPolicy the lag is reported but never blocks, preserving
	// the behaviour of clusters that predate the bound.
	observed, primaryGTID := lagRelativeToPrimary()

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:      observed,
		GTID:          mysqlGTIDModel,
		ReferenceGTID: primaryGTID,
	})
	if elected.Name != testReplica2 {
		t.Fatalf("elected = %+v, want demo-2 promoted with no bound applied", elected)
	}
	if elected.TransactionsBehind != 60 {
		t.Errorf("TransactionsBehind = %d, want 60 reported even when unbounded", elected.TransactionsBehind)
	}
}

func TestDetectDivergedReplicasFlagsErrantTransactions(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testPrimary: "a:1-20",
			// behind the primary: a clean replica that can rejoin.
			testReplica2: "a:1-15",
			// a former primary with its own committed transactions (b:1-3) that the
			// new primary never saw: cannot safely rejoin.
			testReplica3: "a:1-15,b:1-3",
		},
	}
	diverged := (&controllerasync.Reconciler{}).Observe(topologyObservationInput(observed, mysqlFlavorCluster, nil, nil)).DivergedInstances
	if len(diverged) != 1 || diverged[0] != testReplica3 {
		t.Fatalf("diverged = %v, want [%s]", diverged, testReplica3)
	}
}

func TestDetectDivergedReplicasIgnoresUnknownGTID(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{testReplica2: "a:1-5"},
	}
	if diverged := (&controllerasync.Reconciler{}).Observe(topologyObservationInput(observed, mysqlFlavorCluster, nil, nil)).DivergedInstances; diverged != nil {
		t.Fatalf("diverged = %v, want nil when primary GTID is unknown", diverged)
	}
}

func TestDetectDivergedReplicasStaysStickyWhenPrimaryGTIDUnavailable(t *testing.T) {
	t.Parallel()
	// The primary that the replica diverged from is gone, so its GTID is no longer
	// reported. Divergence must survive: clearing it here would let the diverged
	// replica be elected primary at the worst possible moment.
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{testReplica2: "a:1-15,b:1-3"},
	}
	diverged := (&controllerasync.Reconciler{}).
		Observe(topologyObservationInput(observed, mysqlFlavorCluster, nil, []string{testReplica2})).DivergedInstances
	if len(diverged) != 1 || diverged[0] != testReplica2 {
		t.Fatalf("diverged = %v, want sticky [%s] when primary GTID is unavailable", diverged, testReplica2)
	}
}

func TestDetectDivergedReplicasClearsStickyFlagWhenReconverged(t *testing.T) {
	t.Parallel()
	// A previously diverged replica is now fully contained by a live primary, so
	// the sticky flag is positively proven stale and dropped.
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{
			testPrimary:  "a:1-20",
			testReplica2: "a:1-18",
		},
	}
	diverged := (&controllerasync.Reconciler{}).
		Observe(topologyObservationInput(observed, mysqlFlavorCluster, nil, []string{testReplica2})).DivergedInstances
	if diverged != nil {
		t.Fatalf("diverged = %v, want nil once re-convergence is proven against a live primary", diverged)
	}
}

func TestDetectDivergedReplicasDropsStickyFlagForRemovedInstance(t *testing.T) {
	t.Parallel()
	// A prior flag for an instance that is no longer part of the cluster (e.g.
	// scaled down) must not be carried forever once the primary is unavailable.
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{},
	}
	diverged := (&controllerasync.Reconciler{}).
		Observe(topologyObservationInput(observed, mysqlFlavorCluster, nil, []string{testReplica3})).DivergedInstances
	if diverged != nil {
		t.Fatalf("diverged = %v, want nil for an instance no longer in the cluster", diverged)
	}
}

func TestReconcilePrimaryChangeAbortsWhenTargetLagsPastMaxSwitchoverDelay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Spec.MaxSwitchoverDelay = 1
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.TargetPrimary = testReplica2
	// The switchover started well beyond maxSwitchoverDelay ago.
	cluster.Status.TargetPrimaryTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-time.Hour)))
	scheme := testScheme(t)
	pods := []*corev1.Pod{
		readyPod(cluster, testPrimary, rolePrimary),
		readyPod(cluster, testReplica2, roleReplica),
		readyPod(cluster, testReplica3, roleReplica),
	}
	control := &recordingControlClient{}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, pods[0], pods[1], pods[2]).
			Build(),
		Scheme:        scheme,
		ControlClient: control,
	}
	plan := testPlan()
	plan.Instances = 3
	observed := observedCluster{
		Plan:           plan,
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2, testReplica3},
		ReadyInstances: 3,
		// Target is behind the primary and cannot catch up.
		GTIDByInstance: map[string]string{testPrimary: testGTID, testReplica2: "uuid:1-5"},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary:  {InstanceName: testPrimary, Role: webserver.RolePrimary, IsReady: true, GTIDExecuted: testGTID},
			testReplica2: healthyReplicaStatus(testReplica2, "uuid:1-5"),
		},
	}

	switched, err := reconciler.reconcileSwitchover(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !switched {
		t.Fatal("aborted switchover should be reported as handled")
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.TargetPrimary != testPrimary {
		t.Fatalf("targetPrimary = %q, want reset to %q", gotCluster.Status.TargetPrimary, testPrimary)
	}
	if gotCluster.Status.Phase != topology.PhaseBlocked {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, topology.PhaseBlocked)
	}
}

func failoverCluster(t *testing.T, failoverDelay int32) (*mysqlv1alpha1.Cluster, *ClusterReconciler, *recordingControlClient) {
	t.Helper()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Spec.FailoverDelay = failoverDelay
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.TargetPrimary = testPrimary
	cluster.Status.CurrentPrimaryTimestamp = ptr.To(metav1.Now())
	scheme := testScheme(t)
	// The old primary Pod still exists (unreachable, but not yet deleted).
	oldPod := readyPod(cluster, testPrimary, rolePrimary)
	pod2 := readyPod(cluster, testReplica2, roleReplica)
	pod3 := readyPod(cluster, testReplica3, roleReplica)
	control := &recordingControlClient{}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, oldPod, pod2, pod3).
			Build(),
		Scheme:        scheme,
		ControlClient: control,
	}
	return cluster, reconciler, control
}

func unreachablePrimaryObserved() observedCluster {
	plan := testPlan()
	plan.Instances = 3
	return observedCluster{
		Plan:           plan,
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2, testReplica3},
		ReadyInstances: 2,
		GTIDByInstance: map[string]string{testReplica2: testGTID, testReplica3: "uuid:1-8"},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, testGTID),
			testReplica3: healthyReplicaStatus(testReplica3, "uuid:1-8"),
		},
	}
}

func TestReconcileFailoverPromotesBestCandidateImmediately(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, _ := failoverCluster(t, 0)
	observed := unreachablePrimaryObserved()

	handled, result, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("failover was not handled")
	}
	if result.RequeueAfter == 0 {
		t.Fatal("expected a requeue after failover")
	}
	// Old primary Pod is fenced (deleted) so it cannot accept writes on recovery.
	gotPod := &corev1.Pod{}
	err = reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: testPrimary}, gotPod)
	if err == nil && gotPod.DeletionTimestamp == nil {
		t.Fatal("old primary Pod was not fenced")
	} else if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	// The operator points targetPrimary at the candidate and clears the failing
	// marker; the candidate's in-Pod reconciler performs the actual promotion and
	// sets currentPrimary, so currentPrimary stays unchanged here.
	if gotCluster.Status.TargetPrimary != testReplica2 {
		t.Fatalf("targetPrimary = %q, want %q", gotCluster.Status.TargetPrimary, testReplica2)
	}
	if gotCluster.Status.PrimaryFailingSince != nil {
		t.Fatalf("primaryFailingSince = %v, want cleared", gotCluster.Status.PrimaryFailingSince)
	}
	if gotCluster.Status.Phase != topology.PhaseFailingOver {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, topology.PhaseFailingOver)
	}
}

func TestReconcileFailoverWaitsForFailoverDelay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, control := failoverCluster(t, 60)
	observed := unreachablePrimaryObserved()

	handled, result, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("failover was not handled")
	}
	if len(control.promoted) != 0 {
		t.Fatalf("must not promote before failoverDelay elapses, promoted=%v", control.promoted)
	}
	if result.RequeueAfter <= 0 || result.RequeueAfter > 60*time.Second {
		t.Fatalf("requeue = %s, want within failover delay", result.RequeueAfter)
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.PrimaryFailingSince == nil {
		t.Fatal("primaryFailingSince was not recorded")
	}
	if gotCluster.Status.Phase != topology.PhaseDegraded {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, topology.PhaseDegraded)
	}
}

func TestReconcileFailoverWaitsForActivePrimaryLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, _ := failoverCluster(t, 0)
	holder := testPrimary
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
	if err := reconciler.Create(ctx, lease); err != nil {
		t.Fatal(err)
	}
	observed := unreachablePrimaryObserved()

	handled, result, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("failover was not handled")
	}
	if result.RequeueAfter != 15*time.Second {
		t.Fatalf("requeue = %s, want %s", result.RequeueAfter, 15*time.Second)
	}
	gotPod := &corev1.Pod{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: testPrimary}, gotPod); err != nil {
		t.Fatal(err)
	}
	if gotPod.DeletionTimestamp != nil {
		t.Fatal("old primary Pod should not be deleted until the lease expires")
	}
}

func TestReconcileFailoverBlocksWithoutSafeCandidate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, control := failoverCluster(t, 0)
	observed := unreachablePrimaryObserved()
	// Both replicas diverged onto incomparable GTID sets.
	observed.GTIDByInstance[testReplica2] = testGTID
	observed.GTIDByInstance[testReplica3] = "other:1-4"
	observed.StatusByInstance[testReplica2] = healthyReplicaStatus(testReplica2, testGTID)
	observed.StatusByInstance[testReplica3] = healthyReplicaStatus(testReplica3, "other:1-4")

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("blocked failover must not own the reconcile pass (Handled=false) so the former primary Pod can be recreated by reconcileInstances")
	}
	if len(control.promoted) != 0 {
		t.Fatalf("must not promote when no safe candidate exists, promoted=%v", control.promoted)
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.Phase != topology.PhaseBlocked {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, topology.PhaseBlocked)
	}
	// The block must also survive the rest of the reconcile. A blocked failover
	// returns Handled=false so the pass carries on and can recreate the failed
	// primary's Pod, and that pass ends in patchStatus, which writes whatever phase
	// the observation holds. Unless the refusal is folded back into the
	// observation, patchStatus overwrites it with the Degraded that the broken
	// replication thread computes, and the reason the promotion was declined is
	// lost: the cluster ends up reporting the symptom while hiding the decision.
	if observed.Phase != topology.PhaseBlocked {
		t.Errorf("observed.Phase = %q, want %q: the block would be overwritten by patchStatus at the end of the pass",
			observed.Phase, topology.PhaseBlocked)
	}
	if observed.PhaseReason == "" || observed.Ready {
		t.Errorf("observed = {reason:%q ready:%v}, want the block's reason and not ready",
			observed.PhaseReason, observed.Ready)
	}
}

// TestReconcileFailoverYieldsToProvisioningBeforeAnyReplica covers the initial
// bootstrap deadlock: the primary briefly looks unreachable while replicas have
// not been created yet. With no replica to fail over to, failover must yield
// (handled=false) so reconcileInstances can create the replicas, rather than
// Blocking the cluster forever at "1/3 ready".
func TestReconcileFailoverYieldsToProvisioningBeforeAnyReplica(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, control := failoverCluster(t, 0)
	observed := unreachablePrimaryObserved()
	// No replica has been observed yet (still bootstrapping).
	observed.StatusByInstance = map[string]*webserver.Status{}
	observed.GTIDByInstance = map[string]string{}
	observed.ReadyInstances = 0

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("failover must not take over the reconcile before any replica exists")
	}
	if len(control.promoted) != 0 {
		t.Fatalf("nothing to promote during bootstrap, promoted=%v", control.promoted)
	}
	// The old primary Pod must not be fenced during bootstrap.
	gotPod := &corev1.Pod{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: testPrimary}, gotPod); err != nil {
		t.Fatal(err)
	}
	if gotPod.DeletionTimestamp != nil {
		t.Fatal("primary Pod must not be fenced while the cluster is still provisioning")
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.Phase == topology.PhaseBlocked {
		t.Fatalf("cluster must not be Blocked during bootstrap, phase=%q", gotCluster.Status.Phase)
	}
}

func TestReconcileFailoverClearsMarkerWhenPrimaryHealthy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, control := failoverCluster(t, 0)
	cluster.Status.PrimaryFailingSince = ptr.To(metav1.Now())
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := unreachablePrimaryObserved()
	observed.StatusByInstance[testPrimary] = &webserver.Status{
		InstanceName: testPrimary,
		Role:         webserver.RolePrimary,
		IsReady:      true,
		GTIDExecuted: testGTID,
	}

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("healthy primary must not trigger failover handling")
	}
	if len(control.promoted) != 0 {
		t.Fatalf("healthy primary must not be failed over, promoted=%v", control.promoted)
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.PrimaryFailingSince != nil {
		t.Fatalf("failing marker not cleared: %v", gotCluster.Status.PrimaryFailingSince)
	}
}

// laggingReplicaStatus is a healthy replica that reports a heartbeat reading:
// the newest stamp it has applied is heartbeatAge old.
func laggingReplicaStatus(gtid string, heartbeatAge time.Duration) *webserver.Status {
	status := healthyReplicaStatus(testReplica2, gtid)
	millis := heartbeatAge.Milliseconds()
	status.ReplicationLag = &webserver.ReplicationLagStatus{LagMillis: &millis}
	return status
}

// TestSelectFailoverCandidateSubtractsPrimaryDowntimeFromLag is the heart of the
// time bound. Once the primary stops stamping, nothing refreshes the heartbeat
// table, so every replica's reading climbs by one second per second. Here the
// replica is 8s behind the last stamp, but the primary has already been down for
// 5s, so only 3s of that is writes it actually missed. Reading the raw 8s would
// block a failover the bound allows.
func TestSelectFailoverCandidateSubtractsPrimaryDowntimeFromLag(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{testReplica2: "uuid:1-40"},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: laggingReplicaStatus("uuid:1-40", 8*time.Second),
		},
	}
	maxLag := 5 * time.Second

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:          topologyFailoverState(observed),
		GTID:              mysqlGTIDModel,
		ReferenceGTID:     "uuid:1-100",
		MaxReplicationLag: &maxLag,
		PrimaryDownFor:    5 * time.Second,
	})
	if elected.Name != testReplica2 {
		t.Fatalf("candidate = %q (reason %q), want demo-2: 8s behind less 5s of downtime is 3s, within a 5s bound",
			elected.Name, elected.Reason)
	}
	if elected.TimeBehind == nil || *elected.TimeBehind != 3*time.Second {
		t.Errorf("TimeBehind = %v, want 3s", elected.TimeBehind)
	}
}

// TestSelectFailoverCandidateBlocksWhenWritesBehindExceedsBound proves the time
// bound refuses the election in its own right, even when the transaction bound
// is unset. The replica missed transactions and was 30s of writes behind when
// the primary died, well past a 5s objective.
func TestSelectFailoverCandidateBlocksWhenWritesBehindExceedsBound(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{testReplica2: "uuid:1-40"},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: laggingReplicaStatus("uuid:1-40", 32*time.Second),
		},
	}
	maxLag := 5 * time.Second

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:          topologyFailoverState(observed),
		GTID:              mysqlGTIDModel,
		ReferenceGTID:     "uuid:1-100",
		MaxReplicationLag: &maxLag,
		PrimaryDownFor:    2 * time.Second,
	})
	if elected.Name != "" {
		t.Fatalf("candidate = %q, want no election: the replica is 30s of writes behind a 5s bound", elected.Name)
	}
	for _, want := range []string{testReplica2, "30s of writes behind", "maxReplicationLag (5s)"} {
		if !strings.Contains(elected.Reason, want) {
			t.Errorf("Reason = %q, want it to mention %q", elected.Reason, want)
		}
	}
}

// TestSelectFailoverCandidateIgnoresLagOfAReplicaThatMissedNothing proves the
// time bound does not block a replica that holds every transaction the primary
// committed. Such a replica loses nothing by being promoted however far its
// applier trails: draining the relay log costs time, not data. Its heartbeat
// reading here is far outside the bound, and it must still be elected.
func TestSelectFailoverCandidateIgnoresLagOfAReplicaThatMissedNothing(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{testReplica2: "uuid:1-100"},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: laggingReplicaStatus("uuid:1-100", 10*time.Minute),
		},
	}
	maxLag := 5 * time.Second

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:          topologyFailoverState(observed),
		GTID:              mysqlGTIDModel,
		ReferenceGTID:     "uuid:1-100",
		MaxReplicationLag: &maxLag,
	})
	if elected.Name != testReplica2 {
		t.Fatalf("candidate = %q (reason %q), want demo-2: it missed no transactions, so it loses no writes",
			elected.Name, elected.Reason)
	}
	if elected.TimeBehind == nil || *elected.TimeBehind != 0 {
		t.Errorf("TimeBehind = %v, want 0", elected.TimeBehind)
	}
}

// TestSelectFailoverCandidateBlocksWithoutAHeartbeatReading proves a bound that
// cannot be measured is not quietly ignored. The bound is a promise about how
// much data a promotion may destroy; a replica that missed transactions and
// cannot say how far behind it was cannot be shown to keep it.
func TestSelectFailoverCandidateBlocksWithoutAHeartbeatReading(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{testReplica2: "uuid:1-40"},
		StatusByInstance: map[string]*webserver.Status{
			// No ReplicationLag: the heartbeat is off, or has never been read.
			testReplica2: healthyReplicaStatus(testReplica2, "uuid:1-40"),
		},
	}
	maxLag := 5 * time.Second

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:          topologyFailoverState(observed),
		GTID:              mysqlGTIDModel,
		ReferenceGTID:     "uuid:1-100",
		MaxReplicationLag: &maxLag,
	})
	if elected.Name != "" {
		t.Fatalf("candidate = %q, want no election: no replica reported a heartbeat", elected.Name)
	}
	if !strings.Contains(elected.Reason, "no replica reported a replication-lag heartbeat") {
		t.Errorf("Reason = %q, want it to say no heartbeat was reported", elected.Reason)
	}
}

// TestSelectFailoverCandidateWithoutLagBoundIgnoresHeartbeat proves a cluster
// that sets no time bound is unaffected by the heartbeat, however alarming the
// reading. This is the default, and it must keep promoting the best replica.
func TestSelectFailoverCandidateWithoutLagBoundIgnoresHeartbeat(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:    testPrimary,
		InstanceNames:  []string{testPrimary, testReplica2},
		GTIDByInstance: map[string]string{testReplica2: "uuid:1-40"},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: laggingReplicaStatus("uuid:1-40", time.Hour),
		},
	}

	elected := controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:      topologyFailoverState(observed),
		GTID:          mysqlGTIDModel,
		ReferenceGTID: "uuid:1-100",
	})
	if elected.Name != testReplica2 {
		t.Fatalf("candidate = %q (reason %q), want demo-2 with no bound set", elected.Name, elected.Reason)
	}
}
