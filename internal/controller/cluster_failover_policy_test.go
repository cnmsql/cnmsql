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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	controllerasync "github.com/cnmsql/cnmsql/internal/controller/async"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// selectPreferred elects with a preference and no promotion bound.
func selectPreferred(observed topology.FailoverState, preferred []string) string {
	return controllerasync.SelectFailoverCandidate(controllerasync.Election{
		Observed:  observed,
		GTID:      mysqlGTIDModel,
		Preferred: preferred,
	}).Name
}

// policyCluster is a three-instance failover cluster with a failover policy in
// place. The policy goes through Update rather than being set on the struct: the
// fake client refreshes the object it is handed, so a spec that was never stored
// is wiped by the next status write.
func policyCluster(t *testing.T, policy *mysqlv1alpha1.FailoverPolicy) (*mysqlv1alpha1.Cluster, *ClusterReconciler) {
	t.Helper()
	cluster, reconciler, _ := failoverCluster(t, 0)
	cluster.Spec.FailoverPolicy = policy
	if err := reconciler.Update(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	return cluster, reconciler
}

func TestSelectFailoverCandidatePromotesThePreferredReplicaOnEqualGTID(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testReplica2: testGTID,
			testReplica3: testGTID,
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, testGTID),
			testReplica3: healthyReplicaStatus(testReplica3, testGTID),
		},
	}
	// Ordinal order would elect demo-2; the preference reaches past it.
	if got := selectPreferred(topologyFailoverState(observed), []string{testReplica3}); got != testReplica3 {
		t.Fatalf("candidate = %q, want %q", got, testReplica3)
	}
	// With no preference the tie still resolves on ordinal.
	if got := selectPreferred(topologyFailoverState(observed), nil); got != testReplica2 {
		t.Fatalf("candidate = %q, want %q", got, testReplica2)
	}
}

// The preference orders candidates, it does not excuse them from the safety
// rules: a preferred replica that is missing transactions another replica holds
// cannot be proven safe, and is passed over for the one that can.
func TestSelectFailoverCandidateDoesNotPromoteAPreferredReplicaThatIsBehind(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		GTIDByInstance: map[string]string{
			testReplica2: testGTID,
			testReplica3: "uuid:1-8",
		},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: healthyReplicaStatus(testReplica2, testGTID),
			testReplica3: healthyReplicaStatus(testReplica3, "uuid:1-8"),
		},
	}
	if got := selectPreferred(topologyFailoverState(observed), []string{testReplica3}); got != testReplica2 {
		t.Fatalf("candidate = %q, want the complete replica %q despite the preference", got, testReplica2)
	}
}

func TestReconcileFailoverBlocksWithinTheFailoverCooldown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler := policyCluster(t, &mysqlv1alpha1.FailoverPolicy{
		MinTimeBetweenFailovers: &metav1.Duration{Duration: 10 * time.Minute},
	})
	// The previous failover promoted this primary three minutes ago, and it has
	// been healthy ever since. With no stability window it settled on promotion, so
	// the cooldown has seven minutes left to run.
	cluster.Status.LastFailoverTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-3 * time.Minute)))
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := unreachablePrimaryObserved()

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	// Not handled: the pass must go on to recreate the failed primary's Pod, which
	// is how an intermittently failing primary recovers in place.
	if handled {
		t.Fatal("a failover inside the cooldown must not be handled, so the Pod can be recreated")
	}
	if observed.Phase != topology.PhaseBlocked {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhaseBlocked)
	}
	if !strings.Contains(observed.PhaseReason, "minTimeBetweenFailovers") {
		t.Fatalf("phase reason = %q, want it to name the bound", observed.PhaseReason)
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.TargetPrimary != testPrimary {
		t.Fatalf("targetPrimary = %q, want it left on the failed primary %q", gotCluster.Status.TargetPrimary, testPrimary)
	}
}

func TestReconcileFailoverProceedsOnceTheCooldownHasElapsed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler := policyCluster(t, &mysqlv1alpha1.FailoverPolicy{
		MinTimeBetweenFailovers: &metav1.Duration{Duration: time.Minute},
	})
	cluster.Status.LastFailoverTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-time.Hour)))
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := unreachablePrimaryObserved()

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("failover was not handled")
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.TargetPrimary != testReplica2 {
		t.Fatalf("targetPrimary = %q, want %q", gotCluster.Status.TargetPrimary, testReplica2)
	}
	// The promotion restarts the cooldown and hands the new primary an unproven
	// health record to rebuild.
	if gotCluster.Status.LastFailoverTimestamp.Time.Before(time.Now().Add(-time.Minute)) {
		t.Fatalf("lastFailoverTimestamp = %v, want it restamped at the promotion", gotCluster.Status.LastFailoverTimestamp)
	}
	if gotCluster.Status.PrimaryHealthySince != nil {
		t.Fatalf("primaryHealthySince = %v, want it cleared for the promoted primary", gotCluster.Status.PrimaryHealthySince)
	}
}

// A primary that keeps dropping out never accumulates an unbroken healthy
// stretch, so it never settles and the cooldown that runs from the settling point
// never starts: the operator declines to replace it, however long ago the last
// failover was.
func TestReconcileFailoverBlocksWhileTheFlappingPrimaryHasNotSettled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler := policyCluster(t, &mysqlv1alpha1.FailoverPolicy{
		MinTimeBetweenFailovers: &metav1.Duration{Duration: time.Minute},
		PrimaryStabilityWindow:  &metav1.Duration{Duration: 10 * time.Minute},
	})
	// The failover that promoted this primary is long past, so the plain cooldown
	// would have expired hours ago...
	cluster.Status.LastFailoverTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-time.Hour)))
	// ...but the primary dropped out and came back a minute ago, restarting the
	// healthy stretch it has to complete before it counts as settled.
	cluster.Status.PrimaryHealthySince = ptr.To(metav1.NewTime(time.Now().Add(-time.Minute)))
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := unreachablePrimaryObserved()

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("a failover away from an unsettled primary must not be handled")
	}
	if observed.Phase != topology.PhaseBlocked {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhaseBlocked)
	}
}

// The stability window must not trap a cluster whose promoted primary never came
// up at all: with no healthy stretch to measure, the plain cooldown decides, and
// the operator is free to replace it once that expires.
func TestReconcileFailoverPromotesWhenThePromotedPrimaryNeverBecameHealthy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler := policyCluster(t, &mysqlv1alpha1.FailoverPolicy{
		MinTimeBetweenFailovers: &metav1.Duration{Duration: time.Minute},
		PrimaryStabilityWindow:  &metav1.Duration{Duration: 10 * time.Minute},
	})
	cluster.Status.LastFailoverTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-time.Hour)))
	cluster.Status.PrimaryHealthySince = nil
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := unreachablePrimaryObserved()

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("failover was not handled")
	}
}

func TestReconcileFailoverStampsPrimaryHealthySince(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, _ := failoverCluster(t, 0)
	observed := healthyPrimaryObserved()

	if _, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed); err != nil {
		t.Fatal(err)
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.PrimaryHealthySince == nil {
		t.Fatal("primaryHealthySince was not stamped for a healthy primary")
	}
}

// The stamp moves forward when the primary recovers: the stretch that matters is
// the current unbroken one, not the one it had before it dropped out.
func TestReconcileFailoverRestampsPrimaryHealthySinceOnRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, _ := failoverCluster(t, 0)
	stale := metav1.NewTime(time.Now().Add(-time.Hour))
	cluster.Status.PrimaryHealthySince = ptr.To(stale)
	cluster.Status.PrimaryFailingSince = ptr.To(metav1.NewTime(time.Now().Add(-time.Minute)))
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := healthyPrimaryObserved()

	if _, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, &observed); err != nil {
		t.Fatal(err)
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.PrimaryFailingSince != nil {
		t.Fatalf("primaryFailingSince = %v, want cleared", gotCluster.Status.PrimaryFailingSince)
	}
	if !gotCluster.Status.PrimaryHealthySince.After(stale.Time) {
		t.Fatalf("primaryHealthySince = %v, want it moved past the pre-failure stretch (%v)",
			gotCluster.Status.PrimaryHealthySince, stale)
	}
}

func TestReconcilePreferredPrimaryRequestsSwitchoverBackToThePreferredInstance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// A failover left the primary on demo-2, but the cluster asks for demo-3.
	cluster, reconciler := policyCluster(t, &mysqlv1alpha1.FailoverPolicy{
		PreferredPrimary: []string{testReplica3},
	})
	cluster.Status.CurrentPrimary = testReplica2
	cluster.Status.TargetPrimary = testReplica2
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := healthyClusterObserved(testReplica2)

	requested, err := reconciler.reconcilePreferredPrimary(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !requested {
		t.Fatal("no switchover to the preferred primary was requested")
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.TargetPrimary != testReplica3 {
		t.Fatalf("targetPrimary = %q, want %q", gotCluster.Status.TargetPrimary, testReplica3)
	}
	if gotCluster.Status.Phase != topology.PhaseSwitchover {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, topology.PhaseSwitchover)
	}
	// A failback is a switchover, not a failover: it must not restart the
	// anti-flapping cooldown.
	if gotCluster.Status.LastFailoverTimestamp != nil {
		t.Fatalf("lastFailoverTimestamp = %v, want a failback to leave it alone", gotCluster.Status.LastFailoverTimestamp)
	}
}

func TestReconcilePreferredPrimaryLeavesThePreferredPrimaryInPlace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler := policyCluster(t, &mysqlv1alpha1.FailoverPolicy{
		PreferredPrimary: []string{testPrimary, testReplica3},
	})
	observed := healthyClusterObserved(testPrimary)

	requested, err := reconciler.reconcilePreferredPrimary(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if requested {
		t.Fatal("the primary is already the most preferred instance; nothing should move")
	}
}

// A preferred instance that is not fit to take the role is not switched to. The
// failback waits for it rather than handing the primary to a replica whose
// replication is broken.
func TestReconcilePreferredPrimarySkipsAnUnfitPreferredInstance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler := policyCluster(t, &mysqlv1alpha1.FailoverPolicy{
		PreferredPrimary: []string{testReplica3},
	})
	cluster.Status.CurrentPrimary = testReplica2
	cluster.Status.TargetPrimary = testReplica2
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := healthyClusterObserved(testReplica2)
	broken := healthyReplicaStatus(testReplica3, testGTID)
	broken.Replication.SQLRunning = false
	observed.StatusByInstance[testReplica3] = broken

	requested, err := reconciler.reconcilePreferredPrimary(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if requested {
		t.Fatal("switched over to a preferred instance whose replication is broken")
	}
}

// The failback is an automatic promotion, so it waits out the anti-flapping
// cooldown like any other. Without this a preferred instance that keeps dying
// would drag the primary back onto itself every time it briefly returned.
func TestReconcilePreferredPrimaryWaitsForTheFailoverCooldown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler := policyCluster(t, &mysqlv1alpha1.FailoverPolicy{
		PreferredPrimary:        []string{testReplica3},
		MinTimeBetweenFailovers: &metav1.Duration{Duration: 10 * time.Minute},
	})
	cluster.Status.CurrentPrimary = testReplica2
	cluster.Status.TargetPrimary = testReplica2
	cluster.Status.LastFailoverTimestamp = ptr.To(metav1.NewTime(time.Now().Add(-time.Minute)))
	if err := reconciler.Status().Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	observed := healthyClusterObserved(testReplica2)

	requested, err := reconciler.reconcilePreferredPrimary(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if requested {
		t.Fatal("failed back inside the cooldown that follows the failover")
	}
}

// healthyPrimaryObserved is the cluster as it looks when nothing is wrong: the
// expected primary is up and acting as primary.
func healthyPrimaryObserved() observedCluster {
	observed := healthyClusterObserved(testPrimary)
	observed.ReadyInstances = 3
	return observed
}

// healthyClusterObserved is a three-instance cluster in which primaryName holds
// the primary role and the other two are caught-up replicas.
func healthyClusterObserved(primaryName string) observedCluster {
	plan := testPlan()
	plan.Instances = 3
	names := []string{testPrimary, testReplica2, testReplica3}
	statuses := make(map[string]*webserver.Status, len(names))
	gtids := make(map[string]string, len(names))
	for _, name := range names {
		gtids[name] = testGTID
		if name == primaryName {
			statuses[name] = &webserver.Status{
				InstanceName: name,
				Role:         webserver.RolePrimary,
				IsReady:      true,
				GTIDExecuted: testGTID,
			}
			continue
		}
		statuses[name] = healthyReplicaStatus(name, testGTID)
	}
	return observedCluster{
		Plan:             plan,
		PrimaryName:      primaryName,
		InstanceNames:    names,
		ReadyInstances:   3,
		GTIDByInstance:   gtids,
		StatusByInstance: statuses,
	}
}
