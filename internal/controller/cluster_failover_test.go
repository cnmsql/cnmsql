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

package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
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
	got, reason := selectFailoverCandidate(observed)
	if got != testReplica3 {
		t.Fatalf("candidate = %q (reason %q), want demo-3", got, reason)
	}

	// Equal GTID: lowest ordinal wins.
	observed.GTIDByInstance[testReplica2] = testGTID
	observed.StatusByInstance[testReplica2] = healthyReplicaStatus(testReplica2, testGTID)
	if got, _ := selectFailoverCandidate(observed); got != testReplica2 {
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
	got, reason := selectFailoverCandidate(observed)
	if got != "" {
		t.Fatalf("candidate = %q, want empty (blocked)", got)
	}
	if reason == "" {
		t.Fatal("expected a block reason")
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
	if got, _ := selectFailoverCandidate(observed); got != testReplica3 {
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

	got, reason := selectFailoverCandidate(observed)
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

	got, reason := selectFailoverCandidate(observed)
	if got != "" {
		t.Fatalf("candidate = %q (reason %q), want empty without GTID status", got, reason)
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
	diverged := detectDivergedReplicas(observed)
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
	if diverged := detectDivergedReplicas(observed); diverged != nil {
		t.Fatalf("diverged = %v, want nil when primary GTID is unknown", diverged)
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
	cluster.Status.TargetPrimaryTimestamp = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
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

	switched, err := reconciler.reconcileSwitchover(ctx, cluster, plan, observed)
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
	if gotCluster.Status.Phase != phaseBlocked {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, phaseBlocked)
	}
}

func failoverCluster(t *testing.T, failoverDelay int32) (*mysqlv1alpha1.Cluster, *ClusterReconciler, *recordingControlClient) {
	t.Helper()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Spec.FailoverDelay = failoverDelay
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.TargetPrimary = testPrimary
	cluster.Status.CurrentPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
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

	handled, result, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, observed)
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
	if gotCluster.Status.PrimaryFailingSince != "" {
		t.Fatalf("primaryFailingSince = %q, want cleared", gotCluster.Status.PrimaryFailingSince)
	}
	if gotCluster.Status.Phase != phaseFailingOver {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, phaseFailingOver)
	}
}

func TestReconcileFailoverWaitsForFailoverDelay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, control := failoverCluster(t, 60)
	observed := unreachablePrimaryObserved()

	handled, result, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, observed)
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
	if gotCluster.Status.PrimaryFailingSince == "" {
		t.Fatal("primaryFailingSince was not recorded")
	}
	if gotCluster.Status.Phase != phaseDegraded {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, phaseDegraded)
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

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("failover was not handled")
	}
	if len(control.promoted) != 0 {
		t.Fatalf("must not promote when no safe candidate exists, promoted=%v", control.promoted)
	}
	gotCluster := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, gotCluster); err != nil {
		t.Fatal(err)
	}
	if gotCluster.Status.Phase != phaseBlocked {
		t.Fatalf("phase = %q, want %q", gotCluster.Status.Phase, phaseBlocked)
	}
}

func TestReconcileFailoverClearsMarkerWhenPrimaryHealthy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, control := failoverCluster(t, 0)
	cluster.Status.PrimaryFailingSince = metav1.Now().Format(time.RFC3339)
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

	handled, _, err := reconciler.reconcileFailover(ctx, cluster, observed.Plan, observed)
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
	if gotCluster.Status.PrimaryFailingSince != "" {
		t.Fatalf("failing marker not cleared: %q", gotCluster.Status.PrimaryFailingSince)
	}
}
