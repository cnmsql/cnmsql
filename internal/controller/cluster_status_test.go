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
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	controllergr "github.com/cnmsql/cnmsql/internal/controller/groupreplication"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// partitionedControlClient simulates an instance the operator cannot reach (e.g.
// behind a NetworkPolicy): Status returns an error for the named instances and
// behaves like the recording client otherwise.
type partitionedControlClient struct {
	*recordingControlClient
	unreachable map[string]bool
}

func (c *partitionedControlClient) Status(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	instanceName string,
) (*webserver.Status, error) {
	if c.unreachable[instanceName] {
		return nil, errors.New("unreachable")
	}
	return c.recordingControlClient.Status(ctx, cluster, instanceName)
}

func TestClusterEstablished(t *testing.T) {
	t.Parallel()
	// Establishment is the sticky EstablishedAt marker, not the live phase: a
	// cluster that was once ready stays established even after its phase is
	// re-stamped back to Provisioning by an intermediate reconcile step.
	notEstablished := &mysqlv1alpha1.Cluster{}
	notEstablished.Status.Phase = topology.PhaseReady // phase alone must not count
	if notEstablished.IsEstablished() {
		t.Error("clusterEstablished with no EstablishedAt = true, want false")
	}
	established := &mysqlv1alpha1.Cluster{}
	established.Status.Phase = topology.PhaseProvisioning // phase says provisioning...
	established.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	if !established.IsEstablished() {
		t.Error("clusterEstablished with EstablishedAt set = false, want true")
	}
}

func TestEstablishedPhase(t *testing.T) {
	t.Parallel()
	tests := map[string]bool{
		"":                         false,
		topology.PhasePending:      false,
		topology.PhaseProvisioning: false,
		topology.PhaseReady:        true,
		topology.PhaseDegraded:     true,
		topology.PhaseSwitchover:   true,
		topology.PhaseFailingOver:  true,
		topology.PhaseBlocked:      true,
	}
	for phase, want := range tests {
		if got := establishedPhase(phase); got != want {
			t.Errorf("establishedPhase(%q) = %t, want %t", phase, got, want)
		}
	}
}

func TestUnreadyInstanceNames(t *testing.T) {
	t.Parallel()
	observed := observedCluster{
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary:  {InstanceName: testPrimary, IsReady: true},
			testReplica2: {InstanceName: testReplica2, IsReady: false}, // reachable, not ready
			// testReplica3 missing entirely: unreachable
		},
	}
	got := unreadyInstanceNames(observed)
	want := []string{testReplica2, testReplica3}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unreadyInstanceNames = %v, want %v", got, want)
	}
}

// observePartitionedReplica builds a two-instance cluster whose primary is ready
// and whose replica Pod is Ready to Kubernetes but unreachable to the operator,
// then observes it with the cluster carrying the given previously-persisted phase.
func observePartitionedReplica(t *testing.T, previousPhase string) observedCluster {
	t.Helper()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.Phase = previousPhase
	// Mirror what patchStatus would have persisted: an operational previous phase
	// means the cluster was established at least once.
	if establishedPhase(previousPhase) {
		cluster.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	}
	scheme := testScheme(t)

	// Both Pods are Ready from Kubernetes' point of view (a NetworkPolicy does
	// not block kubelet probes), but the operator cannot reach the replica.
	primaryPod := readyPod(cluster, testPrimary, rolePrimary)
	replicaPod := readyPod(cluster, testReplica2, roleReplica)
	control := &partitionedControlClient{
		recordingControlClient: &recordingControlClient{
			statuses: map[string]*webserver.Status{
				testPrimary: {
					InstanceName: testPrimary,
					Role:         webserver.RolePrimary,
					IsReady:      true,
					GTIDExecuted: testGTID,
				},
			},
		},
		unreachable: map[string]bool{testReplica2: true},
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, primaryPod, replicaPod).
			Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	plan := testPlan()
	plan.Instances = 2
	observed, err := reconciler.observe(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if observed.ReadyInstances != 1 {
		t.Fatalf("readyInstances = %d, want 1", observed.ReadyInstances)
	}
	return observed
}

func TestObserveEstablishedClusterDegradesWhenInstanceUnreachable(t *testing.T) {
	t.Parallel()
	observed := observePartitionedReplica(t, topology.PhaseReady)
	if observed.Phase != topology.PhaseDegraded {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhaseDegraded)
	}
	if !strings.Contains(observed.PhaseReason, testReplica2) {
		t.Fatalf("phaseReason = %q, want it to name the unreachable instance %q", observed.PhaseReason, testReplica2)
	}
}

func TestObserveEstablishedClusterDegradesOnTotalOutage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 1
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.Phase = topology.PhaseReady
	cluster.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	scheme := testScheme(t)

	// The sole instance's Pod still exists but the operator cannot reach it.
	primaryPod := readyPod(cluster, testPrimary, rolePrimary)
	control := &partitionedControlClient{
		recordingControlClient: &recordingControlClient{statuses: map[string]*webserver.Status{}},
		unreachable:            map[string]bool{testPrimary: true},
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, primaryPod).
			Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	plan := testPlan()
	plan.Instances = 1
	observed, err := reconciler.observe(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if observed.ReadyInstances != 0 {
		t.Fatalf("readyInstances = %d, want 0", observed.ReadyInstances)
	}
	// A fully-down established cluster must read Degraded, not "Pending: waiting
	// for the primary instance" (which implies it is still being provisioned).
	if observed.Phase != topology.PhaseDegraded {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhaseDegraded)
	}
	if !strings.Contains(observed.PhaseReason, testPrimary) {
		t.Fatalf("phaseReason = %q, want it to name the unreachable instance %q", observed.PhaseReason, testPrimary)
	}
}

func TestObserveBootstrappingClusterStaysPending(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 1
	scheme := testScheme(t)

	// Initial bootstrap: the primary Pod is not Ready yet and the cluster has no
	// prior phase. This must stay Pending, not Degraded.
	primaryPod := readyPod(cluster, testPrimary, rolePrimary)
	primaryPod.Status.Conditions = nil
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, primaryPod).
			Build(),
		Scheme: scheme,
		ControlClient: &partitionedControlClient{
			recordingControlClient: &recordingControlClient{statuses: map[string]*webserver.Status{}},
			unreachable:            map[string]bool{testPrimary: true},
		},
	}

	plan := testPlan()
	plan.Instances = 1
	observed, err := reconciler.observe(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Phase != topology.PhasePending {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhasePending)
	}
}

func TestObserveProvisioningClusterStaysProvisioning(t *testing.T) {
	t.Parallel()
	// A cluster still completing initial provisioning must not be reported as
	// Degraded just because not every instance is ready yet.
	observed := observePartitionedReplica(t, topology.PhaseProvisioning)
	if observed.Phase != topology.PhaseProvisioning {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhaseProvisioning)
	}
}

// crashLoopPod builds an instance Pod whose container is stuck in
// CrashLoopBackOff past the restart threshold: it never became Ready.
func crashLoopPod(cluster *mysqlv1alpha1.Cluster, name, role string) *corev1.Pod {
	pod := readyPod(cluster, name, role)
	pod.Status.Conditions = nil
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "mysql",
		RestartCount: crashLoopRestartThreshold,
		State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
		},
	}}
	return pod
}

func TestObserveCrashloopingInstanceDegradesBeforeEstablished(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.CurrentPrimary = testPrimary
	// Never established: the cluster is still in its initial provisioning phase
	// and EstablishedAt is unset. A crashlooping instance must still surface as
	// Degraded rather than sitting silently in Provisioning.
	cluster.Status.Phase = topology.PhaseProvisioning
	scheme := testScheme(t)

	primaryPod := readyPod(cluster, testPrimary, rolePrimary)
	replicaPod := crashLoopPod(cluster, testReplica2, roleReplica)
	control := &recordingControlClient{
		statuses: map[string]*webserver.Status{
			testPrimary: {
				InstanceName: testPrimary,
				Role:         webserver.RolePrimary,
				IsReady:      true,
				GTIDExecuted: testGTID,
			},
		},
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, primaryPod, replicaPod).
			Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	plan := testPlan()
	plan.Instances = 2
	observed, err := reconciler.observe(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.FailedInstances) != 1 || observed.FailedInstances[0] != testReplica2 {
		t.Fatalf("failedInstances = %v, want [%s]", observed.FailedInstances, testReplica2)
	}
	if observed.Phase != topology.PhaseDegraded {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhaseDegraded)
	}
	if !strings.Contains(observed.PhaseReason, testReplica2) {
		t.Fatalf("phaseReason = %q, want it to name the failing instance %q", observed.PhaseReason, testReplica2)
	}
}

// runningUnreadyPod builds an instance Pod that has started (Running phase) but
// whose readiness probe is failing — the state of a replica whose replication
// thread has aborted: mysqld is up, but it is not Ready. The operator must still
// poll its control endpoint to learn why.
func runningUnreadyPod(cluster *mysqlv1alpha1.Cluster, name, role string) *corev1.Pod {
	pod := readyPod(cluster, name, role)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionFalse,
	}}
	return pod
}

func TestObserveBrokenReplicationDegradesBeforeEstablished(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.CurrentPrimary = testPrimary
	// Never established: still in initial provisioning. A replica whose replication
	// has aborted with an error is positive evidence of a fault and must surface as
	// Degraded rather than sitting silently in Provisioning.
	cluster.Status.Phase = topology.PhaseProvisioning
	scheme := testScheme(t)

	primaryPod := readyPod(cluster, testPrimary, rolePrimary)
	// The replica is Running but not Ready: its SQL thread stopped on a duplicate
	// key. Its GTID set is not diverged from the primary, so only the SQL-layer
	// signal reveals the fault.
	replicaPod := runningUnreadyPod(cluster, testReplica2, roleReplica)
	control := &recordingControlClient{
		statuses: map[string]*webserver.Status{
			testPrimary: {
				InstanceName: testPrimary,
				Role:         webserver.RolePrimary,
				IsReady:      true,
				GTIDExecuted: testGTID,
			},
			testReplica2: {
				InstanceName: testReplica2,
				Role:         webserver.RoleReplica,
				IsReady:      false,
				GTIDExecuted: testGTID,
				Replication: &webserver.ReplicationStatus{
					SourceHost: testPrimary,
					IORunning:  true,
					SQLRunning: false,
					LastError:  "Error 1062: Duplicate entry '1' for key 'PRIMARY'",
				},
			},
		},
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, primaryPod, replicaPod).
			Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	plan := testPlan()
	plan.Instances = 2
	observed, err := reconciler.observe(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.ReplicationBrokenInstances) != 1 || observed.ReplicationBrokenInstances[0] != testReplica2 {
		t.Fatalf("replicationBrokenInstances = %v, want [%s]", observed.ReplicationBrokenInstances, testReplica2)
	}
	if len(observed.DivergedInstances) != 0 {
		t.Fatalf("divergedInstances = %v, want none (GTID not diverged)", observed.DivergedInstances)
	}
	if observed.Phase != topology.PhaseDegraded {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhaseDegraded)
	}
	if !strings.Contains(observed.PhaseReason, testReplica2) || !strings.Contains(observed.PhaseReason, "1062") {
		t.Fatalf("phaseReason = %q, want it to name %q and the replication error", observed.PhaseReason, testReplica2)
	}
}

func TestObserveDivergedReplicaDetectedWhenNotReady(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.Phase = topology.PhaseProvisioning
	scheme := testScheme(t)

	primaryPod := readyPod(cluster, testPrimary, rolePrimary)
	// The diverged replica is Running but not Ready (its SQL thread aborted). Before
	// the operator polled Running-but-unready Pods it went blind on exactly this
	// instance and could never compare its GTID; now it must detect the divergence.
	replicaPod := runningUnreadyPod(cluster, testReplica2, roleReplica)
	control := &recordingControlClient{
		statuses: map[string]*webserver.Status{
			testPrimary: {
				InstanceName: testPrimary,
				Role:         webserver.RolePrimary,
				IsReady:      true,
				GTIDExecuted: "a:1-20",
			},
			testReplica2: {
				InstanceName: testReplica2,
				Role:         webserver.RoleReplica,
				IsReady:      false,
				// Errant transaction b:1-3 the primary never saw.
				GTIDExecuted: "a:1-15,b:1-3",
			},
		},
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, primaryPod, replicaPod).
			Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	plan := testPlan()
	plan.Instances = 2
	observed, err := reconciler.observe(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.DivergedInstances) != 1 || observed.DivergedInstances[0] != testReplica2 {
		t.Fatalf("divergedInstances = %v, want [%s]", observed.DivergedInstances, testReplica2)
	}
	if observed.Phase != topology.PhaseDegraded {
		t.Fatalf("phase = %q, want %q", observed.Phase, topology.PhaseDegraded)
	}
}

func TestPatchStatusEstablishedAtIsSticky(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 1
	scheme := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster).
		Build()
	reconciler := &ClusterReconciler{Client: c, Scheme: scheme}
	plan := testPlan()
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}

	// 1) The cluster becomes fully ready: EstablishedAt is recorded.
	if err := reconciler.patchStatus(ctx, cluster, observedCluster{
		Plan:           plan,
		InstanceNames:  []string{testPrimary},
		Phase:          topology.PhaseReady,
		Ready:          true,
		ReadyInstances: 1,
	}); err != nil {
		t.Fatal(err)
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.EstablishedAt == nil {
		t.Fatal("EstablishedAt not set after the cluster first became Ready")
	}
	first := got.Status.EstablishedAt.DeepCopy()

	// 2) An intermediate step re-stamps the phase to Provisioning. EstablishedAt
	// must survive, so the cluster is still considered established.
	if err := reconciler.patchStatus(ctx, got, observedCluster{
		Plan:           plan,
		InstanceNames:  []string{testPrimary},
		Phase:          topology.PhaseProvisioning,
		Ready:          false,
		ReadyInstances: 0,
	}); err != nil {
		t.Fatal(err)
	}
	got2 := &mysqlv1alpha1.Cluster{}
	if err := c.Get(ctx, key, got2); err != nil {
		t.Fatal(err)
	}
	if got2.Status.EstablishedAt == nil {
		t.Fatal("EstablishedAt was erased by a later Provisioning patch")
	}
	if !got2.Status.EstablishedAt.Equal(first) {
		t.Fatalf("EstablishedAt changed: was %v, now %v", first, got2.Status.EstablishedAt)
	}
	if !got2.IsEstablished() {
		t.Fatal("cluster no longer reports established after a Provisioning re-stamp")
	}
}

func storageStatus(instance string, usedPercent int) *webserver.Status {
	const capacity = 100 << 30
	return &webserver.Status{
		InstanceName: instance,
		IsReady:      true,
		Storage: &webserver.StorageStatus{
			CapacityBytes:  capacity,
			UsedBytes:      int64(capacity) * int64(usedPercent) / 100,
			AvailableBytes: int64(capacity) * int64(100-usedPercent) / 100,
		},
	}
}

func TestEvaluateStoragePressure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		statuses     map[string]*webserver.Status
		wantObserved bool
		wantPressure bool
		wantContains string
	}{
		{
			name: "no instance reports storage",
			statuses: map[string]*webserver.Status{
				testPrimary: {InstanceName: testPrimary, IsReady: true},
			},
		},
		{
			name: "all below threshold",
			statuses: map[string]*webserver.Status{
				testPrimary:  storageStatus(testPrimary, 50),
				testReplica2: storageStatus(testReplica2, 84),
			},
			wantObserved: true,
		},
		{
			name: "one at threshold",
			statuses: map[string]*webserver.Status{
				testPrimary:  storageStatus(testPrimary, 85),
				testReplica2: storageStatus(testReplica2, 50),
			},
			wantObserved: true,
			wantPressure: true,
			wantContains: testPrimary,
		},
		{
			name: "ignores zero-capacity volume",
			statuses: map[string]*webserver.Status{
				testPrimary: {InstanceName: testPrimary, Storage: &webserver.StorageStatus{}},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observed := observedCluster{
				InstanceNames:    []string{testPrimary, testReplica2, testReplica3},
				StatusByInstance: tc.statuses,
			}
			gotObserved, gotPressure, gotReason := evaluateStoragePressure(observed)
			if gotObserved != tc.wantObserved || gotPressure != tc.wantPressure {
				t.Fatalf("evaluateStoragePressure = (observed=%v, pressure=%v), want (observed=%v, pressure=%v)",
					gotObserved, gotPressure, tc.wantObserved, tc.wantPressure)
			}
			if tc.wantContains != "" && !strings.Contains(gotReason, tc.wantContains) {
				t.Fatalf("reason = %q, want to contain %q", gotReason, tc.wantContains)
			}
		})
	}
}

func TestPatchStatusStoragePressureConditionAndEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 1
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(4)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster).
		Build()
	reconciler := &ClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}
	plan := testPlan()
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}

	// 1) Volume crosses the threshold: condition flips True and a Warning fires.
	if err := reconciler.patchStatus(ctx, cluster, observedCluster{
		Plan:                  plan,
		InstanceNames:         []string{testPrimary},
		Phase:                 topology.PhaseReady,
		Ready:                 true,
		ReadyInstances:        1,
		StorageObserved:       true,
		StoragePressure:       true,
		StoragePressureReason: "Data volume usage is at or above 85% on instance(s): " + testPrimary,
	}); err != nil {
		t.Fatal(err)
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatal(err)
	}
	if cond := apimeta.FindStatusCondition(got.Status.Conditions, conditionStoragePressure); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("StoragePressure condition = %+v, want True", cond)
	}
	assertEvent(t, recorder, eventStoragePressure)

	// 2) Steady-state resync while still pressured: no duplicate event.
	if err := reconciler.patchStatus(ctx, got, observedCluster{
		Plan:                  plan,
		InstanceNames:         []string{testPrimary},
		Phase:                 topology.PhaseReady,
		Ready:                 true,
		ReadyInstances:        1,
		StorageObserved:       true,
		StoragePressure:       true,
		StoragePressureReason: "Data volume usage is at or above 85% on instance(s): " + testPrimary,
	}); err != nil {
		t.Fatal(err)
	}
	assertNoEvent(t, recorder, eventStoragePressure)

	// 3) Usage drops back: condition flips False and a resolved event fires.
	got2 := &mysqlv1alpha1.Cluster{}
	if err := c.Get(ctx, key, got2); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.patchStatus(ctx, got2, observedCluster{
		Plan:                  plan,
		InstanceNames:         []string{testPrimary},
		Phase:                 topology.PhaseReady,
		Ready:                 true,
		ReadyInstances:        1,
		StorageObserved:       true,
		StoragePressure:       false,
		StoragePressureReason: "All instance data volumes are below 85% usage",
	}); err != nil {
		t.Fatal(err)
	}
	got3 := &mysqlv1alpha1.Cluster{}
	if err := c.Get(ctx, key, got3); err != nil {
		t.Fatal(err)
	}
	if cond := apimeta.FindStatusCondition(got3.Status.Conditions, conditionStoragePressure); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("StoragePressure condition = %+v, want False", cond)
	}
	assertEvent(t, recorder, eventStoragePressureResolved)
}

// assertEvent drains every currently buffered event and fails unless one of them
// contains reason. A patchStatus call may emit several events (e.g. a phase
// transition alongside the storage one), so it scans rather than peeking the head.
func assertEvent(t *testing.T, recorder *record.FakeRecorder, reason string) {
	t.Helper()
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, reason) {
				return
			}
		default:
			t.Fatalf("expected an event containing %q", reason)
		}
	}
}

func assertNoEvent(t *testing.T, recorder *record.FakeRecorder, reason string) {
	t.Helper()
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, reason) {
				t.Fatalf("unexpected event containing %q: %q", reason, event)
			}
		default:
			return
		}
	}
}

func TestPatchStatusEmitsObservedGroupFailover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := grCluster(&mysqlv1alpha1.GroupReplicationStatus{
		GroupName:     "group-uuid",
		Bootstrapped:  true,
		PrimaryMember: testPrimary,
	})
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.TargetPrimary = testPrimary
	cluster.Status.Phase = topology.PhaseReady
	cluster.Spec.Instances = 3
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(2)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster).
		Build()
	reconciler := &ClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}
	plan := testPlan()
	plan.Instances = 3

	err := reconciler.patchStatus(ctx, cluster, observedCluster{
		Plan:          plan,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		Phase:         topology.PhaseReady,
		Ready:         true,
		GroupReplication: &mysqlv1alpha1.GroupReplicationStatus{
			PrimaryMember: testReplica2,
			HasQuorum:     true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, eventFailoverObserved) ||
			!strings.Contains(event, testPrimary+" to "+testReplica2) {
			t.Fatalf("event = %q, want observed failover from %s to %s", event, testPrimary, testReplica2)
		}
	default:
		t.Fatal("expected a FailoverObserved event")
	}
}

func TestObservedGroupFailoverExcludesPlannedAndBootstrapChanges(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		before *mysqlv1alpha1.Cluster
		after  *mysqlv1alpha1.Cluster
	}{
		{
			name: "planned switchover",
			before: func() *mysqlv1alpha1.Cluster {
				cluster := grCluster(nil)
				cluster.Status.CurrentPrimary = testPrimary
				cluster.Status.TargetPrimary = testReplica2
				return cluster
			}(),
			after: func() *mysqlv1alpha1.Cluster {
				cluster := grCluster(nil)
				cluster.Status.CurrentPrimary = testReplica2
				return cluster
			}(),
		},
		{
			name:   "initial bootstrap",
			before: grCluster(nil),
			after: func() *mysqlv1alpha1.Cluster {
				cluster := grCluster(nil)
				cluster.Status.CurrentPrimary = testPrimary
				return cluster
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if from, to, ok := controllergr.NewReconciler(nil, nil, nil, nil).ObservedFailover(tt.before, tt.after); ok {
				t.Fatalf("observedGroupFailover = (%q, %q, true), want no automatic failover", from, to)
			}
		})
	}
}
