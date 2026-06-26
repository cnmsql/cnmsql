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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	controllerasync "github.com/cnmsql/cnmsql/internal/controller/async"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func pdbReconciler(t *testing.T, cluster *mysqlv1alpha1.Cluster) *ClusterReconciler {
	t.Helper()
	scheme := testScheme(t)
	return &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
}

func getPDB(t *testing.T, r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster, name string) (*policyv1.PodDisruptionBudget, error) {
	t.Helper()
	pdb := &policyv1.PodDisruptionBudget{}
	err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pdb)
	return pdb, err
}

func TestReconcilePDBSingleInstance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 1
	r := pdbReconciler(t, cluster)

	plan := clusterPlan{Instances: 1}
	if err := r.reconcilePDB(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	primary, err := getPDB(t, r, cluster, primaryPDBName(cluster))
	if err != nil {
		t.Fatalf("primary PDB get = %v, want created", err)
	}
	if !metav1.IsControlledBy(primary, cluster) {
		t.Fatalf("primary PDB not owned by cluster")
	}
	if mu := primary.Spec.MaxUnavailable; mu == nil || mu.IntValue() != 1 {
		t.Fatalf("primary maxUnavailable = %v, want 1", mu)
	}
	if got := primary.Spec.Selector.MatchLabels[roleLabel]; got != rolePrimary {
		t.Fatalf("primary selector role = %q, want %q", got, rolePrimary)
	}

	// Single-instance cluster has no replicas, so no replica PDB.
	if _, err := getPDB(t, r, cluster, replicaPDBName(cluster)); !apierrors.IsNotFound(err) {
		t.Fatalf("replica PDB get = %v, want not found", err)
	}
}

func TestReconcilePDBReplicaMaxUnavailable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		instances          int
		wantMaxUnavailable int
	}{
		{instances: 2, wantMaxUnavailable: 0}, // 1 replica → floor(1/2)=0
		{instances: 3, wantMaxUnavailable: 1}, // 2 replicas → floor(2/2)=1
		{instances: 5, wantMaxUnavailable: 2}, // 4 replicas → floor(4/2)=2
	}
	for _, tc := range cases {
		cluster := baseCluster()
		cluster.Spec.Instances = tc.instances
		r := pdbReconciler(t, cluster)

		if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: tc.instances}); err != nil {
			t.Fatal(err)
		}
		replica, err := getPDB(t, r, cluster, replicaPDBName(cluster))
		if err != nil {
			t.Fatalf("instances=%d replica PDB get = %v, want created", tc.instances, err)
		}
		if mu := replica.Spec.MaxUnavailable; mu == nil || mu.IntValue() != tc.wantMaxUnavailable {
			t.Fatalf("instances=%d replica maxUnavailable = %v, want %d", tc.instances, mu, tc.wantMaxUnavailable)
		}
		if got := replica.Spec.Selector.MatchLabels[roleLabel]; got != roleReplica {
			t.Fatalf("replica selector role = %q, want %q", got, roleReplica)
		}
	}
}

func TestReconcilePDBDisabledDeletes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	r := pdbReconciler(t, cluster)

	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 3}); err != nil {
		t.Fatal(err)
	}
	// Both PDBs should exist now.
	if _, err := getPDB(t, r, cluster, primaryPDBName(cluster)); err != nil {
		t.Fatalf("primary PDB get = %v, want created", err)
	}
	if _, err := getPDB(t, r, cluster, replicaPDBName(cluster)); err != nil {
		t.Fatalf("replica PDB get = %v, want created", err)
	}

	// Disable and reconcile again: both PDBs are removed.
	disabled := false
	cluster.Spec.EnablePDB = &disabled
	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := getPDB(t, r, cluster, primaryPDBName(cluster)); !apierrors.IsNotFound(err) {
		t.Fatalf("primary PDB get = %v, want not found after disable", err)
	}
	if _, err := getPDB(t, r, cluster, replicaPDBName(cluster)); !apierrors.IsNotFound(err) {
		t.Fatalf("replica PDB get = %v, want not found after disable", err)
	}
}

func TestReconcilePDBNodeMaintenanceWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	r := pdbReconciler(t, cluster)

	// Establish both PDBs first.
	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 3}); err != nil {
		t.Fatal(err)
	}

	// Open a maintenance window: both the replica and the primary PDB are removed
	// so nodes can drain. The primary PDB must go even for a multi-instance cluster
	// because its narrow role=primary selector makes Kubernetes allow zero
	// voluntary disruptions, which would otherwise block the drain outright.
	cluster.Spec.NodeMaintenanceWindow = &mysqlv1alpha1.NodeMaintenanceWindow{InProgress: true}
	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := getPDB(t, r, cluster, replicaPDBName(cluster)); !apierrors.IsNotFound(err) {
		t.Fatalf("replica PDB get = %v, want not found during maintenance", err)
	}
	if _, err := getPDB(t, r, cluster, primaryPDBName(cluster)); !apierrors.IsNotFound(err) {
		t.Fatalf("primary PDB get = %v, want not found during maintenance", err)
	}

	// Close the window: both PDBs are restored.
	cluster.Spec.NodeMaintenanceWindow.InProgress = false
	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := getPDB(t, r, cluster, replicaPDBName(cluster)); err != nil {
		t.Fatalf("replica PDB get = %v, want restored after maintenance", err)
	}
	if _, err := getPDB(t, r, cluster, primaryPDBName(cluster)); err != nil {
		t.Fatalf("primary PDB get = %v, want restored after maintenance", err)
	}
}

func TestReconcilePDBSingleInstanceMaintenanceDeletesPrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 1
	r := pdbReconciler(t, cluster)

	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := getPDB(t, r, cluster, primaryPDBName(cluster)); err != nil {
		t.Fatalf("primary PDB get = %v, want created", err)
	}

	// For a single-instance cluster the lone pod must be allowed to drain, so the
	// primary PDB is removed during the window.
	cluster.Spec.NodeMaintenanceWindow = &mysqlv1alpha1.NodeMaintenanceWindow{InProgress: true}
	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := getPDB(t, r, cluster, primaryPDBName(cluster)); !apierrors.IsNotFound(err) {
		t.Fatalf("primary PDB get = %v, want not found during single-instance maintenance", err)
	}
}

// fencedPod is a ready instance Pod that optionally carries the fencing
// annotation and starts out routable.
func fencedPod(cluster *mysqlv1alpha1.Cluster, name string, fenced bool) *corev1.Pod {
	pod := readyPod(cluster, name, roleReplica)
	pod.Labels[routableLabel] = routableTrue
	if fenced {
		pod.Annotations = map[string]string{fencingAnnotation: routableTrue}
	}
	return pod
}

func podRoutable(t *testing.T, r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster, name string) string {
	t.Helper()
	pod := &corev1.Pod{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
		t.Fatal(err)
	}
	return pod.Labels[routableLabel]
}

func TestReconcileFencingTogglesRoutableLabel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			cluster,
			fencedPod(cluster, testPrimary, false),
			fencedPod(cluster, testReplica2, true),
			fencedPod(cluster, testReplica3, false),
		).Build(),
		Scheme: scheme,
	}

	observed := observedCluster{
		InstanceNames:   []string{testPrimary, testReplica2, testReplica3},
		FencedInstances: []string{testReplica2},
	}
	if err := r.reconcileFencing(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	if got := podRoutable(t, r, cluster, testReplica2); got != routableFalse {
		t.Fatalf("%s routable = %q, want %q (fenced)", testReplica2, got, routableFalse)
	}
	if got := podRoutable(t, r, cluster, testPrimary); got != routableTrue {
		t.Fatalf("%s routable = %q, want %q (not fenced)", testPrimary, got, routableTrue)
	}

	// Clearing the fence restores routability.
	observed.FencedInstances = nil
	if err := r.reconcileFencing(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	if got := podRoutable(t, r, cluster, testReplica2); got != routableTrue {
		t.Fatalf("%s routable = %q, want %q after unfencing", testReplica2, got, routableTrue)
	}
}

func TestReconcileFencingDeRoutesUnreachableReplica(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.Phase = topology.PhaseReady // established
	cluster.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	scheme := testScheme(t)

	primaryPod := readyPod(cluster, testPrimary, rolePrimary)
	primaryPod.Labels[routableLabel] = routableTrue
	replicaPod := readyPod(cluster, testReplica2, roleReplica)
	replicaPod.Labels[routableLabel] = routableTrue
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, primaryPod, replicaPod).Build(),
		Scheme: scheme,
	}
	// Primary reachable; replica absent from StatusByInstance (unreachable).
	observed := observedCluster{
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: {InstanceName: testPrimary, Role: webserver.RolePrimary, IsReady: true},
		},
	}

	// First pass only stamps the unreachable marker; routing is untouched until
	// the grace period elapses.
	if err := r.reconcileFencing(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	if got := podRoutable(t, r, cluster, testReplica2); got != routableTrue {
		t.Fatalf("replica routable = %q within grace, want %q", got, routableTrue)
	}
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: testReplica2}, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Annotations[unreachableSinceAnnotation] == "" {
		t.Fatal("unreachable-since annotation was not stamped")
	}
	// Fast-forward past the grace period by backdating the marker.
	before := pod.DeepCopy()
	pod.Annotations[unreachableSinceAnnotation] = time.Now().Add(-2 * deRouteGracePeriod).Format(time.RFC3339)
	if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}

	// Second pass de-routes the still-unreachable replica; the primary stays in.
	if err := r.reconcileFencing(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	if got := podRoutable(t, r, cluster, testReplica2); got != routableFalse {
		t.Fatalf("replica routable = %q after grace, want %q (de-routed)", got, routableFalse)
	}
	if got := podRoutable(t, r, cluster, testPrimary); got != routableTrue {
		t.Fatalf("primary routable = %q, want %q (never de-routed)", got, routableTrue)
	}

	// Recovery restores routing and clears the marker.
	observed.StatusByInstance[testReplica2] = healthyReplicaStatus(testReplica2, testGTID)
	if err := r.reconcileFencing(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	if got := podRoutable(t, r, cluster, testReplica2); got != routableTrue {
		t.Fatalf("replica routable = %q after recovery, want %q", got, routableTrue)
	}
	recovered := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: testReplica2}, recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.Annotations[unreachableSinceAnnotation] != "" {
		t.Fatalf("unreachable-since = %q, want cleared", recovered.Annotations[unreachableSinceAnnotation])
	}
}

func TestReconcileFencingDoesNotDeRouteDuringProvisioning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.Phase = topology.PhaseProvisioning // not yet established
	scheme := testScheme(t)

	replicaPod := readyPod(cluster, testReplica2, roleReplica)
	replicaPod.Labels[routableLabel] = routableTrue
	// A stale marker from a prior life must be cleared, not acted on.
	replicaPod.Annotations = map[string]string{
		unreachableSinceAnnotation: time.Now().Add(-2 * deRouteGracePeriod).Format(time.RFC3339),
	}
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, replicaPod).Build(),
		Scheme: scheme,
	}
	observed := observedCluster{
		PrimaryName:      testPrimary,
		InstanceNames:    []string{testReplica2},
		StatusByInstance: map[string]*webserver.Status{}, // replica unreachable
	}
	if err := r.reconcileFencing(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	if got := podRoutable(t, r, cluster, testReplica2); got != routableTrue {
		t.Fatalf("replica routable = %q, want %q (no de-route while provisioning)", got, routableTrue)
	}
	got := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: testReplica2}, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[unreachableSinceAnnotation] != "" {
		t.Fatalf("stale marker not cleared: %q", got.Annotations[unreachableSinceAnnotation])
	}
}

func TestRoleSelectorGatesOnRoutable(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	for _, role := range []mysqlv1alpha1.ServiceSelectorType{
		mysqlv1alpha1.ServiceSelectorTypeRW,
		mysqlv1alpha1.ServiceSelectorTypeRO,
		mysqlv1alpha1.ServiceSelectorTypeR,
	} {
		if got := roleSelector(cluster, role)[routableLabel]; got != routableTrue {
			t.Fatalf("role %q selector routable = %q, want %q", role, got, routableTrue)
		}
	}
}

func TestSelectFailoverCandidateSkipsFenced(t *testing.T) {
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
		// demo-2 has the most complete GTID but is fenced, so demo-3 is promoted.
		FencedInstances: []string{testReplica2},
	}
	if got, reason := controllerasync.SelectFailoverCandidate(topologyFailoverState(observed), nil); got != testReplica3 {
		t.Fatalf("candidate = %q (reason %q), want demo-3 (demo-2 is fenced)", got, reason)
	}
}
