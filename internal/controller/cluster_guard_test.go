/*
Copyright 2026 The cloudnative-mysql Authors.

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

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
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

	// Open a maintenance window: the replica PDB is removed so nodes can drain,
	// the primary PDB stays (multi-instance cluster).
	cluster.Spec.NodeMaintenanceWindow = &mysqlv1alpha1.NodeMaintenanceWindow{InProgress: true}
	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := getPDB(t, r, cluster, replicaPDBName(cluster)); !apierrors.IsNotFound(err) {
		t.Fatalf("replica PDB get = %v, want not found during maintenance", err)
	}
	if _, err := getPDB(t, r, cluster, primaryPDBName(cluster)); err != nil {
		t.Fatalf("primary PDB get = %v, want still present during maintenance", err)
	}

	// Close the window: the replica PDB is restored.
	cluster.Spec.NodeMaintenanceWindow.InProgress = false
	if err := r.reconcilePDB(ctx, cluster, clusterPlan{Instances: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := getPDB(t, r, cluster, replicaPDBName(cluster)); err != nil {
		t.Fatalf("replica PDB get = %v, want restored after maintenance", err)
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
	if got, reason := selectFailoverCandidate(observed); got != testReplica3 {
		t.Fatalf("candidate = %q (reason %q), want demo-3 (demo-2 is fenced)", got, reason)
	}
}
