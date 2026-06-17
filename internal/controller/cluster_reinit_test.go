/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func instancePVC(cluster *mysqlv1alpha1.Cluster, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: cluster.Namespace,
	}}
}

func instanceMissing(t *testing.T, c client.Client, cluster *mysqlv1alpha1.Cluster, name string, obj client.Object) bool {
	t.Helper()
	err := c.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: name}, obj)
	return apierrors.IsNotFound(err)
}

func TestReconcileReinitTearsDownThenClears(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Annotations = map[string]string{reinitAnnotation: testReplica2}
	scheme := testScheme(t)
	pod := readyPod(cluster, testReplica2, roleReplica)
	pvc := instancePVC(cluster, testReplica2)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster, pod, pvc).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	plan := testPlan()
	plan.Instances = 2
	inst := plan.instanceFor(cluster, 2)

	// First pass: teardown begins. The Pod and PVC are deleted and the instance is
	// not yet recreated, so the request stays set.
	handled, err := r.reconcileReinit(ctx, cluster, inst)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("reconcileReinit handled = false during teardown, want true")
	}
	if !instanceMissing(t, c, cluster, testReplica2, &corev1.Pod{}) {
		t.Fatal("Pod was not deleted during re-init teardown")
	}
	if !instanceMissing(t, c, cluster, testReplica2, &corev1.PersistentVolumeClaim{}) {
		t.Fatal("PVC was not deleted during re-init teardown")
	}
	if !reinitRequested(cluster, testReplica2) {
		t.Fatal("re-init request cleared before teardown completed")
	}

	// Second pass: Pod and PVC are gone, so the request is cleared and the caller
	// is allowed to recreate the instance (handled=false).
	handled, err = r.reconcileReinit(ctx, cluster, inst)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("reconcileReinit handled = true after teardown completed, want false")
	}
	if reinitRequested(cluster, testReplica2) {
		t.Fatal("re-init request not cleared after teardown completed")
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Annotations[reinitAnnotation]; ok {
		t.Fatalf("reinit annotation still present on persisted Cluster: %v", got.Annotations)
	}
}

func TestReconcileReinitRefusesPrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Annotations = map[string]string{reinitAnnotation: testPrimary}
	scheme := testScheme(t)
	pod := readyPod(cluster, testPrimary, rolePrimary)
	pvc := instancePVC(cluster, testPrimary)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster, pod, pvc).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	plan := testPlan()
	plan.Instances = 2
	inst := plan.instanceFor(cluster, 1) // the primary

	handled, err := r.reconcileReinit(ctx, cluster, inst)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("reconcileReinit handled = true for the primary, want false (must refuse)")
	}
	// The primary's Pod and PVC must be untouched.
	if instanceMissing(t, c, cluster, testPrimary, &corev1.Pod{}) {
		t.Fatal("primary Pod was deleted by a refused re-init")
	}
	if instanceMissing(t, c, cluster, testPrimary, &corev1.PersistentVolumeClaim{}) {
		t.Fatal("primary PVC was deleted by a refused re-init")
	}
	// The refused request must be dropped so it does not loop.
	if reinitRequested(cluster, testPrimary) {
		t.Fatal("refused re-init request was not cleared")
	}
}

func TestReconcileReinitNoopWhenNotRequested(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	scheme := testScheme(t)
	pod := readyPod(cluster, testReplica2, roleReplica)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster, pod).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	plan := testPlan()
	plan.Instances = 2

	handled, err := r.reconcileReinit(ctx, cluster, plan.instanceFor(cluster, 2))
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("reconcileReinit handled = true with no request, want false")
	}
	if instanceMissing(t, c, cluster, testReplica2, &corev1.Pod{}) {
		t.Fatal("Pod was deleted without a re-init request")
	}
}

func TestReinitRequestedInstancesParsing(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Annotations = map[string]string{reinitAnnotation: " demo-2 , ,demo-3 "}
	got := reinitRequestedInstances(cluster)
	if len(got) != 2 || got[0] != "demo-2" || got[1] != testReplica3 {
		t.Fatalf("reinitRequestedInstances = %v, want [demo-2 demo-3]", got)
	}
	if reinitRequested(cluster, "demo-4") {
		t.Error("reinitRequested(demo-4) = true, want false")
	}
}
