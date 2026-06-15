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

func TestRoleLabelsPrimaryVsReplica(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	plan.Instances = 2

	if got := roleOf(plan.instanceFor(cluster, 1)); got != rolePrimary {
		t.Fatalf("instance 1 role = %q, want primary", got)
	}
	if got := roleOf(plan.instanceFor(cluster, 2)); got != roleReplica {
		t.Fatalf("instance 2 role = %q, want replica", got)
	}
	labels := labelsFor(cluster, "demo-2", roleReplica)
	if labels[roleLabel] != roleReplica {
		t.Fatalf("replica label = %q", labels[roleLabel])
	}
}

func TestEnsureDefaultServicesSelectorsAndDisable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan := testPlan()

	if err := reconciler.ensureDefaultServices(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	rw := getService(t, ctx, reconciler, "demo-rw")
	if rw.Spec.Selector[roleLabel] != rolePrimary {
		t.Fatalf("rw selector = %#v, want role=primary", rw.Spec.Selector)
	}
	if rw.Spec.PublishNotReadyAddresses {
		t.Fatal("rw must not publish not-ready addresses")
	}
	ro := getService(t, ctx, reconciler, "demo-ro")
	if ro.Spec.Selector[roleLabel] != roleReplica {
		t.Fatalf("ro selector = %#v, want role=replica", ro.Spec.Selector)
	}
	r := getService(t, ctx, reconciler, "demo-r")
	if _, hasRole := r.Spec.Selector[roleLabel]; hasRole {
		t.Fatalf("r selector should not pin a role: %#v", r.Spec.Selector)
	}

	// Disabling ro deletes it on the next reconcile.
	plan.DisabledServices = map[mysqlv1alpha1.ServiceSelectorType]bool{mysqlv1alpha1.ServiceSelectorTypeRO: true}
	if err := reconciler.ensureDefaultServices(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-ro"}, &corev1.Service{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("disabled ro service get = %v, want not found", err)
	}
}

func TestScaleDownRemovesPodRetainsPVC(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	// Three instances exist; desired count is 1.
	objects := []*corev1.Pod{}
	for i := 1; i <= 3; i++ {
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      instanceName(cluster, i),
			Namespace: cluster.Namespace,
			Labels:    map[string]string{clusterLabel: cluster.Name},
		}})
	}
	pvc3 := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-3", Namespace: cluster.Namespace}}
	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pvc3)
	for _, p := range objects {
		builder = builder.WithObjects(p)
	}
	reconciler := &ClusterReconciler{Client: builder.Build(), Scheme: scheme}

	plan := testPlan() // Instances == 1
	if err := reconciler.scaleDownReplicas(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	// Replica pods removed, primary kept.
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-1"}, &corev1.Pod{}); err != nil {
		t.Fatalf("primary pod should be kept: %v", err)
	}
	for _, name := range []string{"demo-2", "demo-3"} {
		err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, &corev1.Pod{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("pod %s get = %v, want removed", name, err)
		}
	}
	// PVC retained per the M4 policy.
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: "demo-3"}, &corev1.PersistentVolumeClaim{}); err != nil {
		t.Fatalf("replica PVC should be retained: %v", err)
	}
}

func TestScaleDownKeepsCurrentPrimaryByName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Status.CurrentPrimary = "demo-3"
	scheme := testScheme(t)
	objects := []client.Object{cluster}
	for i := 1; i <= 3; i++ {
		name := instanceName(cluster, i)
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    map[string]string{clusterLabel: cluster.Name},
		}})
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 2
	plan.PrimaryName = "demo-3"

	if err := reconciler.scaleDownReplicas(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-3"}, &corev1.Pod{}); err != nil {
		t.Fatalf("current primary should be kept: %v", err)
	}
}

func getService(t *testing.T, ctx context.Context, reconciler *ClusterReconciler, name string) *corev1.Service {
	t.Helper()
	svc := &corev1.Service{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, svc); err != nil {
		t.Fatalf("get service %s: %v", name, err)
	}
	return svc
}
