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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
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

func getService(t *testing.T, ctx context.Context, reconciler *ClusterReconciler, name string) *corev1.Service {
	t.Helper()
	svc := &corev1.Service{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, svc); err != nil {
		t.Fatalf("get service %s: %v", name, err)
	}
	return svc
}
