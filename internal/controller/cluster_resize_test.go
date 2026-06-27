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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// existingPVC builds the data PVC for an instance at the given current size,
// optionally carrying a pending node-side resize condition.
func existingPVC(cluster *mysqlv1alpha1.Cluster, name, size string, pending bool) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
	if pending {
		pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{
			Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
			Status: corev1.ConditionTrue,
		}}
	}
	return pvc
}

func TestEnsurePVCGrowsRequest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Storage.Size = "2Gi"
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	pvc := existingPVC(cluster, inst.PVCName, "1Gi", false)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pvc).Build(),
		Scheme: scheme,
	}

	rolled, err := reconciler.ensurePVC(ctx, cluster, inst)
	if err != nil {
		t.Fatal(err)
	}
	if rolled {
		t.Fatal("growing an online-expandable volume must not request a roll")
	}
	got := &corev1.PersistentVolumeClaim{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, got); err != nil {
		t.Fatal(err)
	}
	if want := resource.MustParse("2Gi"); got.Spec.Resources.Requests.Storage().Cmp(want) != 0 {
		t.Fatalf("request = %s, want 2Gi", got.Spec.Resources.Requests.Storage())
	}
}

func TestEnsurePVCResizeRollGatedOnResizeInUseVolumes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		resizeInUse   bool
		pending       bool
		wantNeedsRoll bool
	}{
		{name: "online expansion never rolls", resizeInUse: true, pending: true, wantNeedsRoll: false},
		{name: "offline pending resize rolls", resizeInUse: false, pending: true, wantNeedsRoll: true},
		{name: "offline but nothing pending", resizeInUse: false, pending: false, wantNeedsRoll: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			cluster := baseCluster()
			cluster.Spec.Storage.Size = "1Gi"
			cluster.Spec.Storage.ResizeInUseVolumes = ptr.To(tc.resizeInUse)
			scheme := testScheme(t)
			inst := testPlan().instanceFor(cluster, 1)
			// Request already matches desired; only the pending condition should drive the roll.
			pvc := existingPVC(cluster, inst.PVCName, "1Gi", tc.pending)
			reconciler := &ClusterReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pvc).Build(),
				Scheme: scheme,
			}

			rolled, err := reconciler.ensurePVC(ctx, cluster, inst)
			if err != nil {
				t.Fatal(err)
			}
			if rolled != tc.wantNeedsRoll {
				t.Fatalf("needsRoll = %v, want %v", rolled, tc.wantNeedsRoll)
			}
		})
	}
}

func TestRollForResizeDeletesPodWhenAllowed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	pod := readyPod(cluster, inst.Name, rolePrimary)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pod).Build(),
		Scheme: scheme,
	}

	rolled, err := reconciler.rollForResize(ctx, cluster, inst, true)
	if err != nil {
		t.Fatal(err)
	}
	if !rolled {
		t.Fatal("rollForResize should report rolled=true after deleting the Pod")
	}
	err = reconciler.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("pod get = %v, want not found", err)
	}
}

func TestRollForResizeDefersWhenRollDisallowed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	pod := readyPod(cluster, inst.Name, rolePrimary)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pod).Build(),
		Scheme: scheme,
	}

	// allowRoll=false models the primary, which is rolled last: the Pod must survive.
	rolled, err := reconciler.rollForResize(ctx, cluster, inst, false)
	if err != nil {
		t.Fatal(err)
	}
	if rolled {
		t.Fatal("rollForResize must defer (rolled=false) when allowRoll is false")
	}
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
		t.Fatalf("deferred pod should still exist, got %v", err)
	}
}

func TestRollForResizeMissingPodIsNoError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}

	// No Pod yet: ensurePod will create it and the fresh mount completes the resize.
	rolled, err := reconciler.rollForResize(ctx, cluster, inst, true)
	if err != nil {
		t.Fatal(err)
	}
	if rolled {
		t.Fatal("a missing Pod must not report a roll")
	}
}
