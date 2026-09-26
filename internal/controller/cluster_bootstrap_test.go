package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPVCBootstrapped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{"legacy volume without annotation", nil, true},
		{"initializing", map[string]string{pvcStatusAnnotation: pvcStatusInitializing}, false},
		{"ready", map[string]string{pvcStatusAnnotation: pvcStatusReady}, true},
		{"unknown value", map[string]string{pvcStatusAnnotation: "detached"}, false},
	} {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations}}
		if got := pvcBootstrapped(pvc); got != tc.want {
			t.Errorf("%s: pvcBootstrapped = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestEnsurePVCMarksNewVolumeInitializing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	inst := testPlan().instanceFor(cluster, 1)

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Annotations[pvcStatusAnnotation]; got != pvcStatusInitializing {
		t.Fatalf("new PVC %s = %q, want %q", pvcStatusAnnotation, got, pvcStatusInitializing)
	}
}

func TestEnsurePVCBackfillsLegacyVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	legacy := instancePVC(cluster, inst.PVCName)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, legacy).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Annotations[pvcStatusAnnotation]; got != pvcStatusReady {
		t.Fatalf("legacy PVC %s = %q, want %q", pvcStatusAnnotation, got, pvcStatusReady)
	}
}

func TestEnsurePVCKeepsInitializingVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	inst := testPlan().instanceFor(cluster, 1)
	pvc := instancePVC(cluster, inst.PVCName)
	pvc.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusInitializing}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pvc).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	if _, err := r.ensurePVC(ctx, cluster, inst); err != nil {
		t.Fatal(err)
	}
	got := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.PVCName}, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[pvcStatusAnnotation] != pvcStatusInitializing {
		t.Fatalf("ensurePVC rewrote an initializing volume to %q", got.Annotations[pvcStatusAnnotation])
	}
}
