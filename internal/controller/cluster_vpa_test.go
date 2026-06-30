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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestEnsurePodDoesNotFightVPA is the regression guard for vertical autoscaling.
//
// The Vertical Pod Autoscaler resizes a Pod by mutating its container resource
// requests/limits in place (its admission webhook rewrites them on Pod
// creation, and the in-place updater patches the live Pod). Those values live
// on the running Pod, not on cluster.Spec.Resources. ensurePod must therefore
// never treat a VPA-driven resource change as template drift: if it did, every
// reconcile would delete the Pod to "fix" its resources and fight the
// autoscaler into an eviction loop.
//
// This works because the Pod template hash is computed purely from the desired
// PodSpec (cluster.Spec.Resources), so a change to the *live* Pod's resources
// leaves the hash equal to the stored annotation and does not roll the Pod. The
// test pins that contract so a future change to drift detection cannot silently
// reintroduce the fight.
func TestEnsurePodDoesNotFightVPA(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	reconciler := &ClusterReconciler{Client: c, Scheme: scheme}
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)

	// First pass creates the Pod with the cluster's desired resources.
	if rolled, err := reconciler.ensurePod(ctx, cluster, plan, inst, true); err != nil {
		t.Fatal(err)
	} else if rolled {
		t.Fatal("creating a Pod must not report a roll")
	}

	pod := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: inst.Name}, pod); err != nil {
		t.Fatal(err)
	}

	// Simulate the VPA resizing the live Pod: bump CPU and memory on the mysql
	// container well above the cluster's declared requests.
	vpaCPU := resource.MustParse("2")
	vpaMemory := resource.MustParse("4Gi")
	before := pod.DeepCopy()
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
		corev1.ResourceCPU:    vpaCPU,
		corev1.ResourceMemory: vpaMemory,
	}
	if err := c.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}

	// Reconcile again: the operator must leave the VPA-resized Pod alone.
	rolled, err := reconciler.ensurePod(ctx, cluster, plan, inst, true)
	if err != nil {
		t.Fatal(err)
	}
	if rolled {
		t.Fatal("ensurePod rolled a Pod whose only change was a VPA resource resize; the operator is fighting the autoscaler")
	}

	got := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: inst.Name}, got); err != nil {
		t.Fatalf("Pod should survive a VPA resize, got %v", err)
	}
	if got.DeletionTimestamp != nil {
		t.Fatal("ensurePod deleted the VPA-resized Pod")
	}
	gotRequests := got.Spec.Containers[0].Resources.Requests
	if gotRequests.Cpu().Cmp(vpaCPU) != 0 || gotRequests.Memory().Cmp(vpaMemory) != 0 {
		t.Fatalf("ensurePod reset the VPA resources to the cluster spec: requests = %v, want cpu=%s memory=%s",
			gotRequests, vpaCPU.String(), vpaMemory.String())
	}
}

// TestEnsurePodRollsOnSpecResourceChange is the companion to
// TestEnsurePodDoesNotFightVPA: a change to the *desired* resources
// (cluster.Spec.Resources, i.e. an operator/user edit, not a VPA resize) must
// still roll the Pod so the new request is applied. This confirms the
// no-fight-with-VPA behavior comes from ignoring live-Pod drift, not from
// ignoring resource changes entirely.
func TestEnsurePodRollsOnSpecResourceChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
	}
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	reconciler := &ClusterReconciler{Client: c, Scheme: scheme}
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)

	if _, err := reconciler.ensurePod(ctx, cluster, plan, inst, true); err != nil {
		t.Fatal(err)
	}

	// The user edits the desired requests: the template hash must change and the
	// Pod must be rolled to pick it up.
	cluster.Spec.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1Gi")
	rolled, err := reconciler.ensurePod(ctx, cluster, plan, inst, true)
	if err != nil {
		t.Fatal(err)
	}
	if !rolled {
		t.Fatal("a change to cluster.Spec.Resources must roll the Pod")
	}
}

// TestGetInstancesSelectorMatchesStampedLabels keeps the published scale
// sub-resource selector in lock-step with the labels the controller actually
// stamps on instance Pods. The VPA discovers the Pods to resize through this
// selector, so a divergence between GetInstancesSelector and labelsFor would
// silently leave the autoscaler with an empty target set.
func TestGetInstancesSelectorMatchesStampedLabels(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)

	stamped := labelsFor(cluster, inst.Name, roleOf(inst))

	parsed, err := labels.Parse(cluster.GetInstancesSelector())
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Matches(labels.Set(stamped)) {
		t.Fatalf("published selector %q does not match stamped Pod labels %v",
			cluster.GetInstancesSelector(), stamped)
	}
}
