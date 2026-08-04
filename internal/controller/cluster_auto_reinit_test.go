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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func TestShouldAutoReinit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "CrashLoopBackOff with high restart count",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
						},
						RestartCount: autoReinitRestartThreshold,
					}},
				},
			},
			want: true,
		},
		{
			name: "CrashLoopBackOff with low restart count",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
						},
						RestartCount: autoReinitRestartThreshold - 1,
					}},
				},
			},
			want: false,
		},
		{
			name: "PodFailed phase",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodFailed},
			},
			want: true,
		},
		{
			name: "Running pod",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldAutoReinit(tt.pod); got != tt.want {
				t.Errorf("shouldAutoReinit() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileAutoReinitReClonesFailedReplica(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	cluster.Status.CurrentPrimary = testPrimary

	scheme := testScheme(t)

	replicaPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testReplica2,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				clusterLabel:  cluster.Name,
				instanceLabel: testReplica2,
				roleLabel:     roleReplica,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
				RestartCount: autoReinitRestartThreshold,
			}},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster, replicaPod).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	observed := observedCluster{
		PrimaryName:     testPrimary,
		FailedInstances: []string{testReplica2},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: {InstanceName: testPrimary, IsReady: true, Role: webserver.RolePrimary},
		},
	}

	if err := r.reconcileAutoReinit(ctx, cluster, observed); err != nil {
		t.Fatalf("reconcileAutoReinit: %v", err)
	}

	if !reinitRequested(cluster, testReplica2) {
		t.Fatal("auto-reinit did not set the reinit annotation for the failed replica")
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[reinitAnnotation] != testReplica2 {
		t.Fatalf("persisted reinit annotation = %q, want %q", got.Annotations[reinitAnnotation], testReplica2)
	}
}

func TestReconcileAutoReinitSkipsPrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	cluster.Status.CurrentPrimary = testPrimary

	scheme := testScheme(t)

	primaryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPrimary,
			Namespace: cluster.Namespace,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster, primaryPod).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	observed := observedCluster{
		PrimaryName:     testPrimary,
		FailedInstances: []string{testPrimary},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: {InstanceName: testPrimary, IsReady: true, Role: webserver.RolePrimary},
		},
	}

	if err := r.reconcileAutoReinit(ctx, cluster, observed); err != nil {
		t.Fatalf("reconcileAutoReinit: %v", err)
	}

	if reinitRequested(cluster, testPrimary) {
		t.Fatal("auto-reinit must not re-initialise the primary")
	}
}

func TestReconcileAutoReinitSkipsUnestablishedCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2

	scheme := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	observed := observedCluster{
		PrimaryName:     testPrimary,
		FailedInstances: []string{testReplica2},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: {InstanceName: testPrimary, IsReady: true, Role: webserver.RolePrimary},
		},
	}

	if err := r.reconcileAutoReinit(ctx, cluster, observed); err != nil {
		t.Fatalf("reconcileAutoReinit: %v", err)
	}

	if reinitRequested(cluster, testReplica2) {
		t.Fatal("auto-reinit must not run on an unestablished cluster")
	}
}

func TestReconcileAutoReinitSkipsAlreadyRequested(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	cluster.Annotations = map[string]string{reinitAnnotation: testReplica2}

	scheme := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	observed := observedCluster{
		PrimaryName:     testPrimary,
		FailedInstances: []string{testReplica2},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: {InstanceName: testPrimary, IsReady: true, Role: webserver.RolePrimary},
		},
	}

	if err := r.reconcileAutoReinit(ctx, cluster, observed); err != nil {
		t.Fatalf("reconcileAutoReinit: %v", err)
	}

	if got := cluster.Annotations[reinitAnnotation]; got != testReplica2 {
		t.Fatalf("reinit annotation = %q, want %q (should not duplicate)", got, testReplica2)
	}
}
