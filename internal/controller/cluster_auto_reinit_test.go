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
	"github.com/cnmsql/cnmsql/pkg/management/mysql/instance"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// reinitCandidatePod builds a Pod whose mysqld container is in CrashLoopBackOff
// with the given restart count, optionally carrying a termination message.
func reinitCandidatePod(restarts int32, terminationMessage string) *corev1.Pod {
	cs := corev1.ContainerStatus{
		Name: instanceContainerName,
		State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
		},
		RestartCount: restarts,
	}
	if terminationMessage != "" {
		cs.LastTerminationState.Terminated = &corev1.ContainerStateTerminated{
			Message:  terminationMessage,
			ExitCode: 1,
		}
	}
	return &corev1.Pod{Status: corev1.PodStatus{
		Phase:             corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{cs},
	}}
}

func corruptionMessage() string {
	return instance.CorruptionSentinel + ": mysqld cannot start, InnoDB data is corrupt: " +
		"InnoDB: Database page corruption on disk"
}

func TestShouldAutoReinit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "CrashLoopBackOff with high restart count",
			pod:  reinitCandidatePod(autoReinitRestartThreshold, ""),
			want: true,
		},
		{
			name: "CrashLoopBackOff with low restart count",
			pod:  reinitCandidatePod(autoReinitRestartThreshold-1, ""),
			want: false,
		},
		{
			// A diagnosed corruption does not need the full restart budget:
			// restarting cannot repair InnoDB data, so waiting only extends the
			// outage.
			name: "diagnosed corruption re-clones at the lower threshold",
			pod:  reinitCandidatePod(corruptionReinitRestartThreshold, corruptionMessage()),
			want: true,
		},
		{
			name: "diagnosed corruption still needs one retry",
			pod:  reinitCandidatePod(corruptionReinitRestartThreshold-1, corruptionMessage()),
			want: false,
		},
		{
			// An ordinary crash message must not be mistaken for the diagnosis.
			name: "unrelated termination message uses the high threshold",
			pod:  reinitCandidatePod(corruptionReinitRestartThreshold, "mysqld: out of memory"),
			want: false,
		},
		{
			// Eviction, preemption and node shutdown all land here with the data
			// volume intact. Re-cloning would turn a reschedule into a full resync.
			name: "PodFailed phase is not enough on its own",
			pod:  &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}},
			want: false,
		},
		{
			name: "evicted pod with no restarts",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				Phase:  corev1.PodFailed,
				Reason: "Evicted",
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:         instanceContainerName,
					RestartCount: 0,
				}},
			}},
			want: false,
		},
		{
			name: "Running pod",
			pod:  &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}},
			want: false,
		},
		{
			// Only the mysqld container counts; a crash-looping sidecar is not
			// grounds for discarding the instance's data.
			name: "crash-looping non-instance container is ignored",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{{
					Name: "sidecar",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
					RestartCount: autoReinitRestartThreshold,
				}},
			}},
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
				Name: instanceContainerName,
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

// autoReinitFixture builds an established 2-instance cluster with a ready
// primary and the given replica Pod, wired for reconcileAutoReinit.
func autoReinitFixture(t *testing.T, replicaStatus corev1.PodStatus) (
	*ClusterReconciler, *mysqlv1alpha1.Cluster, observedCluster,
) {
	t.Helper()
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
		Status: replicaStatus,
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster, replicaPod).
		Build()

	return &ClusterReconciler{Client: c, Scheme: scheme}, cluster, observedCluster{
		PrimaryName:     testPrimary,
		FailedInstances: []string{testReplica2},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: {InstanceName: testPrimary, IsReady: true, Role: webserver.RolePrimary},
		},
	}
}

// A replica whose manager diagnosed InnoDB corruption is re-cloned without
// waiting out the full restart budget: more restarts cannot repair the data.
func TestReconcileAutoReinitActsEarlyOnDiagnosedCorruption(t *testing.T) {
	t.Parallel()
	r, cluster, observed := autoReinitFixture(t, reinitCandidatePod(
		corruptionReinitRestartThreshold, corruptionMessage()).Status)

	if err := r.reconcileAutoReinit(context.Background(), cluster, observed); err != nil {
		t.Fatalf("reconcileAutoReinit: %v", err)
	}
	if !reinitRequested(cluster, testReplica2) {
		t.Fatal("a replica with diagnosed corruption must be re-cloned at the lower threshold")
	}
}

// An evicted replica still has its data; re-cloning it would turn a routine
// reschedule into a full resync.
func TestReconcileAutoReinitLeavesEvictedReplicaAlone(t *testing.T) {
	t.Parallel()
	r, cluster, observed := autoReinitFixture(t, corev1.PodStatus{
		Phase:  corev1.PodFailed,
		Reason: "Evicted",
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:         instanceContainerName,
			RestartCount: 0,
		}},
	})

	if err := r.reconcileAutoReinit(context.Background(), cluster, observed); err != nil {
		t.Fatalf("reconcileAutoReinit: %v", err)
	}
	if reinitRequested(cluster, testReplica2) {
		t.Fatal("an evicted replica must not be re-cloned: its data volume is intact")
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
