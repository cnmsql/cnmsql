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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// switchoverOntoTerminatingTarget is a healthy two-instance cluster asked to
// switch over onto demo-2 while the demo-2 Pod is being deleted, which is what a
// switchover requested just as a scale-down removes that instance looks like.
func switchoverOntoTerminatingTarget(t *testing.T) (*mysqlv1alpha1.Cluster, *ClusterReconciler, observedCluster) {
	t.Helper()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.TargetPrimary = testReplica2
	cluster.Status.TargetPrimaryTimestamp = new(metav1.Now())
	scheme := testScheme(t)
	pods := []*corev1.Pod{
		readyPod(cluster, testPrimary, rolePrimary),
		readyPod(cluster, testReplica2, roleReplica),
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, pods[0], pods[1]).
			Build(),
		Scheme:        scheme,
		ControlClient: &recordingControlClient{},
		Recorder:      record.NewFakeRecorder(4),
	}
	plan := testPlan()
	plan.Instances = 2
	observed := observedCluster{
		Plan:                 plan,
		PrimaryName:          testPrimary,
		InstanceNames:        []string{testPrimary, testReplica2},
		ReadyInstances:       2,
		TerminatingInstances: []string{testReplica2},
		GTIDByInstance:       map[string]string{testPrimary: testGTID, testReplica2: testGTID},
		StatusByInstance: map[string]*webserver.Status{
			testPrimary:  {InstanceName: testPrimary, Role: webserver.RolePrimary, IsReady: true, GTIDExecuted: testGTID},
			testReplica2: healthyReplicaStatus(testReplica2, testGTID),
		},
	}
	return cluster, reconciler, observed
}

func TestReconcileSwitchoverCancelsARequestOntoATerminatingInstance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, observed := switchoverOntoTerminatingTarget(t)

	switched, err := reconciler.reconcileSwitchover(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !switched {
		t.Fatal("a cancelled switchover should be reported as handled")
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.TargetPrimary != testPrimary {
		t.Fatalf("targetPrimary = %q, want it pointed back at the current primary %q",
			got.Status.TargetPrimary, testPrimary)
	}
	if got.Status.TargetPrimaryTimestamp != nil {
		t.Fatalf("targetPrimaryTimestamp = %v, want it cleared with the request", got.Status.TargetPrimaryTimestamp)
	}
	select {
	case event := <-reconciler.Recorder.(*record.FakeRecorder).Events:
		if !strings.HasPrefix(event, corev1.EventTypeWarning) || !strings.Contains(event, testReplica2) {
			t.Fatalf("event = %q, want a Warning naming the refused target %s", event, testReplica2)
		}
	default:
		t.Fatal("cancelling a switchover should record an event explaining why")
	}
}

func TestReconcileSwitchoverKeepsATerminatingTargetThatAlreadyPromoted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, reconciler, observed := switchoverOntoTerminatingTarget(t)
	// The target took the primary role before the operator saw the request. Pointing
	// targetPrimary back now would be a second handoff, not a cancellation: the
	// promotion is recorded first, then the drain switchover moves the role off.
	observed.StatusByInstance[testReplica2] = &webserver.Status{
		InstanceName: testReplica2, Role: webserver.RolePrimary, IsReady: true, GTIDExecuted: testGTID,
	}

	if _, err := reconciler.reconcileSwitchover(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.TargetPrimary != testReplica2 {
		t.Fatalf("targetPrimary = %q, want the promoted target %q kept", got.Status.TargetPrimary, testReplica2)
	}
}
