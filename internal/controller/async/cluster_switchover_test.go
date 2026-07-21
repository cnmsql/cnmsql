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

package async

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
)

// TestReconcileSwitchoverAbortFencesTarget checks that when a switchover blows
// past maxSwitchoverDelay, the target Pod is deleted so a partially-promoted
// target restarts as a replica instead of lingering as a second primary.
func TestReconcileSwitchoverAbortFencesTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cluster := testCluster()
	cluster.Spec.MaxSwitchoverDelay = 30
	cluster.Status.CurrentPrimary = drainPrimary
	cluster.Status.TargetPrimary = drainReplica
	// Started well beyond maxSwitchoverDelay, so this pass aborts.
	started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	cluster.Status.TargetPrimaryTimestamp = &started

	targetPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: drainReplica, Namespace: cluster.Namespace}}
	scheme := testScheme(t)
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, targetPod).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		Build()
	r := NewReconciler(client, scheme, nil, record.NewFakeRecorder(8), "")

	observed := topology.FailoverState{
		PrimaryName:   drainPrimary,
		InstanceNames: []string{drainPrimary, drainReplica},
		Instances: map[string]topology.FailoverInstance{
			drainPrimary: {Ready: true, Primary: true, Role: "primary"},
			drainReplica: {Ready: true, Replica: true, Role: "replica", SQLRunning: true, IORunning: true},
		},
	}

	result, err := r.ReconcileSwitchover(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Handled {
		t.Fatal("expected the aborted switchover to be handled")
	}

	pod := &corev1.Pod{}
	err = r.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: drainReplica}, pod)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("target pod get after abort: got err %v, want NotFound (fenced)", err)
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.TargetPrimary != drainPrimary {
		t.Fatalf("TargetPrimary = %q, want it restored to %s", got.Status.TargetPrimary, drainPrimary)
	}
	if got.Status.Phase != topology.PhaseBlocked {
		t.Fatalf("Phase = %q, want %q", got.Status.Phase, topology.PhaseBlocked)
	}
}
