/*
Copyright 2026 The cloudnative-mysql Authors.

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

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsurePrimaryLeaseCreatesOwnedLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	if err := r.ensurePrimaryLease(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	lease := &coordinationv1.Lease{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-primary"}, lease); err != nil {
		t.Fatal(err)
	}
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 15 {
		t.Fatalf("leaseDurationSeconds = %v, want 15", lease.Spec.LeaseDurationSeconds)
	}
	if len(lease.OwnerReferences) != 1 || lease.OwnerReferences[0].Name != cluster.Name {
		t.Fatalf("ownerReferences = %#v, want cluster owner", lease.OwnerReferences)
	}
}

func TestIsPrimaryLeaseHeldHonorsExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	holder := "demo-1"
	duration := int32(15)
	renewed := metav1.MicroTime{Time: time.Now().Add(-20 * time.Second)}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-primary", Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			RenewTime:            &renewed,
			LeaseDurationSeconds: &duration,
		},
	}
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, lease).Build(),
		Scheme: scheme,
	}
	held, err := r.isPrimaryLeaseHeld(ctx, cluster, holder)
	if err != nil {
		t.Fatal(err)
	}
	if held {
		t.Fatal("expired lease reported as held")
	}
}
