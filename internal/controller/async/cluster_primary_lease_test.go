/*
Copyright 2026 The CloudNative MySQL Authors.

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

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func TestEnsurePrimaryLeaseCreatesOwnedLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := testCluster()
	scheme := testScheme(t)
	r := NewReconciler(fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(), scheme, nil, nil, "")

	if err := r.EnsurePrimaryLease(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	lease := &coordinationv1.Lease{}
	if err := r.client.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-primary"}, lease); err != nil {
		t.Fatal(err)
	}
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 15 {
		t.Fatalf("leaseDurationSeconds = %v, want 15", lease.Spec.LeaseDurationSeconds)
	}
	if len(lease.OwnerReferences) != 1 || lease.OwnerReferences[0].Name != cluster.Name {
		t.Fatalf("ownerReferences = %#v, want cluster owner", lease.OwnerReferences)
	}
}

func TestPrimaryLeaseStatusHonorsExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := testCluster()
	holder := "demo-1"
	duration := int32(15)
	renewed := metav1.MicroTime{Time: time.Now().Add(-20 * time.Second)}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-primary", Namespace: cluster.Namespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			RenewTime:            &renewed,
			LeaseDurationSeconds: &duration,
		},
	}
	scheme := testScheme(t)
	r := NewReconciler(fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, lease).Build(), scheme, nil, nil, "")

	status, err := r.PrimaryLeaseStatus(ctx, cluster, holder)
	if err != nil {
		t.Fatal(err)
	}
	if status.Held {
		t.Fatal("expired lease reported as held")
	}
}

func testCluster() *mysqlv1alpha1.Cluster {
	return &mysqlv1alpha1.Cluster{
		TypeMeta: metav1.TypeMeta{APIVersion: mysqlv1alpha1.GroupVersion.String(), Kind: "Cluster"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
			UID:       types.UID("demo-uid"),
		},
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := mysqlv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
