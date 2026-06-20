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

package controller

import (
	"context"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsureInstanceRBACScopesGroupReplicationDoorbell(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := grCluster(nil)
	cluster.Spec.Instances = 2
	plan := testPlan()
	plan.Instances = 2
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	if err := r.ensureInstanceRBAC(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	shared := &rbacv1.Role{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-instance"}, shared); err != nil {
		t.Fatal(err)
	}
	for _, rule := range shared.Rules {
		if slices.Contains(rule.Resources, "clusters/status") || slices.Contains(rule.Resources, "leases") {
			t.Fatalf("GR shared role retains async-only rule: %+v", rule)
		}
	}

	for _, instance := range []string{"demo-1", "demo-2"} {
		name := instance + "-gr-doorbell"
		role := &rbacv1.Role{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, role); err != nil {
			t.Fatal(err)
		}
		if len(role.Rules) != 1 || !slices.Equal(role.Rules[0].Resources, []string{"pods"}) ||
			!slices.Equal(role.Rules[0].Verbs, []string{"get", "patch"}) ||
			!slices.Equal(role.Rules[0].ResourceNames, []string{instance}) {
			t.Fatalf("doorbell role %s is not scoped to its own Pod: %+v", name, role.Rules)
		}

		binding := &rbacv1.RoleBinding{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, binding); err != nil {
			t.Fatal(err)
		}
		if len(binding.Subjects) != 1 || binding.Subjects[0].Name != instance+"-instance" ||
			binding.RoleRef.Name != name {
			t.Fatalf("doorbell binding %s is not instance-specific: %+v", name, binding)
		}
	}
}

func TestEnsureInstanceRBACKeepsAsyncStatusAndLeasePermissions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	if err := r.ensureInstanceRBAC(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}

	role := &rbacv1.Role{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-instance"}, role); err != nil {
		t.Fatal(err)
	}
	resources := make([]string, 0, len(role.Rules))
	for _, rule := range role.Rules {
		resources = append(resources, rule.Resources...)
	}
	if !slices.Contains(resources, "clusters/status") || !slices.Contains(resources, "leases") {
		t.Fatalf("async role lost status or lease permissions: %v", resources)
	}
}
