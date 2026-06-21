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

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/controller/topology"
)

type recordingSemiSyncControl struct {
	waits map[string]int
}

func (c *recordingSemiSyncControl) SetSemiSyncWaitForReplicaCount(
	_ context.Context,
	_ *mysqlv1alpha1.Cluster,
	instance string,
	count int,
) error {
	c.waits[instance] = count
	return nil
}

func TestReconcileAvailabilityAdjustsSemiSync(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		durability string
		observed   topology.AvailabilityState
		want       int
	}{
		{name: "preferred reduces for unhealthy replica", durability: mysqlv1alpha1.DataDurabilityPreferred, observed: availability("demo-2"), want: 1},
		{name: "preferred reduces for fenced replica", durability: mysqlv1alpha1.DataDurabilityPreferred, observed: fencedAvailability("demo-2"), want: 1},
		{name: "preferred restores configured count", durability: mysqlv1alpha1.DataDurabilityPreferred, observed: availability("demo-2", "demo-3"), want: 2},
		{name: "preferred floors at one", durability: mysqlv1alpha1.DataDurabilityPreferred, observed: availability(), want: 1},
		{name: "required keeps configured count", durability: mysqlv1alpha1.DataDurabilityRequired, observed: availability("demo-2"), want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			control := &recordingSemiSyncControl{waits: map[string]int{}}
			r := &Reconciler{semiSyncControl: control}
			if err := r.ReconcileAvailability(context.Background(), semiSyncTestCluster(tt.durability), tt.observed); err != nil {
				t.Fatal(err)
			}
			if got := control.waits["demo-1"]; got != tt.want {
				t.Fatalf("wait count = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestReconcileAvailabilitySkipsDisabledAndUnreachableSemiSync(t *testing.T) {
	t.Parallel()
	control := &recordingSemiSyncControl{waits: map[string]int{}}
	r := &Reconciler{semiSyncControl: control}

	if err := r.ReconcileAvailability(context.Background(), testCluster(), availability("demo-2")); err != nil {
		t.Fatal(err)
	}
	cluster := semiSyncTestCluster(mysqlv1alpha1.DataDurabilityPreferred)
	unreachable := topology.AvailabilityState{PrimaryName: "demo-1", Instances: map[string]topology.InstanceAvailability{}}
	if err := r.ReconcileAvailability(context.Background(), cluster, unreachable); err != nil {
		t.Fatal(err)
	}
	if len(control.waits) != 0 {
		t.Fatalf("wait calls = %v, want none", control.waits)
	}
}

func semiSyncTestCluster(durability string) *mysqlv1alpha1.Cluster {
	cluster := testCluster()
	cluster.Spec.Instances = 3
	cluster.Spec.MinSyncReplicas = 2
	cluster.Spec.MySQL.SemiSync = &mysqlv1alpha1.SemiSyncConfiguration{Enabled: true, DataDurability: durability}
	return cluster
}

func availability(readyReplicas ...string) topology.AvailabilityState {
	instances := map[string]topology.InstanceAvailability{"demo-1": {Ready: true}}
	for _, name := range readyReplicas {
		instances[name] = topology.InstanceAvailability{Ready: true}
	}
	return topology.AvailabilityState{PrimaryName: "demo-1", Instances: instances}
}

func fencedAvailability(instance string) topology.AvailabilityState {
	observed := availability("demo-2", "demo-3")
	observed.FencedInstances = []string{instance}
	return observed
}
