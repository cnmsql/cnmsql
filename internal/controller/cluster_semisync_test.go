/*
Copyright 2026 The CNMySQL Authors.

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

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

// semiSyncCluster builds a 3-instance cluster with semi-sync on and the given
// minSyncReplicas / data durability.
func semiSyncCluster(minSync int, durability string) *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Spec.MinSyncReplicas = minSync
	cluster.Spec.MySQL.SemiSync = &mysqlv1alpha1.SemiSyncConfiguration{
		Enabled:        true,
		DataDurability: durability,
	}
	return cluster
}

// observedWith builds an observedCluster where primary "demo-1" is reachable and
// the listed replicas report ready=true.
func observedWith(primary string, readyReplicas ...string) observedCluster {
	statuses := map[string]*webserver.Status{
		primary: {InstanceName: primary, Role: webserver.RolePrimary, IsReady: true},
	}
	for _, name := range readyReplicas {
		statuses[name] = &webserver.Status{InstanceName: name, Role: webserver.RoleReplica, IsReady: true}
	}
	return observedCluster{PrimaryName: primary, StatusByInstance: statuses}
}

func TestReconcileSemiSyncPreferredReducesOnUnhealthyReplica(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}

	// minSync=2 but only one healthy replica: preferred lowers the wait count.
	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityPreferred)
	if err := r.reconcileSemiSync(context.Background(), cluster, observedWith("demo-1", "demo-2")); err != nil {
		t.Fatal(err)
	}
	if got := control.semiSyncWaits["demo-1"]; got != 1 {
		t.Fatalf("wait count = %d, want 1 (one healthy replica)", got)
	}
}

func TestReconcileSemiSyncPreferredRestoresWhenHealthy(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}

	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityPreferred)
	if err := r.reconcileSemiSync(context.Background(), cluster, observedWith("demo-1", "demo-2", "demo-3")); err != nil {
		t.Fatal(err)
	}
	if got := control.semiSyncWaits["demo-1"]; got != 2 {
		t.Fatalf("wait count = %d, want 2 (configured minSyncReplicas restored)", got)
	}
}

func TestReconcileSemiSyncPreferredFloorsAtOne(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}

	// No healthy replicas: never drop below 1 acknowledgement.
	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityPreferred)
	if err := r.reconcileSemiSync(context.Background(), cluster, observedWith("demo-1")); err != nil {
		t.Fatal(err)
	}
	if got := control.semiSyncWaits["demo-1"]; got != 1 {
		t.Fatalf("wait count = %d, want 1 (floor)", got)
	}
}

func TestReconcileSemiSyncRequiredKeepsConfiguredCount(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}

	// Required durability never reduces the count, even with unhealthy replicas.
	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityRequired)
	if err := r.reconcileSemiSync(context.Background(), cluster, observedWith("demo-1", "demo-2")); err != nil {
		t.Fatal(err)
	}
	if got := control.semiSyncWaits["demo-1"]; got != 2 {
		t.Fatalf("wait count = %d, want 2 (required keeps minSyncReplicas)", got)
	}
}

func TestReconcileSemiSyncNoopWhenDisabledOrUnreachable(t *testing.T) {
	t.Parallel()

	// Semi-sync disabled: no control call.
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}
	if err := r.reconcileSemiSync(context.Background(), baseCluster(), observedWith("demo-1", "demo-2")); err != nil {
		t.Fatal(err)
	}
	if len(control.semiSyncWaits) != 0 {
		t.Fatalf("wait calls = %v, want none when semi-sync disabled", control.semiSyncWaits)
	}

	// Semi-sync on but primary unreachable: nothing to drive.
	cluster := semiSyncCluster(1, mysqlv1alpha1.DataDurabilityPreferred)
	if err := r.reconcileSemiSync(context.Background(), cluster, observedCluster{PrimaryName: "demo-1"}); err != nil {
		t.Fatal(err)
	}
	if len(control.semiSyncWaits) != 0 {
		t.Fatalf("wait calls = %v, want none when primary unreachable", control.semiSyncWaits)
	}
}
