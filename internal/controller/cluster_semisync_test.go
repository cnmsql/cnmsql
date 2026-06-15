/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"context"
	"strings"
	"testing"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
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
func observedWith(readyReplicas ...string) observedCluster {
	const primary = "demo-1"
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
	if err := r.reconcileSemiSync(context.Background(), cluster, observedWith("demo-2")); err != nil {
		t.Fatal(err)
	}
	if got := control.semiSyncWaits["demo-1"]; got != 1 {
		t.Fatalf("wait count = %d, want 1 (one healthy replica)", got)
	}
}

func TestReconcileAvailabilityRunsSemiSyncWhenClusterIsDegraded(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}

	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityPreferred)
	observed := observedWith("demo-2")
	observed.Ready = false

	r.reconcileAvailability(context.Background(), cluster, observed)

	if got := control.semiSyncWaits["demo-1"]; got != 1 {
		t.Fatalf("wait count = %d, want 1 while degraded", got)
	}
}

func TestReconcileSemiSyncPreferredReducesForFencedReplica(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}

	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityPreferred)
	observed := observedWith("demo-2", "demo-3")
	observed.FencedInstances = []string{"demo-2"}

	if err := r.reconcileSemiSync(context.Background(), cluster, observed); err != nil {
		t.Fatal(err)
	}
	if got := control.semiSyncWaits["demo-1"]; got != 1 {
		t.Fatalf("wait count = %d, want 1 with one fenced replica", got)
	}
}

func TestReconcileSemiSyncPreferredRestoresWhenHealthy(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}

	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityPreferred)
	if err := r.reconcileSemiSync(context.Background(), cluster, observedWith("demo-2", "demo-3")); err != nil {
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
	if err := r.reconcileSemiSync(context.Background(), cluster, observedWith()); err != nil {
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
	if err := r.reconcileSemiSync(context.Background(), cluster, observedWith("demo-2")); err != nil {
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
	if err := r.reconcileSemiSync(context.Background(), baseCluster(), observedWith("demo-2")); err != nil {
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

func TestRenderMyCnfUsesBootstrapSafeSemiSyncWaitCountForPreferredDurability(t *testing.T) {
	t.Parallel()

	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityPreferred)
	out, err := renderMyCnf(cluster, testPlan(), instancePlan{ServerID: 1, IsPrimary: true, ServiceName: "demo-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "loose-rpl_semi_sync_source_wait_for_replica_count = 1") {
		t.Fatalf("rendered my.cnf should start preferred semi-sync at one ack:\n%s", out)
	}
}

func TestRenderMyCnfKeepsConfiguredSemiSyncWaitCountForRequiredDurability(t *testing.T) {
	t.Parallel()

	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityRequired)
	out, err := renderMyCnf(cluster, testPlan(), instancePlan{ServerID: 1, IsPrimary: true, ServiceName: "demo-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "loose-rpl_semi_sync_source_wait_for_replica_count = 2") {
		t.Fatalf("rendered my.cnf should keep required semi-sync at minSyncReplicas:\n%s", out)
	}
}

func TestRunArgsEnableSemiSyncRuntimeConfiguration(t *testing.T) {
	t.Parallel()

	timeout := int32(5000)
	cluster := semiSyncCluster(2, mysqlv1alpha1.DataDurabilityPreferred)
	cluster.Spec.MySQL.SemiSync.TimeoutMillis = &timeout
	args := runArgs(cluster, testPlan(), instancePlan{})
	for _, want := range []string{
		"--semi-sync",
		"--semi-sync-wait-for-replica-count=1",
		"--semi-sync-timeout-millis=5000",
	} {
		if !strings.Contains(strings.Join(args, "\n"), want) {
			t.Fatalf("run args missing %q: %v", want, args)
		}
	}
}
