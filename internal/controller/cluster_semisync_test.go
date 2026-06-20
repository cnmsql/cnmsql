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
	"strings"
	"testing"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// semiSyncCluster builds a 3-instance cluster with semi-sync on and the given
// minSyncReplicas / data durability.
func semiSyncCluster(durability string) *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Spec.MinSyncReplicas = 2
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

func TestReconcileAvailabilityRunsSemiSyncWhenClusterIsDegraded(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r := &ClusterReconciler{ControlClient: control}

	cluster := semiSyncCluster(mysqlv1alpha1.DataDurabilityPreferred)
	observed := observedWith("demo-2")
	observed.Ready = false

	r.reconcileAvailability(context.Background(), cluster, observed)

	if got := control.semiSyncWaits["demo-1"]; got != 1 {
		t.Fatalf("wait count = %d, want 1 while degraded", got)
	}
}

func TestRenderMyCnfUsesBootstrapSafeSemiSyncWaitCountForPreferredDurability(t *testing.T) {
	t.Parallel()

	cluster := semiSyncCluster(mysqlv1alpha1.DataDurabilityPreferred)
	out, err := (&ClusterReconciler{}).renderMyCnf(cluster, testPlan(), instancePlan{ServerID: 1, IsPrimary: true, ServiceName: "demo-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "loose-rpl_semi_sync_source_wait_for_replica_count = 1") {
		t.Fatalf("rendered my.cnf should start preferred semi-sync at one ack:\n%s", out)
	}
}

func TestRenderMyCnfKeepsConfiguredSemiSyncWaitCountForRequiredDurability(t *testing.T) {
	t.Parallel()

	cluster := semiSyncCluster(mysqlv1alpha1.DataDurabilityRequired)
	out, err := (&ClusterReconciler{}).renderMyCnf(cluster, testPlan(), instancePlan{ServerID: 1, IsPrimary: true, ServiceName: "demo-1"})
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
	cluster := semiSyncCluster(mysqlv1alpha1.DataDurabilityPreferred)
	cluster.Spec.MySQL.SemiSync.TimeoutMillis = &timeout
	args := (&ClusterReconciler{}).runArgs(cluster, testPlan(), instancePlan{})
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
