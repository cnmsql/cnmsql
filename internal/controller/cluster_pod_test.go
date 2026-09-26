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
	"slices"
	"testing"
)

// TestPrestopHookNamesCluster asserts the preStop hook carries the owning
// Cluster's name: prestop reads its control password from the cluster's
// credential Secrets through the Kubernetes API, and needs --cluster-name to
// locate them.
func TestPrestopHookNamesCluster(t *testing.T) {
	t.Parallel()
	cluster := baseCluster() // switchover-on-drain is default-enabled
	plan := testPlan()
	spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1))
	cmd := spec.Containers[0].Lifecycle.PreStop.Exec.Command
	if !slices.Contains(cmd, "--cluster-name="+cluster.Name) {
		t.Fatalf("prestop command %v lacks --cluster-name", cmd)
	}
}

// TestBootstrapArgsNameCluster asserts every bootstrap command's args carry the
// owning Cluster's name: initdb, join, restore and import read their passwords
// from the cluster's credential Secrets through the Kubernetes API, and need
// --cluster-name to locate them.
func TestBootstrapArgsNameCluster(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	plan.ClusterName = cluster.Name
	plan.Recovery = &recoveryPlan{Bucket: "backups", ArchiveKey: "a", MetadataKey: "m"}
	plan.Import = &importPlan{Bucket: "backups", DumpKey: "d", ManifestKey: "m"}
	r := &ClusterReconciler{}
	want := "--cluster-name=" + cluster.Name
	for name, args := range map[string][]string{
		"initdb":  r.initdbArgs(cluster, cluster.Spec.Bootstrap.InitDB),
		"join":    joinArgs(cluster, plan),
		"restore": restoreArgs(plan),
		"import":  importArgs(plan),
	} {
		if !slices.Contains(args, want) {
			t.Fatalf("%s args %v lack %s", name, args, want)
		}
	}
}
