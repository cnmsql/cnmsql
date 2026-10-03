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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
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

// TestInitdbArgsCarryPostInitSQL asserts initdbArgs passes each
// spec.bootstrap.initdb.postInitSQL statement to the bootstrap command as a
// repeatable --post-init-sql flag (issue #142). Without the flag the
// statements never run, and invalid SQL cannot fail the bootstrap.
func TestInitdbArgsCarryPostInitSQL(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Bootstrap.InitDB.PostInitSQL = []string{
		"CREATE TABLE app.t (id INT)",
		"THIS IS NOT SQL",
	}
	args := (&ClusterReconciler{}).initdbArgs(cluster, cluster.Spec.Bootstrap.InitDB)
	for _, stmt := range cluster.Spec.Bootstrap.InitDB.PostInitSQL {
		want := "--post-init-sql=" + stmt
		if !slices.Contains(args, want) {
			t.Errorf("initdb args %v lack %s", args, want)
		}
	}
}

// TestInitdbArgsEscapesPostInitSQLDollars asserts a user statement cannot let
// the kubelet expand $(VAR) references in the argument: "$$" is a literal "$",
// the same escaping importArgs applies to post-import SQL.
func TestInitdbArgsEscapesPostInitSQLDollars(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Bootstrap.InitDB.PostInitSQL = []string{
		`SELECT JSON_VALUE('{}', '$(POD_NAME)')`,
	}
	args := (&ClusterReconciler{}).initdbArgs(cluster, cluster.Spec.Bootstrap.InitDB)
	want := "--post-init-sql=SELECT JSON_VALUE('{}', '$$(POD_NAME)')"
	if !slices.Contains(args, want) {
		t.Errorf("initdb args %v carry an unescaped $(VAR) reference, want %s", args, want)
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

func TestPodSpecHasNoBootstrapContainers(t *testing.T) {
	t.Parallel()
	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = ""
	plan := testPlan()
	plan.Instances = 2
	plan.Recovery = &recoveryPlan{
		Bucket: "bkt", ArchiveKey: "a", MetadataKey: "m",
		StoreEnv: []corev1.EnvVar{{Name: objectstore.EnvBucket, Value: "bkt"}},
	}
	plan.Import = &importPlan{Bucket: "bkt", DumpKey: "d", ManifestKey: "m"}
	r := &ClusterReconciler{}

	for ordinal := 1; ordinal <= 2; ordinal++ {
		spec := r.podSpec(cluster, plan, plan.instanceFor(cluster, ordinal))
		if got := containerNames(spec.InitContainers); !slices.Equal(got, []string{"bootstrap-controller"}) {
			t.Fatalf("instance %d init containers = %v, want only bootstrap-controller", ordinal, got)
		}
		for _, c := range append(spec.InitContainers, spec.Containers...) {
			for _, env := range c.Env {
				if env.Name == objectstore.EnvBucket {
					t.Fatalf("instance %d container %s carries the recovery object-store env", ordinal, c.Name)
				}
			}
		}
	}
}

// TestRunVolumeIsMemoryBackedAndCapped asserts the run volume, which holds
// the slow log, is a tmpfs the kernel caps at 32Mi (design 037): past the cap
// mysqld gets ENOSPC, which only drops slow log entries, instead of the
// eviction a disk-backed sizeLimit triggers.
func TestRunVolumeIsMemoryBackedAndCapped(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1))
	for _, v := range spec.Volumes {
		if v.Name != runVolumeName {
			continue
		}
		ed := v.EmptyDir
		if ed == nil || ed.Medium != corev1.StorageMediumMemory {
			t.Fatalf("run volume = %+v, want a memory-backed emptyDir", v.VolumeSource)
		}
		if ed.SizeLimit == nil || ed.SizeLimit.Cmp(resource.MustParse("32Mi")) != 0 {
			t.Fatalf("run volume sizeLimit = %v, want 32Mi", ed.SizeLimit)
		}
		return
	}
	t.Fatal("pod spec has no run volume")
}
