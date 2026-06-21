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
	"slices"
	"strings"
	"testing"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

func archivingCluster() *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cloudnative-mysql"},
		ContinuousArchiving: &mysqlv1alpha1.ContinuousArchivingConfiguration{
			Enabled:          true,
			TargetRPOSeconds: 120,
		},
	}
	cluster.SetDefaults()
	return cluster
}

func TestArchivingDisabledByDefault(t *testing.T) {
	cluster := baseCluster()
	if cluster.IsArchivingEnabled() {
		t.Fatal("archiving should be off by default")
	}
	args := (&ClusterReconciler{}).runArgs(cluster, testPlan(), instancePlan{})
	for _, a := range args {
		if strings.Contains(a, "continuous-archiving") {
			t.Fatalf("unexpected archiving flag: %v", args)
		}
	}
}

func TestArchivingRunArgsAndEnv(t *testing.T) {
	cluster := archivingCluster()
	if !cluster.IsArchivingEnabled() {
		t.Fatal("archiving should be enabled")
	}

	args := (&ClusterReconciler{}).runArgs(cluster, testPlan(), instancePlan{})
	if !containsArg(args, "--continuous-archiving") {
		t.Fatalf("missing --continuous-archiving: %v", args)
	}
	if !containsArg(args, "--archive-rpo-seconds=120") {
		t.Fatalf("missing rpo arg: %v", args)
	}

	env := runEnv(cluster, testPlan())
	want := map[string]string{"cloudnative-mysql_S3_BUCKET": "backups", "cloudnative-mysql_S3_PATH": "cloudnative-mysql"}
	for name, value := range want {
		found := false
		for _, e := range env {
			if e.Name == name {
				found = true
				if e.Value != value {
					t.Fatalf("%s = %q, want %q", name, e.Value, value)
				}
			}
		}
		if !found {
			t.Fatalf("env %s not injected", name)
		}
	}
}

func TestArchivingMyCnfRendersDurability(t *testing.T) {
	cluster := archivingCluster()
	out, err := (&ClusterReconciler{}).renderMyCnf(cluster, testPlan(), instancePlan{ServerID: 1, IsPrimary: true, ServiceName: "demo-1"}, []string{"demo-1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{"sync_binlog = 1", "max_binlog_size = 16777216", "binlog_expire_logs_seconds = 604800"} {
		if !strings.Contains(out, needle) {
			t.Fatalf("rendered my.cnf missing %q:\n%s", needle, out)
		}
	}
}

func TestAggregateArchivingFromPrimary(t *testing.T) {
	observed := observedCluster{
		PrimaryName: "demo-1",
		StatusByInstance: map[string]*webserver.Status{
			"demo-1": {Archiving: &webserver.ArchivingStatus{
				Active: true, LastArchivedBinlog: "binlog.000005", LastArchivedGTID: "uuid:1-9", PendingFiles: 1,
			}},
		},
	}
	got := aggregateArchiving(observed)
	if !got.Enabled || got.LastArchivedBinlog != "binlog.000005" || got.PendingFiles != 1 {
		t.Fatalf("aggregated = %+v", got)
	}
	if !archivingHealthy(got) {
		t.Fatal("should be healthy with no failure")
	}
	got.LastFailureReason = "uploading binlog.000006: timeout"
	if archivingHealthy(got) {
		t.Fatal("should be unhealthy with a failure")
	}
}

func containsArg(args []string, want string) bool {
	return slices.Contains(args, want)
}
