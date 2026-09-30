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
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func archivingCluster() *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cnmsql"},
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
	// The purge gate is off unless asked for, so binlogs stay on disk until
	// binlogExpireSeconds and a lagged replica can still catch up.
	if !containsArg(args, "--archive-purge=false") {
		t.Fatalf("missing --archive-purge=false: %v", args)
	}

	env := runEnv(cluster, testPlan())
	want := map[string]string{"cnmsql_S3_BUCKET": "backups", "cnmsql_S3_PATH": "cnmsql"}
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

func TestArchivingPurgeGateOptIn(t *testing.T) {
	cluster := archivingCluster()
	enabled := true
	cluster.Spec.Backup.ContinuousArchiving.PurgeAfterArchive = &enabled

	if !cluster.IsPurgeAfterArchiveEnabled() {
		t.Fatal("purge gate should be enabled when purgeAfterArchive is true")
	}
	args := (&ClusterReconciler{}).runArgs(cluster, testPlan(), instancePlan{})
	if !containsArg(args, "--archive-purge=true") {
		t.Fatalf("missing --archive-purge=true: %v", args)
	}
}

// An unset purgeAfterArchive must read as off, so a cluster created before the
// field existed keeps its binlogs rather than purging them on archive.
func TestArchivingPurgeGateDefaultsOff(t *testing.T) {
	cluster := archivingCluster()
	cluster.Spec.Backup.ContinuousArchiving.PurgeAfterArchive = nil
	if cluster.IsPurgeAfterArchiveEnabled() {
		t.Fatal("purge gate should be off when purgeAfterArchive is unset")
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

func TestAggregateArchivingCarriesPurgeHold(t *testing.T) {
	observed := observedCluster{
		PrimaryName: "demo-1",
		StatusByInstance: map[string]*webserver.Status{
			"demo-1": {Archiving: &webserver.ArchivingStatus{
				Active: true, PurgeHeldBy: []string{"demo-3"}, PurgeHeldSince: "2026-09-30T10:00:00Z",
			}},
		},
	}
	got := aggregateArchiving(observed)
	if !slices.Equal(got.PurgeHeldBy, []string{"demo-3"}) || got.PurgeHeldSince == nil ||
		!got.PurgeHeldSince.Time.Equal(time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("aggregated = %+v", got)
	}
}

func purgingCluster(heldBy []string, since *time.Time) *mysqlv1alpha1.Cluster {
	cluster := archivingCluster()
	cluster.Spec.Backup.ContinuousArchiving.PurgeAfterArchive = new(true)
	cluster.Status.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingStatus{Enabled: true, PurgeHeldBy: heldBy}
	if since != nil {
		cluster.Status.ContinuousArchiving.PurgeHeldSince = &metav1.Time{Time: *since}
	}
	return cluster
}

func TestBinlogPurgeHeldCondition(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-time.Minute)
	old := now.Add(-binlogPurgeHeldAfter)

	cases := []struct {
		name    string
		cluster *mysqlv1alpha1.Cluster
		want    *metav1.ConditionStatus
	}{
		{"nothing held", purgingCluster(nil, nil), new(metav1.ConditionFalse)},
		// The newest files wait for the next GTID snapshot: normal, not reported.
		{"held briefly", purgingCluster([]string{"demo-2"}, &recent), new(metav1.ConditionFalse)},
		{"held past the threshold", purgingCluster([]string{"demo-2", "demo-3"}, &old), new(metav1.ConditionTrue)},
		{"purge gate off", archivingCluster(), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyBinlogPurgeHeldCondition(tc.cluster, now)
			got := apimeta.FindStatusCondition(tc.cluster.Status.Conditions, mysqlv1alpha1.ConditionBinlogPurgeHeld)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("condition should be absent with the gate off, got %+v", got)
				}
				return
			}
			if got == nil || got.Status != *tc.want {
				t.Fatalf("condition = %+v, want status %s", got, *tc.want)
			}
			if *tc.want == metav1.ConditionTrue && !strings.Contains(got.Message, "demo-2, demo-3") {
				t.Fatalf("message should name the holders: %q", got.Message)
			}
		})
	}
}

// Turning the gate off drops a condition that was set while it was on.
func TestBinlogPurgeHeldConditionRemovedWhenGateTurnsOff(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	cluster := purgingCluster([]string{"demo-2"}, &old)
	applyBinlogPurgeHeldCondition(cluster, time.Now())
	cluster.Spec.Backup.ContinuousArchiving.PurgeAfterArchive = new(false)
	applyBinlogPurgeHeldCondition(cluster, time.Now())
	if apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionBinlogPurgeHeld) != nil {
		t.Fatal("condition should be removed")
	}
}

func TestBinlogPurgeHeldEventOnlyOnTransition(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	cluster := purgingCluster([]string{"demo-2"}, &old)
	applyBinlogPurgeHeldCondition(cluster, time.Now())

	recorder := record.NewFakeRecorder(4)
	r := &ClusterReconciler{Recorder: recorder}
	r.recordBinlogPurgeHeldEvent(cluster, true)
	if len(recorder.Events) != 0 {
		t.Fatal("no event while the hold was already reported")
	}
	r.recordBinlogPurgeHeldEvent(cluster, false)
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "Warning BinlogPurgeHeld") || !strings.Contains(ev, "demo-2") {
			t.Fatalf("event = %q", ev)
		}
	default:
		t.Fatal("expected a warning event on the transition")
	}
}

func containsArg(args []string, want string) bool {
	return slices.Contains(args, want)
}
