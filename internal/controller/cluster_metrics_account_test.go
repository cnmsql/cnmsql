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
	"context"
	"errors"
	"strings"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
)

func metricsCondition(t *testing.T, r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster) (*metav1.Condition, string) {
	t.Helper()
	latest := &mysqlv1alpha1.Cluster{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, latest); err != nil {
		t.Fatal(err)
	}
	return apimeta.FindStatusCondition(latest.Status.Conditions, mysqlv1alpha1.ConditionMetricsAccountReady), latest.ResourceVersion
}

func recordedEvents(r *ClusterReconciler) []string {
	var out []string
	rec := r.Recorder.(*record.FakeRecorder)
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestMetricsAccountWaitsForAReadyPrimary(t *testing.T) {
	cluster := baseCluster()
	control := &recordingControlClient{}
	r := dumpAccountReconciler(t, control, cluster)

	if err := r.reconcileMetricsAccount(context.Background(), cluster, observedWithPrimary(false, mysqlDefaultServerVersion)); err != nil {
		t.Fatal(err)
	}
	if len(control.metricsAccount) != 0 {
		t.Fatalf("called the primary while it is not ready")
	}
	cond, _ := metricsCondition(t, r, cluster)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "PrimaryNotReady" {
		t.Fatalf("condition = %+v", cond)
	}
}

func TestMetricsAccountSendsDeclaredPrivileges(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Monitoring = &mysqlv1alpha1.MonitoringConfiguration{Privileges: []mysqlv1alpha1.RolePrivilege{
		{Privileges: []string{"SELECT"}, On: "app.*"},
	}}
	control := &recordingControlClient{}
	r := dumpAccountReconciler(t, control, cluster)
	observed := observedWithPrimary(true, mysqlDefaultServerVersion)

	if err := r.reconcileMetricsAccount(context.Background(), cluster, observed); err != nil {
		t.Fatal(err)
	}
	if len(control.metricsAccount) != 1 || control.metricsAccount[0].Privileges[0].On != "app.*" {
		t.Fatalf("requests = %+v", control.metricsAccount)
	}
	cond, rv := metricsCondition(t, r, cluster)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Applied" {
		t.Fatalf("condition = %+v", cond)
	}
	if events := recordedEvents(r); len(events) != 0 {
		t.Fatalf("no change must not emit an event, got %q", events)
	}

	// A second pass with nothing to change must not write the status again.
	if err := r.reconcileMetricsAccount(context.Background(), cluster, observed); err != nil {
		t.Fatal(err)
	}
	if _, rv2 := metricsCondition(t, r, cluster); rv2 != rv {
		t.Fatalf("status rewritten in steady state: %s -> %s", rv, rv2)
	}

	// A spec edit bumps the generation: the condition records it even though
	// its text is unchanged.
	cluster.Generation++
	if err := r.reconcileMetricsAccount(context.Background(), cluster, observed); err != nil {
		t.Fatal(err)
	}
	if cond, _ := metricsCondition(t, r, cluster); cond.ObservedGeneration != cluster.Generation {
		t.Fatalf("observedGeneration = %d, want %d", cond.ObservedGeneration, cluster.Generation)
	}
}

func TestMetricsAccountReportsChanges(t *testing.T) {
	cluster := baseCluster()
	control := &recordingControlClient{metricsAccountResp: &user.MetricsAccountResponse{
		Created: true, Revoked: []string{"INSERT ON `app`.*"},
	}}
	r := dumpAccountReconciler(t, control, cluster)

	if err := r.reconcileMetricsAccount(context.Background(), cluster, observedWithPrimary(true, mysqlDefaultServerVersion)); err != nil {
		t.Fatal(err)
	}
	events := recordedEvents(r)
	if len(events) != 1 || !strings.Contains(events[0], "MetricsAccountUpdated") ||
		!strings.Contains(events[0], "created") || !strings.Contains(events[0], "INSERT ON `app`.*") {
		t.Fatalf("events = %q", events)
	}
}

func TestMetricsAccountSurfacesFailures(t *testing.T) {
	cluster := baseCluster()
	control := &recordingControlClient{metricsAccountErr: errors.New("instance /monitoring/account returned 404 Not Found")}
	r := dumpAccountReconciler(t, control, cluster)

	err := r.reconcileMetricsAccount(context.Background(), cluster, observedWithPrimary(true, mysqlDefaultServerVersion))
	if err == nil {
		t.Fatal("expected the error to be returned")
	}
	cond, _ := metricsCondition(t, r, cluster)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ApplyFailed" ||
		!strings.Contains(cond.Message, "404") {
		t.Fatalf("condition = %+v", cond)
	}

	// The best-effort wrapper swallows the error and warns.
	r.reconcileMetricsAccountBestEffort(context.Background(), cluster, observedWithPrimary(true, mysqlDefaultServerVersion))
	if events := recordedEvents(r); len(events) == 0 || !strings.Contains(events[len(events)-1], "MetricsAccountFailed") {
		t.Fatalf("events = %q", events)
	}
}
