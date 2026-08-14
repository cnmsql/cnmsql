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

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// statusJSONResponse builds a /status JSON body for a fake control client.
func statusJSONResponse(t *testing.T, st *webserver.Status) string {
	t.Helper()
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatalf("marshalling status: %v", err)
	}
	return string(b)
}

// fakeStatusDialer returns a ControlDialer that serves st for the named
// instance and returns an error for any other (simulating an unreachable
// instance).
func fakeStatusDialer(t *testing.T, reachable string, st *webserver.Status) plugin.ControlDialer {
	t.Helper()
	body := statusJSONResponse(t, st)
	return func(_ context.Context, _ *mysqlv1alpha1.Cluster, instance string) (*plugin.ControlClient, error) {
		if instance != reachable {
			return nil, &testStatusErr{instance + " unreachable"}
		}
		return plugin.NewControlClientForTesting("instance.test", &http.Client{
			Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader(body)),
				}, nil
			}),
		}), nil
	}
}

type testStatusErr struct{ msg string }

func (e *testStatusErr) Error() string { return e.msg }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestRunStatusEnrichedSectionsAndDegradedRow(t *testing.T) {
	var buf bytes.Buffer
	origOut := plugin.Out
	plugin.Out = &buf
	t.Cleanup(func() { plugin.Out = origOut })

	cluster := testCluster()
	cluster.Status.Phase = phaseReady
	cluster.Status.Instances = 2
	cluster.Status.ReadyInstances = 2
	cluster.Status.CurrentPrimary = firstInstance
	cluster.Status.GTIDExecutedByInstance = map[string]string{firstInstance: "uuid:1-5"}
	cluster.Status.ReplicationLagByInstance = map[string]int64{"demo-2": 3}
	cluster.Status.Certificates = &mysqlv1alpha1.CertificatesStatus{
		Expirations: map[string]string{"ca.crt": "2099-01-01T00:00:00Z"},
	}
	cluster.Status.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingStatus{
		Enabled:            true,
		LastArchivedBinlog: "binlog.000005",
		LastArchivedGTID:   "uuid:1-5",
		PendingFiles:       0,
	}
	cluster.Status.ManagedRolesStatus = &mysqlv1alpha1.ManagedRolesStatus{
		ByStatus: map[mysqlv1alpha1.ManagedRoleStatus][]string{
			mysqlv1alpha1.ManagedRoleReconciled: {"app"},
		},
	}

	pods := []corev1.Pod{
		readyPod(firstInstance),
		readyPod("demo-2"),
	}
	installFakeEnv(t, cluster, pods)

	// demo-1 reachable with a live status; demo-2 unreachable (degraded row).
	st := &webserver.Status{
		InstanceName:  firstInstance,
		Role:          webserver.RolePrimary,
		IsReady:       true,
		UptimeSeconds: 3600,
		Replication:   &webserver.ReplicationStatus{IORunning: true, SQLRunning: true},
		Storage:       &webserver.StorageStatus{UsedBytes: 50, CapacityBytes: 100},
	}
	origDialer := statusDialer
	statusDialer = fakeStatusDialer(t, firstInstance, st)
	t.Cleanup(func() { statusDialer = origDialer })

	if err := runStatus(context.Background(), "demo", ""); err != nil {
		t.Fatalf("runStatus() error = %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"Cluster Summary",
		"Instances",
		firstInstance,
		"demo-2",
		"Continuous Archiving",
		"Managed Roles",
		"Certificates",
		"<unreachable>", // degraded row for demo-2 lag/uptime
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status output missing %q\noutput:\n%s", want, out)
		}
	}
}

func TestRunStatusJSONSnapshotIncludesInstances(t *testing.T) {
	var buf bytes.Buffer
	origOut := plugin.Out
	plugin.Out = &buf
	t.Cleanup(func() { plugin.Out = origOut })

	cluster := testCluster()
	cluster.Status.Phase = "Ready"
	cluster.Status.Instances = 1
	cluster.Status.ReadyInstances = 1
	cluster.Status.CurrentPrimary = firstInstance
	pods := []corev1.Pod{readyPod(firstInstance)}
	installFakeEnv(t, cluster, pods)

	st := &webserver.Status{InstanceName: firstInstance, IsReady: true}
	origDialer := statusDialer
	statusDialer = fakeStatusDialer(t, firstInstance, st)
	t.Cleanup(func() { statusDialer = origDialer })

	if err := runStatus(context.Background(), "demo", "json"); err != nil {
		t.Fatalf("runStatus() error = %v", err)
	}

	var report clusterStatusReport
	if err := json.Unmarshal(buf.Bytes(), &report); err != nil {
		t.Fatalf("decoding json snapshot: %v\nbody: %s", err, buf.String())
	}
	if report.Cluster == nil || report.Cluster.Name != "demo" {
		t.Errorf("snapshot cluster = %v, want demo", report.Cluster)
	}
	if len(report.Instances) != 1 || report.Instances[0].Instance != firstInstance {
		t.Errorf("snapshot instances = %v, want demo-1", report.Instances)
	}
	if report.Instances[0].Status == nil || !report.Instances[0].Status.IsReady {
		t.Errorf("snapshot instance status not populated: %+v", report.Instances[0].Status)
	}
}

func readyPod(name string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "test",
			Labels: map[string]string{
				plugin.ClusterLabel: "demo",
				plugin.RoleLabel:    "replica",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionTrue,
			}},
		},
	}
}
