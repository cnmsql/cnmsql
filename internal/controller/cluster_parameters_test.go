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
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func TestDiffParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		desired map[string]string
		known   map[string]string
		want    map[string]*parametersChange
	}{
		{
			name:    "no change",
			desired: map[string]string{"max_connections": "500"},
			known:   map[string]string{"max_connections": "500"},
			want:    map[string]*parametersChange{},
		},
		{
			name:    "dash and underscore spellings are equivalent",
			desired: map[string]string{"max-connections": "500"},
			known:   map[string]string{"max_connections": "500"},
			want:    map[string]*parametersChange{},
		},
		{
			name:    "value change",
			desired: map[string]string{"max_connections": "800"},
			known:   map[string]string{"max_connections": "500"},
			want: map[string]*parametersChange{
				"max_connections": {Key: "max_connections", From: "500", To: "800"},
			},
		},
		{
			name:    "added parameter",
			desired: map[string]string{"wait_timeout": "600"},
			known:   map[string]string{},
			want: map[string]*parametersChange{
				"wait_timeout": {Key: "wait_timeout", From: "", To: "600"},
			},
		},
		{
			name:    "removed parameter is classified restart-required",
			desired: map[string]string{},
			known:   map[string]string{"max_connections": "500"},
			want: map[string]*parametersChange{
				"max_connections": {Key: "max_connections", From: "500", Removed: true, Status: paramRestartRequired},
			},
		},
		{
			name:    "spelling-only change of an existing value is no change",
			desired: map[string]string{"max_connections": "500"},
			known:   map[string]string{"max-connections": "500"},
			want:    map[string]*parametersChange{},
		},
		{
			name:    "spelling change with value change diffs the new value",
			desired: map[string]string{"max-connections": "800"},
			known:   map[string]string{"max_connections": "500"},
			want: map[string]*parametersChange{
				"max_connections": {Key: "max-connections", From: "500", To: "800"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := diffParameters(tt.desired, tt.known)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("diffParameters() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestClassifyReloadResults(t *testing.T) {
	t.Parallel()

	changes := map[string]*parametersChange{
		"max_connections":    {From: "500", To: "800"},
		"wait_timeout":       {From: "600", To: "14400"},
		"performance_schema": {From: "ON", To: "OFF", Status: paramRestartRequired},
	}

	classifyReloadResults(changes, &webserver.ReloadResponse{
		Applied: []string{"max_connections", "wait_timeout"},
		Skipped: map[string]string{"innodb_flush_method": "Variable 'innodb_flush_method' is a read only variable"},
	})

	if changes["max_connections"].Status != paramDynamic {
		t.Fatalf("max_connections status = %q, want dynamic", changes["max_connections"].Status)
	}
	if changes["wait_timeout"].Status != paramDynamic {
		t.Fatalf("wait_timeout status = %q, want dynamic", changes["wait_timeout"].Status)
	}
	if changes["innodb_flush_method"] != nil {
		t.Fatal("a parameter outside the change set must not gain an entry")
	}
	if changes["performance_schema"].Status != paramRestartRequired {
		t.Fatal("an already restart-required classification must stay sticky")
	}
}

func TestClassifyReloadResultsSkipIsRestartRequired(t *testing.T) {
	t.Parallel()

	changes := map[string]*parametersChange{
		"innodb_flush_method": {From: "fsync", To: "O_DSYNC"},
	}
	classifyReloadResults(changes, &webserver.ReloadResponse{
		Skipped: map[string]string{"innodb_flush_method": "read only"},
	})
	if changes["innodb_flush_method"].Status != paramRestartRequired {
		t.Fatalf("status = %q, want restartRequired", changes["innodb_flush_method"].Status)
	}
}

func TestAbsorbParameterChanges(t *testing.T) {
	t.Parallel()

	t.Run("refuses to absorb while any change is unclassified", func(t *testing.T) {
		t.Parallel()
		state := &parametersState{Runtime: map[string]string{"max_connections": "500"}}
		changes := map[string]*parametersChange{
			"max_connections": {Key: "max_connections", From: "500", To: "800", Status: paramDynamic},
			"wait_timeout":    {Key: "wait_timeout", From: "600", To: "900"},
		}
		if _, _, ok := state.absorbParameterChanges(changes); ok {
			t.Fatal("absorb must refuse while wait_timeout is unclassified")
		}
		if state.Runtime["max_connections"] != "500" {
			t.Fatal("a refused absorb must not mutate state")
		}
	})

	t.Run("dynamic change reaches runtime only, template keeps the old value", func(t *testing.T) {
		t.Parallel()
		state := &parametersState{
			Runtime:  map[string]string{"max_connections": "500"},
			Template: map[string]string{"max_connections": "500"},
		}
		changes := map[string]*parametersChange{
			"max_connections": {Key: "max_connections", From: "500", To: "800", Status: paramDynamic},
		}
		dynamic, restart, ok := state.absorbParameterChanges(changes)
		if !ok {
			t.Fatal("absorb refused a fully classified change set")
		}
		if !reflect.DeepEqual(dynamic, []string{"max_connections"}) || len(restart) != 0 {
			t.Fatalf("dynamic = %v, restart = %v", dynamic, restart)
		}
		if state.Runtime["max_connections"] != "800" {
			t.Fatalf("runtime = %q, want 800", state.Runtime["max_connections"])
		}
		if state.Template["max_connections"] != "500" {
			t.Fatalf("template = %q, want the old 500 so the pod template hash stays", state.Template["max_connections"])
		}
	})

	t.Run("restart-required change reaches runtime and template", func(t *testing.T) {
		t.Parallel()
		state := &parametersState{
			Runtime:  map[string]string{"innodb_flush_method": "fsync"},
			Template: map[string]string{"innodb_flush_method": "fsync"},
		}
		changes := map[string]*parametersChange{
			"innodb_flush_method": {Key: "innodb_flush_method", From: "fsync", To: "O_DSYNC", Status: paramRestartRequired},
		}
		dynamic, restart, ok := state.absorbParameterChanges(changes)
		if !ok {
			t.Fatal("absorb refused a fully classified change set")
		}
		if len(dynamic) != 0 || !reflect.DeepEqual(restart, []string{"innodb_flush_method"}) {
			t.Fatalf("dynamic = %v, restart = %v", dynamic, restart)
		}
		if state.Runtime["innodb_flush_method"] != "O_DSYNC" || state.Template["innodb_flush_method"] != "O_DSYNC" {
			t.Fatalf("state = %+v, want O_DSYNC in both maps", state)
		}
	})

	t.Run("an added dynamic parameter never enters the template basis", func(t *testing.T) {
		t.Parallel()
		state := &parametersState{Runtime: map[string]string{}}
		changes := map[string]*parametersChange{
			"wait_timeout": {Key: "wait_timeout", From: "", To: "600", Status: paramDynamic},
		}
		if _, _, ok := state.absorbParameterChanges(changes); !ok {
			t.Fatal("absorb refused an added parameter")
		}
		if _, exists := state.Template["wait_timeout"]; exists {
			t.Fatal("the template basis must not gain a runtime-applied parameter: the pod hash was computed without it")
		}
		if got := templateHashParametersFor(t, state); len(got) != 0 {
			t.Fatalf("hash basis = %v, want empty (never fall back to the spec, it now holds the added parameter)", got)
		}
	})

	t.Run("removal drops the key from both maps", func(t *testing.T) {
		t.Parallel()
		state := &parametersState{
			Runtime:  map[string]string{"wait_timeout": "600"},
			Template: map[string]string{"wait_timeout": "600"},
		}
		changes := map[string]*parametersChange{
			"wait_timeout": {Key: "wait_timeout", From: "600", Removed: true, Status: paramRestartRequired},
		}
		if _, _, ok := state.absorbParameterChanges(changes); !ok {
			t.Fatal("absorb refused a removal")
		}
		if _, exists := state.Runtime["wait_timeout"]; exists {
			t.Fatal("runtime must drop a removed parameter")
		}
		if _, exists := state.Template["wait_timeout"]; exists {
			t.Fatal("template must drop a removed parameter")
		}
	})
}

// templateHashParametersFor resolves the hash basis exactly like
// templateHashParameters does for a cluster carrying the given state record.
func templateHashParametersFor(t *testing.T, state *parametersState) map[string]string {
	t.Helper()
	if state == nil {
		return nil
	}
	if state.Template == nil {
		return map[string]string{}
	}
	return state.Template
}

func TestParameterChangesFingerprint(t *testing.T) {
	t.Parallel()

	a := map[string]*parametersChange{"max_connections": {From: "500", To: "800"}}
	b := map[string]*parametersChange{"max_connections": {From: "500", To: "800"}}
	if fingerprintParameterChanges(a) != fingerprintParameterChanges(b) {
		t.Fatal("equal change sets must share a fingerprint")
	}
	c := map[string]*parametersChange{"max_connections": {From: "500", To: "900"}}
	if fingerprintParameterChanges(a) == fingerprintParameterChanges(c) {
		t.Fatal("a value change must move the fingerprint")
	}
}

// reloadOutcomeClient overrides recordingControlClient.Reload with a scripted
// per-parameter outcome: names in applied are reported Applied, everything else
// is reported Skipped with a fixed reason.
type reloadOutcomeClient struct {
	*recordingControlClient
	applied map[string]bool
}

func (c *reloadOutcomeClient) Reload(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName string, req webserver.ReloadRequest) (*webserver.ReloadResponse, error) {
	if c.reloaded == nil {
		c.reloaded = map[string]webserver.ReloadRequest{}
	}
	c.reloaded[instanceName] = req
	resp := &webserver.ReloadResponse{Skipped: map[string]string{}}
	for name := range req.Parameters {
		if c.applied[name] {
			resp.Applied = append(resp.Applied, name)
		} else {
			resp.Skipped[name] = "read only"
		}
	}
	return resp, nil
}

// parametersTestCluster builds a cluster with two instance names in status.
func parametersTestCluster(t *testing.T, params map[string]string) *mysqlv1alpha1.Cluster {
	t.Helper()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Spec.MySQL.Parameters = params
	cluster.Status.InstanceNames = []string{"demo-1", "demo-2"}
	return cluster
}

// readyInstancePod creates a ready instance Pod carrying the given annotations.
func readyInstancePod(name string, annotations map[string]string) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:        name,
		Namespace:   "default",
		Annotations: annotations,
	}}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return pod
}

func parametersStateFrom(t *testing.T, cluster *mysqlv1alpha1.Cluster) *parametersState {
	t.Helper()
	raw, ok := cluster.Annotations[parametersStateAnnotation]
	if !ok {
		return nil
	}
	state := &parametersState{}
	if err := json.Unmarshal([]byte(raw), state); err != nil {
		t.Fatalf("state annotation is not valid JSON: %v", err)
	}
	return state
}

func TestReconcileParametersSeedsStateOnFirstReconcile(t *testing.T) {
	t.Parallel()

	cluster := parametersTestCluster(t, map[string]string{"max_connections": "500"})
	scheme := testScheme(t)
	control := &reloadOutcomeClient{recordingControlClient: &recordingControlClient{}, applied: map[string]bool{}}
	reconciler := &ClusterReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	if err := reconciler.reconcileParameters(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	if len(control.reloaded) != 0 {
		t.Fatalf("seeding must not probe instances, got reloads on %v", control.reloaded)
	}
	state := parametersStateFrom(t, cluster)
	if state == nil || state.Runtime["max_connections"] != "500" {
		t.Fatalf("state = %+v, want runtime seeded with the spec parameters", state)
	}
	if state.Pending != nil {
		t.Fatalf("state = %+v, want no pending change set", state)
	}
	if cluster.Annotations[reloadAnnotation] != "" {
		t.Fatal("seeding must not request a reload")
	}
}

func TestReconcileParametersDynamicOnlyChange(t *testing.T) {
	t.Parallel()

	cluster := parametersTestCluster(t, map[string]string{"max_connections": "800"})
	cluster.Annotations = map[string]string{}
	if err := cluster.Annotations; err == nil {
		t.Fatal("unreachable")
	}
	seed := &parametersState{Runtime: map[string]string{"max_connections": "500"}, Template: map[string]string{"max_connections": "500"}}
	raw, err := json.Marshal(seed)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Annotations[parametersStateAnnotation] = string(raw)
	cluster.Annotations[reloadAnnotation] = "2026-01-01T00:00:00Z"

	pod1 := readyInstancePod("demo-1", map[string]string{reloadAppliedAnnotation: "2026-01-01T00:00:00Z"})
	pod2 := readyInstancePod("demo-2", nil)
	scheme := testScheme(t)
	control := &reloadOutcomeClient{recordingControlClient: &recordingControlClient{}, applied: map[string]bool{"max_connections": true}}
	reconciler := &ClusterReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pod1, pod2).Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	if err := reconciler.reconcileParameters(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	if len(control.reloaded) != 2 {
		t.Fatalf("reloads = %v, want both instances probed (demo-1 carries only the older token)", control.reloaded)
	}
	for name, req := range control.reloaded {
		if !reflect.DeepEqual(req.Parameters, map[string]string{"max_connections": "800"}) {
			t.Fatalf("probe parameters on %s = %v, want only the changed parameter", name, req.Parameters)
		}
	}

	state := parametersStateFrom(t, cluster)
	if state.Runtime["max_connections"] != "800" {
		t.Fatalf("runtime = %q, want 800 absorbed", state.Runtime["max_connections"])
	}
	if state.Template["max_connections"] != "500" {
		t.Fatalf("template = %q, want 500 so the pod template hash stays", state.Template["max_connections"])
	}
	// The verdict is absorbed but the pending record lingers until a later pass
	// confirms every instance was stamped, so catch-up probes stay changed-only.
	if state.Pending == nil {
		t.Fatal("pending = nil, want it kept until every instance recorded the token")
	}
	if cluster.Annotations[reloadAnnotation] == "2026-01-01T00:00:00Z" {
		t.Fatal("a parameter change must request a reload with a fresh token")
	}
	// Both pods were probed (demo-1 only carried the older token) and now carry
	// the fresh token, so the pass is idempotent on the next reconcile.
	token := cluster.Annotations[reloadAnnotation]
	if token == "" || token == "2026-01-01T00:00:00Z" {
		t.Fatalf("reload token = %q, want a fresh stamp", token)
	}
	for _, key := range []client.ObjectKey{client.ObjectKeyFromObject(pod1), client.ObjectKeyFromObject(pod2)} {
		updated := &corev1.Pod{}
		if err := reconciler.Get(context.Background(), key, updated); err != nil {
			t.Fatal(err)
		}
		if updated.Annotations[reloadAppliedAnnotation] != token {
			t.Fatalf("pod %s token = %q, want %q", key.Name, updated.Annotations[reloadAppliedAnnotation], token)
		}
	}

	// A later pass, with the change absorbed and every Pod stamped, retires the
	// pending record.
	if err := reconciler.reconcileParameters(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	state = parametersStateFrom(t, cluster)
	if state.Pending != nil {
		t.Fatalf("pending = %+v, want retired once every instance recorded the token", state.Pending)
	}
	if len(control.reloaded) != 2 {
		t.Fatalf("reloads = %v, want the retiring pass to probe nothing", control.reloaded)
	}
}

func TestReconcileParametersRestartRequiredChangeMovesTemplate(t *testing.T) {
	t.Parallel()

	cluster := parametersTestCluster(t, map[string]string{"innodb_flush_method": "O_DSYNC"})
	seed := &parametersState{Runtime: map[string]string{"innodb_flush_method": "fsync"}, Template: map[string]string{"innodb_flush_method": "fsync"}}
	raw, err := json.Marshal(seed)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Annotations = map[string]string{parametersStateAnnotation: string(raw)}

	pod := readyInstancePod("demo-1", nil)
	pod2 := readyInstancePod("demo-2", nil)
	scheme := testScheme(t)
	control := &reloadOutcomeClient{recordingControlClient: &recordingControlClient{}, applied: map[string]bool{}}
	reconciler := &ClusterReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pod, pod2).Build(),
		Scheme:        scheme,
		ControlClient: control,
		Recorder:      record.NewFakeRecorder(10),
	}

	if err := reconciler.reconcileParameters(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	state := parametersStateFrom(t, cluster)
	if state.Template["innodb_flush_method"] != "O_DSYNC" {
		t.Fatalf("template = %q, want O_DSYNC so the pod template hash moves and the roll applies my.cnf", state.Template["innodb_flush_method"])
	}
	if state.Runtime["innodb_flush_method"] != "O_DSYNC" {
		t.Fatalf("runtime = %q, want O_DSYNC accounted for by the roll", state.Runtime["innodb_flush_method"])
	}
	// The pending record lingers for changed-only catch-up until a later pass
	// retires it; both pods were stamped here, so one more pass clears it.
	if state.Pending == nil {
		t.Fatal("pending = nil, want it kept for catch-up bookkeeping")
	}
	var sawRestartEvent, sawAppliedEvent bool
	for _, event := range recorderEvents(reconciler) {
		switch event {
		case "ParametersRequireRestart":
			sawRestartEvent = true
		case "ParametersAppliedAtRuntime":
			sawAppliedEvent = true
		}
	}
	if !sawRestartEvent {
		t.Fatal("want a ParametersRequireRestart event")
	}
	if sawAppliedEvent {
		t.Fatal("no dynamic parameter was applied, want no ParametersAppliedAtRuntime event")
	}
}

func TestReconcileParametersDefersWithoutReadyInstances(t *testing.T) {
	t.Parallel()

	cluster := parametersTestCluster(t, map[string]string{"max_connections": "800"})
	cluster.Status.InstanceNames = []string{"demo-1"}
	seed := &parametersState{Runtime: map[string]string{"max_connections": "500"}}
	raw, err := json.Marshal(seed)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Annotations = map[string]string{parametersStateAnnotation: string(raw)}

	// demo-1 exists but is not Ready: no instance can be probed.
	notReady := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-1", Namespace: "default"}}
	notReady.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	scheme := testScheme(t)
	control := &reloadOutcomeClient{recordingControlClient: &recordingControlClient{}, applied: map[string]bool{}}
	reconciler := &ClusterReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, notReady).Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	if err := reconciler.reconcileParameters(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	if len(control.reloaded) != 0 {
		t.Fatalf("reloads = %v, want none while no instance is ready", control.reloaded)
	}
	state := parametersStateFrom(t, cluster)
	if state.Pending == nil || state.Pending.Changes["max_connections"] == nil {
		t.Fatalf("state = %+v, want the change kept pending", state)
	}
	if state.Runtime["max_connections"] != "500" {
		t.Fatal("an unprobed change must not be absorbed")
	}
	if cluster.Annotations[reloadAnnotation] == "" {
		t.Fatal("the deferred change must still request a reload token")
	}
}

// recorderEvents drains a FakeRecorder attached to the reconciler and returns
// the bare event reasons.
func recorderEvents(r *ClusterReconciler) []string {
	rec, ok := r.Recorder.(*record.FakeRecorder)
	if !ok {
		return nil
	}
	var reasons []string
	for {
		select {
		case evt := <-rec.Events:
			fields := strings.SplitN(evt, " ", 3)
			if len(fields) >= 2 {
				reasons = append(reasons, fields[1])
			}
		default:
			return reasons
		}
	}
}

func seededParametersCluster(t *testing.T, spec, runtime, template map[string]string) *mysqlv1alpha1.Cluster {
	t.Helper()
	cluster := parametersTestCluster(t, spec)
	cluster.Status.InstanceNames = []string{"demo-1"}
	seed := &parametersState{Runtime: runtime, Template: template}
	raw, err := json.Marshal(seed)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Annotations = map[string]string{parametersStateAnnotation: string(raw)}
	return cluster
}

func pendingWithToken(t *testing.T, cluster *mysqlv1alpha1.Cluster, token string) {
	t.Helper()
	state := parametersStateFrom(t, cluster)
	if state == nil {
		t.Fatal("cluster has no parameters state")
	}
	state.Pending = &parametersPending{
		Token:       token,
		Fingerprint: fingerprintParameterChanges(diffParameters(cluster.Spec.MySQL.Parameters, state.Runtime)),
		Changes:     diffParameters(cluster.Spec.MySQL.Parameters, state.Runtime),
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Annotations[parametersStateAnnotation] = string(raw)
	cluster.Annotations[reloadAnnotation] = token
}

func TestReconcileReloadSendsChangedParametersWhenPendingMatchesToken(t *testing.T) {
	t.Parallel()

	cluster := seededParametersCluster(t,
		map[string]string{"max_connections": "800", "wait_timeout": "600"},
		map[string]string{"max_connections": "500", "wait_timeout": "600"},
		map[string]string{"max_connections": "500", "wait_timeout": "600"})
	pendingWithToken(t, cluster, "2026-11-24T10:00:00Z")

	pod := readyInstancePod("demo-1", nil)
	scheme := testScheme(t)
	control := &reloadOutcomeClient{recordingControlClient: &recordingControlClient{}, applied: map[string]bool{"max_connections": true}}
	reconciler := &ClusterReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pod).Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	if err := reconciler.reconcileReload(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	req, ok := control.reloaded["demo-1"]
	if !ok {
		t.Fatal("demo-1 was not probed")
	}
	if !reflect.DeepEqual(req.Parameters, map[string]string{"max_connections": "800"}) {
		t.Fatalf("probe parameters = %v, want only the pending changed parameter", req.Parameters)
	}
}

func TestReconcileReloadSendsFullSpecForManualToken(t *testing.T) {
	t.Parallel()

	cluster := seededParametersCluster(t,
		map[string]string{"max_connections": "800", "wait_timeout": "600"},
		map[string]string{"max_connections": "800", "wait_timeout": "600"},
		map[string]string{"max_connections": "800", "wait_timeout": "600"})
	pendingWithToken(t, cluster, "2026-11-24T10:00:00Z")
	// A manual reload stamped a newer token afterwards.
	cluster.Annotations[reloadAnnotation] = "2026-11-24T11:00:00Z"

	pod := readyInstancePod("demo-1", nil)
	scheme := testScheme(t)
	control := &reloadOutcomeClient{recordingControlClient: &recordingControlClient{}, applied: map[string]bool{}}
	reconciler := &ClusterReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pod).Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	if err := reconciler.reconcileReload(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	req := control.reloaded["demo-1"]
	if !reflect.DeepEqual(req.Parameters, cluster.Spec.MySQL.Parameters) {
		t.Fatalf("manual probe parameters = %v, want the full spec for drift correction", req.Parameters)
	}
}

func TestReconcileReloadRecordsEvidenceIntoPending(t *testing.T) {
	t.Parallel()

	cluster := seededParametersCluster(t,
		map[string]string{"max_connections": "800"},
		map[string]string{"max_connections": "500"},
		map[string]string{"max_connections": "500"})
	pendingWithToken(t, cluster, "2026-11-24T10:00:00Z")
	// The instance the operator probes with refuses the parameter; evidence must
	// reach the pending record no matter which pass drove the reload.
	pod := readyInstancePod("demo-1", nil)
	scheme := testScheme(t)
	control := &reloadOutcomeClient{recordingControlClient: &recordingControlClient{}, applied: map[string]bool{}}
	reconciler := &ClusterReconciler{
		Client:        fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, pod).Build(),
		Scheme:        scheme,
		ControlClient: control,
	}

	if err := reconciler.reconcileReload(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	state := parametersStateFrom(t, cluster)
	change := state.Pending.Changes["max_connections"]
	if change == nil || change.Status != paramRestartRequired {
		t.Fatalf("pending change = %+v, want restartRequired recorded from the reload outcome", change)
	}
}

func TestReloadTokenAppliedGuardsAgainstNewerPodToken(t *testing.T) {
	t.Parallel()

	if !reloadTokenApplied("2026-11-24T11:00:00Z", "2026-11-24T10:00:00Z") {
		t.Fatal("a pod stamped by a newer pass must not be re-probed")
	}
	if reloadTokenApplied("2026-11-24T09:00:00Z", "2026-11-24T10:00:00Z") {
		t.Fatal("a pod stamped before the pass must be probed")
	}
	if !reloadTokenApplied("2026-11-24T10:00:00Z", "2026-11-24T10:00:00Z") {
		t.Fatal("an equal token means already applied")
	}
	if reloadTokenApplied("", "2026-11-24T10:00:00Z") {
		t.Fatal("an unstamped pod must be probed")
	}
	if reloadTokenApplied("garbage", "2026-11-24T10:00:00Z") {
		t.Fatal("an unparseable pod token must be re-probed, it proves nothing")
	}
}

func TestPodAnnotationsTemplateHashBasis(t *testing.T) {
	t.Parallel()

	r := &ClusterReconciler{Scheme: testScheme(t)}
	hashFor := func(t *testing.T, specParams map[string]string, state *parametersState) string {
		t.Helper()
		cluster := baseCluster()
		cluster.Spec.MySQL.Parameters = specParams
		if state != nil {
			raw, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			cluster.Annotations = map[string]string{parametersStateAnnotation: string(raw)}
		}
		plan := testPlan()
		inst := plan.instanceFor(cluster, 1)
		spec := r.podSpec(cluster, plan, inst)
		annotations, err := r.podAnnotations(cluster, plan, inst, labelsFor(cluster, inst.Name, roleOf(inst)), spec)
		if err != nil {
			t.Fatal(err)
		}
		return annotations[podTemplateHashAnnotation]
	}

	baseline := hashFor(t, map[string]string{"max_connections": "500"}, nil)
	if baseline == "" {
		t.Fatal("template hash is empty")
	}

	t.Run("a dynamic-only change does not move the template hash", func(t *testing.T) {
		t.Parallel()
		got := hashFor(t, map[string]string{"max_connections": "800"}, &parametersState{
			Runtime:  map[string]string{"max_connections": "800"},
			Template: map[string]string{"max_connections": "500"},
		})
		if got != baseline {
			t.Fatalf("template hash moved from %q to %q for a runtime-applied change; the Pod would be rolled", baseline, got)
		}
	})

	t.Run("a restart-required change moves the template hash", func(t *testing.T) {
		t.Parallel()
		got := hashFor(t, map[string]string{"max_connections": "800"}, &parametersState{
			Runtime:  map[string]string{"max_connections": "800"},
			Template: map[string]string{"max_connections": "800"},
		})
		if got == baseline {
			t.Fatalf("template hash %q did not move for a restart-required change; my.cnf would never be applied", got)
		}
	})

	t.Run("an absent record keeps the spec basis", func(t *testing.T) {
		t.Parallel()
		got := hashFor(t, map[string]string{"max_connections": "500"}, nil)
		if got != baseline {
			t.Fatal("an absent record must default to the spec parameters")
		}
	})
}
