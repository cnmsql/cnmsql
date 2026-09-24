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
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func TestRunRestartRejectsForeignInstance(t *testing.T) {
	cluster := testCluster()
	cluster.Status.InstanceNames = []string{firstInstance}
	installFakeEnv(t, cluster, []corev1.Pod{testPod("other-app-0")})
	err := runRestart(context.Background(), "demo", "other-app-0", true)
	if err == nil || !strings.Contains(err.Error(), "not part of cluster") {
		t.Fatalf("runRestart() error = %v, want a membership error", err)
	}
}

func TestPrimaryRestartPrompt(t *testing.T) {
	cluster := testCluster()
	cluster.Status.InstanceNames = []string{firstInstance}
	if got := primaryRestartPrompt(&cluster, firstInstance); !strings.Contains(got, "only instance") {
		t.Errorf("single instance prompt = %q", got)
	}
	cluster.Status.InstanceNames = []string{firstInstance, "demo-2"}
	if got := primaryRestartPrompt(&cluster, firstInstance); !strings.Contains(got, "switches over") {
		t.Errorf("switchover prompt = %q", got)
	}
	disabled := false
	cluster.Spec.EnableSwitchoverOnDrain = &disabled
	if got := primaryRestartPrompt(&cluster, firstInstance); !strings.Contains(got, "failover") {
		t.Errorf("failover prompt = %q", got)
	}
}

func TestFenceConsequence(t *testing.T) {
	cluster := testCluster()
	cluster.Status.CurrentPrimary = firstInstance
	pods := []corev1.Pod{testPod(firstInstance), testPod("demo-2")}
	if got := fenceConsequence(&cluster, "*", pods); !strings.Contains(got, "all 2 instances") {
		t.Errorf("fence '*' prompt = %q", got)
	}
	if got := fenceConsequence(&cluster, firstInstance, pods[:1]); !strings.Contains(got, "stops all writes") {
		t.Errorf("fence primary prompt = %q", got)
	}
	if got := fenceConsequence(&cluster, "demo-2", pods[1:]); got != "" {
		t.Errorf("fence replica prompt = %q, want none", got)
	}
}

func TestRunMaintenanceWritesReusePVCExplicitly(t *testing.T) {
	env := installFakeEnv(t, testCluster(), nil)
	if err := runMaintenance(context.Background(), "demo", true, false, true); err != nil {
		t.Fatalf("runMaintenance(set) error = %v", err)
	}
	window := getTestCluster(t, env).Spec.NodeMaintenanceWindow
	if window == nil || window.ReusePVC == nil || *window.ReusePVC {
		t.Fatalf("maintenance window = %#v, want reusePVC explicitly false", window)
	}
}

func TestMutatingCommandsRefuseAmbiguousCluster(t *testing.T) {
	first, second := testCluster(), testCluster()
	second.Name = "zeta"
	env := &plugin.Env{
		Namespace: "test",
		Client: clientfake.NewClientBuilder().WithScheme(plugin.Scheme).
			WithObjects(first.DeepCopy(), second.DeepCopy()).Build(),
		Clientset: clientsetfake.NewClientset(),
	}
	previous := newEnv
	newEnv = func() (*plugin.Env, error) { return env, nil }
	t.Cleanup(func() { newEnv = previous })

	err := runRestart(context.Background(), "", "", true)
	if err == nil || !strings.Contains(err.Error(), "specify which CLUSTER") {
		t.Fatalf("runRestart() error = %v, want an ambiguity error", err)
	}
	if _, ok := getTestCluster(t, env).Annotations[plugin.RestartAnnotation]; ok {
		t.Error("rolling restart was requested on a guessed cluster")
	}
}

// installDestroyEnv installs a fake environment holding the cluster, a Pod and
// a PVC named instance, with the PVC labeled for pvcCluster.
func installDestroyEnv(t *testing.T, instance, pvcCluster string) *plugin.Env {
	t.Helper()
	cluster := testCluster()
	pod := testPod(instance)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: instance, Namespace: "test",
		Labels: map[string]string{plugin.ClusterLabel: pvcCluster},
	}}
	env := &plugin.Env{
		Namespace: "test",
		Client: clientfake.NewClientBuilder().WithScheme(plugin.Scheme).
			WithObjects(cluster.DeepCopy(), pvc).Build(),
		Clientset: clientsetfake.NewClientset([]runtime.Object{pod.DeepCopy()}...),
	}
	previous := newEnv
	newEnv = func() (*plugin.Env, error) { return env, nil }
	t.Cleanup(func() { newEnv = previous })
	return env
}

func TestRunDestroyDeletesInstancePodAndPVC(t *testing.T) {
	env := installDestroyEnv(t, "demo-3", "demo")
	if err := runDestroy(context.Background(), "demo", "demo-3", false, true); err != nil {
		t.Fatalf("runDestroy() error = %v", err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	err := env.Client.Get(context.Background(), types.NamespacedName{Namespace: "test", Name: "demo-3"}, pvc)
	if !apierrors.IsNotFound(err) {
		t.Errorf("PVC still present: %v", err)
	}
	if _, err := env.Clientset.CoreV1().Pods("test").Get(context.Background(), "demo-3", metav1.GetOptions{}); err == nil {
		t.Error("Pod still present")
	}
}

func TestRunDestroyRefusesForeignPVC(t *testing.T) {
	env := installDestroyEnv(t, "demo-3", "someone-else")
	err := runDestroy(context.Background(), "demo", "demo-3", false, true)
	if err == nil || !strings.Contains(err.Error(), "does not belong to cluster") {
		t.Fatalf("runDestroy() error = %v, want a membership error", err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKey{Namespace: "test", Name: "demo-3"}
	if err := env.Client.Get(context.Background(), key, pvc); err != nil {
		t.Errorf("foreign PVC was touched: %v", err)
	}
}

func TestShellCommandTargetsAndArgs(t *testing.T) {
	cluster := testCluster()
	cluster.Status.CurrentPrimary = firstInstance
	cluster.Status.InstanceNames = []string{firstInstance, "demo-2"}
	installFakeEnv(t, cluster, nil)

	var got plugin.RootClientOptions
	previous := rootClient
	rootClient = func(_ context.Context, _ *plugin.Env, opts plugin.RootClientOptions) error {
		got = opts
		return nil
	}
	t.Cleanup(func() { rootClient = previous })

	tests := []struct {
		name     string
		args     []string
		instance string
		client   []string
		wantErr  string
	}{
		{name: "defaults to primary", args: []string{"demo"}, instance: firstInstance},
		{name: "explicit replica", args: []string{"demo", "demo-2"}, instance: "demo-2"},
		{
			name: "client args after dash", args: []string{"demo", "--", "-e", "SELECT 1"},
			instance: firstInstance, client: []string{"-e", "SELECT 1"},
		},
		{name: "foreign instance", args: []string{"demo", "other-0"}, wantErr: "not part of cluster"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got = plugin.RootClientOptions{}
			command := newShellCommand()
			command.SetArgs(tt.args)
			err := command.Execute()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Execute() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if got.Instance != tt.instance || strings.Join(got.Args, "|") != strings.Join(tt.client, "|") {
				t.Errorf("rootClient called with instance %q args %q, want %q %q",
					got.Instance, got.Args, tt.instance, tt.client)
			}
		})
	}
}
