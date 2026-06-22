package cmd

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func TestBackupCommandCreatesBackup(t *testing.T) {
	env := installFakeEnv(t, testCluster(), nil)
	command := newBackupCommand()
	command.SetArgs([]string{"demo", "--name=before-upgrade", "--target=primary"})
	if err := command.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	backup := &mysqlv1alpha1.Backup{}
	key := types.NamespacedName{Namespace: "test", Name: "before-upgrade"}
	if err := env.Client.Get(context.Background(), key, backup); err != nil {
		t.Fatalf("getting created Backup: %v", err)
	}
	if backup.Spec.Cluster.Name != "demo" || backup.Spec.Target != mysqlv1alpha1.BackupTargetPrimary {
		t.Errorf("created Backup spec = %#v", backup.Spec)
	}
}

func TestRunMaintenance(t *testing.T) {
	env := installFakeEnv(t, testCluster(), nil)
	if err := runMaintenance(context.Background(), "demo", true, true); err != nil {
		t.Fatalf("runMaintenance(set) error = %v", err)
	}
	cluster := getTestCluster(t, env)
	if cluster.Spec.NodeMaintenanceWindow == nil || !cluster.Spec.NodeMaintenanceWindow.InProgress ||
		cluster.Spec.NodeMaintenanceWindow.ReusePVC == nil || !*cluster.Spec.NodeMaintenanceWindow.ReusePVC {
		t.Fatalf("maintenance window = %#v", cluster.Spec.NodeMaintenanceWindow)
	}
	if err := runMaintenance(context.Background(), "demo", false, false); err != nil {
		t.Fatalf("runMaintenance(unset) error = %v", err)
	}
	if getTestCluster(t, env).Spec.NodeMaintenanceWindow.InProgress {
		t.Error("maintenance window still in progress")
	}
}

func TestRunReloadSetsAnnotation(t *testing.T) {
	env := installFakeEnv(t, testCluster(), nil)
	if err := runReload(context.Background(), "demo"); err != nil {
		t.Fatalf("runReload() error = %v", err)
	}
	if value := getTestCluster(t, env).Annotations[plugin.ReloadAnnotation]; value == "" {
		t.Error("reload annotation was not set")
	}
}

func TestRunFence(t *testing.T) {
	pod := testPod("demo-2")
	env := installFakeEnv(t, testCluster(), []corev1.Pod{pod})
	if err := runFence(context.Background(), true, "demo", pod.Name); err != nil {
		t.Fatalf("runFence(on) error = %v", err)
	}
	got := &corev1.Pod{}
	if err := env.Client.Get(context.Background(), client.ObjectKeyFromObject(&pod), got); err != nil {
		t.Fatalf("getting Pod: %v", err)
	}
	if got.Annotations[plugin.FencingAnnotation] != plugin.FencingValue {
		t.Errorf("fencing annotation = %q", got.Annotations[plugin.FencingAnnotation])
	}
	pod.Annotations = map[string]string{plugin.FencingAnnotation: plugin.FencingValue}
	env = installFakeEnv(t, testCluster(), []corev1.Pod{pod})
	if err := runFence(context.Background(), false, "demo", pod.Name); err != nil {
		t.Fatalf("runFence(off) error = %v", err)
	}
	if err := env.Client.Get(context.Background(), client.ObjectKeyFromObject(&pod), got); err != nil {
		t.Fatalf("getting Pod: %v", err)
	}
	if _, exists := got.Annotations[plugin.FencingAnnotation]; exists {
		t.Error("fencing annotation was not removed")
	}
}

func TestRunRestart(t *testing.T) {
	pod := testPod("demo-2")
	env := installFakeEnv(t, testCluster(), []corev1.Pod{pod})
	if err := runRestart(context.Background(), "demo", pod.Name, true); err != nil {
		t.Fatalf("runRestart(instance) error = %v", err)
	}
	if _, err := env.Clientset.CoreV1().Pods("test").Get(context.Background(), pod.Name, metav1.GetOptions{}); err == nil {
		t.Error("restarted Pod still exists")
	}
}

func TestRunRollingRestartSetsAnnotation(t *testing.T) {
	env := installFakeEnv(t, testCluster(), nil)
	if err := runRestart(context.Background(), "demo", "", true); err != nil {
		t.Fatalf("runRestart(rolling) error = %v", err)
	}
	if value := getTestCluster(t, env).Annotations[plugin.RestartAnnotation]; value == "" {
		t.Error("restart annotation was not set")
	}
}

func TestRunReinit(t *testing.T) {
	cluster := testCluster()
	cluster.Annotations = map[string]string{plugin.ReinitAnnotation: firstInstance}
	pod := testPod("demo-2")
	env := installFakeEnv(t, cluster, []corev1.Pod{pod})
	if err := runReinit(context.Background(), "demo", pod.Name, true); err != nil {
		t.Fatalf("runReinit() error = %v", err)
	}
	if got := getTestCluster(t, env).Annotations[plugin.ReinitAnnotation]; got != firstInstance+",demo-2" {
		t.Errorf("reinit annotation = %q", got)
	}
}

func TestRunReinitRejectsInvalidTarget(t *testing.T) {
	cluster := testCluster()
	cluster.Status.CurrentPrimary = firstInstance
	installFakeEnv(t, cluster, []corev1.Pod{testPod("demo-2")})
	err := runReinit(context.Background(), "demo", firstInstance, true)
	if err == nil || !strings.Contains(err.Error(), "current primary") {
		t.Fatalf("primary error = %v", err)
	}
	err = runReinit(context.Background(), "demo", "missing", true)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing instance error = %v", err)
	}
}

func TestRunFenceRejectsMissingInstance(t *testing.T) {
	installFakeEnv(t, testCluster(), []corev1.Pod{testPod(firstInstance)})
	err := runFence(context.Background(), true, "demo", "missing")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("runFence() error = %v", err)
	}
}

func TestRunPromoteValidationAndPatch(t *testing.T) {
	cluster := testCluster()
	cluster.Status.CurrentPrimary = firstInstance
	cluster.Status.InstanceNames = []string{firstInstance, "demo-2"}
	env := installFakeEnv(t, cluster, nil)
	if err := runPromote(context.Background(), "demo", "demo-2"); err != nil {
		t.Fatalf("runPromote() error = %v", err)
	}
	got := getTestCluster(t, env)
	if got.Status.TargetPrimary != "demo-2" || got.Status.TargetPrimaryTimestamp == "" {
		t.Errorf("target primary status = %#v", got.Status)
	}

	tests := []struct {
		name     string
		mutate   func(*mysqlv1alpha1.Cluster)
		instance string
		want     string
	}{
		{name: "already primary", instance: firstInstance, want: "already the current primary"},
		{name: "unknown", instance: "demo-3", want: "not part of cluster"},
		{
			name: "diverged", instance: "demo-2", want: "diverged",
			mutate: func(c *mysqlv1alpha1.Cluster) { c.Status.DivergedInstances = []string{"demo-2"} },
		},
		{
			name: "fenced", instance: "demo-2", want: "fenced",
			mutate: func(c *mysqlv1alpha1.Cluster) { c.Status.FencedInstances = []string{"demo-2"} },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := testCluster()
			candidate.Status.CurrentPrimary = firstInstance
			candidate.Status.InstanceNames = []string{firstInstance, "demo-2"}
			if tt.mutate != nil {
				tt.mutate(&candidate)
			}
			installFakeEnv(t, candidate, nil)
			err := runPromote(context.Background(), "demo", tt.instance)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func installFakeEnv(t *testing.T, cluster mysqlv1alpha1.Cluster, pods []corev1.Pod) *plugin.Env {
	t.Helper()
	objects := make([]client.Object, 0, 1+len(pods))
	objects = append(objects, cluster.DeepCopy())
	coreObjects := make([]runtime.Object, 0, len(pods))
	for i := range pods {
		objects = append(objects, pods[i].DeepCopy())
		coreObjects = append(coreObjects, pods[i].DeepCopy())
	}
	env := &plugin.Env{
		Namespace: "test",
		Client: clientfake.NewClientBuilder().WithScheme(plugin.Scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).WithObjects(objects...).Build(),
		Clientset: clientsetfake.NewClientset(coreObjects...),
	}
	previous := newEnv
	newEnv = func() (*plugin.Env, error) { return env, nil }
	t.Cleanup(func() { newEnv = previous })
	return env
}

func testCluster() mysqlv1alpha1.Cluster {
	return mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "test"}}
}

func testPod(name string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "test", Labels: map[string]string{plugin.ClusterLabel: "demo"},
	}}
}

func getTestCluster(t *testing.T, env *plugin.Env) *mysqlv1alpha1.Cluster {
	t.Helper()
	cluster := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: "test", Name: "demo"}
	if err := env.Client.Get(context.Background(), key, cluster); err != nil {
		t.Fatalf("getting Cluster: %v", err)
	}
	return cluster
}
