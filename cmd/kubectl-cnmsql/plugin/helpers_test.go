package plugin

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func TestResolveCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusters  []mysqlv1alpha1.Cluster
		requested string
		want      string
		wantErr   string
	}{
		{name: "named", clusters: []mysqlv1alpha1.Cluster{cluster("wanted")}, requested: "wanted", want: "wanted"},
		{name: "sole cluster", clusters: []mysqlv1alpha1.Cluster{cluster("only")}, want: "only"},
		{
			name:     "multiple choose first alphabetically",
			clusters: []mysqlv1alpha1.Cluster{cluster("zeta"), cluster("alpha")}, want: "alpha",
		},
		{name: "none", wantErr: "no clusters found"},
		{name: "missing named", requested: "missing", wantErr: "getting cluster"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			objects := make([]runtime.Object, len(tt.clusters))
			for i := range tt.clusters {
				objects[i] = tt.clusters[i].DeepCopy()
			}
			env := &Env{
				Namespace: "test",
				Client:    clientfake.NewClientBuilder().WithScheme(Scheme).WithRuntimeObjects(objects...).Build(),
			}

			got, err := env.ResolveCluster(context.Background(), tt.requested)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ResolveCluster() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveCluster() error = %v", err)
			}
			if got.Name != tt.want {
				t.Errorf("ResolveCluster() = %q, want %q", got.Name, tt.want)
			}
		})
	}
}

func TestListPodsSelectsCluster(t *testing.T) {
	t.Parallel()

	env := &Env{Clientset: fake.NewClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "demo-1", Namespace: "test", Labels: map[string]string{ClusterLabel: "demo"},
		}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "other-1", Namespace: "test", Labels: map[string]string{ClusterLabel: "other"},
		}},
	)}
	testCluster := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "test"}}
	pods, err := env.ListPods(context.Background(), testCluster)
	if err != nil {
		t.Fatalf("ListPods() error = %v", err)
	}
	if len(pods) != 1 || pods[0].Name != "demo-1" {
		t.Errorf("ListPods() = %v, want demo-1", pods)
	}
}

func TestSmallHelpers(t *testing.T) {
	t.Parallel()

	t.Run("primary current", func(t *testing.T) {
		t.Parallel()
		c := &mysqlv1alpha1.Cluster{Status: mysqlv1alpha1.ClusterStatus{CurrentPrimary: "current", TargetPrimary: "target"}}
		if got := PrimaryInstance(c); got != "current" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("primary target fallback", func(t *testing.T) {
		t.Parallel()
		c := &mysqlv1alpha1.Cluster{Status: mysqlv1alpha1.ClusterStatus{TargetPrimary: "target"}}
		if got := PrimaryInstance(c); got != "target" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("pod ready", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}}}}
		if !PodReady(pod) {
			t.Error("PodReady() = false")
		}
		pod.Status.Conditions[0].Status = corev1.ConditionFalse
		if PodReady(pod) {
			t.Error("PodReady() = true for false condition")
		}
	})
	t.Run("contains", func(t *testing.T) {
		t.Parallel()
		if !Contains([]string{"a", "b"}, "b") || Contains([]string{"a"}, "b") {
			t.Error("Contains returned unexpected result")
		}
	})
}

func TestMonitoringTLSEnabled(t *testing.T) {
	t.Parallel()

	if MonitoringTLSEnabled(&mysqlv1alpha1.Cluster{}) {
		t.Error("MonitoringTLSEnabled() = true without monitoring config")
	}
	cluster := &mysqlv1alpha1.Cluster{Spec: mysqlv1alpha1.ClusterSpec{
		Monitoring: &mysqlv1alpha1.MonitoringConfiguration{
			TLSConfig: &mysqlv1alpha1.ClusterMonitoringTLSConfig{Enabled: true},
		},
	}}
	if !MonitoringTLSEnabled(cluster) {
		t.Error("MonitoringTLSEnabled() = false with TLS enabled")
	}
}

func TestPrintObjectRejectsUnsupportedFormat(t *testing.T) {
	t.Parallel()

	err := PrintObject(map[string]string{"key": "value"}, "toml")
	if err == nil || !strings.Contains(err.Error(), "unsupported output format") {
		t.Errorf("PrintObject() error = %v", err)
	}
}

func TestPortForwardClose(t *testing.T) {
	t.Parallel()

	stop := make(chan struct{})
	done := make(chan struct{})
	close(done)
	forward := &PortForward{stopCh: stop, doneCh: done}
	forward.Close()
	select {
	case <-stop:
	default:
		t.Error("Close did not close stop channel")
	}
	var nilForward *PortForward
	nilForward.Close()
}

func cluster(name string) mysqlv1alpha1.Cluster {
	return mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test"}}
}
