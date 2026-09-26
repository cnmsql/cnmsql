package credentials

import (
	"context"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func TestSecretNames(t *testing.T) {
	c := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	got := SecretNames(c)
	if _, ok := got[App]; ok {
		t.Fatal("app must be absent without initdb")
	}
	if len(got) != 4 {
		t.Fatalf("SecretNames = %v, want exactly root, control, backup, dump", got)
	}
	if got[Root] != "demo-root" || got[Control] != "demo-control" ||
		got[Backup] != "demo-backup" || got[Dump] != "demo-dump" {
		t.Fatalf("SecretNames = %v", got)
	}
}

func TestOpenSecretsMode(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = mysqlv1alpha1.AddToScheme(scheme)
	cluster := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: ns}}
	src, err := Open(context.Background(), Options{
		Mode: ModeSecrets, Namespace: ns, ClusterName: "demo",
		Cluster: crfake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Secrets: fake.NewClientset(secret("demo-control", "c1")),
	}, Control)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := src.Password(Control); p != "c1" {
		t.Fatalf("control = %q", p)
	}
	if _, ok := src.(*Provider); !ok {
		t.Fatal("secrets mode must return a *Provider so run can start its watch")
	}
}

func TestOpenEnvMode(t *testing.T) {
	t.Setenv("MYSQL_ROOT_PASSWORD", "r")
	src, err := Open(context.Background(), Options{Mode: ModeEnv}, Root)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := src.Password(Root); p != "r" {
		t.Fatalf("root = %q", p)
	}
	t.Setenv("MYSQL_BACKUP_PASSWORD", "") // restores the original on cleanup
	_ = os.Unsetenv("MYSQL_BACKUP_PASSWORD")
	if _, err := Open(context.Background(), Options{Mode: ModeEnv}, Backup); err == nil {
		t.Fatal("env mode must fail when a required variable is unset")
	}
}

func TestOpenSecretsModeNeedsCluster(t *testing.T) {
	if _, err := Open(context.Background(), Options{Mode: ModeSecrets, Namespace: ns}); err == nil {
		t.Fatal("secrets mode without --cluster-name must fail")
	}
}

func TestOpenEnvModeDumpHasNoSource(t *testing.T) {
	_, err := Open(context.Background(), Options{Mode: ModeEnv}, Dump)
	const want = "credentials: dump has no environment variable source"
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
}
