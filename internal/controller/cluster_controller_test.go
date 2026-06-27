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
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := mysqlv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := monitoringv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func baseCluster() *mysqlv1alpha1.Cluster {
	cluster := &mysqlv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduledTestCluster,
			Namespace: "default",
		},
		Spec: mysqlv1alpha1.ClusterSpec{
			Instances: 1,
			Storage:   mysqlv1alpha1.StorageConfiguration{Size: "1Gi"},
			Bootstrap: &mysqlv1alpha1.BootstrapConfiguration{
				InitDB: &mysqlv1alpha1.BootstrapInitDB{
					Database: appName,
					Owner:    appName,
				},
			},
		},
	}
	cluster.SetDefaults()
	return cluster
}

type readyStatusClient struct{}

func (readyStatusClient) Status(context.Context, *mysqlv1alpha1.Cluster, string) (*webserver.Status, error) {
	return &webserver.Status{
		InstanceName:  "demo-1",
		Role:          webserver.RolePrimary,
		Version:       defaultMySQL80ServerVersion,
		IsReady:       true,
		GTIDExecuted:  "uuid:1-10",
		UptimeSeconds: int64(time.Minute.Seconds()),
	}, nil
}

func (readyStatusClient) Promote(context.Context, *mysqlv1alpha1.Cluster, string) error {
	return nil
}

func (readyStatusClient) Demote(context.Context, *mysqlv1alpha1.Cluster, string) error {
	return nil
}

func (readyStatusClient) ConfigureReplica(context.Context, *mysqlv1alpha1.Cluster, string, replication.SourceOptions) error {
	return nil
}

func (readyStatusClient) ListUsers(context.Context, *mysqlv1alpha1.Cluster, string) (*user.ListUsersResponse, error) {
	return &user.ListUsersResponse{}, nil
}

func (readyStatusClient) CreateUser(context.Context, *mysqlv1alpha1.Cluster, string, user.CreateUserRequest) error {
	return nil
}

func (readyStatusClient) AlterUser(context.Context, *mysqlv1alpha1.Cluster, string, user.AlterUserRequest) error {
	return nil
}

func (readyStatusClient) DropUser(context.Context, *mysqlv1alpha1.Cluster, string, user.DropUserRequest) error {
	return nil
}

func (readyStatusClient) CreateDatabase(context.Context, *mysqlv1alpha1.Cluster, string, user.CreateDatabaseRequest) error {
	return nil
}

func (readyStatusClient) DropDatabase(context.Context, *mysqlv1alpha1.Cluster, string, user.DropDatabaseRequest) error {
	return nil
}

func (readyStatusClient) ListDatabases(context.Context, *mysqlv1alpha1.Cluster, string) (*user.ListDatabasesResponse, error) {
	return &user.ListDatabasesResponse{}, nil
}

func (readyStatusClient) SetSemiSyncWaitForReplicaCount(context.Context, *mysqlv1alpha1.Cluster, string, int) error {
	return nil
}

func (readyStatusClient) Reload(context.Context, *mysqlv1alpha1.Cluster, string, webserver.ReloadRequest) (*webserver.ReloadResponse, error) {
	return &webserver.ReloadResponse{}, nil
}

func (readyStatusClient) UpgradeInstanceManager(context.Context, *mysqlv1alpha1.Cluster, string, io.Reader, string) error {
	return nil
}

func (readyStatusClient) SetAsPrimary(context.Context, *mysqlv1alpha1.Cluster, string, string) error {
	return nil
}

func (readyStatusClient) SetGroupCommunicationProtocol(context.Context, *mysqlv1alpha1.Cluster, string, string) error {
	return nil
}

type recordingControlClient struct {
	statuses   map[string]*webserver.Status
	demoted    []string
	promoted   []string
	configured map[string]replication.SourceOptions

	users   []user.UserInfo
	created []user.CreateUserRequest
	altered []user.AlterUserRequest
	dropped []user.DropUserRequest

	databases       []string
	createdDatabase []user.CreateDatabaseRequest
	droppedDatabase []user.DropDatabaseRequest

	semiSyncWaits map[string]int
	reloaded      map[string]webserver.ReloadRequest

	upgraded     []string
	upgradeHash  map[string]string
	upgradeBytes map[string][]byte

	setAsPrimaryInstances []string
	setAsPrimaryUUIDs     []string

	setCommunicationProtocolInstances []string
	setCommunicationProtocolVersions  []string
}

func (c *recordingControlClient) Status(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName string) (*webserver.Status, error) {
	return c.statuses[instanceName], nil
}

func (c *recordingControlClient) UpgradeInstanceManager(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName string, binary io.Reader, expectedHash string) error {
	c.upgraded = append(c.upgraded, instanceName)
	if c.upgradeHash == nil {
		c.upgradeHash = map[string]string{}
		c.upgradeBytes = map[string][]byte{}
	}
	c.upgradeHash[instanceName] = expectedHash
	body, err := io.ReadAll(binary)
	if err != nil {
		return err
	}
	c.upgradeBytes[instanceName] = body
	return nil
}

func (c *recordingControlClient) SetAsPrimary(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName, memberUUID string) error {
	c.setAsPrimaryInstances = append(c.setAsPrimaryInstances, instanceName)
	c.setAsPrimaryUUIDs = append(c.setAsPrimaryUUIDs, memberUUID)
	return nil
}

func (c *recordingControlClient) SetGroupCommunicationProtocol(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName, targetVersion string) error {
	c.setCommunicationProtocolInstances = append(c.setCommunicationProtocolInstances, instanceName)
	c.setCommunicationProtocolVersions = append(c.setCommunicationProtocolVersions, targetVersion)
	return nil
}

func (c *recordingControlClient) Promote(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName string) error {
	c.promoted = append(c.promoted, instanceName)
	return nil
}

func (c *recordingControlClient) Demote(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName string) error {
	c.demoted = append(c.demoted, instanceName)
	return nil
}

func (c *recordingControlClient) ConfigureReplica(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName string, source replication.SourceOptions) error {
	if c.configured == nil {
		c.configured = map[string]replication.SourceOptions{}
	}
	c.configured[instanceName] = source
	return nil
}

func (c *recordingControlClient) ListUsers(_ context.Context, _ *mysqlv1alpha1.Cluster, _ string) (*user.ListUsersResponse, error) {
	return &user.ListUsersResponse{Users: c.users}, nil
}

func (c *recordingControlClient) CreateUser(_ context.Context, _ *mysqlv1alpha1.Cluster, _ string, req user.CreateUserRequest) error {
	c.created = append(c.created, req)
	return nil
}

func (c *recordingControlClient) AlterUser(_ context.Context, _ *mysqlv1alpha1.Cluster, _ string, req user.AlterUserRequest) error {
	c.altered = append(c.altered, req)
	return nil
}

func (c *recordingControlClient) DropUser(_ context.Context, _ *mysqlv1alpha1.Cluster, _ string, req user.DropUserRequest) error {
	c.dropped = append(c.dropped, req)
	return nil
}

func (c *recordingControlClient) CreateDatabase(_ context.Context, _ *mysqlv1alpha1.Cluster, _ string, req user.CreateDatabaseRequest) error {
	c.createdDatabase = append(c.createdDatabase, req)
	return nil
}

func (c *recordingControlClient) DropDatabase(_ context.Context, _ *mysqlv1alpha1.Cluster, _ string, req user.DropDatabaseRequest) error {
	c.droppedDatabase = append(c.droppedDatabase, req)
	return nil
}

func (c *recordingControlClient) ListDatabases(_ context.Context, _ *mysqlv1alpha1.Cluster, _ string) (*user.ListDatabasesResponse, error) {
	return &user.ListDatabasesResponse{Databases: c.databases}, nil
}

func (c *recordingControlClient) SetSemiSyncWaitForReplicaCount(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName string, count int) error {
	if c.semiSyncWaits == nil {
		c.semiSyncWaits = map[string]int{}
	}
	c.semiSyncWaits[instanceName] = count
	return nil
}

func (c *recordingControlClient) Reload(_ context.Context, _ *mysqlv1alpha1.Cluster, instanceName string, req webserver.ReloadRequest) (*webserver.ReloadResponse, error) {
	if c.reloaded == nil {
		c.reloaded = map[string]webserver.ReloadRequest{}
	}
	c.reloaded[instanceName] = req
	applied := make([]string, 0, len(req.Parameters))
	for name := range req.Parameters {
		applied = append(applied, name)
	}
	return &webserver.ReloadResponse{Applied: applied}, nil
}

func TestBuildPlanDefaultsToLocalInstanceImage(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build(),
		Scheme: testScheme(t),
	}

	plan, err := reconciler.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Image != defaultInstanceImage {
		t.Fatalf("image = %q, want %q", plan.Image, defaultInstanceImage)
	}
	if plan.ServerVersion != defaultMySQL80ServerVersion {
		t.Fatalf("server version = %q, want %q", plan.ServerVersion, defaultMySQL80ServerVersion)
	}
}

func TestBuildPlanResolvesNamespacedImageCatalog(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.ImageCatalogRef = &mysqlv1alpha1.ImageCatalogRef{
		TypedLocalObjectReference: corev1.TypedLocalObjectReference{
			Name: "images",
			Kind: "ImageCatalog",
		},
		Series: "8.0",
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(&mysqlv1alpha1.ImageCatalog{
			ObjectMeta: metav1.ObjectMeta{Name: "images", Namespace: "default"},
			Spec: mysqlv1alpha1.ImageCatalogSpec{Images: []mysqlv1alpha1.CatalogImage{
				{Series: "8.0", Image: "registry.example/cnmsql:8.0"},
			}},
		}).Build(),
		Scheme: scheme,
	}

	plan, err := reconciler.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Image != "registry.example/cnmsql:8.0" {
		t.Fatalf("image = %q", plan.Image)
	}
	if plan.ServerVersion != defaultMySQL80ServerVersion {
		t.Fatalf("server version = %q", plan.ServerVersion)
	}
}

func TestResolveServerVersionFromImageTag(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"ghcr.io/cnmsql/cnmsql-instance:8.0": defaultMySQL80ServerVersion,
		"ghcr.io/cnmsql/cnmsql-instance:8.4": defaultMySQL84ServerVersion,
		"ghcr.io/cnmsql/cnmsql-instance:9.x": defaultMySQL9xServerVersion,
		"registry/cnmsql:8.0.46-37":          "8.0.46-37",
	}

	for image, want := range tests {
		got, err := resolveServerVersion(image)
		if err != nil {
			t.Fatalf("resolveServerVersion(%q): %v", image, err)
		}
		if got != want {
			t.Fatalf("resolveServerVersion(%q) = %q, want %q", image, got, want)
		}
	}
}

func TestResolveServerVersionRejectsMySQL56(t *testing.T) {
	t.Parallel()
	if _, err := resolveServerVersion("ghcr.io/cnmsql/cnmsql-instance:5.6"); err == nil {
		t.Fatal("expected MySQL 5.6 image tag to be unsupported")
	}
}

func TestEnsurePasswordSecretDoesNotOverwriteExistingSecret(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	scheme := testScheme(t)
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-root", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("keep")},
	}
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, existing).Build(),
		Scheme: scheme,
	}

	if err := reconciler.ensurePasswordSecret(context.Background(), cluster, "demo-root", map[string]string{"username": "root"}); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Secret{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-root"}, got); err != nil {
		t.Fatal(err)
	}
	if string(got.Data["password"]) != "keep" {
		t.Fatalf("password was overwritten: %q", got.Data["password"])
	}
}

func TestPodSpecUsesInitContainerAndCertManagerSecrets(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1))

	t.Run("init containers", func(t *testing.T) {
		testInitContainers(t, &spec)
	})
	t.Run("container args", func(t *testing.T) {
		testContainerArgs(t, &spec)
	})
	t.Run("probe endpoints", func(t *testing.T) {
		testProbeEndpoints(t, &spec)
	})
	t.Run("probe timing", func(t *testing.T) {
		testProbeTiming(t, &spec)
	})
	t.Run("secret volumes", func(t *testing.T) {
		testSecretVolumes(t, &spec)
	})
}

func TestPodSpecSwitchoverPreStopHook(t *testing.T) {
	t.Parallel()

	maxStop := int64(baseCluster().GetMaxStopDelay())

	t.Run("single instance has no blocking preStop hook", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster() // Instances: 1, switchover-on-drain default-enabled
		plan := testPlan()       // plan.Instances: 1
		spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1))

		if lc := spec.Containers[0].Lifecycle; lc != nil {
			t.Fatalf("single-instance Pod must not get a preStop hook (it can never be demoted): %+v", lc)
		}
		// Grace period stays at the bare mysqld stop budget — no handoff extension.
		if got := *spec.TerminationGracePeriodSeconds; got != maxStop {
			t.Fatalf("grace period = %d, want %d (no handoff extension)", got, maxStop)
		}
	})

	t.Run("multi instance gets a bounded preStop handoff", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster()
		cluster.Spec.Instances = 3
		plan := testPlan()
		plan.Instances = 3
		spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1))

		lc := spec.Containers[0].Lifecycle
		if lc == nil || lc.PreStop == nil || lc.PreStop.Exec == nil {
			t.Fatalf("multi-instance Pod must carry a preStop exec hook, got %+v", lc)
		}
		cmd := strings.Join(lc.PreStop.Exec.Command, " ")
		if !strings.Contains(cmd, "instance prestop") {
			t.Fatalf("preStop command = %q, want the prestop handoff", cmd)
		}
		// The handoff timeout must be the small fixed budget, not the full stop delay.
		wantTimeout := fmt.Sprintf("--timeout=%ds", switchoverHandoffSeconds)
		if !strings.Contains(cmd, wantTimeout) {
			t.Fatalf("preStop command = %q, want %q", cmd, wantTimeout)
		}
		// Grace period is extended only by the small handoff budget.
		if got, want := *spec.TerminationGracePeriodSeconds, maxStop+switchoverHandoffSeconds; got != want {
			t.Fatalf("grace period = %d, want %d (stop delay + handoff)", got, want)
		}
	})

	t.Run("disabling switchover-on-drain drops the hook", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster()
		cluster.Spec.Instances = 3
		cluster.Spec.EnableSwitchoverOnDrain = ptr.To(false)
		plan := testPlan()
		plan.Instances = 3
		spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1))

		if lc := spec.Containers[0].Lifecycle; lc != nil {
			t.Fatalf("disabled switchover-on-drain must not attach a preStop hook: %+v", lc)
		}
	})
}

func testInitContainers(t *testing.T, spec *corev1.PodSpec) {
	t.Helper()
	if len(spec.InitContainers) != 2 {
		t.Fatalf("init containers = %d", len(spec.InitContainers))
	}
	if got := strings.Join(spec.InitContainers[0].Args, " "); got != "bootstrap /controller/manager" {
		t.Fatalf("bootstrap-controller init container args = %q", got)
	}
	if got := strings.Join(spec.InitContainers[1].Args, " "); !strings.Contains(got, "instance initdb") {
		t.Fatalf("init container args = %q", got)
	}
}

func testContainerArgs(t *testing.T, spec *corev1.PodSpec) {
	t.Helper()
	if got := strings.Join(spec.Containers[0].Args, " "); !strings.Contains(got, "instance run") {
		t.Fatalf("main container args = %q", got)
	}
	runArgsStr := strings.Join(spec.Containers[0].Args, " ")
	if !strings.Contains(runArgsStr, "--cluster-name=demo") || !strings.Contains(runArgsStr, "--namespace=$(POD_NAMESPACE)") {
		t.Fatalf("run container should carry the Cluster identity: %q", runArgsStr)
	}
	if strings.Contains(runArgsStr, "--role=") {
		t.Fatalf("run container must not declare a static role: %q", runArgsStr)
	}
	if !strings.Contains(runArgsStr, "--health-addr=:8081") {
		t.Fatalf("run container should expose the plain health listener: %q", runArgsStr)
	}
}

func testProbeEndpoints(t *testing.T, spec *corev1.PodSpec) {
	t.Helper()
	const healthPortName = "health"
	if got := spec.Containers[0].ReadinessProbe.HTTPGet; got == nil || got.Path != "/readyz" || got.Port.String() != healthPortName {
		t.Fatalf("readiness probe = %#v, want HTTP /readyz on health", got)
	}
	if got := spec.Containers[0].LivenessProbe.HTTPGet; got == nil || got.Path != "/livez" || got.Port.String() != healthPortName {
		t.Fatalf("liveness probe = %#v, want HTTP /livez on health", got)
	}
	if got := spec.Containers[0].StartupProbe.HTTPGet; got == nil || got.Path != "/startupz" || got.Port.String() != healthPortName {
		t.Fatalf("startup probe = %#v, want HTTP /startupz on health", got)
	}
}

func testProbeTiming(t *testing.T, spec *corev1.PodSpec) {
	t.Helper()
	if rp := spec.Containers[0].ReadinessProbe; rp.PeriodSeconds != 2 || rp.TimeoutSeconds != 5 || rp.FailureThreshold != 3 {
		t.Fatalf("readiness probe timing = period %d timeout %d threshold %d, want period 2 timeout 5 threshold 3",
			rp.PeriodSeconds, rp.TimeoutSeconds, rp.FailureThreshold)
	}
	if lp := spec.Containers[0].LivenessProbe; lp.PeriodSeconds != 10 || lp.TimeoutSeconds != 5 || lp.FailureThreshold != 6 {
		t.Fatalf("liveness probe timing = period %d timeout %d threshold %d, want period 10 timeout 5 threshold 6",
			lp.PeriodSeconds, lp.TimeoutSeconds, lp.FailureThreshold)
	}
	if sp := spec.Containers[0].StartupProbe; sp.PeriodSeconds != 2 || sp.TimeoutSeconds != 5 || sp.FailureThreshold != 90 {
		t.Fatalf("startup probe timing = period %d timeout %d threshold %d, want period 2 timeout 5 threshold 90",
			sp.PeriodSeconds, sp.TimeoutSeconds, sp.FailureThreshold)
	}
}

func testSecretVolumes(t *testing.T, spec *corev1.PodSpec) {
	t.Helper()
	volumes := map[string]string{}
	for _, volume := range spec.Volumes {
		if volume.Secret != nil {
			volumes[volume.Name] = volume.Secret.SecretName
		}
	}
	if volumes["server-tls"] != "demo-1-server-tls" {
		t.Fatalf("server tls volume = %q", volumes["server-tls"])
	}
	if volumes["client-ca"] != "demo-ca" {
		t.Fatalf("client ca volume = %q", volumes["client-ca"])
	}
}

func TestPodSpecReplicaUsesJoin(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	plan.Instances = 3

	spec := (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 2))
	got := strings.Join(spec.InitContainers[1].Args, " ")
	if !strings.Contains(got, "instance join") {
		t.Fatalf("replica init container should join: %q", got)
	}
	if !strings.Contains(got, "--source-manager-url=https://demo-1.default.svc:8080/cluster/backup") {
		t.Fatalf("replica should clone from the primary manager: %q", got)
	}
	if !strings.Contains(got, "--source-host=demo-1.default.svc") {
		t.Fatalf("replica should replicate from the primary: %q", got)
	}
	got = strings.Join(spec.Containers[0].Args, " ")
	// The run container is role-agnostic: no --role/--source-host. It gets the
	// Cluster identity plus the static replication connection parameters; the
	// source host is derived from currentPrimary at runtime.
	if strings.Contains(got, "--role=") || strings.Contains(got, "--source-host=") {
		t.Fatalf("run container must be role-agnostic (no --role/--source-host): %q", got)
	}
	if !strings.Contains(got, "--cluster-name=demo") {
		t.Fatalf("replica run container should carry the Cluster identity: %q", got)
	}
	if !strings.Contains(got, "--replication-user="+replicationUser) {
		t.Fatalf("run container should carry the replication connection user: %q", got)
	}
	for _, container := range []corev1.Container{spec.InitContainers[1], spec.Containers[0]} {
		for _, env := range container.Env {
			if env.Name == "MYSQL_REPLICATION_PASSWORD" {
				t.Fatalf("%s must use mTLS-only replication auth, found MYSQL_REPLICATION_PASSWORD env", container.Name)
			}
		}
	}
}

func TestEnsurePodRecreatesWhenTemplateHashChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      inst.Name,
			Namespace: cluster.Namespace,
			Annotations: map[string]string{
				podTemplateHashAnnotation: "stale",
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  "mysql",
			Image: "old",
		}}},
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, stalePod).Build(),
		Scheme: scheme,
	}

	if _, err := reconciler.ensurePod(ctx, cluster, plan, inst, true); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Pod{}
	err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("stale Pod get error = %v, want not found", err)
	}

	if _, err := reconciler.ensurePod(ctx, cluster, plan, inst, true); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[podTemplateHashAnnotation] == "" {
		t.Fatalf("pod template hash annotation is empty")
	}
	if got.Annotations[configHashAnnotation] == "" {
		t.Fatalf("config hash annotation is empty")
	}
	if got.Spec.Containers[0].Image != plan.Image {
		t.Fatalf("container image = %q, want %q", got.Spec.Containers[0].Image, plan.Image)
	}
}

func TestPodTemplateHashIgnoresOperatorImage(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	labels := labelsFor(cluster, inst.Name, roleOf(inst))

	plan.OperatorImage = "example.com/operator:v1.0.0"
	spec1 := (&ClusterReconciler{}).podSpec(cluster, plan, inst)
	annotations1, err := (&ClusterReconciler{}).podAnnotations(cluster, plan, inst, labels, spec1)
	if err != nil {
		t.Fatal(err)
	}
	hash1 := annotations1[podTemplateHashAnnotation]
	if hash1 == "" {
		t.Fatal("pod template hash is empty for first image")
	}

	plan.OperatorImage = "example.com/operator:v2.0.0"
	spec2 := (&ClusterReconciler{}).podSpec(cluster, plan, inst)
	annotations2, err := (&ClusterReconciler{}).podAnnotations(cluster, plan, inst, labels, spec2)
	if err != nil {
		t.Fatal(err)
	}
	hash2 := annotations2[podTemplateHashAnnotation]
	if hash2 == "" {
		t.Fatal("pod template hash is empty for second image")
	}

	if hash1 != hash2 {
		t.Fatalf("pod template hash changed after operator image bump (hash1=%s, hash2=%s)", hash1, hash2)
	}
}

func TestEnsurePodDoesNotRecreateForPrimaryRoleChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	plan := testPlan()
	plan.Instances = 2
	inst := plan.instanceFor(cluster, 1)
	labels := labelsFor(cluster, inst.Name, roleOf(inst))
	spec := (&ClusterReconciler{}).podSpec(cluster, plan, inst)
	annotations, err := (&ClusterReconciler{}).podAnnotations(cluster, plan, inst, labels, spec)
	if err != nil {
		t.Fatal(err)
	}
	existingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        inst.Name,
			Namespace:   cluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: spec,
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, existingPod).Build(),
		Scheme: scheme,
	}

	plan.PrimaryName = testReplica2
	inst = plan.instanceFor(cluster, 1)
	if _, err := reconciler.ensurePod(ctx, cluster, plan, inst, true); err != nil {
		t.Fatal(err)
	}

	got := &corev1.Pod{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.DeletionTimestamp != nil {
		t.Fatal("pod should not be deleted when only the primary role changes")
	}
	if got.Labels[roleLabel] != roleReplica {
		t.Fatalf("role label = %q, want replica", got.Labels[roleLabel])
	}
}

func TestEnsurePodPreservesFencingAnnotation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	inst := plan.instanceFor(cluster, 1)
	labels := labelsFor(cluster, inst.Name, roleOf(inst))
	spec := (&ClusterReconciler{}).podSpec(cluster, plan, inst)
	annotations, err := (&ClusterReconciler{}).podAnnotations(cluster, plan, inst, labels, spec)
	if err != nil {
		t.Fatal(err)
	}
	annotations[fencingAnnotation] = routableTrue
	existingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        inst.Name,
			Namespace:   cluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: spec,
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, existingPod).Build(),
		Scheme: scheme,
	}

	if _, err := reconciler.ensurePod(ctx, cluster, plan, inst, true); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Pod{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: inst.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[fencingAnnotation] != routableTrue {
		t.Fatalf("fencing annotation = %q, want true", got.Annotations[fencingAnnotation])
	}
}

func TestUnsupportedReasonNamesDeferredMilestones(t *testing.T) {
	t.Parallel()
	// Replicas are now supported.
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	if got := unsupportedReason(cluster); got != "" {
		t.Fatalf("3-instance cluster should be supported, got %q", got)
	}

	// Recovery without a backup reference is rejected.
	cluster = baseCluster()
	cluster.Spec.Bootstrap.InitDB = nil
	cluster.Spec.Bootstrap.Recovery = &mysqlv1alpha1.BootstrapRecovery{}
	if got := unsupportedReason(cluster); !strings.Contains(got, "backup reference") {
		t.Fatalf("recovery without backup unsupported reason = %q", got)
	}

	// Recovery from a referenced backup is supported.
	cluster = baseCluster()
	cluster.Spec.Bootstrap.InitDB = nil
	cluster.Spec.Bootstrap.Recovery = &mysqlv1alpha1.BootstrapRecovery{
		Backup: &mysqlv1alpha1.LocalObjectReference{Name: "demo-backup"},
	}
	if got := unsupportedReason(cluster); got != "" {
		t.Fatalf("recovery from backup should be supported, got %q", got)
	}

	// A denied my.cnf parameter blocks the cluster.
	cluster = baseCluster()
	cluster.Spec.MySQL.Parameters = map[string]string{"datadir": "/evil"}
	if got := unsupportedReason(cluster); !strings.Contains(got, "spec.mysql.parameters") || !strings.Contains(got, "datadir") {
		t.Fatalf("denied parameter unsupported reason = %q", got)
	}
}

func TestWarnDeprecatedParametersEmitsEvent(t *testing.T) {
	t.Parallel()
	recorder := record.NewFakeRecorder(10)
	r := &ClusterReconciler{Recorder: recorder}

	cluster := baseCluster()
	cluster.Spec.MySQL.Parameters = map[string]string{"slave_parallel_workers": "4"}
	r.warnDeprecatedParameters(cluster)

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "DeprecatedParameter") || !strings.Contains(event, "slave_parallel_workers") {
			t.Fatalf("event = %q, want DeprecatedParameter naming the key", event)
		}
	default:
		t.Fatal("expected a DeprecatedParameter event")
	}
}

func TestReconcileBlocksUnsupportedClusterShape(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Replica = &mysqlv1alpha1.ReplicaClusterConfiguration{Source: "external"}
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(10)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme:   scheme,
		Recorder: recorder,
	}

	result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("requeue after = %s, want 0", result.RequeueAfter)
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != topology.PhaseBlocked {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, topology.PhaseBlocked)
	}
	if !strings.Contains(got.Status.PhaseReason, "replica") {
		t.Fatalf("phase reason = %q, want replica-cluster block", got.Status.PhaseReason)
	}
	ready := apimeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("ready condition = %#v, want False", ready)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "Warning") || !strings.Contains(event, topology.PhaseBlocked) {
			t.Fatalf("blocked event = %q, want Warning %s", event, topology.PhaseBlocked)
		}
	default:
		t.Fatalf("expected a Warning %s event", topology.PhaseBlocked)
	}
}

func TestReconcileBootstrapsSingleInstanceToReady(t *testing.T) {
	t.Parallel()
	const primaryName = demoPrimaryInstance
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(10)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme:        scheme,
		Recorder:      recorder,
		ControlClient: readyStatusClient{},
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}}

	result, err := reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("first reconcile should requeue while waiting for cert-manager secrets")
	}
	assertOwnedObject(t, ctx, reconciler, &corev1.Secret{}, "demo-root")
	assertOwnedObject(t, ctx, reconciler, &corev1.Secret{}, "demo-app")
	assertOwnedObject(t, ctx, reconciler, &corev1.Secret{}, "demo-replication")
	assertOwnedObject(t, ctx, reconciler, &corev1.Secret{}, "demo-backup")
	assertOwnedObject(t, ctx, reconciler, &corev1.Secret{}, "demo-control")
	assertOwnedObject(t, ctx, reconciler, &corev1.Service{}, "demo-rw")
	assertOwnedObject(t, ctx, reconciler, &corev1.Service{}, "demo-ro")
	assertOwnedObject(t, ctx, reconciler, &corev1.Service{}, "demo-r")
	assertOwnedUnstructuredResource(t, ctx, reconciler, issuerGVK.Kind, issuerGVK, "demo-selfsigned")
	assertOwnedUnstructuredResource(t, ctx, reconciler, certificateGVK.Kind, certificateGVK, "demo-1-server")

	for _, name := range []string{"demo-ca", "demo-1-server-tls", "demo-client-tls"} {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
			Data: map[string][]byte{
				"ca.crt":  []byte("ca"),
				"tls.crt": []byte("cert"),
				"tls.key": []byte("key"),
			},
		}
		if err := reconciler.Create(ctx, secret); err != nil {
			t.Fatal(err)
		}
	}

	result, err = reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("second reconcile should requeue while waiting for pod readiness")
	}
	assertOwnedObject(t, ctx, reconciler, &corev1.ConfigMap{}, "demo-1-config")
	assertOwnedObject(t, ctx, reconciler, &corev1.PersistentVolumeClaim{}, "demo-1")
	assertOwnedObject(t, ctx, reconciler, &corev1.Service{}, "demo-1")
	pod := &corev1.Pod{}
	assertOwnedObject(t, ctx, reconciler, pod, "demo-1")
	if pod.Annotations[podTemplateHashAnnotation] == "" {
		t.Fatalf("pod template hash annotation is empty")
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	}}
	if err := reconciler.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}

	// Simulate the in-Pod reconciler promoting itself and recording the current
	// primary (the operator no longer writes currentPrimary).
	primed := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, request.NamespacedName, primed); err != nil {
		t.Fatal(err)
	}
	primed.Status.CurrentPrimary = primaryName
	if err := reconciler.Status().Update(ctx, primed); err != nil {
		t.Fatal(err)
	}

	result, err = reconciler.Reconcile(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != readyResync {
		t.Fatalf("ready reconcile requeue after = %s, want %s", result.RequeueAfter, readyResync)
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != topology.PhaseReady {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, topology.PhaseReady)
	}
	if got.Status.CurrentPrimary != primaryName {
		t.Fatalf("current primary = %q, want %s", got.Status.CurrentPrimary, primaryName)
	}
	if got.Status.ReadyInstances != 1 {
		t.Fatalf("ready instances = %d, want 1", got.Status.ReadyInstances)
	}
	if got.Status.Image != defaultInstanceImage {
		t.Fatalf("status image = %q, want %q", got.Status.Image, defaultInstanceImage)
	}
	if got.Status.GTIDExecutedByInstance[primaryName] != testGTID {
		t.Fatalf("gtid status = %#v", got.Status.GTIDExecutedByInstance)
	}
	ready := apimeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("ready condition = %#v, want True", ready)
	}

	if !drainEvents(recorder.Events, topology.PhaseReady) {
		t.Fatalf("expected a %q phase-transition event", topology.PhaseReady)
	}

	// A steady-state resync with no phase change must not emit another event.
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected event on steady-state resync: %q", event)
	default:
	}
}

// drainEvents reports whether any buffered event mentions the given phase.
func drainEvents(events <-chan string, phase string) bool {
	found := false
	for {
		select {
		case event := <-events:
			if strings.Contains(event, phase) {
				found = true
			}
		default:
			return found
		}
	}
}

func testPlan() clusterPlan {
	return clusterPlan{
		Image:              "ghcr.io/cnmsql/cnmsql-instance:8.0",
		ServerVersion:      "8.0.46",
		Instances:          1,
		RootSecretName:     "demo-root",
		AppSecretName:      "demo-app",
		ReplicationSecret:  "demo-replication",
		ControlSecretName:  "demo-control",
		BackupSecretName:   "demo-backup",
		ServerCASecretName: "demo-ca",
		ClientCASecretName: "demo-ca",
		ClientTLSSecret:    "demo-client-tls",
		RWServiceName:      "demo-rw",
		ROServiceName:      "demo-ro",
		RServiceName:       "demo-r",
	}
}

func readyPod(cluster *mysqlv1alpha1.Cluster, name, role string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				clusterLabel:  cluster.Name,
				instanceLabel: name,
				roleLabel:     role,
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}}},
	}
}

func assertOwnedObject(t *testing.T, ctx context.Context, reconciler *ClusterReconciler, obj client.Object, name string) {
	t.Helper()
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, obj); err != nil {
		t.Fatal(err)
	}
	if len(obj.GetOwnerReferences()) != 1 || obj.GetOwnerReferences()[0].Name != scheduledTestCluster {
		t.Fatalf("%T owner refs = %#v, want demo owner", obj, obj.GetOwnerReferences())
	}
}

func assertOwnedUnstructuredResource(
	t *testing.T,
	ctx context.Context,
	reconciler *ClusterReconciler,
	resourceName string,
	gvk schema.GroupVersionKind,
	name string,
) {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: "default", Name: name}, obj); err != nil {
		t.Fatalf("%s %s: %v", resourceName, name, err)
	}
	if len(obj.GetOwnerReferences()) != 1 || obj.GetOwnerReferences()[0].Name != scheduledTestCluster {
		t.Fatalf("%s owner refs = %#v, want demo owner", resourceName, obj.GetOwnerReferences())
	}
}

func TestReconcileSwitchoverWaitsForInstancePromotion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.TargetPrimary = testReplica2
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 3
	observed := observedCluster{
		Plan:          plan,
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		StatusByInstance: map[string]*webserver.Status{
			testReplica2: {
				InstanceName: testReplica2,
				Role:         webserver.RoleReplica,
				IsReady:      true,
				Replication:  &webserver.ReplicationStatus{IORunning: true, SQLRunning: true},
			},
		},
	}

	handled, err := reconciler.reconcileSwitchover(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("an in-flight switchover should be handled")
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	// The operator must not promote/demote; it only records intent and waits for
	// the target's in-Pod reconciler to flip currentPrimary.
	if got.Status.CurrentPrimary != testPrimary {
		t.Fatalf("currentPrimary = %q, want unchanged %q", got.Status.CurrentPrimary, testPrimary)
	}
	if got.Status.TargetPrimary != testReplica2 {
		t.Fatalf("targetPrimary = %q, want %q", got.Status.TargetPrimary, testReplica2)
	}
	if got.Status.TargetPrimaryTimestamp == "" {
		t.Fatal("targetPrimaryTimestamp should be stamped")
	}
	if got.Status.Phase != topology.PhaseSwitchover {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, topology.PhaseSwitchover)
	}
}

func TestReconcileSwitchoverDoesNotBlockBootstrapTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Status.TargetPrimary = testPrimary
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 3
	observed := observedCluster{
		Plan:          plan,
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
	}

	handled, err := reconciler.reconcileSwitchover(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if handled {
		t.Fatal("bootstrap target should not be treated as a switchover")
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase == topology.PhaseBlocked {
		t.Fatalf("phase = %q, want bootstrap to keep waiting for currentPrimary", got.Status.Phase)
	}
	if got.Status.TargetPrimary != testPrimary {
		t.Fatalf("targetPrimary = %q, want unchanged %q", got.Status.TargetPrimary, testPrimary)
	}
}

func TestReconcileSwitchoverBlocksUnhealthyTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Status.CurrentPrimary = testPrimary
	cluster.Status.TargetPrimary = testReplica2
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme: scheme,
	}
	plan := testPlan()
	plan.Instances = 3
	observed := observedCluster{
		Plan:          plan,
		PrimaryName:   testPrimary,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
		StatusByInstance: map[string]*webserver.Status{
			// target reports unhealthy replication
			testReplica2: {InstanceName: testReplica2, Role: webserver.RoleReplica, IsReady: false},
		},
	}

	handled, err := reconciler.reconcileSwitchover(ctx, cluster, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("a blocked switchover should be handled")
	}
	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != topology.PhaseBlocked {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, topology.PhaseBlocked)
	}
}

func TestReconcileRoleLabelsTrackCurrentPrimary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	cluster.Spec.Instances = 3
	cluster.Status.CurrentPrimary = testReplica2
	scheme := testScheme(t)
	pod1 := readyPod(cluster, testPrimary, rolePrimary)
	pod2 := readyPod(cluster, testReplica2, roleReplica)
	pod3 := readyPod(cluster, testReplica3, roleReplica)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod1, pod2, pod3).Build(),
		Scheme: scheme,
	}
	observed := observedCluster{
		PrimaryName:   testReplica2,
		InstanceNames: []string{testPrimary, testReplica2, testReplica3},
	}
	if err := reconciler.reconcileRoleLabels(ctx, cluster, observed); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{testPrimary: roleReplica, testReplica2: rolePrimary, testReplica3: roleReplica} {
		pod := &corev1.Pod{}
		if err := reconciler.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, pod); err != nil {
			t.Fatal(err)
		}
		if pod.Labels[roleLabel] != want {
			t.Fatalf("%s role label = %q, want %q", name, pod.Labels[roleLabel], want)
		}
	}
}
