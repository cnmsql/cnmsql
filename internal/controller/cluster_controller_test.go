/*
Copyright 2026 The CNMySQL Authors.

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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
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
	return scheme
}

func baseCluster() *mysqlv1alpha1.Cluster {
	cluster := &mysqlv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "default",
		},
		Spec: mysqlv1alpha1.ClusterSpec{
			Instances: 1,
			Storage:   mysqlv1alpha1.StorageConfiguration{Size: "1Gi"},
			Bootstrap: &mysqlv1alpha1.BootstrapConfiguration{
				InitDB: &mysqlv1alpha1.BootstrapInitDB{
					Database: "app",
					Owner:    "app",
				},
			},
		},
	}
	cluster.SetDefaults()
	return cluster
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
		Major: 8,
	}
	scheme := testScheme(t)
	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(&mysqlv1alpha1.ImageCatalog{
			ObjectMeta: metav1.ObjectMeta{Name: "images", Namespace: "default"},
			Spec: mysqlv1alpha1.ImageCatalogSpec{Images: []mysqlv1alpha1.CatalogImage{
				{Major: 8, Image: "registry.example/cnmysql:8.0"},
			}},
		}).Build(),
		Scheme: scheme,
	}

	plan, err := reconciler.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Image != "registry.example/cnmysql:8.0" {
		t.Fatalf("image = %q", plan.Image)
	}
	if plan.ServerVersion != defaultMySQL80ServerVersion {
		t.Fatalf("server version = %q", plan.ServerVersion)
	}
}

func TestResolveServerVersionFromImageTag(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"cnmysql-instance:5.6":       defaultMySQL56ServerVersion,
		"cnmysql-instance:8.0":       defaultMySQL80ServerVersion,
		"cnmysql-instance:8.4":       defaultMySQL84ServerVersion,
		"cnmysql-instance:9.x":       defaultMySQL9xServerVersion,
		"registry/cnmysql:8.0.46-37": "8.0.46-37",
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
	plan := clusterPlan{
		Image:             "cnmysql-instance:8.0",
		ServerVersion:     "8.0.46",
		InstanceName:      "demo-1",
		ConfigMapName:     "demo-config",
		DataPVCName:       "demo-1",
		RootSecretName:    "demo-root",
		AppSecretName:     "demo-app",
		ReplicationSecret: "demo-replication",
		ControlSecretName: "demo-control",
		CASecretName:      "demo-ca",
		ServerTLSSecret:   "demo-server-tls",
		ClientTLSSecret:   "demo-client-tls",
	}

	spec := (&ClusterReconciler{}).podSpec(cluster, plan)
	if len(spec.InitContainers) != 1 {
		t.Fatalf("init containers = %d", len(spec.InitContainers))
	}
	if got := strings.Join(spec.InitContainers[0].Args, " "); !strings.Contains(got, "instance initdb") {
		t.Fatalf("init container args = %q", got)
	}
	if got := strings.Join(spec.Containers[0].Args, " "); !strings.Contains(got, "instance run") {
		t.Fatalf("main container args = %q", got)
	}
	if spec.Containers[0].ReadinessProbe.TCPSocket == nil {
		t.Fatalf("readiness probe must be TCP because the HTTP API requires mTLS")
	}
	volumes := map[string]string{}
	for _, volume := range spec.Volumes {
		if volume.Secret != nil {
			volumes[volume.Name] = volume.Secret.SecretName
		}
	}
	if volumes["server-tls"] != "demo-server-tls" {
		t.Fatalf("server tls volume = %q", volumes["server-tls"])
	}
	if volumes["client-ca"] != "demo-ca" {
		t.Fatalf("client ca volume = %q", volumes["client-ca"])
	}
}

func TestUnsupportedReasonNamesDeferredMilestones(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	if got := unsupportedReason(cluster); !strings.Contains(got, "M4") {
		t.Fatalf("replica unsupported reason = %q", got)
	}

	cluster = baseCluster()
	cluster.Spec.Bootstrap.InitDB = nil
	cluster.Spec.Bootstrap.Recovery = &mysqlv1alpha1.BootstrapRecovery{}
	if got := unsupportedReason(cluster); !strings.Contains(got, "M6") {
		t.Fatalf("recovery unsupported reason = %q", got)
	}
}
