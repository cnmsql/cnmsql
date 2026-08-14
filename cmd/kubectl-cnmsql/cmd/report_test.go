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
	"archive/zip"
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientsetfake "k8s.io/client-go/kubernetes/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func parseTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time %q: %v", s, err)
	}
	return ts
}

func TestRedactSecretBlanksValuesKeepsKeys(t *testing.T) {
	t.Parallel()
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-ca"},
		Data:       map[string][]byte{"ca.crt": []byte("SECRET"), "tls.key": []byte("KEY")},
	}
	r := plugin.RedactSecret(secret)
	if r.Data["ca.crt"] == nil || len(r.Data["ca.crt"]) != 0 {
		t.Errorf("ca.crt not blanked: %q", r.Data["ca.crt"])
	}
	if r.Data["tls.key"] == nil || len(r.Data["tls.key"]) != 0 {
		t.Errorf("tls.key not blanked: %q", r.Data["tls.key"])
	}
	if _, ok := r.Data["ca.crt"]; !ok {
		t.Error("ca.crt key removed")
	}
}

func TestRedactConfigMapBlanksValues(t *testing.T) {
	t.Parallel()
	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-cm"},
		Data:       map[string]string{"config": "sensitive"},
	}
	r := plugin.RedactConfigMap(cm)
	if r.Data["config"] != "" {
		t.Errorf("config not blanked: %q", r.Data["config"])
	}
	if _, ok := r.Data["config"]; !ok {
		t.Error("config key removed")
	}
}

func TestRedactWebhookClientConfigBlanksCA(t *testing.T) {
	t.Parallel()
	cfg := admissionregistrationv1.WebhookClientConfig{CABundle: []byte("REALCA")}
	r := plugin.RedactWebhookClientConfig(cfg)
	if string(r.CABundle) != "-" {
		t.Errorf("CABundle = %q, want %q", r.CABundle, "-")
	}
	empty := admissionregistrationv1.WebhookClientConfig{}
	if got := plugin.RedactWebhookClientConfig(empty); len(got.CABundle) != 0 {
		t.Error("empty CABundle should remain empty")
	}
}

func TestReportNameIsTimestamped(t *testing.T) {
	t.Parallel()
	got := plugin.ReportName("cluster", parseTime(t, "2026-08-14T10:00:00Z"), "demo")
	want := "report_cluster_demo_20260814_100000"
	if got != want {
		t.Errorf("ReportName() = %q, want %q", got, want)
	}
}

func TestRunReportClusterProducesNonEmptyZip(t *testing.T) {
	var buf strings.Builder
	origOut := plugin.Out
	plugin.Out = &stringWriter{&buf}
	t.Cleanup(func() { plugin.Out = origOut })

	cluster := testCluster()
	cluster.Status.Phase = phaseReady
	pods := []corev1.Pod{
		testPod(firstInstance),
		testPod("demo-2"),
	}
	env := installFakeEnv(t, cluster, pods)
	// Add a Backup for the cluster via the controller-runtime fake client.
	if err := env.Client.Create(context.Background(), &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "test"},
		Spec:       mysqlv1alpha1.BackupSpec{Cluster: mysqlv1alpha1.LocalObjectReference{Name: testClusterName}},
	}); err != nil {
		t.Fatalf("creating backup: %v", err)
	}

	dir := t.TempDir()
	out := filepath.Join(dir, "report.zip")
	if err := runReportCluster(context.Background(), testClusterName,
		plugin.ReportFormatYAML, out, false, false, parseTime(t, "2026-08-14T10:00:00Z")); err != nil {
		t.Fatalf("runReportCluster() error = %v", err)
	}

	zr, err := zip.OpenReader(out)
	if err != nil {
		t.Fatalf("opening zip: %v", err)
	}
	names := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		names = append(names, f.Name)
	}
	if len(names) == 0 {
		t.Fatalf("zip is empty")
	}
	wantEntries := []string{
		"report_cluster_demo_20260814_100000/manifests/cluster.yaml",
		"report_cluster_demo_20260814_100000/manifests/cluster-pods.yaml",
		"report_cluster_demo_20260814_100000/manifests/backups.yaml",
		"report_cluster_demo_20260814_100000/manifests/events.yaml",
	}
	for _, want := range wantEntries {
		if !containsEntry(names, want) {
			t.Errorf("zip missing %q; got %v", want, names)
		}
	}
	if buf.Len() == 0 || !strings.Contains(buf.String(), "Successfully written report") {
		t.Errorf("expected success message, got %q", buf.String())
	}
	if err := zr.Close(); err != nil {
		t.Fatalf("closing zip: %v", err)
	}
}

func TestRunReportOperatorRedactsByDefault(t *testing.T) {
	var buf strings.Builder
	origOut := plugin.Out
	plugin.Out = &stringWriter{&buf}
	t.Cleanup(func() { plugin.Out = origOut })

	cluster := testCluster()
	env := installFakeEnv(t, cluster, nil)
	// Build a fake clientset carrying the operator deployment + a secret, so
	// the operator report can collect them. The controller-runtime fake client
	// (env.Client) already has the cluster; the typed clientset (env.Clientset)
	// is empty, so replace it with one carrying the operator resources.
	operatorDeployment := makeOperatorDeployment()
	operatorDeployment.Namespace = "test"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "op-secret", Namespace: "test"},
		Data:       map[string][]byte{"token": []byte("supersecret")},
	}
	env.Clientset = clientsetfake.NewClientset(operatorDeployment, secret)

	dir := t.TempDir()
	out := filepath.Join(dir, "op-report.zip")
	if err := runReportOperator(context.Background(), plugin.ReportFormatYAML,
		out, false, false, false, parseTime(t, "2026-08-14T10:00:00Z")); err != nil {
		t.Fatalf("runReportOperator() error = %v", err)
	}

	zr, err := zip.OpenReader(out)
	if err != nil {
		t.Fatalf("opening zip: %v", err)
	}
	var secretContent string
	for _, f := range zr.File {
		if !strings.Contains(f.Name, "op-secret(secret)") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("opening entry: %v", err)
		}
		b := make([]byte, 4096)
		n, _ := rc.Read(b)
		_ = rc.Close()
		secretContent = string(b[:n])
	}
	if err := zr.Close(); err != nil {
		t.Fatalf("closing zip: %v", err)
	}
	if secretContent == "" {
		t.Fatal("redacted secret entry not found in zip")
	}
	if strings.Contains(secretContent, "supersecret") {
		t.Errorf("secret value not redacted:\n%s", secretContent)
	}
	if !strings.Contains(secretContent, "token") {
		t.Errorf("secret key lost after redaction:\n%s", secretContent)
	}
}

// stringWriter is a minimal io.Writer over a strings.Builder.
type stringWriter struct{ b *strings.Builder }

func (w *stringWriter) Write(p []byte) (int, error) { return w.b.Write(p) }

func containsEntry(names []string, want string) bool {
	return slices.Contains(names, want)
}

func makeOperatorDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "controller-manager", Namespace: "cnmsql-system",
			Labels: map[string]string{operatorNameLabel: operatorNameValue},
		},
	}
}
