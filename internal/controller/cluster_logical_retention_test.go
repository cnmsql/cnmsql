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
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// memS3 is a path-style, single-bucket S3 with the calls retention makes:
// ListObjectsV2, GET, HEAD and DELETE.
type memS3 struct {
	mu      sync.Mutex
	bucket  string
	objects map[string][]byte
}

func (m *memS3) put(t *testing.T, key string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	m.objects[key] = b
}

func (m *memS3) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.objects))
	for k := range m.objects {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (m *memS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/")
	if strings.TrimSuffix(path, "/") == m.bucket && r.Method == http.MethodGet {
		type content struct{ Key, LastModified, ETag string }
		type result struct {
			XMLName  xml.Name `xml:"ListBucketResult"`
			Name     string
			Prefix   string
			KeyCount int
			MaxKeys  int
			Contents []content
		}
		prefix := r.URL.Query().Get("prefix")
		out := result{Name: m.bucket, Prefix: prefix, MaxKeys: 1000}
		for k := range m.objects {
			if strings.HasPrefix(k, prefix) {
				out.Contents = append(out.Contents, content{Key: k, LastModified: "2026-09-01T00:00:00Z", ETag: `"x"`})
			}
		}
		sort.Slice(out.Contents, func(i, j int) bool { return out.Contents[i].Key < out.Contents[j].Key })
		out.KeyCount = len(out.Contents)
		_ = xml.NewEncoder(w).Encode(out)
		return
	}
	key := strings.TrimPrefix(path, m.bucket+"/")
	body, ok := m.objects[key]
	switch r.Method {
	case http.MethodDelete:
		delete(m.objects, key)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet, http.MethodHead:
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Last-Modified", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("ETag", `"x"`)
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func TestReconcileRetentionExpiresLogicalBackupsOnTheirOwn(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	day := 24 * time.Hour
	s3 := &memS3{bucket: "backups", objects: map[string][]byte{}}
	// One old physical backup, which is also the newest one: the floor keeps it.
	s3.put(t, "p/demo/base/b1/metadata.json", objectstore.BackupMetadata{
		BackupID: "b1", StartedAt: now.Add(-40 * day), CompletedAt: now.Add(-40 * day)})
	s3.objects["p/demo/base/b1/backup.xbstream"] = []byte("xb")
	// Three dumps: two past the 30d window, one inside it.
	for id, age := range map[string]time.Duration{"d1": 60 * day, "d2": 35 * day, "d3": 2 * day} {
		s3.put(t, "p/demo/dump/"+id+"/logical.json", objectstore.LogicalBackupMetadata{
			BackupID: id, Method: "logical", CompletedAt: now.Add(-age)})
		s3.objects["p/demo/dump/"+id+"/dump.sql.zst"] = []byte("zst")
	}
	server := httptest.NewServer(s3)
	defer server.Close()

	cluster := baseCluster()
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		RetentionPolicy: "30d",
		ObjectStore: &mysqlv1alpha1.S3ObjectStore{
			Bucket: "backups", Path: "p", Endpoint: server.URL, ForcePathStyle: new(true),
			Credentials: mysqlv1alpha1.S3Credentials{
				AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "s3", Key: "access"},
				SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "s3", Key: "secret"},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: cluster.Namespace},
		Data:       map[string][]byte{"access": []byte("k"), "secret": []byte("s")},
	}
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).WithObjects(cluster, secret).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if err := r.reconcileRetention(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	got := strings.Join(s3.keys(), " ")
	for _, gone := range []string{"p/demo/dump/d1/", "p/demo/dump/d2/"} {
		if strings.Contains(got, gone) {
			t.Errorf("%s should have expired: %s", gone, got)
		}
	}
	for _, kept := range []string{
		"p/demo/dump/d3/logical.json", "p/demo/dump/d3/dump.sql.zst",
		// The dumps never count as recovery points, so the lone physical backup
		// stays as the floor even though it is older than every dump.
		"p/demo/base/b1/metadata.json", "p/demo/base/b1/backup.xbstream",
	} {
		if !strings.Contains(got, kept) {
			t.Errorf("%s should have been kept: %s", kept, got)
		}
	}
}

func TestEnsureCredentialsCreatesDumpSecret(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := baseCluster()
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}
	plan := clusterPlan{
		RootSecretName: "demo-root", AppSecretName: "demo-app", ReplicationSecret: "demo-replication",
		ControlSecretName: "demo-control", BackupSecretName: "demo-backup",
	}
	if err := r.ensureCredentials(context.Background(), cluster, plan); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-dump"}, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["username"]) != "cnmsql_dump" || len(secret.Data["password"]) == 0 {
		t.Fatalf("dump secret data = %v", secret.Data)
	}
	if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].Name != cluster.Name {
		t.Fatalf("dump secret must be owned by the Cluster: %+v", secret.OwnerReferences)
	}
}
