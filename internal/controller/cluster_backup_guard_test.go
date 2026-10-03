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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

const listNonEmpty = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>cluster-backups</Name><Prefix>clusters/demo/</Prefix><KeyCount>1</KeyCount>
  <MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>clusters/demo/old/id/backup.xbstream</Key><Size>42</Size></Contents>
</ListBucketResult>`

const listEmpty = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>cluster-backups</Name><Prefix>clusters/demo/</Prefix><KeyCount>0</KeyCount>
  <MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`

func s3CredentialsSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-s3", Namespace: "default"},
		Data:       map[string][]byte{"access": []byte("key"), "secret": []byte("secret")},
	}
}

func freshArchivingCluster(endpoint string) *mysqlv1alpha1.Cluster {
	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = "" // fresh: no primary established yet
	cluster.SetDefaults()              // path-style + signature defaults on the store
	cluster.Spec.Backup.ObjectStore.Endpoint = endpoint
	return cluster
}

func guardReconciler(t *testing.T) *ClusterReconciler {
	t.Helper()
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s3CredentialsSecret()).Build()
	return &ClusterReconciler{Client: client, Scheme: scheme}
}

func TestCheckBackupDestinationBlocksNonEmpty(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listNonEmpty))
	}))
	defer server.Close()

	cluster := freshArchivingCluster(server.URL)
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil {
		t.Fatalf("unexpected retry: %v", check.Retry)
	}
	if check.Blocked == "" {
		t.Fatal("expected a non-empty destination to block the cluster")
	}
}

func TestCheckBackupDestinationAllowsEmpty(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listEmpty))
	}))
	defer server.Close()

	cluster := freshArchivingCluster(server.URL)
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil || check.Blocked != "" {
		t.Fatalf("empty destination should pass, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}

func TestCheckBackupDestinationSkipsEstablishedCluster(t *testing.T) {
	t.Parallel()

	// A reachable primary means the cluster already owns its archive; the check
	// must not run (and must not need the object store at all).
	cluster := baseBackupCluster()                                  // CurrentPrimary = "demo-1"
	cluster.Spec.Backup.ObjectStore.Endpoint = "http://127.0.0.1:1" // would fail if dialed
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Blocked != "" || check.Retry != nil {
		t.Fatalf("established cluster should skip the check, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}

func TestCheckBackupDestinationSkipsRecovery(t *testing.T) {
	t.Parallel()

	cluster := freshArchivingCluster("http://127.0.0.1:1") // would fail if dialed
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{
			Backup: &mysqlv1alpha1.LocalObjectReference{Name: "backup-sample"},
		},
	}
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Blocked != "" || check.Retry != nil {
		t.Fatalf("recovery bootstrap should skip the check, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}

func TestCheckBackupDestinationSkipsWithoutObjectStore(t *testing.T) {
	t.Parallel()

	cluster := baseCluster()
	cluster.Status.CurrentPrimary = ""
	reconciler := guardReconciler(t)

	check := reconciler.checkBackupDestination(context.Background(), cluster)
	if check.Blocked != "" || check.Retry != nil {
		t.Fatalf("cluster without a backup store should skip the check, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}

func TestCheckRecoveryTargetReadsArchiveStore(t *testing.T) {
	t.Parallel()

	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	base := httptest.NewServer(http.NotFoundHandler())
	defer base.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/binlogs/_index.json") {
			body := `{"clusterName":"src","segments":[],"coveredGTIDSet":"` + uuid + `:1-10","updatedAt":"2026-10-01T00:00:00Z"}`
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write([]byte(body))
			return
		}
		http.NotFound(w, r)
	}))
	defer archive.Close()

	cluster := baseBackupCluster()
	cluster.Status.CurrentPrimary = ""
	baseStore := cluster.Spec.Backup.ObjectStore.DeepCopy()
	baseStore.Endpoint = base.URL
	baseStore.SetDefaults()
	archiveStore := baseStore.DeepCopy()
	archiveStore.Endpoint = archive.URL
	archiveStore.Bucket = "binlogs"

	plan := clusterPlan{Recovery: &recoveryPlan{
		HasTarget:     true,
		TargetGTID:    uuid + ":1-5",
		SourceCluster: "src",
		Store:         *baseStore,
		BinlogStore:   *archiveStore,
	}}
	check := guardReconciler(t).checkRecoveryTarget(context.Background(), cluster, plan)
	if check.Retry != nil || check.Blocked != "" {
		t.Fatalf("target inside the archive store's coverage must pass, got blocked=%q retry=%v",
			check.Blocked, check.Retry)
	}
}

func TestCheckBackupDestinationBlocksNonEmptyArchiveStore(t *testing.T) {
	t.Parallel()

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listEmpty))
	}))
	defer empty.Close()
	nonEmpty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listNonEmpty))
	}))
	defer nonEmpty.Close()

	cluster := freshArchivingCluster(empty.URL)
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: true, ObjectStore: storeAt(nonEmpty.URL, "binlogs", "archive"),
	}

	check := guardReconciler(t).checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil {
		t.Fatalf("unexpected retry: %v", check.Retry)
	}
	if !strings.Contains(check.Blocked, "binlogs") {
		t.Fatalf("a non-empty archive store must block and name its bucket, got %q", check.Blocked)
	}
}

func TestCheckBackupDestinationAllowsBothEmpty(t *testing.T) {
	t.Parallel()

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listEmpty))
	}))
	defer empty.Close()

	cluster := freshArchivingCluster(empty.URL)
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: true, ObjectStore: storeAt(empty.URL, "binlogs", "archive"),
	}
	check := guardReconciler(t).checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil || check.Blocked != "" {
		t.Fatalf("empty stores should pass, got blocked=%q retry=%v", check.Blocked, check.Retry)
	}
}

func TestCheckBackupDestinationIgnoresArchiveStoreWhenArchivingIsOff(t *testing.T) {
	t.Parallel()

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(listEmpty))
	}))
	defer empty.Close()

	cluster := freshArchivingCluster(empty.URL)
	// A leftover archive store whose endpoint is unreachable must not hold the
	// cluster in provisioning while archiving is off.
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: false, ObjectStore: storeAt("http://127.0.0.1:1", "binlogs", "archive"),
	}
	check := guardReconciler(t).checkBackupDestination(context.Background(), cluster)
	if check.Retry != nil || check.Blocked != "" {
		t.Fatalf("archive store must be ignored while archiving is off, got blocked=%q retry=%v",
			check.Blocked, check.Retry)
	}
}
