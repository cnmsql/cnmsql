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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// reconcileRetention is gated: it must short-circuit (touching no object store)
// when there is no policy, no object store, no established primary, or the
// throttle window has not elapsed. These paths return before any S3 access, so
// they are safe to exercise with only a fake client.
func TestReconcileRetentionGating(t *testing.T) {
	t.Parallel()

	withRetention := func(mutate func(*mysqlv1alpha1.Cluster)) *mysqlv1alpha1.Cluster {
		cluster := baseCluster()
		cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
			ObjectStore:     &mysqlv1alpha1.S3ObjectStore{Bucket: "backups"},
			RetentionPolicy: "30d",
		}
		cluster.Status.CurrentPrimary = instanceName(cluster, 1)
		mutate(cluster)
		return cluster
	}

	cases := map[string]*mysqlv1alpha1.Cluster{
		"no backup config": func() *mysqlv1alpha1.Cluster {
			c := baseCluster()
			c.Status.CurrentPrimary = instanceName(c, 1)
			return c
		}(),
		"no retention policy": withRetention(func(c *mysqlv1alpha1.Cluster) {
			c.Spec.Backup.RetentionPolicy = ""
		}),
		"no object store": withRetention(func(c *mysqlv1alpha1.Cluster) {
			c.Spec.Backup.ObjectStore = nil
		}),
		"no primary yet": withRetention(func(c *mysqlv1alpha1.Cluster) {
			c.Status.CurrentPrimary = ""
		}),
		"throttled": withRetention(func(c *mysqlv1alpha1.Cluster) {
			now := metav1.Now()
			c.Status.LastRetentionRunTime = &now
		}),
	}

	for name, cluster := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			reconciler := &ClusterReconciler{
				Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cluster).Build(),
				Scheme: testScheme(t),
			}
			if err := reconciler.reconcileRetention(context.Background(), cluster); err != nil {
				t.Fatalf("reconcileRetention returned error: %v", err)
			}
		})
	}
}

// An expired throttle plus a reachable (but here unreachable) store should
// attempt object-store access and surface the error for a retry rather than
// silently succeeding.
func TestReconcileRetentionThrottleExpired(t *testing.T) {
	t.Parallel()

	cluster := baseCluster()
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore: &mysqlv1alpha1.S3ObjectStore{
			Bucket:   "backups",
			Endpoint: "http://127.0.0.1:1", // nothing listening → list fails
		},
		RetentionPolicy: "30d",
	}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	old := metav1.NewTime(time.Now().Add(-2 * retentionInterval))
	cluster.Status.LastRetentionRunTime = &old

	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cluster).Build(),
		Scheme: testScheme(t),
	}
	if err := reconciler.reconcileRetention(context.Background(), cluster); err == nil {
		t.Fatal("expected an object-store error to be surfaced for retry")
	}
}

// recordingS3Server answers LIST with an empty result, HEAD with 404 and
// DELETE with 204, and records "METHOD /path?prefix=..." for each request.
func recordingS3Server(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path+"?prefix="+r.URL.Query().Get("prefix"))
		mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
		}
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(seen) }
}

func storeAt(endpoint, bucket, path string) *mysqlv1alpha1.S3ObjectStore {
	store := &mysqlv1alpha1.S3ObjectStore{
		Bucket: bucket, Path: path, Endpoint: endpoint,
		Credentials: mysqlv1alpha1.S3Credentials{
			AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "access"},
			SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "secret"},
		},
	}
	store.SetDefaults()
	return store
}

func anyContains(seen []string, sub string) bool {
	return slices.ContainsFunc(seen, func(s string) bool { return strings.Contains(s, sub) })
}

func TestReconcileRetentionListsBinlogsInArchiveStore(t *testing.T) {
	t.Parallel()

	baseSrv, baseSeen := recordingS3Server(t)
	logSrv, logSeen := recordingS3Server(t)
	cluster := baseCluster()
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore:     storeAt(baseSrv.URL, "backups", "base"),
		RetentionPolicy: "30d",
		ContinuousArchiving: &mysqlv1alpha1.ContinuousArchivingConfiguration{
			Enabled: true, ObjectStore: storeAt(logSrv.URL, "binlogs", "archive"),
		},
	}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, s3CredentialsSecret()).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if err := r.reconcileRetention(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if anyContains(baseSeen(), "binlogs/") {
		t.Fatalf("base store was asked for binlogs: %v", baseSeen())
	}
	if !anyContains(logSeen(), "prefix=archive/demo/binlogs/") {
		t.Fatalf("archive store was not listed for binlogs: %v", logSeen())
	}
	if !anyContains(baseSeen(), "prefix=base/demo/") {
		t.Fatalf("base store was not listed for base backups: %v", baseSeen())
	}
}
