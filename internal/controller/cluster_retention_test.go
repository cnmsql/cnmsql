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
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
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
		// Nothing listens there, so the list fails.
		ObjectStore:     storeAt("http://127.0.0.1:1", "backups", ""),
		RetentionPolicy: "30d",
	}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	old := metav1.NewTime(time.Now().Add(-2 * retentionInterval))
	cluster.Status.LastRetentionRunTime = &old

	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(cluster, s3CredentialsSecret()).Build(),
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

// memS3Server is an in-memory S3 bucket store: LIST filters keys by prefix,
// GET/HEAD serve stored bodies, DELETE removes and PUT stores. It records
// "METHOD /bucket/key" for every DELETE, PUT and GET of an object.
func memS3Server(t *testing.T, objects map[string]string) (*httptest.Server, func() []string) {
	t.Helper()
	return memS3ServerModified(t, objects, nil)
}

// memS3ServerModified is memS3Server with a listing that says when each object
// was last modified. modified returns the zero time for an object it has no
// time for, and the listing then leaves it out, as memS3Server's does.
func memS3ServerModified(
	t *testing.T, objects map[string]string, modified func(key string) time.Time,
) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var writes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		bucket, key := parts[0], ""
		if len(parts) == 2 {
			key = parts[1]
		}
		switch {
		case r.Method == http.MethodGet && key == "":
			prefix := r.URL.Query().Get("prefix")
			var b strings.Builder
			b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>` + bucket +
				`</Name><IsTruncated>false</IsTruncated>`)
			for k, body := range objects {
				if strings.HasPrefix(k, bucket+"/"+prefix) {
					lastModified := ""
					if modified != nil && !modified(k).IsZero() {
						lastModified = "<LastModified>" + modified(k).UTC().Format(time.RFC3339) + "</LastModified>"
					}
					fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>%d</Size>%s</Contents>",
						strings.TrimPrefix(k, bucket+"/"), len(body), lastModified)
				}
			}
			b.WriteString(`</ListBucketResult>`)
			_, _ = w.Write([]byte(b.String()))
		case r.Method == http.MethodGet || r.Method == http.MethodHead:
			if r.Method == http.MethodGet {
				writes = append(writes, "GET /"+bucket+"/"+key)
			}
			body, ok := objects[bucket+"/"+key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte(body))
			}
		case r.Method == http.MethodDelete:
			writes = append(writes, "DELETE /"+bucket+"/"+key)
			delete(objects, bucket+"/"+key)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPut:
			writes = append(writes, "PUT /"+bucket+"/"+key)
			w.Header().Set("ETag", `"0"`)
		}
	}))
	t.Cleanup(server.Close)
	return server, func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(writes) }
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Retention across two stores: the expired base backup is deleted through the
// base store and the binlog it alone covered, plus the rewritten index, through
// the archive store. Swapping the two clients fails this test.
func TestReconcileRetentionAppliesExpiryPerStore(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	old, recent := now.Add(-60*24*time.Hour), now.Add(-24*time.Hour)
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	baseSrv, baseWrites := memS3Server(t, map[string]string{
		"backups/base/demo/bk-old/id-old/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: testClusterUID, StartedAt: old, CompletedAt: old}),
		"backups/base/demo/bk-new/id-new/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: testClusterUID, StartedAt: recent, CompletedAt: recent}),
		"backups/base/demo/bk-old/id-old/backup.xbstream": "x",
	})
	logSrv, logWrites := memS3Server(t, map[string]string{
		"binlogs/archive/demo/binlogs/" + uuid + "/binlog.000001.json": mustJSON(t, objectstore.BinlogMetadata{
			ServerUUID: uuid, BinlogName: "binlog.000001", LastEventTime: old.Add(time.Hour)}),
		"binlogs/archive/demo/binlogs/" + uuid + "/binlog.000001": "b",
		"binlogs/archive/demo/binlogs/_index.json": mustJSON(t, objectstore.ArchiveIndex{
			ClusterName: "demo",
			ClusterUID:  testClusterUID,
			Segments:    []objectstore.ArchiveSegment{{ServerUUID: uuid, Binlogs: []string{"binlog.000001"}}},
		}),
	})

	cluster := baseCluster()
	cluster.UID = testClusterUID
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

	if got := baseWrites(); !slices.Contains(got, "DELETE /backups/base/demo/bk-old/id-old/metadata.json") ||
		anyContains(got, "binlogs/") {
		t.Fatalf("base store writes = %v, want the expired backup deleted and no binlog touched", got)
	}
	got := logWrites()
	for _, want := range []string{
		"DELETE /binlogs/archive/demo/binlogs/" + uuid + "/binlog.000001",
		"DELETE /binlogs/archive/demo/binlogs/" + uuid + "/binlog.000001.json",
		"PUT /binlogs/archive/demo/binlogs/_index.json",
	} {
		if !slices.Contains(got, want) {
			t.Fatalf("archive store writes %v missing %q", got, want)
		}
	}
	if anyContains(got, "bk-old") {
		t.Fatalf("archive store was asked to delete a base backup: %v", got)
	}
}

// A pass that expires no base backup has no binlog to delete, so it reads no
// per-file manifest: on a long archive those are thousands of GETs an hour
// for nothing. The index is still read, since it proves the archive is ours.
func TestReconcileRetentionReadsNoManifestWhenNothingExpired(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	recent := now.Add(-24 * time.Hour)
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	baseSrv, _ := memS3Server(t, map[string]string{
		"backups/base/demo/bk-new/id-new/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: testClusterUID, StartedAt: recent, CompletedAt: recent}),
	})
	logSrv, logRequests := memS3Server(t, map[string]string{
		"binlogs/archive/demo/binlogs/" + uuid + "/binlog.000001.json": mustJSON(t, objectstore.BinlogMetadata{
			ServerUUID: uuid, BinlogName: "binlog.000001", LastEventTime: recent}),
		"binlogs/archive/demo/binlogs/" + uuid + "/binlog.000001": "b",
		"binlogs/archive/demo/binlogs/_index.json": mustJSON(t, objectstore.ArchiveIndex{
			ClusterName: "demo",
			ClusterUID:  testClusterUID,
			Segments:    []objectstore.ArchiveSegment{{ServerUUID: uuid, Binlogs: []string{"binlog.000001"}}},
		}),
	})

	cluster := baseCluster()
	cluster.UID = testClusterUID
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
	got := logRequests()
	if anyContains(got, "binlog.000001.json") {
		t.Fatalf("archive store requests %v include a per-file manifest read with nothing expired", got)
	}
	if !slices.Contains(got, "GET /binlogs/archive/demo/binlogs/_index.json") {
		t.Fatalf("archive store requests %v do not read the index", got)
	}
	if anyContains(got, "DELETE") || anyContains(got, "PUT") {
		t.Fatalf("archive store was written to with nothing expired: %v", got)
	}
}

// A pass that does expire a base backup finds the binlogs behind the horizon
// from the listing and a binary search: on a long archive, reading every
// manifest holds the reconcile for minutes.
func TestReconcileRetentionExpiresBinlogsWithoutReadingEveryManifest(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	old, recent := now.Add(-60*24*time.Hour), now.Add(-24*time.Hour)
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	baseSrv, _ := memS3Server(t, map[string]string{
		"backups/base/demo/bk-old/id-old/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: testClusterUID, StartedAt: old, CompletedAt: old}),
		"backups/base/demo/bk-new/id-new/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: testClusterUID, StartedAt: recent, CompletedAt: recent}),
	})
	// 200 files, one every 8 hours since the old backup: files 1 to 176 end
	// before the retained backup started.
	const files, expired = 200, 176
	objects := map[string]string{}
	uploaded := map[string]time.Time{}
	names := make([]string, 0, files)
	for i := 1; i <= files; i++ {
		name := fmt.Sprintf("binlog.%06d", i)
		key := "binlogs/archive/demo/binlogs/" + uuid + "/" + name
		last := old.Add(time.Duration(i) * 8 * time.Hour)
		objects[key] = "b"
		objects[key+".json"] = mustJSON(t, objectstore.BinlogMetadata{
			ServerUUID: uuid, BinlogName: name, LastEventTime: last})
		uploaded[key+".json"] = last.Add(time.Minute)
		names = append(names, name)
	}
	objects["binlogs/archive/demo/binlogs/_index.json"] = mustJSON(t, objectstore.ArchiveIndex{
		ClusterName: "demo",
		ClusterUID:  testClusterUID,
		Segments:    []objectstore.ArchiveSegment{{ServerUUID: uuid, Binlogs: names}},
	})
	logSrv, logRequests := memS3ServerModified(t, objects, func(key string) time.Time { return uploaded[key] })

	cluster := baseCluster()
	cluster.UID = testClusterUID
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

	manifestReads, deletedFiles := 0, 0
	for _, request := range logRequests() {
		switch {
		case strings.HasPrefix(request, "GET ") && strings.Contains(request, "/binlog.") &&
			strings.HasSuffix(request, ".json"):
			manifestReads++
		case strings.HasPrefix(request, "DELETE ") && !strings.HasSuffix(request, ".json"):
			deletedFiles++
		}
	}
	if deletedFiles != expired {
		t.Fatalf("deleted %d binlogs, want the %d that end before the retained backup", deletedFiles, expired)
	}
	if manifestReads > 12 {
		t.Fatalf("read %d of %d manifests, want a binary search", manifestReads, files)
	}
}

// testClusterUID is the UID the retention fixtures stamp on what the cluster's
// workers wrote.
const testClusterUID = "8f0f2a4c-1d9e-4f6b-9a51-2c3d4e5f6a7b"

func TestRetentionOwnership(t *testing.T) {
	t.Parallel()

	ours := func(uid string) []objectstore.BackupEntry {
		return []objectstore.BackupEntry{{Meta: objectstore.BackupMetadata{ClusterUID: uid}}}
	}
	dump := func(uid string) []objectstore.LogicalBackupEntry {
		return []objectstore.LogicalBackupEntry{{Meta: objectstore.LogicalBackupMetadata{ClusterUID: uid}}}
	}
	index := func(uid string) *objectstore.ArchiveIndex { return &objectstore.ArchiveIndex{ClusterUID: uid} }
	binlogs := 1

	cases := []struct {
		name    string
		uid     string
		same    bool
		backups []objectstore.BackupEntry
		logical []objectstore.LogicalBackupEntry
		binlogs int
		index   *objectstore.ArchiveIndex
		owned   bool
	}{
		{name: "nothing to delete", uid: testClusterUID, owned: true},
		{name: "own base backup and index", uid: testClusterUID,
			backups: ours(testClusterUID), binlogs: binlogs, index: index(testClusterUID), owned: true},
		{name: "own logical backup proves the backup store", uid: testClusterUID,
			backups: ours(""), logical: dump(testClusterUID), owned: true},
		{name: "same location: the index proves the backups", uid: testClusterUID, same: true,
			backups: ours(""), binlogs: binlogs, index: index(testClusterUID), owned: true},
		{name: "same location: a backup proves the archive", uid: testClusterUID, same: true,
			backups: ours(testClusterUID), binlogs: binlogs, index: index(""), owned: true},
		{name: "another cluster's backups", uid: testClusterUID,
			backups: ours("victim"), logical: dump("victim")},
		{name: "legacy backups without a UID", uid: testClusterUID, backups: ours("")},
		{name: "another cluster's archive", uid: testClusterUID,
			backups: ours(testClusterUID), binlogs: binlogs, index: index("victim")},
		{name: "separate stores: a backup does not prove the archive", uid: testClusterUID,
			backups: ours(testClusterUID), binlogs: binlogs, index: index("")},
		{name: "binlogs without an index", uid: testClusterUID, binlogs: binlogs},
		{name: "cluster without a UID matches no legacy object", uid: "", backups: ours("")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reason := retentionOwnership(tc.uid, tc.same, tc.backups, tc.logical, tc.binlogs, tc.index)
			if owned := reason == ""; owned != tc.owned {
				t.Fatalf("owned = %v (reason %q), want %v", owned, reason, tc.owned)
			}
		})
	}
}

// A Cluster pointed at another namespace's same-named archive (the
// GHSA-qxwf-59cm-cj4x attack) lists it but deletes nothing, warns, and still
// stamps the run time so the warning repeats once an interval.
func TestReconcileRetentionSkipsArchiveItDoesNotOwn(t *testing.T) {
	t.Parallel()

	old := time.Now().UTC().Add(-60 * 24 * time.Hour)
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	srv, writes := memS3Server(t, map[string]string{
		"backups/p/demo/bk-1/id-1/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: "victim", StartedAt: old, CompletedAt: old}),
		"backups/p/demo/bk-2/id-2/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: "victim", StartedAt: old.Add(time.Hour), CompletedAt: old.Add(time.Hour)}),
		"backups/p/demo/dump/d1/logical.json": mustJSON(t, objectstore.LogicalBackupMetadata{
			ClusterUID: "victim", Method: "logical", CompletedAt: old}),
		"backups/p/demo/binlogs/" + uuid + "/binlog.000001.json": mustJSON(t, objectstore.BinlogMetadata{
			ServerUUID: uuid, BinlogName: "binlog.000001", LastEventTime: old}),
		"backups/p/demo/binlogs/_index.json": mustJSON(t, objectstore.ArchiveIndex{
			ClusterName: "demo", ClusterUID: "victim",
			Segments: []objectstore.ArchiveSegment{{ServerUUID: uuid, Binlogs: []string{"binlog.000001"}}},
		}),
	})

	cluster := baseCluster()
	cluster.UID = testClusterUID
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore:     storeAt(srv.URL, "backups", "p"),
		RetentionPolicy: "1d",
	}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(10)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, s3CredentialsSecret()).Build(),
		Scheme:   scheme,
		Recorder: recorder,
	}
	if err := r.reconcileRetention(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	if got := slices.DeleteFunc(writes(), func(s string) bool { return strings.HasPrefix(s, "GET ") }); len(got) != 0 {
		t.Fatalf("retention wrote to a store it does not own: %v", got)
	}
	if event := <-recorder.Events; !strings.Contains(event, eventRetentionNotOwned) {
		t.Fatalf("event = %q, want %s", event, eventRetentionNotOwned)
	}
	assertRetentionStamped(t, r, cluster)
}

// Without the operator flag, a store with no static credentials is never
// contacted with the operator's own identity.
func TestReconcileRetentionRefusesAmbientCredentials(t *testing.T) {
	t.Parallel()

	srv, seen := recordingS3Server(t)
	store := &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Endpoint: srv.URL,
		Credentials: mysqlv1alpha1.S3Credentials{InheritFromIAMRole: true}}
	store.SetDefaults()
	cluster := baseCluster()
	cluster.UID = testClusterUID
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{ObjectStore: store, RetentionPolicy: "1d"}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(10)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).WithObjects(cluster).Build(),
		Scheme:   scheme,
		Recorder: recorder,
	}
	if err := r.reconcileRetention(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	if got := seen(); len(got) != 0 {
		t.Fatalf("operator contacted the store: %v", got)
	}
	if event := <-recorder.Events; !strings.Contains(event, eventObjectStoreCredentialsRequired) {
		t.Fatalf("event = %q, want %s", event, eventObjectStoreCredentialsRequired)
	}
	assertRetentionStamped(t, r, cluster)
}

func assertRetentionStamped(t *testing.T, r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster) {
	t.Helper()
	latest := &mysqlv1alpha1.Cluster{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(cluster), latest); err != nil {
		t.Fatal(err)
	}
	if latest.Status.LastRetentionRunTime == nil {
		t.Fatal("lastRetentionRunTime not stamped; the skip would repeat on every reconcile")
	}
}

// A store naming a Secret that is not opted in as credentials (GHSA-h4q2) is
// skipped like one the operator has no credentials for, instead of retrying
// on every reconcile.
func TestReconcileRetentionSkipsRefusedCredentialSecrets(t *testing.T) {
	t.Parallel()

	srv, seen := recordingS3Server(t)
	cluster := baseCluster()
	cluster.UID = testClusterUID
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore: storeAt(srv.URL, "backups", ""), RetentionPolicy: "1d",
	}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	unlabelled := s3CredentialsSecret()
	unlabelled.Labels = nil
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(10)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).WithObjects(cluster, unlabelled).Build(),
		Scheme:   scheme,
		Recorder: recorder,
	}
	if err := r.reconcileRetention(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if got := seen(); len(got) != 0 {
		t.Fatalf("operator contacted the store: %v", got)
	}
	if event := <-recorder.Events; !strings.Contains(event, reasonObjectStoreCredentialsRefused) {
		t.Fatalf("event = %q, want %s", event, reasonObjectStoreCredentialsRefused)
	}
	assertRetentionStamped(t, r, cluster)
}

// completedSnapshotBackup is a completed volumeSnapshot Backup of cluster.
func completedSnapshotBackup(name, cluster string, started, completed time.Time) *mysqlv1alpha1.Backup {
	startedAt, stoppedAt := metav1.NewTime(started), metav1.NewTime(completed)
	return &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: cluster},
			Method:  mysqlv1alpha1.BackupMethodVolumeSnapshot,
		},
		Status: mysqlv1alpha1.BackupStatus{
			Phase:     mysqlv1alpha1.BackupPhaseCompleted,
			StartedAt: &startedAt,
			StoppedAt: &stoppedAt,
		},
	}
}

func TestReconcileRetentionExpiresSnapshotBackupsWithoutAnObjectStore(t *testing.T) {
	t.Parallel()

	now := time.Now()
	day := 24 * time.Hour
	cluster := baseCluster()
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		VolumeSnapshot:  &mysqlv1alpha1.VolumeSnapshotConfiguration{},
		RetentionPolicy: "7d",
	}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	expired := completedSnapshotBackup("snap-old", "demo", now.Add(-30*day), now.Add(-30*day))
	newest := completedSnapshotBackup("snap-new", "demo", now.Add(-20*day), now.Add(-20*day))
	otherCluster := completedSnapshotBackup("other-old", "other", now.Add(-30*day), now.Add(-30*day))
	running := completedSnapshotBackup("snap-running", "demo", now.Add(-40*day), now.Add(-40*day))
	running.Status.Phase = mysqlv1alpha1.BackupPhaseRunning

	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, expired, newest, otherCluster, running).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if err := r.reconcileRetention(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	got := &mysqlv1alpha1.Backup{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(expired), got); err != nil {
		t.Fatal(err)
	}
	if got.DeletionTimestamp.IsZero() || got.Annotations[mysqlv1alpha1.BackupDeleteVolumeSnapshotsAnnotation] != "true" ||
		!slices.Contains(got.Finalizers, mysqlv1alpha1.BackupCleanupFinalizer) {
		t.Fatalf("expired snapshot Backup not deleted with its cleanup: %+v", got.ObjectMeta)
	}
	for _, kept := range []*mysqlv1alpha1.Backup{newest, otherCluster, running} {
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(kept), got); err != nil {
			t.Fatalf("%s: %v", kept.Name, err)
		}
		if !got.DeletionTimestamp.IsZero() {
			t.Fatalf("%s was expired; only the cluster's completed, superseded snapshots expire", kept.Name)
		}
	}
	assertRetentionStamped(t, r, cluster)
}

// A retained volume snapshot started before an archived binlog's last event
// still needs that binlog for point-in-time recovery, though the object-store
// backup the binlog followed expired.
func TestReconcileRetentionKeepsBinlogsARetainedSnapshotNeeds(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	old, recent := now.Add(-60*24*time.Hour), now.Add(-24*time.Hour)
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	baseSrv, baseWrites := memS3Server(t, map[string]string{
		"backups/base/demo/bk-old/id-old/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: testClusterUID, StartedAt: old, CompletedAt: old}),
		"backups/base/demo/bk-new/id-new/metadata.json": mustJSON(t, objectstore.BackupMetadata{
			ClusterUID: testClusterUID, StartedAt: recent, CompletedAt: recent}),
		"backups/base/demo/bk-old/id-old/backup.xbstream": "x",
		"backups/base/demo/binlogs/" + uuid + "/binlog.000001.json": mustJSON(t, objectstore.BinlogMetadata{
			ServerUUID: uuid, BinlogName: "binlog.000001", LastEventTime: old.Add(2 * time.Hour)}),
		"backups/base/demo/binlogs/" + uuid + "/binlog.000001": "b",
		"backups/base/demo/binlogs/_index.json": mustJSON(t, objectstore.ArchiveIndex{
			ClusterName: "demo",
			ClusterUID:  testClusterUID,
			Segments:    []objectstore.ArchiveSegment{{ServerUUID: uuid, Binlogs: []string{"binlog.000001"}}},
		}),
	})

	cluster := baseCluster()
	cluster.UID = testClusterUID
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore:     storeAt(baseSrv.URL, "backups", "base"),
		VolumeSnapshot:  &mysqlv1alpha1.VolumeSnapshotConfiguration{},
		RetentionPolicy: "30d",
	}
	cluster.Status.CurrentPrimary = instanceName(cluster, 1)
	snapshot := completedSnapshotBackup("snap", "demo", old.Add(time.Hour), recent)
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster, s3CredentialsSecret(), snapshot).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if err := r.reconcileRetention(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	got := baseWrites()
	if !slices.Contains(got, "DELETE /backups/base/demo/bk-old/id-old/metadata.json") {
		t.Fatalf("store writes = %v, want the expired backup deleted", got)
	}
	for _, req := range got {
		if strings.HasPrefix(req, "DELETE ") && strings.Contains(req, "binlog.000001") {
			t.Fatalf("store writes = %v, want the binlog the retained snapshot needs kept", got)
		}
	}
}
