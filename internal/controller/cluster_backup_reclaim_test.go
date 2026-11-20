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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// listClusterArchive is the S3 LIST response for a cluster's whole archive
// prefix: one base backup archive plus an archived binlog segment.
const listClusterArchive = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>cluster-backups</Name><Prefix>clusters/demo/</Prefix>
  <KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>clusters/demo/bkp/bkp-1/backup.xbstream</Key><Size>42</Size></Contents>
  <Contents><Key>clusters/demo/binlogs/uuid/bin.000001</Key><Size>10</Size></Contents>
</ListBucketResult>`

func reclaimCluster(store *mysqlv1alpha1.S3ObjectStore) *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore:   store,
		ReclaimPolicy: mysqlv1alpha1.BackupReclaimDelete,
	}
	return cluster
}

func TestClusterReclaimDeleteAddsFinalizer(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := reclaimCluster(&mysqlv1alpha1.S3ObjectStore{Bucket: "cluster-backups"})
	reconciler := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	changed, err := reconciler.reconcileBackupReclaimFinalizer(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected the finalizer to be added")
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(got.Finalizers, clusterBackupFinalizer) {
		t.Fatalf("reclaimPolicy Delete should add the cluster finalizer, got %v", got.Finalizers)
	}
}

func TestClusterReclaimRetainRemovesFinalizer(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseCluster()
	// Retain (default) but the object still carries the teardown finalizer.
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore:   &mysqlv1alpha1.S3ObjectStore{Bucket: "cluster-backups"},
		ReclaimPolicy: mysqlv1alpha1.BackupReclaimRetain,
	}
	cluster.Finalizers = []string{clusterBackupFinalizer}
	reconciler := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	changed, err := reconciler.reconcileBackupReclaimFinalizer(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected the finalizer to be removed")
	}

	got := &mysqlv1alpha1.Cluster{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: cluster.Name}, got); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(got.Finalizers, clusterBackupFinalizer) {
		t.Fatalf("reclaimPolicy Retain should remove the cluster finalizer, got %v", got.Finalizers)
	}
}

func TestClusterDeleteWipesWholeArchive(t *testing.T) {
	t.Parallel()

	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted = append(deleted, r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write([]byte(listClusterArchive))
	}))
	defer server.Close()

	scheme := testScheme(t)
	forcePath := true
	store := &mysqlv1alpha1.S3ObjectStore{
		Bucket:         "cluster-backups",
		Path:           "clusters",
		Endpoint:       server.URL,
		ForcePathStyle: &forcePath,
		Credentials: mysqlv1alpha1.S3Credentials{
			AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "access"},
			SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "secret"},
		},
	}
	cluster := reclaimCluster(store)
	cluster.Finalizers = []string{clusterBackupFinalizer}
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-s3", Namespace: "default", Labels: map[string]string{objectstore.CredentialsSecretLabel: "true"}},
		Data:       map[string][]byte{"access": []byte("key"), "secret": []byte("secret")},
	}
	reconciler := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, secret).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	if _, err := reconciler.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}

	if len(deleted) != 2 {
		t.Fatalf("expected the whole archive prefix (2 objects) to be deleted, got %d: %v", len(deleted), deleted)
	}

	// Finalizer released, so the Cluster object is gone.
	got := &mysqlv1alpha1.Cluster{}
	err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: cluster.Name}, got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("cluster should be deleted after cleanup, got err=%v finalizers=%v", err, got.Finalizers)
	}
}

func TestClusterDeleteWithoutFinalizerIsNoop(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseCluster()
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	// A cluster stuck in deletion needs a finalizer to persist in the fake
	// client; use an unrelated one so our teardown path stays a no-op.
	cluster.Finalizers = []string{"example.com/other"}
	reconciler := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	if _, err := reconciler.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatalf("delete without our finalizer should be a no-op, got %v", err)
	}
}

func TestClusterDeleteWipesBothStores(t *testing.T) {
	t.Parallel()

	baseSrv, baseSeen := recordingS3Server(t)
	logSrv, logSeen := recordingS3Server(t)
	cluster := reclaimCluster(storeAt(baseSrv.URL, "backups", "base"))
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: true, ObjectStore: storeAt(logSrv.URL, "binlogs", "archive"),
	}
	cluster.Finalizers = []string{clusterBackupFinalizer}
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, s3CredentialsSecret()).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if _, err := r.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if !anyContains(baseSeen(), "prefix=base/demo/") {
		t.Fatalf("base prefix not removed: %v", baseSeen())
	}
	if !anyContains(logSeen(), "prefix=archive/demo/") {
		t.Fatalf("archive prefix not removed: %v", logSeen())
	}
}

func TestClusterDeleteWipesSharedStoreOnce(t *testing.T) {
	t.Parallel()

	srv, seen := recordingS3Server(t)
	cluster := reclaimCluster(storeAt(srv.URL, "backups", "base"))
	cluster.Spec.Backup.ContinuousArchiving = &mysqlv1alpha1.ContinuousArchivingConfiguration{
		Enabled: true, ObjectStore: storeAt(srv.URL, "backups", "/base/"),
	}
	cluster.Finalizers = []string{clusterBackupFinalizer}
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, s3CredentialsSecret()).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if _, err := r.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	lists := 0
	for _, s := range seen() {
		if strings.HasPrefix(s, "GET ") && strings.Contains(s, "prefix=base/demo/") {
			lists++
		}
	}
	if lists != 1 {
		t.Fatalf("one location must be listed once, got %d: %v", lists, seen())
	}
}

// deletingReclaimCluster is a Cluster being deleted with reclaimPolicy Delete,
// archiving to srv.
func deletingReclaimCluster(srv *httptest.Server) *mysqlv1alpha1.Cluster {
	cluster := reclaimCluster(storeAt(srv.URL, "backups", "base"))
	cluster.UID = "cluster-uid"
	cluster.Finalizers = []string{clusterBackupFinalizer}
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	return cluster
}

// clusterPod is a running Pod labelled for cluster; owned makes it one of the
// cluster's instance Pods, otherwise it stands for a backup worker Pod.
func clusterPod(cluster *mysqlv1alpha1.Cluster, name string, owned bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: cluster.Namespace,
			Labels: map[string]string{clusterLabel: cluster.Name},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if owned {
		isController := true
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: mysqlv1alpha1.GroupVersion.String(), Kind: "Cluster",
			Name: cluster.Name, UID: cluster.UID, Controller: &isController,
		}}
	}
	return pod
}

func wipedPrefix(seen []string) bool { return anyContains(seen, "prefix=base/demo/") }

func TestClusterDeleteStopsInstancesBeforeWipingArchive(t *testing.T) {
	t.Parallel()

	srv, seen := recordingS3Server(t)
	cluster := deletingReclaimCluster(srv)
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, s3CredentialsSecret(), clusterPod(cluster, "demo-1", true)).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	result, err := r.reconcileClusterDelete(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("a running instance must delay the wipe, got %+v", result)
	}
	if wipedPrefix(seen()) {
		t.Fatalf("archive removed while an instance could still archive: %v", seen())
	}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-1"}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the instance Pod should have been deleted, got err=%v", err)
	}

	// The instance is gone: the next pass removes the archive.
	if _, err := r.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if !wipedPrefix(seen()) {
		t.Fatalf("archive not removed once the instances stopped: %v", seen())
	}
}

func TestClusterDeleteWaitsForBackupWorkers(t *testing.T) {
	t.Parallel()

	srv, seen := recordingS3Server(t)
	cluster := deletingReclaimCluster(srv)
	worker := clusterPod(cluster, "demo-backup-x", false)
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, s3CredentialsSecret(), worker).WithStatusSubresource(worker).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	result, err := r.reconcileClusterDelete(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 0 || wipedPrefix(seen()) {
		t.Fatalf("a running backup worker must delay the wipe, got %+v, requests %v", result, seen())
	}
	got := &corev1.Pod{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: worker.Name}, got); err != nil {
		t.Fatalf("a backup worker the cluster does not own must not be deleted: %v", err)
	}

	got.Status.Phase = corev1.PodSucceeded
	if err := r.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if !wipedPrefix(seen()) {
		t.Fatalf("archive not removed once the worker finished: %v", seen())
	}
}

func TestClusterDeleteSkipsPodsStuckTerminating(t *testing.T) {
	t.Parallel()

	srv, seen := recordingS3Server(t)
	cluster := deletingReclaimCluster(srv)
	stuck := clusterPod(cluster, "demo-1", true)
	gone := metav1.NewTime(time.Now().Add(-stuckPodMargin - time.Minute))
	stuck.DeletionTimestamp = &gone
	stuck.Finalizers = []string{"example.com/hold"} // keeps it Terminating in the fake client
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, s3CredentialsSecret(), stuck).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	if _, err := r.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if !wipedPrefix(seen()) {
		t.Fatalf("a Pod stuck Terminating past its grace period must not block the wipe: %v", seen())
	}
}

func TestMayWriteArchive(t *testing.T) {
	t.Parallel()

	now := time.Now()
	at := func(d time.Duration) *metav1.Time { ts := metav1.NewTime(now.Add(d)); return &ts }
	cases := map[string]struct {
		phase    corev1.PodPhase
		deletion *metav1.Time
		want     bool
	}{
		"running":                    {corev1.PodRunning, nil, true},
		"pending":                    {corev1.PodPending, nil, true},
		"succeeded":                  {corev1.PodSucceeded, nil, false},
		"failed":                     {corev1.PodFailed, nil, false},
		"terminating within grace":   {corev1.PodRunning, at(time.Minute), true},
		"just past grace":            {corev1.PodRunning, at(-stuckPodMargin / 2), true},
		"stuck well past its grace":  {corev1.PodRunning, at(-stuckPodMargin - time.Second), false},
		"finished while terminating": {corev1.PodSucceeded, at(time.Minute), false},
	}
	for name, tc := range cases {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: tc.deletion}, Status: corev1.PodStatus{Phase: tc.phase}}
		if got := mayWriteArchive(pod, now); got != tc.want {
			t.Errorf("%s: mayWriteArchive = %v, want %v", name, got, tc.want)
		}
	}
}

func TestClusterDeleteOrphaningLeavesPodsAlone(t *testing.T) {
	t.Parallel()

	srv, seen := recordingS3Server(t)
	cluster := deletingReclaimCluster(srv)
	cluster.Finalizers = append(cluster.Finalizers, metav1.FinalizerOrphanDependents)
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(cluster, s3CredentialsSecret(), clusterPod(cluster, "demo-1", true)).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	if _, err := r.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-1"}, &corev1.Pod{}); err != nil {
		t.Fatalf("an orphaning delete must keep the instance Pods: %v", err)
	}
	if !wipedPrefix(seen()) {
		t.Fatalf("archive not removed: %v", seen())
	}
}
