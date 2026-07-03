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
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
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
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-s3", Namespace: "default"},
		Data:       map[string][]byte{"access": []byte("key"), "secret": []byte("secret")},
	}
	reconciler := &ClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, secret).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	if err := reconciler.reconcileClusterDelete(context.Background(), cluster); err != nil {
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

	if err := reconciler.reconcileClusterDelete(context.Background(), cluster); err != nil {
		t.Fatalf("delete without our finalizer should be a no-op, got %v", err)
	}
}
