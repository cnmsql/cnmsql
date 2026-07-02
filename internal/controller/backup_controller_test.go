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
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// listBackupArchive is the S3 LIST response for a single backup's archive
// directory: the xbstream and its metadata.json.
const listBackupArchive = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>override-backups</Name><Prefix>manual/demo/backup-sample/backup-sample-123/</Prefix>
  <KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>manual/demo/backup-sample/backup-sample-123/backup.xbstream</Key><Size>42</Size></Contents>
  <Contents><Key>manual/demo/backup-sample/backup-sample-123/metadata.json</Key><Size>10</Size></Contents>
</ListBucketResult>`

// demoPrimaryInstance is the conventional primary instance name used across
// the controller unit tests.
const demoPrimaryInstance = "demo-1"

func baseBackupCluster() *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Status.CurrentPrimary = demoPrimaryInstance
	cluster.Status.Image = "ghcr.io/cnmsql/cnmsql-instance:8.4"
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		ObjectStore: &mysqlv1alpha1.S3ObjectStore{
			Bucket: "cluster-backups",
			Path:   "clusters",
			Credentials: mysqlv1alpha1.S3Credentials{
				AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "access"},
				SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "secret"},
			},
		},
	}
	return cluster
}

func baseBackup() *mysqlv1alpha1.Backup {
	return &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backup-sample",
			Namespace: "default",
		},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: "demo"},
		},
	}
}

// reconcileBackup runs a single reconcile pass and returns its result.
func reconcileBackup(t *testing.T, r *BackupReconciler, backup *mysqlv1alpha1.Backup) ctrl.Result {
	t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: backup.Namespace, Name: backup.Name}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func readyReplicaPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				clusterLabel: "demo",
				roleLabel:    roleReplica,
			},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		}}},
	}
}

func TestBackupReconcileCreatesWorkerJobFromClusterObjectStore(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseBackupCluster()
	backup := baseBackup()
	reconciler := &BackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(cluster, backup, readyReplicaPod("demo-2")).
			Build(),
		Scheme: scheme,
	}

	result := reconcileBackup(t, reconciler, backup)
	if result.RequeueAfter != provisioningRequeue {
		t.Fatalf("requeue = %s, want %s", result.RequeueAfter, provisioningRequeue)
	}

	job := &batchv1.Job{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample-backup"}, job); err != nil {
		t.Fatal(err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "ghcr.io/cnmsql/cnmsql-instance:8.4" {
		t.Fatalf("worker image = %q", container.Image)
	}
	args := strings.Join(container.Args, " ")
	for _, want := range []string{
		"instance backup upload",
		"--source-manager-url=https://demo-2.default.svc:8080/cluster/backup",
		"--bucket=cluster-backups",
		"--archive-key=clusters/demo/backup-sample/",
		"--sha256",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("worker args missing %q:\n%s", want, args)
		}
	}

	updated := &mysqlv1alpha1.Backup{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample"}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != mysqlv1alpha1.BackupPhaseRunning {
		t.Fatalf("phase = %q", updated.Status.Phase)
	}
	if updated.Status.InstanceName != "demo-2" {
		t.Fatalf("instance = %q", updated.Status.InstanceName)
	}
	if updated.Status.JobName != "backup-sample-backup" {
		t.Fatalf("jobName = %q", updated.Status.JobName)
	}
	if !strings.HasPrefix(updated.Status.DestinationPath, "s3://cluster-backups/clusters/demo/backup-sample/") {
		t.Fatalf("destination = %q", updated.Status.DestinationPath)
	}
	if cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionProgressing); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("progressing condition = %#v", cond)
	}
}

func TestBackupSpecObjectStoreOverridesCluster(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseBackupCluster()
	backup := baseBackup()
	backup.Spec.ObjectStore = &mysqlv1alpha1.S3ObjectStore{
		Bucket: "override-backups",
		Path:   "manual",
		Credentials: mysqlv1alpha1.S3Credentials{
			AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "override-s3", Key: "access"},
			SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "override-s3", Key: "secret"},
		},
	}
	reconciler := &BackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(cluster, backup).
			Build(),
		Scheme: scheme,
	}

	reconcileBackup(t, reconciler, backup)

	job := &batchv1.Job{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample-backup"}, job); err != nil {
		t.Fatal(err)
	}
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--bucket=override-backups") || !strings.Contains(args, "--archive-key=manual/demo/backup-sample/") {
		t.Fatalf("worker args did not use override object store:\n%s", args)
	}
}

func TestBackupPrimaryTargetUsesCurrentPrimary(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseBackupCluster()
	backup := baseBackup()
	backup.Spec.Target = mysqlv1alpha1.BackupTargetPrimary
	reconciler := &BackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(cluster, backup, readyReplicaPod("demo-2")).
			Build(),
		Scheme: scheme,
	}

	reconcileBackup(t, reconciler, backup)

	updated := &mysqlv1alpha1.Backup{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample"}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.InstanceName != demoPrimaryInstance {
		t.Fatalf("instance = %q, want primary", updated.Status.InstanceName)
	}
}

func TestRecoveryBootstrapRestoresPrimaryFromObjectStore(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseBackupCluster()
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{
			Backup: &mysqlv1alpha1.LocalObjectReference{Name: "backup-sample"},
		},
	}

	backup := baseBackup()
	backup.Status = mysqlv1alpha1.BackupStatus{
		Phase:    mysqlv1alpha1.BackupPhaseCompleted,
		BackupID: "backup-sample-123",
	}

	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, backup).Build(),
		Scheme: scheme,
	}

	plan, err := reconciler.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Recovery == nil {
		t.Fatal("plan.Recovery should be set for a recovery bootstrap")
	}
	wantKey := "clusters/demo/backup-sample/backup-sample-123/backup.xbstream"
	if plan.Recovery.Bucket != "cluster-backups" || plan.Recovery.ArchiveKey != wantKey {
		t.Fatalf("recovery target = %s/%s", plan.Recovery.Bucket, plan.Recovery.ArchiveKey)
	}

	spec := reconciler.podSpec(cluster, plan, plan.instanceFor(cluster, 1))
	initArgs := strings.Join(spec.InitContainers[1].Args, " ")
	for _, want := range []string{
		"instance restore",
		"--bucket=cluster-backups",
		"--archive-key=" + wantKey,
		"--metadata-key=clusters/demo/backup-sample/backup-sample-123/metadata.json",
	} {
		if !strings.Contains(initArgs, want) {
			t.Fatalf("restore init args missing %q:\n%s", want, initArgs)
		}
	}
	if strings.Contains(initArgs, "instance initdb") {
		t.Fatalf("recovery primary must not run initdb: %s", initArgs)
	}

	// The recovering primary's init container carries the object-store creds.
	var hasEndpoint, hasAccessKey bool
	for _, env := range spec.InitContainers[1].Env {
		switch env.Name {
		case "cnmsql_S3_ENDPOINT":
			hasEndpoint = true
		case "cnmsql_S3_ACCESS_KEY_ID":
			hasAccessKey = true
		}
	}
	if !hasEndpoint || !hasAccessKey {
		t.Fatalf("recovery init container missing S3 env (endpoint=%t accessKey=%t)", hasEndpoint, hasAccessKey)
	}

	// Recovery generates no app Secret, so the init container must not reference
	// one; a non-optional secretKeyRef would wedge the Pod in
	// CreateContainerConfigError.
	for _, env := range spec.InitContainers[1].Env {
		if env.Name == "MYSQL_APP_PASSWORD" {
			t.Fatal("recovery init container must not reference the app password secret")
		}
	}

	// A replica still clones from the primary via join, not restore.
	replicaSpec := reconciler.podSpec(cluster, plan, plan.instanceFor(cluster, 2))
	if got := strings.Join(replicaSpec.InitContainers[1].Args, " "); !strings.Contains(got, "instance join") {
		t.Fatalf("replica should join the primary, got: %s", got)
	}
}

func TestRecoveryBootstrapPITRTargetReplaysBinlogs(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseBackupCluster()
	// Fresh recovery: no primary established yet.
	cluster.Status.CurrentPrimary = ""
	immediate := false
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{
			Backup: &mysqlv1alpha1.LocalObjectReference{Name: "backup-sample"},
			RecoveryTarget: &mysqlv1alpha1.RecoveryTarget{
				TargetTime:      "2026-06-12T10:30:00Z",
				TargetImmediate: &immediate,
			},
		},
	}

	backup := baseBackup()
	backup.Status = mysqlv1alpha1.BackupStatus{
		Phase:    mysqlv1alpha1.BackupPhaseCompleted,
		BackupID: "backup-sample-123",
	}

	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, backup).Build(),
		Scheme: scheme,
	}

	plan, err := reconciler.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Recovery == nil || !plan.Recovery.HasTarget {
		t.Fatal("plan.Recovery.HasTarget should be set for a recovery with a target")
	}
	if plan.Recovery.SourceCluster != cluster.Name {
		t.Fatalf("source cluster = %q, want %q", plan.Recovery.SourceCluster, cluster.Name)
	}

	spec := reconciler.podSpec(cluster, plan, plan.instanceFor(cluster, 1))
	initArgs := strings.Join(spec.InitContainers[1].Args, " ")
	for _, want := range []string{
		"instance restore",
		"--source-cluster=demo",
		"--target-time=2026-06-12T10:30:00Z",
	} {
		if !strings.Contains(initArgs, want) {
			t.Fatalf("PITR restore init args missing %q:\n%s", want, initArgs)
		}
	}
	if strings.Contains(initArgs, "--target-immediate") {
		t.Fatalf("targetImmediate=false must not emit the flag: %s", initArgs)
	}

	// The bucket/path env the replay worker needs to rebuild binlog keys.
	var hasBucket, hasPath bool
	for _, env := range spec.InitContainers[1].Env {
		switch env.Name {
		case "cnmsql_S3_BUCKET":
			hasBucket = env.Value == "cluster-backups"
		case "cnmsql_S3_PATH":
			hasPath = env.Value == "clusters"
		}
	}
	if !hasBucket || !hasPath {
		t.Fatalf("recovery init container missing bucket/path env (bucket=%t path=%t)", hasBucket, hasPath)
	}
}

func TestRecoveryBootstrapWaitsForCompletedBackup(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseBackupCluster()
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{
			Backup: &mysqlv1alpha1.LocalObjectReference{Name: "backup-sample"},
		},
	}
	backup := baseBackup()
	backup.Status.Phase = mysqlv1alpha1.BackupPhaseRunning

	reconciler := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, backup).Build(),
		Scheme: scheme,
	}
	if _, err := reconciler.buildPlan(context.Background(), cluster); err == nil {
		t.Fatal("buildPlan should fail while the recovery backup is not completed")
	}
}

func TestBackupFailsWithoutObjectStore(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)
	cluster := baseBackupCluster()
	cluster.Spec.Backup = nil
	backup := baseBackup()
	reconciler := &BackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(cluster, backup).
			Build(),
		Scheme: scheme,
	}

	reconcileBackup(t, reconciler, backup)

	updated := &mysqlv1alpha1.Backup{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample"}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != mysqlv1alpha1.BackupPhaseFailed {
		t.Fatalf("phase = %q", updated.Status.Phase)
	}
	if !strings.Contains(updated.Status.Error, "objectStore") {
		t.Fatalf("error = %q", updated.Status.Error)
	}
}

func TestBackupDeleteWithFinalizerCleansObjectStore(t *testing.T) {
	t.Parallel()

	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted = append(deleted, r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write([]byte(listBackupArchive))
	}))
	defer server.Close()

	scheme := testScheme(t)
	forcePath := true
	backup := baseBackup()
	backup.Finalizers = []string{backupFinalizer}
	now := metav1.Now()
	backup.DeletionTimestamp = &now
	backup.Status.BackupID = "backup-sample-123"
	backup.Spec.ObjectStore = &mysqlv1alpha1.S3ObjectStore{
		Bucket:         "override-backups",
		Path:           "manual",
		Endpoint:       server.URL,
		ForcePathStyle: &forcePath,
		Credentials: mysqlv1alpha1.S3Credentials{
			AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "override-s3", Key: "access"},
			SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "override-s3", Key: "secret"},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "override-s3", Namespace: "default"},
		Data:       map[string][]byte{"access": []byte("key"), "secret": []byte("secret")},
	}
	reconciler := &BackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(backup, secret).
			Build(),
		Scheme: scheme,
	}

	reconcileBackup(t, reconciler, backup)

	if len(deleted) != 2 {
		t.Fatalf("expected 2 object deletions, got %d: %v", len(deleted), deleted)
	}
	for _, want := range []string{"backup.xbstream", "metadata.json"} {
		if !strings.Contains(strings.Join(deleted, " "), want) {
			t.Fatalf("cleanup did not delete %q, deleted=%v", want, deleted)
		}
	}

	// The finalizer released, so the object is gone.
	got := &mysqlv1alpha1.Backup{}
	err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample"}, got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("backup should be deleted after cleanup, got err=%v finalizers=%v", err, got.Finalizers)
	}
}

func TestBackupDeleteReleasesFinalizerWhenStoreUnresolvable(t *testing.T) {
	t.Parallel()

	// No cluster and no spec.ObjectStore: the destination cannot be resolved, so
	// cleanup is skipped rather than blocking deletion of the Kubernetes object.
	scheme := testScheme(t)
	backup := baseBackup()
	backup.Finalizers = []string{backupFinalizer}
	now := metav1.Now()
	backup.DeletionTimestamp = &now
	backup.Status.BackupID = "backup-sample-123"
	reconciler := &BackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(backup).
			Build(),
		Scheme: scheme,
	}

	reconcileBackup(t, reconciler, backup)

	got := &mysqlv1alpha1.Backup{}
	err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample"}, got)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("backup should be deleted after finalizer release, got err=%v finalizers=%v", err, got.Finalizers)
	}
}
