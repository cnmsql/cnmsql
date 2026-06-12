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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

func baseBackupCluster() *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Status.CurrentPrimary = "demo-1"
	cluster.Status.Image = "cnmysql-instance:8.4"
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

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "backup-sample",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != provisioningRequeue {
		t.Fatalf("requeue = %s, want %s", result.RequeueAfter, provisioningRequeue)
	}

	job := &batchv1.Job{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample-backup"}, job); err != nil {
		t.Fatal(err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "cnmysql-instance:8.4" {
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

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "backup-sample",
	}}); err != nil {
		t.Fatal(err)
	}

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

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "backup-sample",
	}}); err != nil {
		t.Fatal(err)
	}

	updated := &mysqlv1alpha1.Backup{}
	if err := reconciler.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample"}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.InstanceName != "demo-1" {
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
	initArgs := strings.Join(spec.InitContainers[0].Args, " ")
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
	for _, env := range spec.InitContainers[0].Env {
		switch env.Name {
		case "CNMYSQL_S3_ENDPOINT":
			hasEndpoint = true
		case "CNMYSQL_S3_ACCESS_KEY_ID":
			hasAccessKey = true
		}
	}
	if !hasEndpoint || !hasAccessKey {
		t.Fatalf("recovery init container missing S3 env (endpoint=%t accessKey=%t)", hasEndpoint, hasAccessKey)
	}

	// Recovery generates no app Secret, so the init container must not reference
	// one; a non-optional secretKeyRef would wedge the Pod in
	// CreateContainerConfigError.
	for _, env := range spec.InitContainers[0].Env {
		if env.Name == "MYSQL_APP_PASSWORD" {
			t.Fatal("recovery init container must not reference the app password secret")
		}
	}

	// A replica still clones from the primary via join, not restore.
	replicaSpec := reconciler.podSpec(cluster, plan, plan.instanceFor(cluster, 2))
	if got := strings.Join(replicaSpec.InitContainers[0].Args, " "); !strings.Contains(got, "instance join") {
		t.Fatalf("replica should join the primary, got: %s", got)
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

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "backup-sample",
	}}); err != nil {
		t.Fatal(err)
	}

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
