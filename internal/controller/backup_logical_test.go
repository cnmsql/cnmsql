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
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

func logicalBackup() *mysqlv1alpha1.Backup {
	b := baseBackup()
	b.Spec.Method = mysqlv1alpha1.BackupMethodLogical
	return b
}

func dumpAccountReady(cluster *mysqlv1alpha1.Cluster) *mysqlv1alpha1.Cluster {
	apimeta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type: mysqlv1alpha1.ConditionDumpAccountReady, Status: metav1.ConditionTrue, Reason: dumpAccountReasonApplied,
	})
	return cluster
}

func backupReconcilerWith(t *testing.T, objs ...client.Object) *BackupReconciler {
	t.Helper()
	scheme := testScheme(t)
	return &BackupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(objs...).Build(),
		Scheme: scheme,
	}
}

func getBackup(t *testing.T, r *BackupReconciler) *mysqlv1alpha1.Backup {
	t.Helper()
	b := &mysqlv1alpha1.Backup{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample"}, b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestLogicalBackupWaitsForDumpAccount(t *testing.T) {
	t.Parallel()
	r := backupReconcilerWith(t, baseBackupCluster(), logicalBackup(), readyReplicaPod())

	result := reconcileBackup(t, r, logicalBackup())
	if result.RequeueAfter != provisioningRequeue {
		t.Fatalf("requeue = %s", result.RequeueAfter)
	}
	err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample-backup"}, &batchv1.Job{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("no worker Job may start before the dump account is ready, got %v", err)
	}
	updated := getBackup(t, r)
	if updated.Status.Phase != mysqlv1alpha1.BackupPhasePending {
		t.Fatalf("phase = %q, want pending", updated.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionProgressing)
	if cond == nil || cond.Reason != "DumpAccountNotReady" {
		t.Fatalf("progressing = %+v", cond)
	}
}

func TestLogicalBackupCreatesDumpWorkerJob(t *testing.T) {
	t.Parallel()
	cluster := dumpAccountReady(baseBackupCluster())
	cluster.Spec.Backup.LogicalOptions = []string{"--cluster-wide"}
	backup := logicalBackup()
	backup.Spec.Logical = &mysqlv1alpha1.LogicalBackupOptions{
		Databases: []string{"billing", "a,b"},
		ExtraArgs: []string{"--max-allowed-packet=1G"},
	}
	r := backupReconcilerWith(t, cluster, backup, readyReplicaPod())
	reconcileBackup(t, r, backup)

	job := &batchv1.Job{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "backup-sample-backup"}, job); err != nil {
		t.Fatal(err)
	}
	container := job.Spec.Template.Spec.Containers[0]
	for _, want := range []string{
		"--source-manager-url=https://demo-2.default.svc:8080/cluster/dump",
		"--method=logical",
		"--database=billing",
		"--database=a,b",
		"--dump-arg=--max-allowed-packet=1G",
	} {
		if !slices.Contains(container.Args, want) {
			t.Fatalf("worker args missing %q: %v", want, container.Args)
		}
	}
	if slices.Contains(container.Args, "--dump-arg=--cluster-wide") {
		t.Fatalf("spec.logical.extraArgs must replace the cluster's logicalOptions: %v", container.Args)
	}
	args := strings.Join(container.Args, " ")
	if !strings.Contains(args, "/dump.sql.zst") || !strings.Contains(args, "/logical.json") {
		t.Fatalf("worker keys are not the logical ones: %s", args)
	}
	// The worker carries no dump password any more (design 030): the instance
	// manager reads the <cluster>-dump Secret itself, so no env var may
	// reference it.
	for _, env := range container.Env {
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil && env.ValueFrom.SecretKeyRef.Name == "demo-dump" {
			t.Fatalf("the dump password Secret must not reach the worker: %+v", env)
		}
	}

	updated := getBackup(t, r)
	if updated.Status.Phase != mysqlv1alpha1.BackupPhaseRunning || updated.Status.Method != mysqlv1alpha1.BackupMethodLogical ||
		!strings.HasSuffix(updated.Status.DestinationPath, "/dump.sql.zst") {
		t.Fatalf("status = %+v", updated.Status)
	}
}

func TestLogicalBackupFallsBackToClusterLogicalOptions(t *testing.T) {
	t.Parallel()
	cluster := dumpAccountReady(baseBackupCluster())
	cluster.Spec.Backup.LogicalOptions = []string{"--skip-extended-insert"}
	args := logicalWorkerArgs(logicalBackup(), cluster)
	if !slices.Equal(args, []string{"--method=logical", "--dump-arg=--skip-extended-insert"}) {
		t.Fatalf("args = %v", args)
	}
}

func TestLogicalBackupRejectsInvalidSpecs(t *testing.T) {
	t.Parallel()
	offline := logicalBackup()
	offline.Spec.Online = new(false)
	logicalOnPhysical := baseBackup()
	logicalOnPhysical.Spec.Logical = &mysqlv1alpha1.LogicalBackupOptions{Databases: []string{"a"}}

	for reason, backup := range map[string]*mysqlv1alpha1.Backup{
		"OnlineRequired": offline,
		"InvalidSpec":    logicalOnPhysical,
	} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			r := backupReconcilerWith(t, dumpAccountReady(baseBackupCluster()), backup, readyReplicaPod())
			reconcileBackup(t, r, backup)
			updated := getBackup(t, r)
			cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionDegraded)
			if updated.Status.Phase != mysqlv1alpha1.BackupPhaseFailed || cond == nil || cond.Reason != reason {
				t.Fatalf("phase = %q, degraded = %+v", updated.Status.Phase, cond)
			}
		})
	}
}

func TestLogicalBackupCompletionRecordsManifest(t *testing.T) {
	t.Parallel()
	manifest := objectstore.LogicalBackupMetadata{
		FormatVersion:  1,
		BackupID:       testBackupID,
		Method:         "logical",
		SHA256:         "abc123",
		Databases:      []string{"billing", "shop"},
		SnapshotGTID:   "0-1-13",
		SnapshotBinlog: "mysql-bin.000001:4",
		CompletedAt:    time.Now().UTC(),
	}
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		if !strings.HasSuffix(r.URL.Path, "/logical.json") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		_ = json.NewEncoder(w).Encode(manifest)
	}))
	defer server.Close()

	backup := logicalBackup()
	backup.Spec.ObjectStore = &mysqlv1alpha1.S3ObjectStore{
		Bucket: "override-backups", Path: "manual", Endpoint: server.URL, ForcePathStyle: new(true),
		Credentials: mysqlv1alpha1.S3Credentials{
			AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "override-s3", Key: "access"},
			SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "override-s3", Key: "secret"},
		},
	}
	backup.Status = mysqlv1alpha1.BackupStatus{
		Phase: mysqlv1alpha1.BackupPhaseRunning, BackupID: testBackupID, JobName: "backup-sample-backup",
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-sample-backup", Namespace: "default"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "override-s3", Namespace: "default"},
		Data:       map[string][]byte{"access": []byte("key"), "secret": []byte("secret")},
	}
	r := backupReconcilerWith(t, dumpAccountReady(baseBackupCluster()), backup, readyReplicaPod(), job, secret)
	reconcileBackup(t, r, backup)

	updated := getBackup(t, r)
	if updated.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted {
		t.Fatalf("phase = %q", updated.Status.Phase)
	}
	if updated.Status.SHA256 != "abc123" || !slices.Equal(updated.Status.Databases, []string{"billing", "shop"}) ||
		updated.Status.BeginBinlog != "mysql-bin.000001:4" || updated.Status.EndBinlog != "mysql-bin.000001:4" ||
		updated.Status.BeginGTID != "0-1-13" || updated.Status.EndGTID != "0-1-13" {
		t.Fatalf("status = %+v", updated.Status)
	}
	want := "/override-backups/manual/demo/backup-sample/" + testBackupID + "/logical.json"
	if !slices.Contains(requested, want) {
		t.Fatalf("manifest not read from %s, requested %v", want, requested)
	}
}

func TestBackupFailsWithWorkerTerminationReason(t *testing.T) {
	t.Parallel()
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-sample-backup", Namespace: "default"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
			Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded",
		}}},
	}
	pod := func(name string, created time.Time, reason string) *corev1.Pod {
		msg, _ := json.Marshal(backupworker.TerminationMessage{Reason: reason, Message: reason + " on demo-2"})
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "default", CreationTimestamp: metav1.NewTime(created),
				Labels: map[string]string{batchv1.JobNameLabel: "backup-sample-backup"},
			},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
				Name:  backupWorkerContainer,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: string(msg)}},
			}}},
		}
	}
	now := time.Now()
	r := backupReconcilerWith(t, dumpAccountReady(baseBackupCluster()), logicalBackup(), readyReplicaPod(), job,
		pod("attempt-1", now.Add(-time.Minute), "DumpAccountMissing"),
		pod("attempt-2", now, "LogicalToolUnavailable"),
	)
	reconcileBackup(t, r, logicalBackup())

	updated := getBackup(t, r)
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionDegraded)
	if updated.Status.Phase != mysqlv1alpha1.BackupPhaseFailed || cond == nil || cond.Reason != "LogicalToolUnavailable" {
		t.Fatalf("phase = %q, degraded = %+v (the newest attempt's reason wins)", updated.Status.Phase, cond)
	}
	if updated.Status.Error != "LogicalToolUnavailable on demo-2" {
		t.Fatalf("error = %q", updated.Status.Error)
	}
}
