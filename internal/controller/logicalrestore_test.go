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
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore/objectstoretest"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// restoreTarget is cluster "prod" (the fixture the import tests dump from)
// with prod-1 as its settled primary.
func restoreTarget(endpoint string) *mysqlv1alpha1.Cluster {
	cluster := sourceCluster(endpoint)
	cluster.Status.CurrentPrimary = "prod-1"
	cluster.Status.TargetPrimary = "prod-1"
	cluster.Status.Image = "ghcr.io/cnmsql/cnmsql-instance:8.4"
	cluster.Spec.ExternalClusters = []mysqlv1alpha1.ExternalCluster{{Name: "prod", ObjectStore: importStore(endpoint)}}
	return cluster
}

// restorePod is prod-1, the primary Pod.
func restorePod(ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-1", Namespace: "default", Labels: map[string]string{clusterLabel: "prod"}},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

func newLogicalRestore(mutate func(*mysqlv1alpha1.LogicalRestoreSpec)) *mysqlv1alpha1.LogicalRestore {
	r := &mysqlv1alpha1.LogicalRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-shop", Namespace: "default", UID: "uid-1"},
		Spec: mysqlv1alpha1.LogicalRestoreSpec{
			Cluster:   mysqlv1alpha1.LocalObjectReference{Name: "prod"},
			Backup:    &mysqlv1alpha1.LocalObjectReference{Name: "nightly"},
			Databases: []string{"shop"},
			Policy:    mysqlv1alpha1.LogicalRestoreDropAndRecreate,
		},
	}
	if mutate != nil {
		mutate(&r.Spec)
	}
	return r
}

func restoreReconciler(t *testing.T, objs ...client.Object) (*LogicalRestoreReconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(20)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.LogicalRestore{}).
		WithObjects(append(objs, s3CredentialsSecret())...).Build()
	return &LogicalRestoreReconciler{Client: c, Scheme: scheme, Recorder: recorder, OperatorImageName: "operator:1"}, recorder
}

func reconcileRestore(t *testing.T, r *LogicalRestoreReconciler, restore *mysqlv1alpha1.LogicalRestore) (ctrl.Result, *mysqlv1alpha1.LogicalRestore) {
	t.Helper()
	key := types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatal(err)
	}
	updated := &mysqlv1alpha1.LogicalRestore{}
	if err := r.Get(context.Background(), key, updated); err != nil {
		t.Fatal(err)
	}
	return result, updated
}

func manifestStore(t *testing.T) string {
	t.Helper()
	srv, _ := objectstoretest.NewServer(t, "backups", map[string][]byte{importManifestKey: importManifest(t, nil)})
	return srv.URL
}

func TestLogicalRestoreStartsAWorkerJobOnThePrimary(t *testing.T) {
	t.Parallel()
	endpoint := manifestStore(t)
	cluster := restoreTarget(endpoint)
	cluster.Spec.Backup.JobTemplate = &mysqlv1alpha1.BackupJobTemplate{NodeSelector: map[string]string{"pool": "jobs"}}
	restore := newLogicalRestore(func(s *mysqlv1alpha1.LogicalRestoreSpec) {
		s.Databases = []string{"shop", "bill$(x)ing"}
		s.JobTemplate = &mysqlv1alpha1.BackupJobTemplate{Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
		}}
	})
	r, _ := restoreReconciler(t, cluster, restorePod(true), importBackup(mysqlv1alpha1.BackupPhaseCompleted), restore)

	// The manifest does not list "bill$(x)ing": the restore fails before any
	// Job exists. A valid selection comes next.
	_, failed := reconcileRestore(t, r, restore)
	if failed.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseFailed ||
		!strings.Contains(failed.Status.Error, restoreUnchangedNote) {
		t.Fatalf("status = %+v", failed.Status)
	}

	restore = newLogicalRestore(func(s *mysqlv1alpha1.LogicalRestoreSpec) {
		s.JobTemplate = &mysqlv1alpha1.BackupJobTemplate{Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
		}}
	})
	restore.Name = "restore-shop-2"
	if err := r.Create(context.Background(), restore); err != nil {
		t.Fatal(err)
	}
	result, updated := reconcileRestore(t, r, restore)
	if result.RequeueAfter == 0 {
		t.Error("a running restore must requeue")
	}
	if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseRunning || updated.Status.TargetInstance != "prod-1" ||
		updated.Status.BackupID != "b-1" || updated.Status.SourcePath != "s3://backups/"+importDumpKey ||
		updated.Status.JobName != "restore-shop-2-restore" {
		t.Fatalf("status = %+v", updated.Status)
	}

	job := &batchv1.Job{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "restore-shop-2-restore"}, job); err != nil {
		t.Fatal(err)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %v, want 0", job.Spec.BackoffLimit)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].Name != "restore-shop-2" {
		t.Errorf("owner = %+v", job.OwnerReferences)
	}
	pod := job.Spec.Template.Spec
	if pod.NodeSelector["pool"] != "jobs" || pod.InitContainers[0].Image != "operator:1" {
		t.Errorf("pod spec = %+v", pod)
	}
	worker := pod.Containers[0]
	if worker.Name != restoreWorkerContainer || worker.Image != "ghcr.io/cnmsql/cnmsql-instance:8.4" ||
		worker.Resources.Limits.Memory().String() != "512Mi" {
		t.Errorf("worker = %+v", worker)
	}
	args := strings.Join(worker.Args, " ")
	for _, want := range []string{
		"instance logical-restore",
		"--target-manager-url=https://prod-1.default.svc:8080/cluster/load",
		"--target-manager-server-name=prod-1.default.svc",
		"--bucket=backups",
		"--dump-key=" + importDumpKey,
		"--manifest-key=" + importManifestKey,
		"--database=shop",
		"--policy=DropAndRecreate",
		"--flavor=mysql",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("worker args missing %q:\n%s", want, args)
		}
	}
	env := map[string]bool{}
	for _, e := range worker.Env {
		env[e.Name] = true
	}
	if !env[objectstore.EnvAccessKeyID] || !env[objectstore.EnvEndpoint] {
		t.Errorf("worker env = %v", worker.Env)
	}
}

func TestLogicalRestoreEscapesDatabaseArgs(t *testing.T) {
	t.Parallel()
	restore := newLogicalRestore(func(s *mysqlv1alpha1.LogicalRestoreSpec) { s.Databases = []string{"a$(HOME)"} })
	job := logicalRestoreJob(restore, restoreTarget("http://s3"), &logicalDump{
		Store: importStore("http://s3"), DumpKey: "k", ManifestKey: "m", Meta: &objectstore.LogicalBackupMetadata{BackupID: "b-1"},
	}, "prod-1", "img", "op", mysqlv1alpha1.BackupJobTemplate{})
	if args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " "); !strings.Contains(args, "--database=a$$(HOME)") {
		t.Errorf("args = %s", args)
	}
}

func TestLogicalRestoreFromSource(t *testing.T) {
	t.Parallel()
	endpoint := manifestStore(t)
	restore := newLogicalRestore(func(s *mysqlv1alpha1.LogicalRestoreSpec) {
		s.Backup = nil
		s.Source = "prod"
		s.BackupID = "b-1"
	})
	r, _ := restoreReconciler(t, restoreTarget(endpoint), restorePod(true), restore)
	_, updated := reconcileRestore(t, r, restore)
	if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseRunning || updated.Status.SourcePath != "s3://backups/"+importDumpKey {
		t.Fatalf("status = %+v", updated.Status)
	}
}

func TestLogicalRestoreWaitsForTheSourceAndThePrimary(t *testing.T) {
	t.Parallel()
	endpoint := manifestStore(t)
	for _, tc := range []struct {
		name    string
		objs    func() []client.Object
		reason  string
		message string
	}{
		{
			name: "the Backup is still running",
			objs: func() []client.Object {
				return []client.Object{restoreTarget(endpoint), restorePod(true), importBackup(mysqlv1alpha1.BackupPhaseRunning)}
			},
			reason: restoreReasonSourceNotReady, message: "is not completed yet",
		},
		{
			name: "no current primary",
			objs: func() []client.Object {
				c := restoreTarget(endpoint)
				c.Status.CurrentPrimary, c.Status.TargetPrimary = "", ""
				return []client.Object{c, importBackup(mysqlv1alpha1.BackupPhaseCompleted)}
			},
			reason: restoreReasonPrimaryNotReady, message: "has no current primary",
		},
		{
			name: "a switchover in flight",
			objs: func() []client.Object {
				c := restoreTarget(endpoint)
				c.Status.TargetPrimary = "prod-2"
				return []client.Object{c, restorePod(true), importBackup(mysqlv1alpha1.BackupPhaseCompleted)}
			},
			reason: restoreReasonPrimaryNotReady, message: "from prod-1 to prod-2",
		},
		{
			name: "the primary Pod is not ready",
			objs: func() []client.Object {
				return []client.Object{restoreTarget(endpoint), restorePod(false), importBackup(mysqlv1alpha1.BackupPhaseCompleted)}
			},
			reason: restoreReasonPrimaryNotReady, message: "is not ready",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			restore := newLogicalRestore(nil)
			r, _ := restoreReconciler(t, append(tc.objs(), restore)...)
			result, updated := reconcileRestore(t, r, restore)
			if result.RequeueAfter == 0 {
				t.Error("a pending restore must requeue")
			}
			cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionProgressing)
			if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhasePending || cond == nil ||
				cond.Reason != tc.reason || !strings.Contains(cond.Message, tc.message) {
				t.Fatalf("status = %+v", updated.Status)
			}
			var jobs batchv1.JobList
			if err := r.List(context.Background(), &jobs); err != nil || len(jobs.Items) != 0 {
				t.Errorf("jobs = %d (%v), want none", len(jobs.Items), err)
			}
		})
	}
}

func TestLogicalRestoreFailsWithoutTouchingTheCluster(t *testing.T) {
	t.Parallel()
	physical := importBackup(mysqlv1alpha1.BackupPhaseCompleted)
	physical.Spec.Method = mysqlv1alpha1.BackupMethodXtrabackup
	for _, tc := range []struct {
		name    string
		restore *mysqlv1alpha1.LogicalRestore
		objs    []client.Object
		reason  string
		message string
	}{
		{
			name:    "no cluster",
			restore: newLogicalRestore(func(s *mysqlv1alpha1.LogicalRestoreSpec) { s.Cluster.Name = "gone" }),
			reason:  restoreReasonClusterNotFound, message: `Cluster "gone" was not found`,
		},
		{
			name:    "a physical Backup",
			restore: newLogicalRestore(nil),
			objs:    []client.Object{physical},
			reason:  restoreReasonPhysicalBackup, message: "is a xtrabackup backup, and a LogicalRestore loads a logical backup",
		},
		{
			name:    "a failed Backup",
			restore: newLogicalRestore(nil),
			objs:    []client.Object{importBackup(mysqlv1alpha1.BackupPhaseFailed)},
			reason:  backupworker.ReasonIncompatible, message: `backup "nightly" failed`,
		},
		{
			name:    "a database not in the dump",
			restore: newLogicalRestore(func(s *mysqlv1alpha1.LogicalRestoreSpec) { s.Databases = []string{"crm"} }),
			objs:    []client.Object{importBackup(mysqlv1alpha1.BackupPhaseCompleted)},
			reason:  backupworker.ReasonIncompatible, message: "databases crm are not in the dump",
		},
		{
			name:    "an unknown source",
			restore: newLogicalRestore(func(s *mysqlv1alpha1.LogicalRestoreSpec) { s.Backup, s.Source = nil, "staging" }),
			reason:  backupworker.ReasonIncompatible, message: `source "staging" is not an externalClusters entry`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			endpoint := manifestStore(t)
			objs := append([]client.Object{restoreTarget(endpoint), restorePod(true), tc.restore}, tc.objs...)
			r, _ := restoreReconciler(t, objs...)
			_, updated := reconcileRestore(t, r, tc.restore)
			cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionReady)
			if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseFailed || cond == nil || cond.Reason != tc.reason ||
				!strings.Contains(updated.Status.Error, tc.message) ||
				!strings.HasSuffix(updated.Status.Error, restoreUnchangedNote) {
				t.Fatalf("status = %+v", updated.Status)
			}
		})
	}
}

// runningRestore is a restore whose worker Job exists.
func runningRestore() *mysqlv1alpha1.LogicalRestore {
	restore := newLogicalRestore(nil)
	started := metav1.NewTime(time.Now().Add(-time.Hour))
	restore.Status = mysqlv1alpha1.LogicalRestoreStatus{
		Phase:          mysqlv1alpha1.LogicalRestorePhaseRunning,
		JobName:        "restore-shop-restore",
		TargetInstance: "prod-1",
		Databases:      []string{"shop"},
		StartedAt:      &started,
	}
	return restore
}

func restoreJob(conditions ...batchv1.JobCondition) *batchv1.Job {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "restore-shop-restore", Namespace: "default"}}
	job.Status.Conditions = conditions
	for _, c := range conditions {
		if c.Type == batchv1.JobComplete {
			job.Status.Succeeded = 1
		}
	}
	return job
}

func workerPod(message *backupworker.TerminationMessage) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "restore-shop-restore-x", Namespace: "default",
		Labels: map[string]string{batchv1.JobNameLabel: "restore-shop-restore"},
	}}
	terminated := &corev1.ContainerStateTerminated{ExitCode: 1}
	if message != nil {
		raw, _ := json.Marshal(message)
		terminated.Message = string(raw)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: restoreWorkerContainer, State: corev1.ContainerState{Terminated: terminated}}}
	return pod
}

func TestLogicalRestoreMirrorsTheJob(t *testing.T) {
	t.Parallel()
	failed := batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}
	for _, tc := range []struct {
		name      string
		objs      []client.Object
		phase     mysqlv1alpha1.LogicalRestorePhase
		reason    string
		errSubstr string
	}{
		{
			name:  "success",
			objs:  []client.Object{restoreJob(batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})},
			phase: mysqlv1alpha1.LogicalRestorePhaseCompleted, reason: restoreReasonRestoreCompleted,
		},
		{
			name: "a refusal leaves the data untouched",
			objs: []client.Object{restoreJob(failed), workerPod(&backupworker.TerminationMessage{
				Reason: webserver.LoadReasonDatabaseNotEmpty, Message: "shop holds tables", Unchanged: true,
			})},
			phase: mysqlv1alpha1.LogicalRestorePhaseFailed, reason: webserver.LoadReasonDatabaseNotEmpty,
			errSubstr: "shop holds tables. " + restoreUnchangedNote,
		},
		{
			name: "a failed load may be partial",
			objs: []client.Object{restoreJob(failed), workerPod(&backupworker.TerminationMessage{
				Reason: webserver.LoadReasonFailed, Message: "ERROR 1146",
			})},
			phase: mysqlv1alpha1.LogicalRestorePhaseFailed, reason: webserver.LoadReasonFailed,
			errSubstr: "ERROR 1146. " + restorePartialNote,
		},
		{
			name:  "a worker that left no message may be partial",
			objs:  []client.Object{restoreJob(failed), workerPod(nil)},
			phase: mysqlv1alpha1.LogicalRestorePhaseFailed, reason: "BackoffLimitExceeded",
			errSubstr: "Restore worker Job failed: BackoffLimitExceeded. " + restorePartialNote,
		},
		{
			name:  "a Job that is gone",
			phase: mysqlv1alpha1.LogicalRestorePhaseFailed, reason: restoreReasonJobMissing,
			errSubstr: restorePartialNote,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			restore := runningRestore()
			r, _ := restoreReconciler(t, append(tc.objs, restore)...)
			_, updated := reconcileRestore(t, r, restore)
			cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionReady)
			if updated.Status.Phase != tc.phase || cond == nil || cond.Reason != tc.reason ||
				!strings.Contains(updated.Status.Error, tc.errSubstr) || updated.Status.StoppedAt == nil {
				t.Fatalf("status = %+v", updated.Status)
			}
		})
	}
}

func TestLogicalRestoreIsNotReconciledOnceFinished(t *testing.T) {
	t.Parallel()
	restore := newLogicalRestore(nil)
	restore.Status.Phase = mysqlv1alpha1.LogicalRestorePhaseCompleted
	// No cluster exists: a finished restore must not look for it.
	r, _ := restoreReconciler(t, restore)
	_, updated := reconcileRestore(t, r, restore)
	if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseCompleted || updated.Status.Error != "" {
		t.Fatalf("status = %+v", updated.Status)
	}
}

func TestLogicalRestoreWarnsOnANewerSeries(t *testing.T) {
	t.Parallel()
	srv, _ := objectstoretest.NewServer(t, "backups", map[string][]byte{
		importManifestKey: importManifest(t, func(m *objectstore.LogicalBackupMetadata) { m.ServerVersion = "9.6.0" }),
	})
	restore := newLogicalRestore(nil)
	r, recorder := restoreReconciler(t, restoreTarget(srv.URL), restorePod(true),
		importBackup(mysqlv1alpha1.BackupPhaseCompleted), restore)
	reconcileRestore(t, r, restore)
	var events []string
	for len(recorder.Events) > 0 {
		events = append(events, <-recorder.Events)
	}
	if !strings.Contains(strings.Join(events, "\n"), "RestoreFromNewerServer") {
		t.Errorf("events = %v", events)
	}
}

// A Job the cache has not seen yet must not fail the restore: the API server
// is asked before JobMissing, which is final.
func TestLogicalRestoreDoesNotFailOnACacheMiss(t *testing.T) {
	t.Parallel()
	restore := runningRestore()
	r, _ := restoreReconciler(t, restore)
	scheme := testScheme(t)
	r.APIReader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(restoreJob()).Build()
	result, updated := reconcileRestore(t, r, restore)
	if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseRunning || result.RequeueAfter == 0 {
		t.Fatalf("phase = %s, requeue = %s; want running and a requeue", updated.Status.Phase, result.RequeueAfter)
	}
}

// A Job of the same name that this restore does not control is never
// adopted: it may be loading another dump.
func TestLogicalRestoreRefusesAForeignJob(t *testing.T) {
	t.Parallel()
	endpoint := manifestStore(t)
	foreign := restoreJob()
	foreign.Name = "restore-shop-restore"
	restore := newLogicalRestore(nil)
	r, _ := restoreReconciler(t, restoreTarget(endpoint), restorePod(true),
		importBackup(mysqlv1alpha1.BackupPhaseCompleted), foreign, restore)
	_, updated := reconcileRestore(t, r, restore)
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionReady)
	if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseFailed || cond == nil ||
		cond.Reason != restoreReasonJobConflict || !strings.HasSuffix(updated.Status.Error, restoreUnchangedNote) {
		t.Fatalf("status = %+v", updated.Status)
	}
}

// The newer-series warning comes with the dump's resolution, so a restore
// still waiting for its primary shows it.
func TestLogicalRestoreWarnsWhilePending(t *testing.T) {
	t.Parallel()
	srv, _ := objectstoretest.NewServer(t, "backups", map[string][]byte{
		importManifestKey: importManifest(t, func(m *objectstore.LogicalBackupMetadata) { m.ServerVersion = "9.6.0" }),
	})
	restore := newLogicalRestore(nil)
	r, recorder := restoreReconciler(t, restoreTarget(srv.URL), restorePod(false),
		importBackup(mysqlv1alpha1.BackupPhaseCompleted), restore)
	_, updated := reconcileRestore(t, r, restore)
	if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhasePending {
		t.Fatalf("phase = %s, want pending", updated.Status.Phase)
	}
	select {
	case e := <-recorder.Events:
		if !strings.Contains(e, "RestoreFromNewerServer") {
			t.Errorf("event = %q", e)
		}
	default:
		t.Error("want a RestoreFromNewerServer warning while pending")
	}
}

// A Job created on a pass whose status write was lost is recorded on the next
// pass, even when the dump can no longer be resolved: the load may be running.
func TestLogicalRestoreRecordsItsJobAfterALostStatusWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint := manifestStore(t)
	restore := newLogicalRestore(nil)
	backup := importBackup(mysqlv1alpha1.BackupPhaseCompleted)
	r, _ := restoreReconciler(t, restoreTarget(endpoint), restorePod(true), backup, restore)
	_, started := reconcileRestore(t, r, restore)
	if started.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseRunning {
		t.Fatalf("status = %+v", started.Status)
	}

	started.Status = mysqlv1alpha1.LogicalRestoreStatus{}
	if err := r.Status().Update(ctx, started); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, backup); err != nil {
		t.Fatal(err)
	}
	_, updated := reconcileRestore(t, r, restore)
	if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhaseRunning ||
		updated.Status.JobName != "restore-shop-restore" || updated.Status.TargetInstance != "prod-1" ||
		updated.Status.BackupID != "b-1" || updated.Status.SourcePath != "s3://backups/"+importDumpKey {
		t.Fatalf("status = %+v", updated.Status)
	}
}

// A Backup that does not exist keeps the restore pending, since it may be
// created later, but warns: it is more likely a typo.
func TestLogicalRestoreWarnsOnAMissingBackup(t *testing.T) {
	t.Parallel()
	restore := newLogicalRestore(nil)
	r, recorder := restoreReconciler(t, restoreTarget(manifestStore(t)), restorePod(true), restore)
	_, updated := reconcileRestore(t, r, restore)
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, mysqlv1alpha1.ConditionProgressing)
	if updated.Status.Phase != mysqlv1alpha1.LogicalRestorePhasePending || cond == nil ||
		cond.Reason != restoreReasonSourceNotReady || !strings.Contains(cond.Message, "does not exist") {
		t.Fatalf("status = %+v", updated.Status)
	}
	select {
	case e := <-recorder.Events:
		if !strings.HasPrefix(e, "Warning "+restoreReasonBackupNotFound) {
			t.Errorf("event = %q", e)
		}
	default:
		t.Error("want a BackupNotFound warning")
	}
}
