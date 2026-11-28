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
	"fmt"
	"strings"
	"testing"
	"time"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

const (
	testCSIDriver     = "hostpath.csi.k8s.io"
	testSnapshotClass = "csi-hostpath-snapclass"
)

// fakeSnapshotClient records the snapshot calls the step machine makes.
type fakeSnapshotClient struct {
	anchor   webserver.SnapshotAnchor
	beginErr error
	endErr   error
	state    webserver.SnapshotState
	begins   []webserver.SnapshotBeginRequest
	ends     []webserver.SnapshotEndRequest
	instance string
}

func (f *fakeSnapshotClient) BeginSnapshot(_ context.Context, _ *mysqlv1alpha1.Cluster, instance string, req webserver.SnapshotBeginRequest) (*webserver.SnapshotAnchor, error) {
	f.instance = instance
	f.begins = append(f.begins, req)
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	anchor := f.anchor
	return &anchor, nil
}

func (f *fakeSnapshotClient) EndSnapshot(_ context.Context, _ *mysqlv1alpha1.Cluster, _ string, req webserver.SnapshotEndRequest) error {
	f.ends = append(f.ends, req)
	return f.endErr
}

func (f *fakeSnapshotClient) SnapshotState(context.Context, *mysqlv1alpha1.Cluster, string) (*webserver.SnapshotState, error) {
	state := f.state
	return &state, nil
}

func snapshotCluster() *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	cluster.Status.CurrentPrimary = demoPrimaryInstance
	cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
		VolumeSnapshot: &mysqlv1alpha1.VolumeSnapshotConfiguration{ClassName: testSnapshotClass},
	}
	return cluster
}

func snapshotBackup() *mysqlv1alpha1.Backup {
	backup := baseBackup()
	backup.UID = "backup-uid"
	backup.Spec.Method = mysqlv1alpha1.BackupMethodVolumeSnapshot
	backup.Spec.Online = new(true)
	return backup
}

// snapshotVolumeObjects is the replica's bound CSI volume and its class.
func snapshotVolumeObjects() []client.Object {
	const instance = "demo-2"
	return []client.Object{
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: instance, Namespace: "default"},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-" + instance},
			Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-" + instance},
			Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: testCSIDriver, VolumeHandle: "h"},
			}},
		},
		&snapshotv1.VolumeSnapshotClass{
			ObjectMeta: metav1.ObjectMeta{Name: testSnapshotClass},
			Driver:     testCSIDriver,
		},
	}
}

func newSnapshotReconciler(t *testing.T, snapshots *fakeSnapshotClient, objs ...client.Object) *BackupReconciler {
	t.Helper()
	scheme := testScheme(t)
	return &BackupReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			// The default fake mapper knows no kind; the snapshot step machine
			// asks it whether the VolumeSnapshot API is installed.
			WithRESTMapper(testrestmapper.TestOnlyStaticRESTMapper(scheme)).
			WithObjects(objs...).
			Build(),
		Scheme:         scheme,
		SnapshotClient: snapshots,
	}
}

func getSnapshotBackup(t *testing.T, r *BackupReconciler, name string) *mysqlv1alpha1.Backup {
	t.Helper()
	backup := &mysqlv1alpha1.Backup{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, backup); err != nil {
		t.Fatal(err)
	}
	return backup
}

func getSnapshot(t *testing.T, r *BackupReconciler, name string) *snapshotv1.VolumeSnapshot {
	t.Helper()
	snap := &snapshotv1.VolumeSnapshot{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

// provisionSnapshot plays the snapshot controller: the content is bound and
// the cut time set, and optionally the upload finished.
func provisionSnapshot(t *testing.T, r *BackupReconciler, name string, ready bool) {
	t.Helper()
	snap := getSnapshot(t, r, name)
	content := "snapcontent-" + name
	created := metav1.NewTime(time.Now().Add(-time.Second).Truncate(time.Second))
	size := resource.MustParse("10Gi")
	snap.Status = &snapshotv1.VolumeSnapshotStatus{
		BoundVolumeSnapshotContentName: &content,
		CreationTime:                   &created,
		ReadyToUse:                     &ready,
		RestoreSize:                    &size,
	}
	if err := r.Update(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
}

func expectBackupFailed(t *testing.T, r *BackupReconciler, name, reason string) {
	t.Helper()
	backup := getSnapshotBackup(t, r, name)
	if backup.Status.Phase != mysqlv1alpha1.BackupPhaseFailed {
		t.Fatalf("phase = %q, want failed (error %q)", backup.Status.Phase, backup.Status.Error)
	}
	cond := apimeta.FindStatusCondition(backup.Status.Conditions, mysqlv1alpha1.ConditionProgressing)
	if cond == nil || cond.Reason != reason {
		t.Fatalf("progressing = %#v, want reason %s", cond, reason)
	}
	if controllerutil.ContainsFinalizer(backup, snapshotReleaseFinalizer) {
		t.Fatal("a failed Backup kept the release finalizer")
	}
}

func TestVolumeSnapshotOnlineBackupRunsTheStepMachine(t *testing.T) {
	t.Parallel()
	snapshots := &fakeSnapshotClient{anchor: webserver.SnapshotAnchor{
		AnchorGTID: "uuid:1-42", BinlogFile: "binlog.000003", BinlogPos: 157, Flavor: "mysql", ServerVersion: "8.4.3",
	}}
	backup := snapshotBackup()
	objs := append(snapshotVolumeObjects(), snapshotCluster(), backup, readyReplicaPod())
	r := newSnapshotReconciler(t, snapshots, objs...)

	// Preflight passes and takes the release finalizer before holding anything.
	reconcileBackup(t, r, backup)
	if !controllerutil.ContainsFinalizer(getSnapshotBackup(t, r, backup.Name), snapshotReleaseFinalizer) {
		t.Fatal("release finalizer not added")
	}
	if len(snapshots.begins) != 0 {
		t.Fatal("began a hold before the finalizer was in place")
	}

	// Prepare and create: hold on the replica, anchor recorded, snapshot cut.
	if result := reconcileBackup(t, r, backup); result.RequeueAfter != snapshotHoldPoll {
		t.Fatalf("requeue while held = %s, want %s", result.RequeueAfter, snapshotHoldPoll)
	}
	if snapshots.instance != "demo-2" || len(snapshots.begins) != 1 ||
		snapshots.begins[0].BackupName != backup.Name || snapshots.begins[0].Timeout != "2m0s" {
		t.Fatalf("begin calls = %+v on %s", snapshots.begins, snapshots.instance)
	}
	expectHeldSnapshot(t, r, backup.Name)

	// Not yet cut: still held, no end.
	reconcileBackup(t, r, backup)
	if len(snapshots.ends) != 0 {
		t.Fatal("released before the snapshot was cut")
	}

	// Cut: release, record, annotate.
	provisionSnapshot(t, r, backup.Name, false)
	if result := reconcileBackup(t, r, backup); result.RequeueAfter != snapshotReadyPoll {
		t.Fatalf("requeue after release = %s, want %s", result.RequeueAfter, snapshotReadyPoll)
	}
	if len(snapshots.ends) != 1 || snapshots.ends[0].BackupName != backup.Name {
		t.Fatalf("end calls = %+v", snapshots.ends)
	}
	expectReleasedSnapshot(t, r, backup.Name)

	// Ready: complete.
	provisionSnapshot(t, r, backup.Name, true)
	reconcileBackup(t, r, backup)
	expectCompletedSnapshot(t, r, backup.Name)
	if len(snapshots.begins) != 1 || len(snapshots.ends) != 1 {
		t.Fatalf("hold taken %d times, released %d times", len(snapshots.begins), len(snapshots.ends))
	}
}

func TestVolumeSnapshotColdBackupFencesTheSource(t *testing.T) {
	t.Parallel()
	snapshots := &fakeSnapshotClient{}
	backup := snapshotBackup()
	backup.Spec.Online = new(false)
	objs := append(snapshotVolumeObjects(), snapshotCluster(), backup, readyReplicaPod())
	r := newSnapshotReconciler(t, snapshots, objs...)

	reconcileBackup(t, r, backup) // finalizer
	reconcileBackup(t, r, backup) // fence
	pod := &corev1.Pod{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-2"}, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Annotations[fencingAnnotation] != routableTrue || pod.Annotations[snapshotFenceAnnotation] != backup.Name {
		t.Fatalf("pod annotations = %v", pod.Annotations)
	}
	if len(snapshots.begins) != 0 {
		t.Fatal("a cold snapshot took a read lock")
	}

	// Fenced but mysqld still stopping: no snapshot yet.
	snapshots.state = webserver.SnapshotState{Fenced: true}
	reconcileBackup(t, r, backup)
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: backup.Name}, &snapshotv1.VolumeSnapshot{}); !apierrors.IsNotFound(err) {
		t.Fatalf("snapshot cut before mysqld stopped: %v", err)
	}

	snapshots.state = webserver.SnapshotState{Fenced: true, MysqldStopped: true}
	reconcileBackup(t, r, backup)
	if getSnapshot(t, r, backup.Name).Annotations[snapshotOnlineAnnotation] != "false" {
		t.Fatal("cold snapshot not marked offline")
	}

	provisionSnapshot(t, r, backup.Name, true)
	reconcileBackup(t, r, backup) // release
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "demo-2"}, pod); err != nil {
		t.Fatal(err)
	}
	if _, ok := pod.Annotations[fencingAnnotation]; ok {
		t.Fatalf("source still fenced after the cut: %v", pod.Annotations)
	}
	if _, ok := pod.Annotations[snapshotFenceAnnotation]; ok {
		t.Fatalf("fence marker left behind: %v", pod.Annotations)
	}
	reconcileBackup(t, r, backup) // complete
	done := getSnapshotBackup(t, r, backup.Name)
	if done.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted || done.Status.Online == nil || *done.Status.Online {
		t.Fatalf("cold status = %+v", done.Status)
	}
	if done.Status.EndGTID != "" {
		t.Fatalf("a cold snapshot recorded an anchor %q it cannot know", done.Status.EndGTID)
	}
	if len(snapshots.ends) != 0 {
		t.Fatal("a cold snapshot ended a read lock")
	}
}

func TestVolumeSnapshotPreflightFailures(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		mutate func(*mysqlv1alpha1.Cluster, *mysqlv1alpha1.Backup, *[]client.Object)
		reason string
	}{
		"no volumeSnapshot section": {
			mutate: func(c *mysqlv1alpha1.Cluster, _ *mysqlv1alpha1.Backup, _ *[]client.Object) { c.Spec.Backup = nil },
			reason: reasonNoVolumeSnapshotSection,
		},
		"a fenced instance": {
			mutate: func(_ *mysqlv1alpha1.Cluster, _ *mysqlv1alpha1.Backup, objs *[]client.Object) {
				for _, obj := range *objs {
					if pod, ok := obj.(*corev1.Pod); ok {
						pod.Annotations = map[string]string{fencingAnnotation: routableTrue}
					}
				}
			},
			reason: reasonFencedInstancesPresent,
		},
		"cold on Group Replication": {
			mutate: func(c *mysqlv1alpha1.Cluster, b *mysqlv1alpha1.Backup, _ *[]client.Object) {
				c.Spec.Replication = &mysqlv1alpha1.ReplicationConfiguration{Mode: mysqlv1alpha1.ReplicationModeGroupReplication}
				b.Spec.Online = new(false)
			},
			reason: reasonUnsupportedOnGroupReplication,
		},
		"a class for another driver": {
			mutate: func(_ *mysqlv1alpha1.Cluster, _ *mysqlv1alpha1.Backup, objs *[]client.Object) {
				for _, obj := range *objs {
					if class, ok := obj.(*snapshotv1.VolumeSnapshotClass); ok {
						class.Driver = "ebs.csi.aws.com"
					}
				}
			},
			reason: reasonVolumeSnapshotClassMismatch,
		},
		"a missing class": {
			mutate: func(_ *mysqlv1alpha1.Cluster, b *mysqlv1alpha1.Backup, _ *[]client.Object) {
				b.Spec.VolumeSnapshot = &mysqlv1alpha1.VolumeSnapshotConfiguration{ClassName: "nope"}
			},
			reason: reasonVolumeSnapshotClassMismatch,
		},
		"no default class for the driver": {
			mutate: func(c *mysqlv1alpha1.Cluster, _ *mysqlv1alpha1.Backup, _ *[]client.Object) {
				c.Spec.Backup.VolumeSnapshot.ClassName = ""
			},
			reason: reasonVolumeSnapshotClassMismatch,
		},
		"an unbound volume": {
			mutate: func(_ *mysqlv1alpha1.Cluster, _ *mysqlv1alpha1.Backup, objs *[]client.Object) {
				for _, obj := range *objs {
					if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok {
						pvc.Status.Phase = corev1.ClaimPending
					}
				}
			},
			reason: reasonPVCNotBound,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			snapshots := &fakeSnapshotClient{}
			cluster, backup := snapshotCluster(), snapshotBackup()
			objs := append(snapshotVolumeObjects(), readyReplicaPod())
			tc.mutate(cluster, backup, &objs)
			r := newSnapshotReconciler(t, snapshots, append(objs, cluster, backup)...)
			reconcileBackup(t, r, backup)
			expectBackupFailed(t, r, backup.Name, tc.reason)
			if len(snapshots.begins) != 0 {
				t.Fatal("a failed preflight took a hold")
			}
		})
	}
}

func TestVolumeSnapshotWithoutTheCRDFails(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := mysqlv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	backup := snapshotBackup()
	r := &BackupReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Backup{}).
			WithObjects(snapshotCluster(), backup, readyReplicaPod()).Build(),
		Scheme:         scheme,
		SnapshotClient: &fakeSnapshotClient{},
	}
	reconcileBackup(t, r, backup)
	expectBackupFailed(t, r, backup.Name, reasonNoVolumeSnapshotCRD)
}

func TestVolumeSnapshotResolvesTheDriversDefaultClass(t *testing.T) {
	t.Parallel()
	cluster, backup := snapshotCluster(), snapshotBackup()
	cluster.Spec.Backup.VolumeSnapshot.ClassName = ""
	objs := snapshotVolumeObjects()
	objs = append(objs, &snapshotv1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{Name: "default-hostpath", Annotations: map[string]string{defaultSnapshotClassAnnotation: "true"}},
		Driver:     testCSIDriver,
	}, &snapshotv1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{Name: "default-ebs", Annotations: map[string]string{defaultSnapshotClassAnnotation: "true"}},
		Driver:     "ebs.csi.aws.com",
	})
	r := newSnapshotReconciler(t, &fakeSnapshotClient{}, append(objs, cluster, backup, readyReplicaPod())...)
	reconcileBackup(t, r, backup)
	reconcileBackup(t, r, backup)
	snap := getSnapshot(t, r, backup.Name)
	if snap.Spec.VolumeSnapshotClassName == nil || *snap.Spec.VolumeSnapshotClassName != "default-hostpath" {
		t.Fatalf("class = %v, want default-hostpath", snap.Spec.VolumeSnapshotClassName)
	}
}

func TestVolumeSnapshotFailsWhenTheHoldExpiredBeforeTheCut(t *testing.T) {
	t.Parallel()
	snapshots := &fakeSnapshotClient{anchor: webserver.SnapshotAnchor{AnchorGTID: "uuid:1-3"}}
	backup := snapshotBackup()
	r := newSnapshotReconciler(t, snapshots, append(snapshotVolumeObjects(), snapshotCluster(), backup, readyReplicaPod())...)
	reconcileBackup(t, r, backup)
	reconcileBackup(t, r, backup)
	provisionSnapshot(t, r, backup.Name, false)
	snapshots.endErr = fmt.Errorf("instance /cluster/snapshot/end: %w", webserver.ErrSnapshotHoldExpired)
	reconcileBackup(t, r, backup)
	expectBackupFailed(t, r, backup.Name, reasonSnapshotLockExpired)
	// The snapshot is left for inspection.
	getSnapshot(t, r, backup.Name)
}

func TestVolumeSnapshotGivesUpAtTheLockTimeout(t *testing.T) {
	t.Parallel()
	snapshots := &fakeSnapshotClient{anchor: webserver.SnapshotAnchor{AnchorGTID: "uuid:1-3"}}
	backup := snapshotBackup()
	backup.Spec.VolumeSnapshot = &mysqlv1alpha1.VolumeSnapshotConfiguration{LockTimeout: &metav1.Duration{Duration: 30 * time.Second}}
	r := newSnapshotReconciler(t, snapshots, append(snapshotVolumeObjects(), snapshotCluster(), backup, readyReplicaPod())...)
	reconcileBackup(t, r, backup)
	reconcileBackup(t, r, backup)
	if snapshots.begins[0].Timeout != "30s" {
		t.Fatalf("hold timeout = %q, want the Backup's 30s", snapshots.begins[0].Timeout)
	}

	// The driver never cuts it; time passes the lock timeout.
	latest := getSnapshotBackup(t, r, backup.Name)
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	latest.Status.StartedAt = &past
	if err := r.Status().Update(context.Background(), latest); err != nil {
		t.Fatal(err)
	}
	reconcileBackup(t, r, backup)
	expectBackupFailed(t, r, backup.Name, reasonSnapshotLockExpired)
	if len(snapshots.ends) != 1 {
		t.Fatalf("the hold was not ended on abort: %+v", snapshots.ends)
	}
}

func TestVolumeSnapshotPrepareFailureFailsTheBackup(t *testing.T) {
	t.Parallel()
	snapshots := &fakeSnapshotClient{beginErr: fmt.Errorf("instance /cluster/snapshot/begin: %w", webserver.ErrSnapshotHoldConflict)}
	backup := snapshotBackup()
	r := newSnapshotReconciler(t, snapshots, append(snapshotVolumeObjects(), snapshotCluster(), backup, readyReplicaPod())...)
	reconcileBackup(t, r, backup)
	reconcileBackup(t, r, backup)
	expectBackupFailed(t, r, backup.Name, reasonSnapshotPrepareFailed)
	if !strings.Contains(getSnapshotBackup(t, r, backup.Name).Status.Error, "another backup") {
		t.Fatalf("error = %q", getSnapshotBackup(t, r, backup.Name).Status.Error)
	}
}

func TestVolumeSnapshotOwnerReference(t *testing.T) {
	t.Parallel()
	for owner, kind := range map[mysqlv1alpha1.SnapshotOwnerReference]string{
		mysqlv1alpha1.SnapshotOwnerBackup:  "Backup",
		mysqlv1alpha1.SnapshotOwnerCluster: "Cluster",
	} {
		backup := snapshotBackup()
		backup.Spec.VolumeSnapshot = &mysqlv1alpha1.VolumeSnapshotConfiguration{SnapshotOwnerReference: owner}
		r := newSnapshotReconciler(t, &fakeSnapshotClient{}, append(snapshotVolumeObjects(), snapshotCluster(), backup, readyReplicaPod())...)
		reconcileBackup(t, r, backup)
		reconcileBackup(t, r, backup)
		refs := getSnapshot(t, r, backup.Name).OwnerReferences
		if len(refs) != 1 || refs[0].Kind != kind || refs[0].Controller == nil || !*refs[0].Controller {
			t.Fatalf("owner %s: references = %+v", owner, refs)
		}
	}
}

func TestVolumeSnapshotBackupDeletedMidHoldReleasesTheSource(t *testing.T) {
	t.Parallel()
	snapshots := &fakeSnapshotClient{anchor: webserver.SnapshotAnchor{AnchorGTID: "uuid:1-3"}}
	backup := snapshotBackup()
	r := newSnapshotReconciler(t, snapshots, append(snapshotVolumeObjects(), snapshotCluster(), backup, readyReplicaPod())...)
	reconcileBackup(t, r, backup)
	reconcileBackup(t, r, backup)
	if err := r.Delete(context.Background(), getSnapshotBackup(t, r, backup.Name)); err != nil {
		t.Fatal(err)
	}
	reconcileBackup(t, r, backup)
	if len(snapshots.ends) != 1 {
		t.Fatalf("deleting a held Backup did not end the hold: %+v", snapshots.ends)
	}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: backup.Name}, &mysqlv1alpha1.Backup{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Backup still present after the release: %v", err)
	}
}

func TestVolumeSnapshotReclaimDeletesTheSnapshot(t *testing.T) {
	t.Parallel()
	backup := snapshotBackup()
	backup.Finalizers = []string{backupFinalizer}
	backup.Spec.ReclaimPolicy = mysqlv1alpha1.BackupReclaimDelete
	backup.Status = mysqlv1alpha1.BackupStatus{Phase: mysqlv1alpha1.BackupPhaseCompleted, Method: mysqlv1alpha1.BackupMethodVolumeSnapshot}
	ours := &snapshotv1.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{
		Name: backup.Name, Namespace: "default", Labels: map[string]string{snapshotBackupLabel: backup.Name},
	}}
	other := &snapshotv1.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{
		Name: "someone-else", Namespace: "default", Labels: map[string]string{snapshotBackupLabel: "other"},
	}}
	r := newSnapshotReconciler(t, &fakeSnapshotClient{}, snapshotCluster(), backup, ours, other)
	if err := r.Delete(context.Background(), getSnapshotBackup(t, r, backup.Name)); err != nil {
		t.Fatal(err)
	}
	reconcileBackup(t, r, backup)
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: backup.Name}, &snapshotv1.VolumeSnapshot{}); !apierrors.IsNotFound(err) {
		t.Fatalf("reclaimPolicy Delete left the VolumeSnapshot: %v", err)
	}
	getSnapshot(t, r, "someone-else")
}

func TestResolveSnapshotConfig(t *testing.T) {
	t.Parallel()
	minute := &metav1.Duration{Duration: time.Minute}
	cases := map[string]struct {
		backup  *mysqlv1alpha1.VolumeSnapshotConfiguration
		online  *bool
		cluster *mysqlv1alpha1.VolumeSnapshotConfiguration
		want    resolvedSnapshotConfig
	}{
		"defaults": {
			online: new(true),
			want:   resolvedSnapshotConfig{Owner: mysqlv1alpha1.SnapshotOwnerNone, Online: true, Deadline: defaultSnapshotDeadline, LockTimeout: defaultSnapshotLockTimeout},
		},
		"cluster values": {
			online:  new(true),
			cluster: &mysqlv1alpha1.VolumeSnapshotConfiguration{ClassName: "c", Online: new(false), Deadline: minute, SnapshotOwnerReference: mysqlv1alpha1.SnapshotOwnerCluster},
			want:    resolvedSnapshotConfig{ClassName: "c", Owner: mysqlv1alpha1.SnapshotOwnerCluster, Online: false, Deadline: time.Minute, LockTimeout: defaultSnapshotLockTimeout},
		},
		"Backup override wins field by field": {
			online:  new(true),
			backup:  &mysqlv1alpha1.VolumeSnapshotConfiguration{ClassName: "b", Online: new(true)},
			cluster: &mysqlv1alpha1.VolumeSnapshotConfiguration{ClassName: "c", Online: new(false), LockTimeout: minute},
			want:    resolvedSnapshotConfig{ClassName: "b", Owner: mysqlv1alpha1.SnapshotOwnerNone, Online: true, Deadline: defaultSnapshotDeadline, LockTimeout: time.Minute},
		},
		"spec.online false beats the cluster default": {
			online:  new(false),
			cluster: &mysqlv1alpha1.VolumeSnapshotConfiguration{Online: new(true)},
			want:    resolvedSnapshotConfig{Owner: mysqlv1alpha1.SnapshotOwnerNone, Online: false, Deadline: defaultSnapshotDeadline, LockTimeout: defaultSnapshotLockTimeout},
		},
		"the override beats spec.online false": {
			online: new(false),
			backup: &mysqlv1alpha1.VolumeSnapshotConfiguration{Online: new(true)},
			want:   resolvedSnapshotConfig{Owner: mysqlv1alpha1.SnapshotOwnerNone, Online: true, Deadline: defaultSnapshotDeadline, LockTimeout: defaultSnapshotLockTimeout},
		},
	}
	for name, tc := range cases {
		backup := snapshotBackup()
		backup.Spec.Online = tc.online
		backup.Spec.VolumeSnapshot = tc.backup
		cluster := snapshotCluster()
		cluster.Spec.Backup.VolumeSnapshot = tc.cluster
		got := resolveSnapshotConfig(backup, cluster)
		if got.ClassName != tc.want.ClassName || got.Owner != tc.want.Owner || got.Online != tc.want.Online ||
			got.Deadline != tc.want.Deadline || got.LockTimeout != tc.want.LockTimeout {
			t.Errorf("%s: got %+v, want %+v", name, got, tc.want)
		}
	}
}

// expectHeldSnapshot checks the Backup and VolumeSnapshot while the replica is
// held: the anchor is recorded, the snapshot targets the replica's volume, and
// nothing that depends on the cut is written yet.
func expectHeldSnapshot(t *testing.T, r *BackupReconciler, name string) {
	t.Helper()
	running := getSnapshotBackup(t, r, name)
	st := running.Status
	if st.Phase != mysqlv1alpha1.BackupPhaseRunning || st.Method != mysqlv1alpha1.BackupMethodVolumeSnapshot ||
		st.InstanceName != "demo-2" || st.EndGTID != "uuid:1-42" || st.BeginGTID != "uuid:1-42" ||
		st.EndBinlog != "binlog.000003:157" || st.JobName != "" || st.ObjectStore != nil {
		t.Fatalf("running status = %+v", st)
	}
	snap := getSnapshot(t, r, name)
	if snap.Spec.Source.PersistentVolumeClaimName == nil || *snap.Spec.Source.PersistentVolumeClaimName != "demo-2" ||
		snap.Spec.VolumeSnapshotClassName == nil || *snap.Spec.VolumeSnapshotClassName != testSnapshotClass {
		t.Fatalf("snapshot spec = %+v", snap.Spec)
	}
	for k, v := range map[string]string{
		clusterLabel: "demo", snapshotBackupLabel: name, instanceLabel: "demo-2", snapshotVolumeLabel: "data",
	} {
		if snap.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, snap.Labels[k], v)
		}
	}
	if snap.Annotations[snapshotServerVersionAnnotation] != "8.4.3" || snap.Annotations[snapshotAnchorGTIDAnnotation] != "" {
		t.Fatalf("annotations before the cut = %v", snap.Annotations)
	}
	if len(snap.OwnerReferences) != 0 {
		t.Fatalf("default owner is none, got %v", snap.OwnerReferences)
	}
}

// expectReleasedSnapshot checks the state after the cut: the finalizer is
// gone, the step is recorded, and the snapshot carries its anchor.
func expectReleasedSnapshot(t *testing.T, r *BackupReconciler, name string) {
	t.Helper()
	released := getSnapshotBackup(t, r, name)
	if controllerutil.ContainsFinalizer(released, snapshotReleaseFinalizer) {
		t.Fatal("release finalizer kept after the release")
	}
	if snapshotProgressReason(released) != reasonSnapshotProvisioned || released.Status.Online == nil || !*released.Status.Online {
		t.Fatalf("released status = %+v", released.Status)
	}
	snap := getSnapshot(t, r, name)
	if snap.Annotations[snapshotAnchorGTIDAnnotation] != "uuid:1-42" ||
		snap.Annotations[snapshotAnchorBinlogAnnotation] != "binlog.000003:157" ||
		snap.Annotations[snapshotOnlineAnnotation] != "true" || snap.Annotations[snapshotBackupEndAnnotation] != "" {
		t.Fatalf("annotations after the cut = %v", snap.Annotations)
	}
}

// expectCompletedSnapshot checks the completed Backup's snapshot record.
func expectCompletedSnapshot(t *testing.T, r *BackupReconciler, name string) {
	t.Helper()
	done := getSnapshotBackup(t, r, name)
	if done.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted || done.Status.StoppedAt == nil {
		t.Fatalf("final status = %+v", done.Status)
	}
	if len(done.Status.VolumeSnapshots) != 1 {
		t.Fatalf("volumeSnapshots = %+v", done.Status.VolumeSnapshots)
	}
	entry := done.Status.VolumeSnapshots[0]
	if entry.Name != name || entry.Volume != "data" || entry.PVCName != "demo-2" || entry.ClassName != testSnapshotClass ||
		!entry.ReadyToUse || entry.RestoreSize == nil || entry.RestoreSize.String() != "10Gi" || entry.CreationTime == nil {
		t.Fatalf("volumeSnapshots[0] = %+v", entry)
	}
	if getSnapshot(t, r, name).Annotations[snapshotBackupEndAnnotation] == "" {
		t.Fatal("backup-end annotation missing")
	}
}
