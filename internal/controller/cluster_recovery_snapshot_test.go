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
	"strings"
	"testing"
	"time"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// snapshotRecoveryFixture is a cluster recovering from a completed
// volumeSnapshot Backup, and that Backup's ready VolumeSnapshot.
func snapshotRecoveryFixture() (*mysqlv1alpha1.Cluster, *mysqlv1alpha1.Backup, *snapshotv1.VolumeSnapshot) {
	cluster := baseCluster()
	cluster.Name = "restored"
	cluster.UID = "restored-uid"
	cluster.Spec.Instances = 2
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{Backup: &mysqlv1alpha1.LocalObjectReference{Name: "nightly"}},
	}
	ready := true
	size := resource.MustParse("20Gi")
	cut := metav1.NewTime(time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC))
	backup := &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: "source"},
			Method:  mysqlv1alpha1.BackupMethodVolumeSnapshot,
		},
		Status: mysqlv1alpha1.BackupStatus{
			Phase:     mysqlv1alpha1.BackupPhaseCompleted,
			Method:    mysqlv1alpha1.BackupMethodVolumeSnapshot,
			EndGTID:   "uuid:1-42",
			Online:    new(true),
			StoppedAt: &metav1.Time{Time: cut.Add(10 * time.Minute)},
			VolumeSnapshots: []mysqlv1alpha1.VolumeSnapshotStatus{{
				Name: "nightly", Volume: "data", PVCName: "source-2", ReadyToUse: true, RestoreSize: &size, CreationTime: &cut,
			}},
		},
	}
	snap := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Status:     &snapshotv1.VolumeSnapshotStatus{ReadyToUse: &ready, RestoreSize: &size},
	}
	return cluster, backup, snap
}

func snapshotRecoveryReconciler(t *testing.T, objs ...client.Object) *ClusterReconciler {
	t.Helper()
	scheme := testScheme(t)
	return &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		Scheme: scheme,
	}
}

func TestSnapshotRecoveryAdoptsTheVolume(t *testing.T) {
	t.Parallel()
	cluster, backup, snap := snapshotRecoveryFixture()
	r := snapshotRecoveryReconciler(t, cluster, backup, snap)

	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Recovery == nil || plan.Recovery.Snapshot == nil {
		t.Fatalf("recovery plan = %+v, want a snapshot plan", plan.Recovery)
	}
	if s := plan.Recovery.Snapshot; s.Name != "nightly" || s.BackupName != "nightly" ||
		s.RestoreSize.String() != "20Gi" || s.AnchorGTID != "uuid:1-42" || !s.Online {
		t.Fatalf("snapshot plan = %+v", *s)
	}
	if plan.Recovery.ArchiveKey != "" || plan.Recovery.HasTarget || len(plan.Recovery.StoreEnv) != 0 {
		t.Fatalf("a recovery without a target reads no object store: %+v", plan.Recovery)
	}

	primary := plan.instanceFor(cluster, 1)
	if mode := r.bootstrapModeFor(cluster, plan, primary); mode != bootstrapModeAdopt {
		t.Fatalf("primary bootstrap mode = %s, want adopt", mode)
	}
	job, err := r.bootstrapJob(cluster, plan, primary, bootstrapModeAdopt, instancePVC(cluster, primary.PVCName))
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	for _, want := range []string{"instance restore", "--from-volume", "--backup-name=nightly", "--cluster-uid=restored-uid", "--cluster-name=restored"} {
		if !strings.Contains(args, want) {
			t.Fatalf("adopt args missing %q:\n%s", want, args)
		}
	}
	for _, unwanted := range []string{"--bucket", "--archive-key", "--source-cluster"} {
		if strings.Contains(args, unwanted) {
			t.Fatalf("adopt args carry %q:\n%s", unwanted, args)
		}
	}

	// The primary's volume is created from the snapshot, at least as large as
	// it; a replica still clones the primary on an empty volume.
	if _, err := r.ensurePVC(context.Background(), cluster, primary, pvcSnapshotSource(plan, primary)); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: primary.PVCName}, pvc); err != nil {
		t.Fatal(err)
	}
	ds := pvc.Spec.DataSource
	if ds == nil || ds.Kind != "VolumeSnapshot" || ds.Name != "nightly" || ds.APIGroup == nil || *ds.APIGroup != snapshotv1.GroupName {
		t.Fatalf("primary dataSource = %+v", ds)
	}
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "20Gi" {
		t.Fatalf("primary request = %s, want the 20Gi restore size over the 1Gi spec", got.String())
	}
	if pvc.Annotations[pvcStatusAnnotation] != pvcStatusInitializing {
		t.Fatalf("a snapshot-restored volume still starts initializing: %v", pvc.Annotations)
	}
	replica := plan.instanceFor(cluster, 2)
	if src := pvcSnapshotSource(plan, replica); src != nil {
		t.Fatalf("replica volume source = %+v, want none", src)
	}
	if mode := r.bootstrapModeFor(cluster, plan, replica); mode != bootstrapModeJoin {
		t.Fatalf("replica bootstrap mode = %s, want join", mode)
	}
}

func TestSnapshotRecoveryWithATargetReplaysTheBackupsArchive(t *testing.T) {
	t.Parallel()
	cluster, backup, snap := snapshotRecoveryFixture()
	cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &mysqlv1alpha1.RecoveryTarget{TargetTime: "2026-11-28T10:05:00Z"}
	backup.Status.BinlogObjectStore = &mysqlv1alpha1.S3ObjectStore{
		Bucket: "archive", Path: "clusters",
		Credentials: mysqlv1alpha1.S3Credentials{
			AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "access"},
			SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "secret"},
		},
	}
	r := snapshotRecoveryReconciler(t, cluster, backup, snap, s3CredentialsSecret())

	// The target is after the cut (10:00) but before the Backup completed
	// (10:10): accepted, the snapshot's consistent point is the cut.
	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	rec := plan.Recovery
	if !rec.HasTarget || rec.SourceCluster != "source" || rec.BinlogStore.Bucket != "archive" {
		t.Fatalf("recovery plan = %+v", rec)
	}
	env := map[string]string{}
	for _, e := range rec.StoreEnv {
		env[e.Name] = e.Value
	}
	if env["cnmsql_S3_BUCKET"] != "archive" {
		t.Fatalf("store env = %v, want the archive bucket", env)
	}
	args := strings.Join(adoptArgs(plan), " ")
	for _, want := range []string{"--from-volume", "--source-cluster=source", "--target-time=2026-11-28T10:05:00Z"} {
		if !strings.Contains(args, want) {
			t.Fatalf("adopt args missing %q:\n%s", want, args)
		}
	}
}

func TestSnapshotRecoveryRejectsATargetBeforeTheCut(t *testing.T) {
	t.Parallel()
	cluster, backup, snap := snapshotRecoveryFixture()
	cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &mysqlv1alpha1.RecoveryTarget{TargetTime: "2026-11-28T09:59:00Z"}
	backup.Status.BinlogObjectStore = &mysqlv1alpha1.S3ObjectStore{Bucket: "archive"}
	r := snapshotRecoveryReconciler(t, cluster, backup, snap)
	if _, err := r.buildPlan(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "consistent point") {
		t.Fatalf("error = %v, want a target before the cut refused", err)
	}
}

func TestSnapshotRecoveryWaitsForAReadySnapshot(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*mysqlv1alpha1.Backup, *snapshotv1.VolumeSnapshot) client.Object{
		"backup running": func(b *mysqlv1alpha1.Backup, s *snapshotv1.VolumeSnapshot) client.Object {
			b.Status.Phase = mysqlv1alpha1.BackupPhaseRunning
			return s
		},
		"snapshot not ready on the Backup": func(b *mysqlv1alpha1.Backup, s *snapshotv1.VolumeSnapshot) client.Object {
			b.Status.VolumeSnapshots[0].ReadyToUse = false
			return s
		},
		"VolumeSnapshot deleted": func(*mysqlv1alpha1.Backup, *snapshotv1.VolumeSnapshot) client.Object {
			return nil
		},
		"VolumeSnapshot not ready": func(_ *mysqlv1alpha1.Backup, s *snapshotv1.VolumeSnapshot) client.Object {
			notReady := false
			s.Status.ReadyToUse = &notReady
			return s
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cluster, backup, snap := snapshotRecoveryFixture()
			objs := []client.Object{cluster, backup}
			if obj := mutate(backup, snap); obj != nil {
				objs = append(objs, obj)
			}
			r := snapshotRecoveryReconciler(t, objs...)
			if _, err := r.buildPlan(context.Background(), cluster); err == nil || !strings.HasPrefix(err.Error(), reasonSnapshotNotReady) {
				t.Fatalf("error = %v, want %s", err, reasonSnapshotNotReady)
			}
		})
	}
}

func TestSnapshotRecoveryRefusesAForeignInitializingVolume(t *testing.T) {
	t.Parallel()
	cluster, backup, snap := snapshotRecoveryFixture()
	foreign := instancePVC(cluster, "restored-1")
	foreign.Annotations = map[string]string{pvcStatusAnnotation: pvcStatusInitializing}
	r := snapshotRecoveryReconciler(t, cluster, backup, snap, foreign)
	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	primary := plan.instanceFor(cluster, 1)
	if _, err := r.ensurePVC(context.Background(), cluster, primary, pvcSnapshotSource(plan, primary)); err == nil ||
		!strings.HasPrefix(err.Error(), reasonRecoverySourceMismatch) {
		t.Fatalf("error = %v, want %s", err, reasonRecoverySourceMismatch)
	}
	// Once bootstrapped, the volume is the cluster's and is left alone.
	foreign.Annotations[pvcStatusAnnotation] = pvcStatusReady
	if err := checkSnapshotSource(foreign, plan.Recovery.Snapshot); err != nil {
		t.Fatalf("a bootstrapped volume was refused: %v", err)
	}
}
