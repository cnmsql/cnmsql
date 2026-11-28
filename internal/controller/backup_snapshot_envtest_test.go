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
	"time"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// The VolumeSnapshot CRDs come from the snapshot client module, so the
// snapshot the step machine builds is checked against the real schema. No
// snapshot controller runs here: the spec plays it by writing the status.
var _ = Describe("VolumeSnapshot backup against the API server", func() {
	ctx := context.Background()
	const ns = "default"

	create := func(obj client.Object) {
		Expect(k8sClient.Create(ctx, obj)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, obj) })
	}

	It("takes an online snapshot of a replica and completes", func() {
		cluster := &mysqlv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "snap-env", Namespace: ns},
			Spec: mysqlv1alpha1.ClusterSpec{
				Instances: 2,
				Storage:   mysqlv1alpha1.StorageConfiguration{Size: "1Gi"},
				Bootstrap: &mysqlv1alpha1.BootstrapConfiguration{InitDB: &mysqlv1alpha1.BootstrapInitDB{}},
				Backup: &mysqlv1alpha1.BackupConfiguration{
					VolumeSnapshot: &mysqlv1alpha1.VolumeSnapshotConfiguration{ClassName: "env-snapclass"},
				},
			},
		}
		create(cluster)
		cluster.Status.CurrentPrimary = "snap-env-1"
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())

		create(&snapshotv1.VolumeSnapshotClass{
			ObjectMeta:     metav1.ObjectMeta{Name: "env-snapclass"},
			Driver:         testCSIDriver,
			DeletionPolicy: snapshotv1.VolumeSnapshotContentDelete,
		})
		create(&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-snap-env-2"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: testCSIDriver, VolumeHandle: "vol-2"},
				},
			},
		})
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "snap-env-2", Namespace: ns},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
				VolumeName: "pv-snap-env-2",
			},
		}
		create(pvc)
		pvc.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "snap-env-2", Namespace: ns, Labels: map[string]string{
				clusterLabel: "snap-env", roleLabel: roleReplica,
			}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql"}}},
		}
		create(pod)
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		backup := &mysqlv1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: "snap-env-backup", Namespace: ns},
			Spec: mysqlv1alpha1.BackupSpec{
				Cluster: mysqlv1alpha1.LocalObjectReference{Name: "snap-env"},
				Method:  mysqlv1alpha1.BackupMethodVolumeSnapshot,
			},
		}
		create(backup)

		snapshots := &fakeSnapshotClient{anchor: webserver.SnapshotAnchor{
			AnchorGTID: "uuid:1-7", BinlogFile: "binlog.000002", BinlogPos: 99, Flavor: "mysql",
		}}
		r := &BackupReconciler{Client: k8sClient, Scheme: scheme.Scheme, SnapshotClient: snapshots}
		reconcile := func() {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: backup.Name}})
			Expect(err).NotTo(HaveOccurred())
		}

		reconcile() // finalizer
		reconcile() // hold + create
		Expect(snapshots.begins).To(HaveLen(1))
		snap := &snapshotv1.VolumeSnapshot{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: backup.Name}, snap)).To(Succeed())
		Expect(*snap.Spec.Source.PersistentVolumeClaimName).To(Equal("snap-env-2"))
		Expect(*snap.Spec.VolumeSnapshotClassName).To(Equal("env-snapclass"))

		content := "snapcontent-env"
		cut := metav1.NewTime(time.Now().Truncate(time.Second))
		notReady := false
		snap.Status = &snapshotv1.VolumeSnapshotStatus{
			BoundVolumeSnapshotContentName: &content, CreationTime: &cut, ReadyToUse: &notReady,
		}
		Expect(k8sClient.Status().Update(ctx, snap)).To(Succeed())
		reconcile() // release
		Expect(snapshots.ends).To(HaveLen(1))

		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: backup.Name}, snap)).To(Succeed())
		Expect(snap.Annotations).To(HaveKeyWithValue(snapshotAnchorGTIDAnnotation, "uuid:1-7"))
		ready := true
		size := resource.MustParse("1Gi")
		snap.Status.ReadyToUse = &ready
		snap.Status.RestoreSize = &size
		Expect(k8sClient.Status().Update(ctx, snap)).To(Succeed())
		reconcile() // complete

		done := &mysqlv1alpha1.Backup{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: backup.Name}, done)).To(Succeed())
		Expect(done.Status.Phase).To(Equal(mysqlv1alpha1.BackupPhaseCompleted))
		Expect(done.Status.EndGTID).To(Equal("uuid:1-7"))
		Expect(done.Status.VolumeSnapshots).To(HaveLen(1))
		Expect(done.Status.VolumeSnapshots[0].ReadyToUse).To(BeTrue())
		Expect(done.Finalizers).NotTo(ContainElement(snapshotReleaseFinalizer))
	})
})
