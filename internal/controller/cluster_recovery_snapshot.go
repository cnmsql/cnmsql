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

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// Reasons a snapshot recovery is blocked, prefixed to the cluster's phase
// reason.
const (
	reasonSnapshotNotReady       = "SnapshotNotReady"
	reasonRecoverySourceMismatch = "RecoverySourceMismatch"
)

// bootstrapModeAdopt bootstraps the primary from a volume created from a
// VolumeSnapshot: the data directory is already there, and the Job only adopts
// it (design 045).
const bootstrapModeAdopt bootstrapMode = "adopt"

// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch

// resolveSnapshotRecovery plans the recovery of a cluster from a volumeSnapshot
// Backup: the Backup is completed, its single snapshot is ready to use, and
// the VolumeSnapshot object still exists. A recovery target replays the
// Backup's binlog archive from the snapshot's anchor.
func (r *ClusterReconciler) resolveSnapshotRecovery(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	rec *mysqlv1alpha1.BootstrapRecovery,
	backup *mysqlv1alpha1.Backup,
) (*recoveryPlan, error) {
	if backup.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted {
		return nil, fmt.Errorf("%s: recovery backup %q is not completed (phase %q)",
			reasonSnapshotNotReady, backup.Name, backup.Status.Phase)
	}
	if len(backup.Status.VolumeSnapshots) != 1 || !backup.Status.VolumeSnapshots[0].ReadyToUse {
		return nil, fmt.Errorf("%s: recovery backup %q has no single VolumeSnapshot ready to use",
			reasonSnapshotNotReady, backup.Name)
	}
	entry := backup.Status.VolumeSnapshots[0]
	snap := &snapshotv1.VolumeSnapshot{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: entry.Name}, snap); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%s: VolumeSnapshot %s of recovery backup %q was not found in namespace %s",
				reasonSnapshotNotReady, entry.Name, backup.Name, cluster.Namespace)
		}
		return nil, fmt.Errorf("reading VolumeSnapshot %s: %w", entry.Name, err)
	}
	if snap.Status == nil || snap.Status.ReadyToUse == nil || !*snap.Status.ReadyToUse {
		return nil, fmt.Errorf("%s: VolumeSnapshot %s is not ready to use", reasonSnapshotNotReady, entry.Name)
	}
	if err := checkNamedBackupTarget(backup, rec.RecoveryTarget); err != nil {
		return nil, err
	}

	snapshot := &snapshotRecoveryPlan{
		Name:       entry.Name,
		BackupName: backup.Name,
		AnchorGTID: backup.Status.EndGTID,
		Online:     backup.Status.Online == nil || *backup.Status.Online,
	}
	switch {
	case snap.Status.RestoreSize != nil:
		snapshot.RestoreSize = snap.Status.RestoreSize.DeepCopy()
	case entry.RestoreSize != nil:
		snapshot.RestoreSize = entry.RestoreSize.DeepCopy()
	}
	plan := &recoveryPlan{
		Snapshot:      snapshot,
		SourceCluster: backup.Spec.Cluster.Name,
	}
	target := rec.RecoveryTarget
	if target == nil {
		return plan, nil
	}

	// Replay reads the archive the Backup was anchored to, or, for a Backup
	// taken without archiving, the recovering cluster's.
	var binlogStore *mysqlv1alpha1.S3ObjectStore
	switch {
	case backup.Status.BinlogObjectStore != nil:
		binlogStore = backup.Status.BinlogObjectStore.DeepCopy()
	case cluster.Spec.Backup != nil && cluster.Spec.Backup.ObjectStore != nil:
		binlogStore = cluster.BinlogObjectStore().DeepCopy()
	default:
		return nil, fmt.Errorf("recovery backup %q records no binlog archive, and this cluster has no "+
			"spec.backup.objectStore to replay binlogs from", backup.Name)
	}
	binlogStore.SetDefaults()
	plan.HasTarget = true
	plan.TargetTime = target.TargetTime
	plan.TargetGTID = target.TargetGTID
	plan.TargetImmediate = target.TargetImmediate != nil && *target.TargetImmediate
	plan.Store = *binlogStore
	plan.BinlogStore = *binlogStore
	plan.StoreEnv = objectStoreLocationEnv(*binlogStore)
	return plan, nil
}

// pvcSnapshotSource is the VolumeSnapshot the instance's volume is created
// from, nil unless inst is the primary of a cluster recovering from one.
func pvcSnapshotSource(plan clusterPlan, inst instancePlan) *snapshotRecoveryPlan {
	if !inst.IsPrimary || plan.Recovery == nil {
		return nil
	}
	return plan.Recovery.Snapshot
}

// applySnapshotSource points a new volume at the VolumeSnapshot and raises its
// request to the snapshot's restore size when the spec asks for less: a
// volume smaller than the snapshot cannot be provisioned from it.
func applySnapshotSource(spec *corev1.PersistentVolumeClaimSpec, snapshot *snapshotRecoveryPlan) {
	group := snapshotv1.GroupName
	spec.DataSource = &corev1.TypedLocalObjectReference{APIGroup: &group, Kind: "VolumeSnapshot", Name: snapshot.Name}
	spec.DataSourceRef = nil
	if snapshot.RestoreSize.IsZero() {
		return
	}
	if spec.Resources.Requests == nil {
		spec.Resources.Requests = corev1.ResourceList{}
	}
	if current := spec.Resources.Requests[corev1.ResourceStorage]; current.Cmp(snapshot.RestoreSize) < 0 {
		spec.Resources.Requests[corev1.ResourceStorage] = snapshot.RestoreSize.DeepCopy()
	}
}

// checkSnapshotSource refuses a primary volume that is still being
// bootstrapped but was not created from the planned VolumeSnapshot: adopting
// it would bootstrap the cluster from whatever it holds.
func checkSnapshotSource(pvc *corev1.PersistentVolumeClaim, snapshot *snapshotRecoveryPlan) error {
	if snapshot == nil || pvcBootstrapped(pvc) {
		return nil
	}
	ds := pvc.Spec.DataSource
	if ds != nil && ds.Kind == "VolumeSnapshot" && ds.APIGroup != nil && *ds.APIGroup == snapshotv1.GroupName &&
		ds.Name == snapshot.Name {
		return nil
	}
	return fmt.Errorf("%s: PersistentVolumeClaim %s exists and was not created from VolumeSnapshot %s; "+
		"delete it to let the operator create it from the snapshot", reasonRecoverySourceMismatch, pvc.Name, snapshot.Name)
}

// adoptArgs builds the recovering primary's bootstrap Job command when its
// volume was created from a VolumeSnapshot: adopt the copied data directory
// (reset replication, credentials and archive identity) and, for
// point-in-time recovery, replay archived binlogs from the snapshot's anchor.
func adoptArgs(plan clusterPlan) []string {
	snapshot := plan.Recovery.Snapshot
	args := []string{
		managerInstanceCmd, "restore",
		"--from-volume",
		"--data-dir=" + dataDir,
		"--backup-dir=" + joinBackupDir,
		"--backup-name=" + snapshot.BackupName,
		"--cluster-uid=" + plan.ClusterUID,
		"--mysqld=" + mysqldBinary,
		"--config=" + configPath,
		"--socket=" + socketPath,
		"--control-user=" + controlUser,
		"--backup-user=" + backupUser,
		"--cluster-name=" + plan.ClusterName,
	}
	return append(args, replayArgs(plan.Recovery)...)
}

// replayArgs are the point-in-time replay flags of a restore or adopt Job.
func replayArgs(rec *recoveryPlan) []string {
	if !rec.HasTarget {
		return nil
	}
	args := []string{"--source-cluster=" + rec.SourceCluster}
	if rec.TargetTime != "" {
		args = append(args, "--target-time="+rec.TargetTime)
	}
	if rec.TargetGTID != "" {
		args = append(args, "--target-gtid="+rec.TargetGTID)
	}
	if rec.TargetImmediate {
		args = append(args, "--target-immediate")
	}
	return args
}

