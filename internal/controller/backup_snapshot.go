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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// Labels and annotations the operator puts on the VolumeSnapshots it creates
// (design 045). The annotations make a snapshot self-describing: a recovery
// that names the snapshot directly needs no Backup object.
const (
	snapshotBackupLabel = "mysql.cnmsql.co/backup"
	snapshotVolumeLabel = "mysql.cnmsql.co/volume"

	snapshotAnchorGTIDAnnotation    = "mysql.cnmsql.co/anchor-gtid"
	snapshotAnchorBinlogAnnotation  = "mysql.cnmsql.co/anchor-binlog"
	snapshotOnlineAnnotation        = "mysql.cnmsql.co/online"
	snapshotFlavorAnnotation        = "mysql.cnmsql.co/flavor"
	snapshotServerVersionAnnotation = "mysql.cnmsql.co/server-version"
	snapshotBackupStartAnnotation   = "mysql.cnmsql.co/backup-start"
	snapshotBackupEndAnnotation     = "mysql.cnmsql.co/backup-end"
	snapshotBinlogStoreAnnotation   = "mysql.cnmsql.co/binlog-object-store"

	// snapshotFenceAnnotation marks a fencing annotation the operator set for a
	// cold snapshot, naming the Backup, so the release only ever clears a fence
	// it set itself.
	snapshotFenceAnnotation = "mysql.cnmsql.co/snapshot-fence"

	// snapshotReleaseFinalizer is held on a volumeSnapshot Backup while its
	// source instance is quiesced, so deleting the Backup mid-hold still
	// releases the lock or the fence.
	snapshotReleaseFinalizer = "mysql.cnmsql.co/snapshot-release"

	// defaultSnapshotClassAnnotation marks a VolumeSnapshotClass as its
	// driver's default.
	defaultSnapshotClassAnnotation = "snapshot.storage.kubernetes.io/is-default-class"
)

// Progressing reasons of a volumeSnapshot Backup. The reason is the step
// machine's memory of whether the source instance was released.
const (
	reasonSnapshotPreparing   = "SnapshotPreparing"
	reasonSnapshotProvisioned = "SnapshotProvisioned"
)

// Failure reasons of a volumeSnapshot Backup.
const (
	reasonNoVolumeSnapshotCRD           = "ClusterHasNoVolumeSnapshotCRD"
	reasonNoVolumeSnapshotSection       = "ClusterHasNoVolumeSnapshotSection"
	reasonFencedInstancesPresent        = "FencedInstancesPresent"
	reasonVolumeSnapshotClassMismatch   = "VolumeSnapshotClassMismatch"
	reasonUnsupportedOnGroupReplication = "UnsupportedOnGroupReplication"
	reasonPVCNotBound                   = "PVCNotBound"
	reasonSnapshotPrepareFailed         = "SnapshotPrepareFailed"
	reasonSnapshotCreateFailed          = "SnapshotCreateFailed"
	reasonSnapshotDeadlineExceeded      = "SnapshotDeadlineExceeded"
	reasonSnapshotLockExpired           = "SnapshotLockExpired"
	reasonSnapshotMissing               = "SnapshotMissing"
)

const (
	defaultSnapshotDeadline    = 10 * time.Minute
	defaultSnapshotLockTimeout = 2 * time.Minute
	// snapshotHoldPoll is the requeue while the source is held: every poll the
	// hold lasts is a write stall on a primary target, so it is short.
	snapshotHoldPoll = 2 * time.Second
	// snapshotReadyPoll is the requeue once the source is released and the
	// snapshot is uploading.
	snapshotReadyPoll = 10 * time.Second
)

// SnapshotControlClient drives a source instance's snapshot hold.
type SnapshotControlClient interface {
	BeginSnapshot(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req webserver.SnapshotBeginRequest) (*webserver.SnapshotAnchor, error)
	EndSnapshot(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req webserver.SnapshotEndRequest) error
	SnapshotState(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*webserver.SnapshotState, error)
}

// resolvedSnapshotConfig is a Backup's volumeSnapshot configuration merged
// over the cluster's, with defaults applied.
type resolvedSnapshotConfig struct {
	ClassName   string
	Labels      map[string]string
	Annotations map[string]string
	Owner       mysqlv1alpha1.SnapshotOwnerReference
	Online      bool
	Deadline    time.Duration
	LockTimeout time.Duration
}

// resolveSnapshotConfig merges the Backup's spec.volumeSnapshot over the
// cluster's spec.backup.volumeSnapshot field by field. spec.online always has
// a value (its CRD default is true), so only an explicit false on it is taken
// as a request, ahead of the cluster's volumeSnapshot.online.
func resolveSnapshotConfig(backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster) resolvedSnapshotConfig {
	levels := []*mysqlv1alpha1.VolumeSnapshotConfiguration{backup.Spec.VolumeSnapshot}
	if cluster.Spec.Backup != nil {
		levels = append(levels, cluster.Spec.Backup.VolumeSnapshot)
	}
	out := resolvedSnapshotConfig{Owner: mysqlv1alpha1.SnapshotOwnerNone, Online: true,
		Deadline: defaultSnapshotDeadline, LockTimeout: defaultSnapshotLockTimeout}
	var deadline, lockTimeout *metav1.Duration
	for _, level := range levels {
		if level == nil {
			continue
		}
		if out.ClassName == "" {
			out.ClassName = level.ClassName
		}
		if out.Owner == mysqlv1alpha1.SnapshotOwnerNone && level.SnapshotOwnerReference != "" {
			out.Owner = level.SnapshotOwnerReference
		}
		if deadline == nil {
			deadline = level.Deadline
		}
		if lockTimeout == nil {
			lockTimeout = level.LockTimeout
		}
		out.Labels = combineStringMaps(level.Labels, out.Labels)
		out.Annotations = combineStringMaps(level.Annotations, out.Annotations)
	}
	switch {
	case backup.Spec.VolumeSnapshot != nil && backup.Spec.VolumeSnapshot.Online != nil:
		out.Online = *backup.Spec.VolumeSnapshot.Online
	case backup.Spec.Online != nil && !*backup.Spec.Online:
		out.Online = false
	case cluster.Spec.Backup != nil && cluster.Spec.Backup.VolumeSnapshot != nil && cluster.Spec.Backup.VolumeSnapshot.Online != nil:
		out.Online = *cluster.Spec.Backup.VolumeSnapshot.Online
	}
	if deadline != nil && deadline.Duration > 0 {
		out.Deadline = deadline.Duration
	}
	if lockTimeout != nil && lockTimeout.Duration > 0 {
		out.LockTimeout = lockTimeout.Duration
	}
	return out
}

// volumeSnapshotAPIAvailable reports whether the VolumeSnapshot CRD is
// installed. It asks the RESTMapper on every call, so a snapshotter installed
// after the operator started is picked up.
func volumeSnapshotAPIAvailable(mapper apimeta.RESTMapper) bool {
	if mapper == nil {
		return false
	}
	_, err := mapper.RESTMapping(schema.GroupKind{Group: snapshotv1.GroupName, Kind: "VolumeSnapshot"},
		snapshotv1.SchemeGroupVersion.Version)
	return err == nil
}

func (r *BackupReconciler) snapshotControl() SnapshotControlClient {
	if r.SnapshotClient == nil {
		r.SnapshotClient = &HTTPControlClient{Client: r.Client}
	}
	return r.SnapshotClient
}

// reconcileVolumeSnapshot drives a volumeSnapshot Backup through its step
// machine: preflight, prepare, create, provisioned, finalize, ready, complete
// (design 045). It runs no Job and needs no object store.
func (r *BackupReconciler) reconcileVolumeSnapshot(
	ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster,
) (ctrl.Result, error) {
	cfg := resolveSnapshotConfig(backup, cluster)
	if backup.Status.Phase != mysqlv1alpha1.BackupPhaseRunning || backup.Status.InstanceName == "" {
		return r.startVolumeSnapshot(ctx, backup, cluster, cfg)
	}
	return r.continueVolumeSnapshot(ctx, backup, cluster, cfg)
}

// startVolumeSnapshot runs the preflight, takes the release finalizer, and
// pins the source instance and mode on the Backup status.
func (r *BackupReconciler) startVolumeSnapshot(
	ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster, cfg resolvedSnapshotConfig,
) (ctrl.Result, error) {
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.VolumeSnapshot == nil {
		return ctrl.Result{}, r.failBackup(ctx, backup, reasonNoVolumeSnapshotSection,
			fmt.Sprintf("Cluster %q has no spec.backup.volumeSnapshot section", cluster.Name))
	}
	if backup.Spec.Logical != nil {
		return ctrl.Result{}, r.failBackup(ctx, backup, "InvalidSpec", "spec.logical is only valid with method: logical")
	}
	if !volumeSnapshotAPIAvailable(r.RESTMapper()) {
		return ctrl.Result{}, r.failBackup(ctx, backup, reasonNoVolumeSnapshotCRD,
			"The VolumeSnapshot API (snapshot.storage.k8s.io/v1) is not installed in this cluster")
	}
	if !cfg.Online && cluster.IsGroupReplication() {
		return ctrl.Result{}, r.failBackup(ctx, backup, reasonUnsupportedOnGroupReplication,
			"A cold (online: false) volume snapshot is not supported on Group Replication clusters")
	}
	if fenced, err := r.fencedInstances(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	} else if len(fenced) > 0 {
		return ctrl.Result{}, r.failBackup(ctx, backup, reasonFencedInstancesPresent,
			fmt.Sprintf("Instances %v are fenced; unfence them before taking a volume snapshot", fenced))
	}
	source, err := r.selectBackupSource(ctx, backup, cluster)
	if err != nil {
		return ctrl.Result{}, r.failBackup(ctx, backup, "NoBackupSource", err.Error())
	}
	className, reason, message, err := r.preflightSnapshotVolume(ctx, backup.Namespace, source, cfg.ClassName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if reason != "" {
		return ctrl.Result{}, r.failBackup(ctx, backup, reason, message)
	}

	// The finalizer comes before anything is held, so a Backup deleted from
	// here on still releases its source.
	if added, err := r.setSnapshotReleaseFinalizer(ctx, backup, true); err != nil || added {
		return ctrl.Result{}, err
	}

	if source == cluster.Status.CurrentPrimary {
		effect := "writes stall while the global read lock is held"
		if !cfg.Online {
			effect = "the cluster is read-only while the primary is fenced"
		}
		r.event(backup, corev1.EventTypeWarning, "SnapshotOnPrimary",
			fmt.Sprintf("Taking the volume snapshot from the primary %s: %s", source, effect))
	}
	online := cfg.Online
	latest, err := r.patchBackupStatusLatest(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
		now := metav1.Now()
		status.StartedAt = &now
		status.Phase = mysqlv1alpha1.BackupPhaseRunning
		status.Method = mysqlv1alpha1.BackupMethodVolumeSnapshot
		status.InstanceName = source
		status.Online = &online
		status.BinlogObjectStore = anchoredBinlogObjectStore(cluster)
		status.Error = ""
		status.VolumeSnapshots = []mysqlv1alpha1.VolumeSnapshotStatus{{
			Name: backup.Name, Volume: mysqlv1alpha1.VolumeSnapshotVolumeData, PVCName: source, ClassName: className,
		}}
		message := "Holding the source instance for the volume snapshot"
		setBackupCondition(status, mysqlv1alpha1.ConditionProgressing, metav1.ConditionTrue, reasonSnapshotPreparing, message, backup.Generation)
		setBackupCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, backupPhaseRunning, message, backup.Generation)
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.continueVolumeSnapshot(ctx, latest, cluster, cfg)
}

// continueVolumeSnapshot runs the steps after the preflight. Which step is
// next is derived from the VolumeSnapshot's existence and state and the
// Backup's Progressing reason, so a pass that lost a status patch converges.
func (r *BackupReconciler) continueVolumeSnapshot(
	ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster, cfg resolvedSnapshotConfig,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("backup", backup.Name, "instance", backup.Status.InstanceName)
	snap, err := r.backupSnapshot(ctx, backup)
	if err != nil {
		return ctrl.Result{}, r.abortVolumeSnapshot(ctx, backup, cluster, reasonSnapshotCreateFailed, err.Error())
	}
	elapsed := time.Duration(0)
	if backup.Status.StartedAt != nil {
		elapsed = time.Since(backup.Status.StartedAt.Time)
	}
	if snapshotProgressReason(backup) == reasonSnapshotProvisioned {
		return r.awaitSnapshotReady(ctx, backup, snap, cfg, elapsed)
	}

	// The source is held from here to the finalize step, bounded on both
	// sides: the instance releases itself after lockTimeout, and the operator
	// gives up at the same bound measured from the start.
	if elapsed > cfg.LockTimeout {
		return ctrl.Result{}, r.abortVolumeSnapshot(ctx, backup, cluster, reasonSnapshotLockExpired,
			fmt.Sprintf("The volume snapshot was not cut within lockTimeout (%s); the source instance was released", cfg.LockTimeout))
	}
	if elapsed > cfg.Deadline {
		return ctrl.Result{}, r.abortVolumeSnapshot(ctx, backup, cluster, reasonSnapshotDeadlineExceeded,
			fmt.Sprintf("The volume snapshot was not provisioned within the deadline (%s)", cfg.Deadline))
	}

	if snap == nil {
		anchor, ready, err := r.prepareSnapshotSource(ctx, backup, cluster, cfg)
		if err != nil || !ready {
			if err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: snapshotHoldPoll}, nil
		}
		if err := r.createVolumeSnapshot(ctx, backup, cluster, cfg, anchor); err != nil {
			return ctrl.Result{}, r.abortVolumeSnapshot(ctx, backup, cluster, reasonSnapshotCreateFailed,
				fmt.Sprintf("Could not create the VolumeSnapshot: %v", err))
		}
		log.Info("Created VolumeSnapshot", "volumeSnapshot", backup.Name)
		return ctrl.Result{RequeueAfter: snapshotHoldPoll}, nil
	}

	// Provisioned is when the driver cut the snapshot: the content is bound
	// and creationTime is set. readyToUse can lag by an upload, and the hold
	// does not wait for it.
	if !snapshotProvisioned(snap) {
		if msg := snapshotErrorMessage(snap); msg != "" {
			log.Info("VolumeSnapshot reports an error, retrying until the deadline", "error", msg)
			if err := r.patchBackupStatus(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
				setBackupCondition(status, mysqlv1alpha1.ConditionProgressing, metav1.ConditionTrue, reasonSnapshotPreparing,
					"The CSI driver reported an error, retrying: "+msg, backup.Generation)
			}); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: snapshotHoldPoll}, nil
	}
	return r.finalizeVolumeSnapshot(ctx, backup, cluster, snap, cfg)
}

// prepareSnapshotSource quiesces the source (step 1). It reports whether the
// source is ready for the cut, and the online hold's anchor. Online, it takes
// the instance's hold and records the anchor on the Backup before any snapshot
// exists; cold, it fences the instance and waits for mysqld to have stopped.
func (r *BackupReconciler) prepareSnapshotSource(
	ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster, cfg resolvedSnapshotConfig,
) (*webserver.SnapshotAnchor, bool, error) {
	instance := backup.Status.InstanceName
	if snapshotOnline(backup) {
		anchor, err := r.snapshotControl().BeginSnapshot(ctx, cluster, instance, webserver.SnapshotBeginRequest{
			BackupName: backup.Name,
			Timeout:    cfg.LockTimeout.String(),
		})
		if err != nil {
			return nil, false, r.abortVolumeSnapshot(ctx, backup, cluster, reasonSnapshotPrepareFailed,
				fmt.Sprintf("Instance %s could not be quiesced: %v", instance, err))
		}
		binlog := ""
		if anchor.BinlogFile != "" {
			binlog = fmt.Sprintf("%s:%d", anchor.BinlogFile, anchor.BinlogPos)
		}
		// The anchor is on the Backup before the snapshot exists, so the
		// finalize step always finds it.
		if backup.Status.EndGTID != anchor.AnchorGTID || backup.Status.EndBinlog != binlog {
			if err := r.patchBackupStatus(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
				status.BeginGTID, status.EndGTID = anchor.AnchorGTID, anchor.AnchorGTID
				status.BeginBinlog, status.EndBinlog = binlog, binlog
			}); err != nil {
				return nil, false, err
			}
			backup.Status.BeginGTID, backup.Status.EndGTID = anchor.AnchorGTID, anchor.AnchorGTID
			backup.Status.BeginBinlog, backup.Status.EndBinlog = binlog, binlog
		}
		return anchor, true, nil
	}

	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: instance}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, r.abortVolumeSnapshot(ctx, backup, cluster, reasonSnapshotPrepareFailed,
				fmt.Sprintf("Instance Pod %s was not found", instance))
		}
		return nil, false, err
	}
	if pod.Annotations[snapshotFenceAnnotation] != backup.Name {
		if isPodFenced(pod) {
			return nil, false, r.abortVolumeSnapshot(ctx, backup, cluster, reasonFencedInstancesPresent,
				fmt.Sprintf("Instance %s was fenced by someone else", instance))
		}
		patch := client.MergeFrom(pod.DeepCopy())
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[fencingAnnotation] = routableTrue
		pod.Annotations[snapshotFenceAnnotation] = backup.Name
		if err := r.Patch(ctx, pod, patch); err != nil {
			return nil, false, err
		}
		logf.FromContext(ctx).Info("Fenced instance for a cold volume snapshot", "backup", backup.Name, "instance", instance)
		return nil, false, nil
	}
	state, err := r.snapshotControl().SnapshotState(ctx, cluster, instance)
	if err != nil {
		// The manager may be briefly away while mysqld stops; retry until
		// the lock timeout.
		logf.FromContext(ctx).V(1).Info("Could not read the instance snapshot state", "error", err.Error())
		return nil, false, nil
	}
	return nil, state.Fenced && state.MysqldStopped, nil
}

// createVolumeSnapshot creates the data volume's VolumeSnapshot (step 2). An
// AlreadyExists is cache lag: the next pass finds the snapshot.
//
// anchor is the online hold's anchor, nil for a cold snapshot. Only the facts
// that do not depend on the hold surviving go on the snapshot here; the anchor
// itself is written once the cut is confirmed.
func (r *BackupReconciler) createVolumeSnapshot(
	ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster,
	cfg resolvedSnapshotConfig, anchor *webserver.SnapshotAnchor,
) error {
	instance := backup.Status.InstanceName
	labels := combineStringMaps(cfg.Labels, map[string]string{
		clusterLabel:        cluster.Name,
		snapshotBackupLabel: backup.Name,
		instanceLabel:       instance,
		snapshotVolumeLabel: mysqlv1alpha1.VolumeSnapshotVolumeData,
	})
	flavor := string(cluster.Spec.Flavor)
	serverVersion := ""
	if anchor != nil {
		flavor = cmp.Or(anchor.Flavor, flavor)
		serverVersion = anchor.ServerVersion
	}
	operatorAnnotations := map[string]string{
		snapshotOnlineAnnotation:      fmt.Sprintf("%t", snapshotOnline(backup)),
		snapshotFlavorAnnotation:      flavor,
		snapshotBackupStartAnnotation: backup.Status.StartedAt.UTC().Format(time.RFC3339),
	}
	if serverVersion != "" {
		operatorAnnotations[snapshotServerVersionAnnotation] = serverVersion
	}
	pvcName := instance
	snap := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        backup.Name,
			Namespace:   backup.Namespace,
			Labels:      labels,
			Annotations: combineStringMaps(cfg.Annotations, operatorAnnotations),
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			Source: snapshotv1.VolumeSnapshotSource{PersistentVolumeClaimName: &pvcName},
		},
	}
	if len(backup.Status.VolumeSnapshots) > 0 && backup.Status.VolumeSnapshots[0].ClassName != "" {
		className := backup.Status.VolumeSnapshots[0].ClassName
		snap.Spec.VolumeSnapshotClassName = &className
	}
	switch cfg.Owner {
	case mysqlv1alpha1.SnapshotOwnerBackup:
		if err := controllerutil.SetControllerReference(backup, snap, r.Scheme); err != nil {
			return err
		}
	case mysqlv1alpha1.SnapshotOwnerCluster:
		if err := controllerutil.SetControllerReference(cluster, snap, r.Scheme); err != nil {
			return err
		}
	}
	if err := r.Create(ctx, snap); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// finalizeVolumeSnapshot releases the source once the snapshot is cut (step
// 4), records the snapshot and anchor, and writes the anchor annotations.
func (r *BackupReconciler) finalizeVolumeSnapshot(
	ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster,
	snap *snapshotv1.VolumeSnapshot, cfg resolvedSnapshotConfig,
) (ctrl.Result, error) {
	instance := backup.Status.InstanceName
	online := snapshotOnline(backup)
	if online {
		err := r.snapshotControl().EndSnapshot(ctx, cluster, instance, webserver.SnapshotEndRequest{BackupName: backup.Name})
		switch {
		case errors.Is(err, webserver.ErrSnapshotHoldExpired), errors.Is(err, webserver.ErrSnapshotNotHeld):
			// The lock was gone before the cut was confirmed, so the anchor
			// may not match the snapshot.
			return ctrl.Result{}, r.failVolumeSnapshot(ctx, backup, reasonSnapshotLockExpired,
				fmt.Sprintf("Instance %s released its hold before the snapshot was confirmed cut: %v", instance, err))
		case err != nil:
			// Transient: retried, bounded by the lock timeout.
			return ctrl.Result{}, fmt.Errorf("releasing the snapshot hold on %s: %w", instance, err)
		}
	} else if err := r.unfenceSnapshotSource(ctx, backup.Namespace, instance, backup.Name); err != nil {
		return ctrl.Result{}, err
	}

	annotations := map[string]string{snapshotOnlineAnnotation: fmt.Sprintf("%t", online)}
	if online {
		annotations[snapshotAnchorGTIDAnnotation] = backup.Status.EndGTID
		if backup.Status.EndBinlog != "" {
			annotations[snapshotAnchorBinlogAnnotation] = backup.Status.EndBinlog
		}
	}
	if store := backup.Status.BinlogObjectStore; store != nil {
		redacted := store.DeepCopy()
		redacted.Credentials = mysqlv1alpha1.S3Credentials{}
		if content, err := json.Marshal(redacted); err == nil {
			annotations[snapshotBinlogStoreAnnotation] = string(content)
		}
	}
	if err := r.annotateSnapshot(ctx, snap, annotations); err != nil {
		return ctrl.Result{}, err
	}

	if _, err := r.patchBackupStatusLatest(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
		status.VolumeSnapshots = []mysqlv1alpha1.VolumeSnapshotStatus{snapshotStatusEntry(backup, snap)}
		status.Online = &online
		setBackupCondition(status, mysqlv1alpha1.ConditionProgressing, metav1.ConditionTrue, reasonSnapshotProvisioned,
			"The volume snapshot is cut and the source instance released; waiting for it to be ready to use", backup.Generation)
	}); err != nil {
		return ctrl.Result{}, err
	}
	if _, err := r.setSnapshotReleaseFinalizer(ctx, backup, false); err != nil {
		return ctrl.Result{}, err
	}
	logf.FromContext(ctx).Info("Released the source instance after the volume snapshot was cut",
		"backup", backup.Name, "instance", instance, "online", online)
	r.event(backup, corev1.EventTypeNormal, reasonSnapshotProvisioned,
		fmt.Sprintf("VolumeSnapshot %s cut from instance %s; the instance was released", snap.Name, instance))
	return ctrl.Result{RequeueAfter: snapshotReadyPoll}, nil
}

// awaitSnapshotReady waits for readyToUse (step 5) and completes the Backup
// (step 6).
func (r *BackupReconciler) awaitSnapshotReady(
	ctx context.Context, backup *mysqlv1alpha1.Backup, snap *snapshotv1.VolumeSnapshot,
	cfg resolvedSnapshotConfig, elapsed time.Duration,
) (ctrl.Result, error) {
	if snap == nil {
		return ctrl.Result{}, r.failVolumeSnapshot(ctx, backup, reasonSnapshotMissing,
			fmt.Sprintf("VolumeSnapshot %s disappeared before it was ready to use", backup.Name))
	}
	entry := snapshotStatusEntry(backup, snap)
	ready := snap.Status != nil && snap.Status.ReadyToUse != nil && *snap.Status.ReadyToUse
	if !ready {
		if elapsed > cfg.Deadline {
			return ctrl.Result{}, r.failVolumeSnapshot(ctx, backup, reasonSnapshotDeadlineExceeded,
				fmt.Sprintf("VolumeSnapshot %s was not ready to use within the deadline (%s)", snap.Name, cfg.Deadline))
		}
		if len(backup.Status.VolumeSnapshots) != 1 || !snapshotEntryEqual(backup.Status.VolumeSnapshots[0], entry) {
			if err := r.patchBackupStatus(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
				status.VolumeSnapshots = []mysqlv1alpha1.VolumeSnapshotStatus{entry}
			}); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: snapshotReadyPoll}, nil
	}

	now := metav1.Now()
	if err := r.annotateSnapshot(ctx, snap, map[string]string{
		snapshotBackupEndAnnotation: now.UTC().Format(time.RFC3339),
	}); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchBackupStatus(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
		status.Phase = mysqlv1alpha1.BackupPhaseCompleted
		status.StoppedAt = &now
		status.Error = ""
		status.VolumeSnapshots = []mysqlv1alpha1.VolumeSnapshotStatus{entry}
		setBackupCondition(status, mysqlv1alpha1.ConditionProgressing, metav1.ConditionFalse, backupPhaseCompleted, "Backup completed", backup.Generation)
		setBackupCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionTrue, backupPhaseCompleted, "Backup completed", backup.Generation)
	}); err != nil {
		return ctrl.Result{}, err
	}
	logf.FromContext(ctx).Info("Backup completed", "backup", backup.Name, "volumeSnapshot", snap.Name)
	r.event(backup, corev1.EventTypeNormal, "SnapshotCompleted",
		fmt.Sprintf("VolumeSnapshot %s is ready to use", snap.Name))
	return ctrl.Result{}, nil
}

// abortVolumeSnapshot releases the source (when the step machine has not done
// so already) and fails the Backup. The VolumeSnapshot, if any, is left for
// inspection.
func (r *BackupReconciler) abortVolumeSnapshot(
	ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster, reason, message string,
) error {
	if err := r.releaseSnapshotSource(ctx, backup, cluster); err != nil {
		return err
	}
	return r.failVolumeSnapshot(ctx, backup, reason, message)
}

// failVolumeSnapshot fails the Backup and drops the release finalizer; the
// caller has released the source.
func (r *BackupReconciler) failVolumeSnapshot(ctx context.Context, backup *mysqlv1alpha1.Backup, reason, message string) error {
	r.event(backup, corev1.EventTypeWarning, reason, message)
	if err := r.failBackup(ctx, backup, reason, message); err != nil {
		return err
	}
	_, err := r.setSnapshotReleaseFinalizer(ctx, backup, false)
	return err
}

// releaseSnapshotSource ends the instance's hold or clears the fence the
// Backup set. Ending a hold that is gone (expired, never taken, held for
// another Backup) is not an error here: the instance is not held for us.
func (r *BackupReconciler) releaseSnapshotSource(ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster) error {
	instance := backup.Status.InstanceName
	if instance == "" || snapshotProgressReason(backup) == reasonSnapshotProvisioned {
		return nil
	}
	if !snapshotOnline(backup) {
		return r.unfenceSnapshotSource(ctx, backup.Namespace, instance, backup.Name)
	}
	if cluster == nil {
		return nil
	}
	err := r.snapshotControl().EndSnapshot(ctx, cluster, instance, webserver.SnapshotEndRequest{BackupName: backup.Name})
	if err != nil && !errors.Is(err, webserver.ErrSnapshotHoldExpired) &&
		!errors.Is(err, webserver.ErrSnapshotNotHeld) && !errors.Is(err, webserver.ErrSnapshotHoldConflict) {
		// The instance releases itself at its lock timeout, so an unreachable
		// instance does not keep the Backup from failing.
		logf.FromContext(ctx).Error(err, "Could not release the snapshot hold; the instance releases it at its lock timeout",
			"backup", backup.Name, "instance", instance)
	}
	return nil
}

// unfenceSnapshotSource clears the fencing annotation on instance when this
// Backup set it.
func (r *BackupReconciler) unfenceSnapshotSource(ctx context.Context, namespace, instance, backupName string) error {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: instance}, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	if pod.Annotations[snapshotFenceAnnotation] != backupName {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	delete(pod.Annotations, fencingAnnotation)
	delete(pod.Annotations, snapshotFenceAnnotation)
	if err := r.Patch(ctx, pod, patch); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("Unfenced instance after a cold volume snapshot", "backup", backupName, "instance", instance)
	return nil
}

// fencedInstances lists the cluster's instances whose Pod is fenced.
func (r *BackupReconciler) fencedInstances(ctx context.Context, cluster *mysqlv1alpha1.Cluster) ([]string, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{clusterLabel: cluster.Name}); err != nil {
		return nil, err
	}
	var fenced []string
	for i := range pods.Items {
		if isPodFenced(&pods.Items[i]) {
			fenced = append(fenced, pods.Items[i].Name)
		}
	}
	return fenced, nil
}

// preflightSnapshotVolume checks the source volume can be snapshotted: its
// PVC is bound, and the class exists and serves the volume's CSI driver (the
// one permanent CSI error that is cheap to catch up front). With no class
// configured it resolves the driver's default class. It returns the class to
// use (empty: let the snapshot controller pick) or a failure reason. A
// cluster-scoped read the operator may not make (namespaced install) skips
// the class checks.
func (r *BackupReconciler) preflightSnapshotVolume(
	ctx context.Context, namespace, instance, className string,
) (string, string, string, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: instance}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return "", reasonPVCNotBound, fmt.Sprintf("PersistentVolumeClaim %s was not found", instance), nil
		}
		return "", "", "", err
	}
	if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
		return "", reasonPVCNotBound, fmt.Sprintf("PersistentVolumeClaim %s is not bound", instance), nil
	}
	skip := func(err error) bool { return apierrors.IsForbidden(err) || apimeta.IsNoMatchError(err) }

	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
		if skip(err) {
			return className, "", "", nil
		}
		return "", "", "", err
	}
	if pv.Spec.CSI == nil {
		return "", reasonVolumeSnapshotClassMismatch,
			fmt.Sprintf("PersistentVolume %s of instance %s is not provisioned by a CSI driver and cannot be snapshotted", pv.Name, instance), nil
	}
	driver := pv.Spec.CSI.Driver

	if className != "" {
		class := &snapshotv1.VolumeSnapshotClass{}
		if err := r.Get(ctx, types.NamespacedName{Name: className}, class); err != nil {
			if apierrors.IsNotFound(err) {
				return "", reasonVolumeSnapshotClassMismatch, fmt.Sprintf("VolumeSnapshotClass %q was not found", className), nil
			}
			if skip(err) {
				return className, "", "", nil
			}
			return "", "", "", err
		}
		if class.Driver != driver {
			return "", reasonVolumeSnapshotClassMismatch, fmt.Sprintf(
				"VolumeSnapshotClass %q is for driver %q, but the volume of instance %s is provisioned by %q",
				className, class.Driver, instance, driver), nil
		}
		return className, "", "", nil
	}

	classes := &snapshotv1.VolumeSnapshotClassList{}
	if err := r.List(ctx, classes); err != nil {
		if skip(err) {
			return "", "", "", nil
		}
		return "", "", "", err
	}
	for i := range classes.Items {
		class := &classes.Items[i]
		if class.Driver == driver && class.Annotations[defaultSnapshotClassAnnotation] == "true" {
			return class.Name, "", "", nil
		}
	}
	return "", reasonVolumeSnapshotClassMismatch, fmt.Sprintf(
		"No default VolumeSnapshotClass for driver %q; set spec.backup.volumeSnapshot.className", driver), nil
}

// backupSnapshot returns the Backup's VolumeSnapshot, nil when it does not
// exist. A snapshot of that name the Backup did not create is an error.
func (r *BackupReconciler) backupSnapshot(ctx context.Context, backup *mysqlv1alpha1.Backup) (*snapshotv1.VolumeSnapshot, error) {
	snap := &snapshotv1.VolumeSnapshot{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backup.Name}, snap); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if snap.Labels[snapshotBackupLabel] != backup.Name {
		return nil, fmt.Errorf("VolumeSnapshot %s exists and was not created for this Backup", backup.Name)
	}
	return snap, nil
}

// annotateSnapshot merges annotations onto snap.
func (r *BackupReconciler) annotateSnapshot(ctx context.Context, snap *snapshotv1.VolumeSnapshot, annotations map[string]string) error {
	patch := client.MergeFrom(snap.DeepCopy())
	if snap.Annotations == nil {
		snap.Annotations = map[string]string{}
	}
	changed := false
	for k, v := range annotations {
		if snap.Annotations[k] != v {
			snap.Annotations[k] = v
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return r.Patch(ctx, snap, patch)
}

// setSnapshotReleaseFinalizer adds or removes the release finalizer on a
// fresh read of the Backup, and reports whether it added it.
func (r *BackupReconciler) setSnapshotReleaseFinalizer(ctx context.Context, backup *mysqlv1alpha1.Backup, want bool) (bool, error) {
	latest := &mysqlv1alpha1.Backup{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(backup), latest); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if controllerutil.ContainsFinalizer(latest, snapshotReleaseFinalizer) == want {
		return false, nil
	}
	patch := client.MergeFromWithOptions(latest.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if want {
		controllerutil.AddFinalizer(latest, snapshotReleaseFinalizer)
	} else {
		controllerutil.RemoveFinalizer(latest, snapshotReleaseFinalizer)
	}
	if err := r.Patch(ctx, latest, patch); err != nil {
		return false, err
	}
	backup.Finalizers = latest.Finalizers
	return want, nil
}

// reconcileSnapshotRelease releases the source of a volumeSnapshot Backup
// deleted while it held it, then drops the release finalizer.
func (r *BackupReconciler) reconcileSnapshotRelease(ctx context.Context, backup *mysqlv1alpha1.Backup) error {
	if !controllerutil.ContainsFinalizer(backup, snapshotReleaseFinalizer) {
		return nil
	}
	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.Cluster.Name}, cluster); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		cluster = nil
	}
	if err := r.releaseSnapshotSource(ctx, backup, cluster); err != nil {
		return err
	}
	_, err := r.setSnapshotReleaseFinalizer(ctx, backup, false)
	return err
}

// cleanupVolumeSnapshots deletes the VolumeSnapshots a Backup created, for
// reclaimPolicy: Delete.
func (r *BackupReconciler) cleanupVolumeSnapshots(ctx context.Context, backup *mysqlv1alpha1.Backup) error {
	if !volumeSnapshotAPIAvailable(r.RESTMapper()) {
		return nil
	}
	snapshots := &snapshotv1.VolumeSnapshotList{}
	if err := r.List(ctx, snapshots, client.InNamespace(backup.Namespace),
		client.MatchingLabels{snapshotBackupLabel: backup.Name}); err != nil {
		return err
	}
	for i := range snapshots.Items {
		snap := &snapshots.Items[i]
		if err := r.Delete(ctx, snap); client.IgnoreNotFound(err) != nil {
			return err
		}
		logf.FromContext(ctx).Info("Deleted VolumeSnapshot", "backup", backup.Name, "volumeSnapshot", snap.Name)
		r.event(backup, corev1.EventTypeNormal, "Cleanup", fmt.Sprintf("Deleted VolumeSnapshot %s", snap.Name))
	}
	return nil
}

// snapshotsForBackup maps a VolumeSnapshot to the Backup that created it.
func snapshotsForBackup(_ context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetLabels()[snapshotBackupLabel]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
}

func snapshotOnline(backup *mysqlv1alpha1.Backup) bool {
	return backup.Status.Online == nil || *backup.Status.Online
}

func snapshotProgressReason(backup *mysqlv1alpha1.Backup) string {
	cond := apimeta.FindStatusCondition(backup.Status.Conditions, mysqlv1alpha1.ConditionProgressing)
	if cond == nil {
		return ""
	}
	return cond.Reason
}

func snapshotProvisioned(snap *snapshotv1.VolumeSnapshot) bool {
	return snap.Status != nil && snap.Status.BoundVolumeSnapshotContentName != nil &&
		*snap.Status.BoundVolumeSnapshotContentName != "" && snap.Status.CreationTime != nil
}

func snapshotErrorMessage(snap *snapshotv1.VolumeSnapshot) string {
	if snap.Status == nil || snap.Status.Error == nil || snap.Status.Error.Message == nil {
		return ""
	}
	return *snap.Status.Error.Message
}

// snapshotStatusEntry mirrors snap into the Backup status entry.
func snapshotStatusEntry(backup *mysqlv1alpha1.Backup, snap *snapshotv1.VolumeSnapshot) mysqlv1alpha1.VolumeSnapshotStatus {
	entry := mysqlv1alpha1.VolumeSnapshotStatus{
		Name:    snap.Name,
		Volume:  mysqlv1alpha1.VolumeSnapshotVolumeData,
		PVCName: backup.Status.InstanceName,
	}
	if snap.Spec.Source.PersistentVolumeClaimName != nil {
		entry.PVCName = *snap.Spec.Source.PersistentVolumeClaimName
	}
	if snap.Spec.VolumeSnapshotClassName != nil {
		entry.ClassName = *snap.Spec.VolumeSnapshotClassName
	}
	if s := snap.Status; s != nil {
		entry.ReadyToUse = s.ReadyToUse != nil && *s.ReadyToUse
		if s.RestoreSize != nil {
			size := s.RestoreSize.DeepCopy()
			entry.RestoreSize = &size
		}
		entry.CreationTime = s.CreationTime.DeepCopy()
	}
	return entry
}

func snapshotEntryEqual(a, b mysqlv1alpha1.VolumeSnapshotStatus) bool {
	sizeEqual := (a.RestoreSize == nil) == (b.RestoreSize == nil) &&
		(a.RestoreSize == nil || a.RestoreSize.Cmp(*b.RestoreSize) == 0)
	timeEqual := (a.CreationTime == nil) == (b.CreationTime == nil) &&
		(a.CreationTime == nil || a.CreationTime.Equal(b.CreationTime))
	return a.Name == b.Name && a.Volume == b.Volume && a.PVCName == b.PVCName && a.ClassName == b.ClassName &&
		a.ReadyToUse == b.ReadyToUse && sizeEqual && timeEqual
}

// patchBackupStatusLatest is patchBackupStatus returning the patched Backup.
func (r *BackupReconciler) patchBackupStatusLatest(
	ctx context.Context, backup *mysqlv1alpha1.Backup, mutate func(*mysqlv1alpha1.BackupStatus),
) (*mysqlv1alpha1.Backup, error) {
	latest := &mysqlv1alpha1.Backup{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(backup), latest); err != nil {
		return nil, err
	}
	before := latest.DeepCopy()
	mutate(&latest.Status)
	if err := r.Status().Patch(ctx, latest, client.MergeFrom(before)); err != nil {
		return nil, err
	}
	latest.SetDefaults()
	return latest, nil
}

func (r *BackupReconciler) event(obj *mysqlv1alpha1.Backup, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(obj, eventType, reason, message)
	}
}
