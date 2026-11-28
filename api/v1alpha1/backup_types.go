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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupJobTemplate is a curated subset of the pod configuration applied to
// the backup, restore and instance bootstrap worker Jobs. It deliberately does
// not expose a full PodTemplateSpec: the operator owns the bootstrap init
// container, the scratch/TLS volumes and mounts, the worker command and args,
// and the object-store credential env, and a free-form template would let
// those be broken. Every field is optional; a per-Backup template overrides
// the cluster-wide spec.backup.jobTemplate field by field.
type BackupJobTemplate struct {
	// TTL is how long the finished backup worker Job is kept before Kubernetes
	// garbage-collects it (its ttlSecondsAfterFinished). When unset on both the
	// Backup and the cluster, the operator keeps the Job for 24h. A zero duration
	// deletes the Job as soon as it finishes.
	// +optional
	TTL *metav1.Duration `json:"ttl,omitempty"`

	// ActiveDeadline bounds how long the backup worker Job may run (its
	// activeDeadlineSeconds). A worker still running past it is killed and the
	// Backup fails with reason DeadlineExceeded, so a stalled upload or source
	// stream surfaces as a failure instead of a Backup left running forever. When
	// unset on both the Backup and the cluster, the operator uses 24h. A zero
	// duration disables the deadline.
	// +optional
	ActiveDeadline *metav1.Duration `json:"activeDeadline,omitempty"`

	// Resources sets the resource requests and limits on the backup worker
	// container. Streaming xbstream can be memory-hungry, so operators often want
	// explicit limits. During recovery the cluster-level template's
	// requests/limits also apply to the restore Job. The other instance
	// bootstrap Jobs (initdb, join, import) always use `spec.resources`.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// NodeSelector constrains the backup worker Job's pod to nodes with matching
	// labels, e.g. to keep backups off the critical nodes. Not applied to
	// instance bootstrap Jobs: they follow the instance's scheduling so the
	// data volume binds where the instance can run.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations allow the backup worker Job's pod to schedule onto tainted
	// nodes. Added to the instance's tolerations on instance bootstrap Jobs.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Affinity sets the affinity/anti-affinity rules for the backup worker Job's
	// pod. Not applied to instance bootstrap Jobs: they follow the instance's
	// scheduling so the data volume binds where the instance can run.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// PriorityClassName sets the pod priority for the backup worker Job. Also
	// applies to instance bootstrap Jobs, over `spec.priorityClassName`.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// Labels are merged onto the generated Job and its pod template. Operator
	// labels take precedence on conflict.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations are merged onto the generated Job and its pod template.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// BackupCleanupFinalizer, when present on a Backup, makes the operator delete
// the backup's object-store artifacts (the backup payload and its manifest) when
// the Backup object is deleted. It is opt-in: the operator only adds it when the
// Backup (or the ScheduledBackup that generated it) sets reclaimPolicy: Delete,
// so default deletes leave remote archives untouched.
const BackupCleanupFinalizer = "mysql.cnmsql.co/cleanup-backup-files"

// BackupRetentionExpiredAnnotation marks a volumeSnapshot Backup that the
// Cluster's retentionPolicy expired. Its VolumeSnapshots are deleted with it
// whatever its reclaimPolicy, as retention deletes an expired object-store
// archive.
const BackupRetentionExpiredAnnotation = "mysql.cnmsql.co/retention-expired"

// ClusterBackupCleanupFinalizer, when present on a Cluster, makes the operator
// delete the cluster's entire object-store archive (every backup, the archived
// binlogs, and the archive index) when the Cluster is deleted. It is
// opt-in via spec.backup.reclaimPolicy: Delete; the default keeps the archive so
// a deleted Cluster can still be recovered.
const ClusterBackupCleanupFinalizer = "mysql.cnmsql.co/cleanup-object-store"

// BackupReclaimPolicy describes what happens to an object-store archive when the
// Kubernetes object that owns it (a Backup or a Cluster) is deleted.
// +kubebuilder:validation:Enum=Retain;Delete
type BackupReclaimPolicy string

const (
	// BackupReclaimRetain keeps object-store artifacts after the owning object is
	// deleted. This is the default: removing a Kubernetes object never destroys
	// the only copy of a recovery point unless the user opts in.
	BackupReclaimRetain BackupReclaimPolicy = "Retain"

	// BackupReclaimDelete removes the object-store artifacts when the owning
	// object is deleted, via the cleanup finalizer.
	BackupReclaimDelete BackupReclaimPolicy = "Delete"
)

// BackupMethod is the method used to take a backup.
// +kubebuilder:validation:Enum=xtrabackup;volumeSnapshot;logical
type BackupMethod string

const (
	// BackupMethodXtrabackup uses Percona XtraBackup to stream a physical backup
	// to the object store.
	BackupMethodXtrabackup BackupMethod = "xtrabackup"

	// BackupMethodVolumeSnapshot uses CSI volume snapshots.
	BackupMethodVolumeSnapshot BackupMethod = "volumeSnapshot"

	// BackupMethodLogical takes a SQL dump of the application schemas with the
	// engine's dump client (mysqldump / mariadb-dump). A logical backup is never a
	// base for recovery or point-in-time replay; load it into a new cluster
	// instead.
	BackupMethodLogical BackupMethod = "logical"
)

// LogicalBackupOptions configures a logical (SQL dump) backup.
type LogicalBackupOptions struct {
	// Databases limits the dump to these schemas. Empty means every application
	// schema. The system schemas (mysql, sys, performance_schema,
	// information_schema) and operator-owned schemas are always excluded.
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="self.all(d, !(d.lowerAscii() in ['mysql', 'sys', 'performance_schema', 'information_schema']))",message="system schemas cannot be dumped"
	// +listType=set
	// +optional
	Databases []string `json:"databases,omitempty"`

	// ExtraArgs are appended to the dump command. They replace the cluster's
	// spec.backup.logicalOptions. The operator does not validate them: flags
	// that change the output format or GTID handling break restore.
	// +optional
	ExtraArgs []string `json:"extraArgs,omitempty"`
}

// SnapshotOwnerReference names the object that owns the VolumeSnapshots a
// volumeSnapshot Backup creates.
// +kubebuilder:validation:Enum=none;backup;cluster
type SnapshotOwnerReference string

const (
	// SnapshotOwnerNone sets no owner: the VolumeSnapshots outlive the Backup and
	// the Cluster. This is the default.
	SnapshotOwnerNone SnapshotOwnerReference = "none"
	// SnapshotOwnerBackup makes the Backup the VolumeSnapshots' controller owner,
	// so they are garbage-collected with it.
	SnapshotOwnerBackup SnapshotOwnerReference = "backup"
	// SnapshotOwnerCluster makes the Cluster the VolumeSnapshots' controller
	// owner, so they are garbage-collected with it.
	SnapshotOwnerCluster SnapshotOwnerReference = "cluster"
)

// VolumeSnapshotConfiguration configures Backups taken with method
// volumeSnapshot. On a Backup it overrides the cluster's
// spec.backup.volumeSnapshot field by field.
type VolumeSnapshotConfiguration struct {
	// ClassName is the VolumeSnapshotClass used for the instance data volume.
	// Empty uses the default class for the volume's CSI driver.
	// +optional
	ClassName string `json:"className,omitempty"`

	// Labels are added to every VolumeSnapshot. Operator keys win on conflict.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations are added to every VolumeSnapshot. Operator keys win on
	// conflict.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// SnapshotOwnerReference sets who owns the VolumeSnapshot objects: none (the
	// default; they outlive the Backup and the Cluster), backup, or cluster.
	// +optional
	SnapshotOwnerReference SnapshotOwnerReference `json:"snapshotOwnerReference,omitempty"`

	// Online selects how the source instance is quiesced for the cut. True holds
	// a global read lock on the source while the snapshot is cut, which stalls
	// writes on a primary target. False fences the source instance (mysqld
	// stopped) for the cut. When unset on the Backup and the cluster, a Backup
	// with spec.online: false is cold and every other one is online.
	// +optional
	Online *bool `json:"online,omitempty"`

	// Deadline bounds how long a snapshot Backup retries CSI errors and waits
	// for the snapshot to be provisioned and ready before failing. Default 10m.
	// +optional
	Deadline *metav1.Duration `json:"deadline,omitempty"`

	// LockTimeout bounds how long the source instance holds the global read lock
	// (online) or stays fenced (cold) waiting for the snapshot to be cut. When it
	// elapses the instance releases on its own and the Backup fails. Default 2m.
	// +optional
	LockTimeout *metav1.Duration `json:"lockTimeout,omitempty"`
}

// VolumeSnapshotStatus records one VolumeSnapshot a Backup produced.
type VolumeSnapshotStatus struct {
	// Name of the VolumeSnapshot, in the Backup's namespace.
	Name string `json:"name"`

	// Volume is the role of the snapshotted volume: data.
	Volume string `json:"volume"`

	// PVCName is the source claim.
	// +optional
	PVCName string `json:"pvcName,omitempty"`

	// ClassName is the VolumeSnapshotClass used, resolved at creation. Empty
	// when the snapshot used the driver's default class.
	// +optional
	ClassName string `json:"className,omitempty"`

	// ReadyToUse mirrors the snapshot's status.readyToUse.
	// +optional
	ReadyToUse bool `json:"readyToUse,omitempty"`

	// RestoreSize mirrors status.restoreSize: the minimum PVC request that can
	// restore it.
	// +optional
	RestoreSize *resource.Quantity `json:"restoreSize,omitempty"`

	// CreationTime mirrors status.creationTime: when the driver cut it.
	// +optional
	CreationTime *metav1.Time `json:"creationTime,omitempty"`
}

// VolumeSnapshotVolumeData is the VolumeSnapshotStatus.Volume value of the
// instance data volume.
const VolumeSnapshotVolumeData = "data"

// BackupPhase is the current phase of a Backup.
type BackupPhase string

const (
	// BackupPhasePending means the backup has not started yet.
	BackupPhasePending BackupPhase = "pending"
	// BackupPhaseRunning means the backup is in progress.
	BackupPhaseRunning BackupPhase = "running"
	// BackupPhaseCompleted means the backup finished successfully.
	BackupPhaseCompleted BackupPhase = "completed"
	// BackupPhaseFailed means the backup failed.
	BackupPhaseFailed BackupPhase = "failed"
)

// BackupSpec defines the desired state of Backup.
// +kubebuilder:validation:XValidation:rule="!has(self.logical) || (has(self.method) && self.method == 'logical')",message="logical is only valid with method: logical"
// +kubebuilder:validation:XValidation:rule="!has(self.method) || self.method != 'logical' || !has(self.online) || self.online",message="a logical backup is always online"
// +kubebuilder:validation:XValidation:rule="!has(self.volumeSnapshot) || (has(self.method) && self.method == 'volumeSnapshot')",message="volumeSnapshot is only valid with method: volumeSnapshot"
type BackupSpec struct {
	// Cluster references the cluster to back up.
	// +kubebuilder:validation:Required
	Cluster LocalObjectReference `json:"cluster"`

	// ObjectStore overrides the destination configured on the referenced
	// Cluster. When omitted, the Cluster's backup object store is used.
	// +optional
	ObjectStore *S3ObjectStore `json:"objectStore,omitempty"`

	// Method is the backup method to use.
	// +kubebuilder:default:=xtrabackup
	// +optional
	Method BackupMethod `json:"method,omitempty"`

	// Target instance to take the backup from.
	// +kubebuilder:default:=prefer-standby
	// +optional
	Target BackupTarget `json:"target,omitempty"`

	// Online, when true, performs a non-blocking (hot) backup. Defaults to true.
	// For method volumeSnapshot, false takes a cold snapshot (the source
	// instance is fenced for the cut); see spec.volumeSnapshot.online.
	// +kubebuilder:default:=true
	// +optional
	Online *bool `json:"online,omitempty"`

	// ReclaimPolicy controls what happens to this backup's object-store archive
	// (the backup payload and its manifest) when the Backup object is deleted.
	// With "Delete" the operator adds the cleanup finalizer and removes the
	// archive on deletion; with "Retain" (the default) the archive is kept.
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default:=Retain
	// +optional
	ReclaimPolicy BackupReclaimPolicy `json:"reclaimPolicy,omitempty"`

	// JobTemplate shapes the backup worker Job for this backup: resources,
	// scheduling (nodeSelector/tolerations/affinity/priorityClassName), extra
	// labels/annotations, and the finished-Job TTL. It overrides the cluster-wide
	// spec.backup.jobTemplate field by field.
	// +optional
	JobTemplate *BackupJobTemplate `json:"jobTemplate,omitempty"`

	// Logical configures the dump when method is "logical".
	// +optional
	Logical *LogicalBackupOptions `json:"logical,omitempty"`

	// VolumeSnapshot overrides the cluster's spec.backup.volumeSnapshot field by
	// field for this Backup. Only valid with method: volumeSnapshot. A snapshot
	// Backup ignores spec.objectStore and spec.jobTemplate: no worker Job runs.
	// +optional
	VolumeSnapshot *VolumeSnapshotConfiguration `json:"volumeSnapshot,omitempty"`
}

// BackupStatus defines the observed state of Backup.
type BackupStatus struct {
	// Phase is the current phase of the backup.
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`

	// InstanceName is the instance the backup was taken from.
	// +optional
	InstanceName string `json:"instanceName,omitempty"`

	// Method is the method that was used.
	// +optional
	Method BackupMethod `json:"method,omitempty"`

	// BackupID is a unique identifier of the backup in the object store.
	// +optional
	BackupID string `json:"backupId,omitempty"`

	// JobName is the Kubernetes Job running this backup.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// DestinationPath is the full path of the backup in the object store.
	// +optional
	DestinationPath string `json:"destinationPath,omitempty"`

	// ObjectStore records the destination the backup was uploaded to, resolved at
	// backup time from the Backup spec or the referenced Cluster. It is snapshotted
	// so the cleanup finalizer can still locate and remove the archive after the
	// referenced Cluster is gone.
	// +optional
	ObjectStore *S3ObjectStore `json:"objectStore,omitempty"`

	// BinlogObjectStore records the cluster's binary-log archive store when the
	// backup ran. Point-in-time recovery from this backup replays binlogs from
	// it. Unset on backups taken without continuous archiving or before this
	// field existed, in which case the archive is looked up in ObjectStore.
	// +optional
	BinlogObjectStore *S3ObjectStore `json:"binlogObjectStore,omitempty"`

	// SHA256 is the checksum of the uploaded backup artifact.
	// +optional
	SHA256 string `json:"sha256,omitempty"`

	// BeginGTID/EndGTID record the GTID range covered by the backup. For a
	// physical backup EndGTID is its anchor, the GTID position of its
	// consistent point, which point-in-time recovery replays from. For a
	// logical backup both hold the dump's snapshot GTID (MariaDB only), for
	// reference.
	// +optional
	BeginGTID string `json:"beginGTID,omitempty"`
	// +optional
	EndGTID string `json:"endGTID,omitempty"`

	// BeginBinlog/EndBinlog record the binary log coordinates. For a logical
	// backup both hold the dump's snapshot position (file:position), for
	// reference.
	// +optional
	BeginBinlog string `json:"beginBinlog,omitempty"`
	// +optional
	EndBinlog string `json:"endBinlog,omitempty"`

	// Databases lists the schemas in a logical backup, which are the ones an
	// import can select.
	// +optional
	Databases []string `json:"databases,omitempty"`

	// VolumeSnapshots lists the snapshots this Backup produced, one per instance
	// volume. Empty for other methods.
	// +listType=map
	// +listMapKey=name
	// +optional
	VolumeSnapshots []VolumeSnapshotStatus `json:"volumeSnapshots,omitempty"`

	// Online records whether a volumeSnapshot Backup was taken under a read lock
	// (true) or on a fenced instance (false). Only set for method
	// volumeSnapshot.
	// +optional
	Online *bool `json:"online,omitempty"`

	// StartedAt/StoppedAt record the backup timing.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	StoppedAt *metav1.Time `json:"stoppedAt,omitempty"`

	// Error holds the error message if the backup failed.
	// +optional
	Error string `json:"error,omitempty"`

	// Conditions represent the latest observations of the backup state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=mybackup
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="Method",type=string,JSONPath=`.status.method`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Backup is a one-shot request to back up a Cluster with XtraBackup, a volume
// snapshot, or a logical dump. Deleting it leaves the backup files in place
// unless it carries the mysql.cnmsql.co/cleanup-backup-files finalizer.
type Backup struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of Backup
	// +required
	Spec BackupSpec `json:"spec"`

	// status defines the observed state of Backup
	// +optional
	Status BackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BackupList contains a list of Backup.
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Backup{}, &BackupList{})
}
