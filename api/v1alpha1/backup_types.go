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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupCleanupFinalizer, when present on a Backup, makes the operator delete
// the backup's object-store artifacts (backup.xbstream + metadata.json) when the
// Backup object is deleted. It is opt-in: the operator only adds it when the
// Backup (or the ScheduledBackup that generated it) sets reclaimPolicy: Delete,
// so default deletes leave remote archives untouched.
const BackupCleanupFinalizer = "mysql.cnmsql.co/cleanup-backup-files"

// ClusterBackupCleanupFinalizer, when present on a Cluster, makes the operator
// delete the cluster's entire object-store archive (every base backup, the
// archived binlogs, and the archive index) when the Cluster is deleted. It is
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

// BackupMethod is the method used to take a physical backup.
// +kubebuilder:validation:Enum=xtrabackup;volumeSnapshot
type BackupMethod string

const (
	// BackupMethodXtrabackup uses Percona XtraBackup to stream a physical backup
	// to the object store.
	BackupMethodXtrabackup BackupMethod = "xtrabackup"

	// BackupMethodVolumeSnapshot uses CSI volume snapshots.
	BackupMethodVolumeSnapshot BackupMethod = "volumeSnapshot"
)

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
	// +kubebuilder:default:=true
	// +optional
	Online *bool `json:"online,omitempty"`

	// ReclaimPolicy controls what happens to this backup's object-store archive
	// (backup.xbstream + metadata.json) when the Backup object is deleted. With
	// "Delete" the operator adds the cleanup finalizer and removes the archive on
	// deletion; with "Retain" (the default) the archive is kept.
	// +kubebuilder:validation:Enum=Retain;Delete
	// +kubebuilder:default:=Retain
	// +optional
	ReclaimPolicy BackupReclaimPolicy `json:"reclaimPolicy,omitempty"`

	// JobTTL is how long the finished backup worker Job is kept before Kubernetes
	// garbage-collects it (its ttlSecondsAfterFinished). It overrides the
	// cluster-wide spec.backup.jobTTL. When unset on both, the operator keeps the
	// Job for 24h. A zero duration deletes the Job as soon as it finishes.
	// +optional
	JobTTL *metav1.Duration `json:"jobTTL,omitempty"`
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

	// SHA256 is the checksum of the uploaded backup artifact.
	// +optional
	SHA256 string `json:"sha256,omitempty"`

	// BeginGTID/EndGTID record the GTID range covered by the backup.
	// +optional
	BeginGTID string `json:"beginGTID,omitempty"`
	// +optional
	EndGTID string `json:"endGTID,omitempty"`

	// BeginBinlog/EndBinlog record the binary log coordinates.
	// +optional
	BeginBinlog string `json:"beginBinlog,omitempty"`
	// +optional
	EndBinlog string `json:"endBinlog,omitempty"`

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

// Backup is the Schema for the backups API.
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
