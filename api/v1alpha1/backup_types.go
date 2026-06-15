/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
