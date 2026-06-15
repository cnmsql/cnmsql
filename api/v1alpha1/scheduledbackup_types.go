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

// ScheduledBackupSpec defines the desired state of ScheduledBackup.
type ScheduledBackupSpec struct {
	// Schedule is a cron expression (6 fields, including seconds) defining when
	// backups are taken.
	// +kubebuilder:validation:Required
	Schedule string `json:"schedule"`

	// Cluster references the cluster to back up.
	// +kubebuilder:validation:Required
	Cluster LocalObjectReference `json:"cluster"`

	// Suspend, when true, pauses the schedule.
	// +kubebuilder:default:=false
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// Immediate, when true, takes a backup as soon as the ScheduledBackup is
	// created, in addition to the schedule.
	// +kubebuilder:default:=false
	// +optional
	Immediate *bool `json:"immediate,omitempty"`

	// BackupOwnerReference controls the owner reference set on the generated
	// Backup objects.
	// +kubebuilder:validation:Enum=none;self;cluster
	// +kubebuilder:default:=self
	// +optional
	BackupOwnerReference string `json:"backupOwnerReference,omitempty"`

	// Method is the backup method used for the generated backups.
	// +kubebuilder:default:=xtrabackup
	// +optional
	Method BackupMethod `json:"method,omitempty"`

	// Target instance to take the generated backups from.
	// +kubebuilder:default:=prefer-standby
	// +optional
	Target BackupTarget `json:"target,omitempty"`

	// Online, when true, performs non-blocking (hot) backups.
	// +kubebuilder:default:=true
	// +optional
	Online *bool `json:"online,omitempty"`
}

// ScheduledBackupStatus defines the observed state of ScheduledBackup.
type ScheduledBackupStatus struct {
	// LastCheckTime is the last time the schedule was evaluated.
	// +optional
	LastCheckTime *metav1.Time `json:"lastCheckTime,omitempty"`

	// LastScheduleTime is the last time a backup was triggered.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// NextScheduleTime is the next time a backup will be triggered.
	// +optional
	NextScheduleTime *metav1.Time `json:"nextScheduleTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=myscheduledbackup
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Last Backup",type=date,JSONPath=`.status.lastScheduleTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ScheduledBackup is the Schema for the scheduledbackups API.
type ScheduledBackup struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of ScheduledBackup
	// +required
	Spec ScheduledBackupSpec `json:"spec"`

	// status defines the observed state of ScheduledBackup
	// +optional
	Status ScheduledBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ScheduledBackupList contains a list of ScheduledBackup.
type ScheduledBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ScheduledBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ScheduledBackup{}, &ScheduledBackupList{})
}
