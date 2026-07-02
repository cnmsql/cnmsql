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

	// ObjectStoreCleanup, when true, adds the object-store cleanup finalizer
	// (mysql.cnmsql.co/cleanup-backup-files) to generated Backups, so deleting a
	// generated Backup also removes its archive (backup.xbstream + metadata.json)
	// from the object store. Defaults to false, matching the non-destructive
	// default for one-shot Backups.
	// +kubebuilder:default:=false
	// +optional
	ObjectStoreCleanup *bool `json:"objectStoreCleanup,omitempty"`

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
