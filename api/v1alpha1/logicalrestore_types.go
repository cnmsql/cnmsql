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

// LogicalRestorePolicy says what a restore does with a selected database that
// already holds objects.
// +kubebuilder:validation:Enum=FailIfExists;DropAndRecreate
type LogicalRestorePolicy string

const (
	// LogicalRestoreFailIfExists refuses the whole restore when a selected
	// database holds a table, view, routine or event. Nothing is changed. An
	// empty database, such as one a Database resource created, is loaded into.
	LogicalRestoreFailIfExists LogicalRestorePolicy = "FailIfExists"
	// LogicalRestoreDropAndRecreate drops each selected database, then loads it
	// from the dump. Schema-level grants survive the drop.
	LogicalRestoreDropAndRecreate LogicalRestorePolicy = "DropAndRecreate"
)

// LogicalRestorePhase is the current phase of a LogicalRestore.
type LogicalRestorePhase string

const (
	// LogicalRestorePhasePending means the restore has not started: the dump or
	// the cluster's primary is not ready yet.
	LogicalRestorePhasePending LogicalRestorePhase = "pending"
	// LogicalRestorePhaseRunning means the restore worker Job is loading the
	// dump.
	LogicalRestorePhaseRunning LogicalRestorePhase = "running"
	// LogicalRestorePhaseCompleted means every selected database was loaded.
	LogicalRestorePhaseCompleted LogicalRestorePhase = "completed"
	// LogicalRestorePhaseFailed means the restore failed. The status error says
	// whether the selected databases were changed.
	LogicalRestorePhaseFailed LogicalRestorePhase = "failed"
)

// LogicalRestoreSpec defines the desired state of LogicalRestore. It is
// immutable: a restore is a one-shot action.
// +kubebuilder:validation:XValidation:rule="has(self.backup) != (has(self.source) && size(self.source) > 0)",message="set exactly one of backup or source"
// +kubebuilder:validation:XValidation:rule="!has(self.backupID) || size(self.backupID) == 0 || (has(self.source) && size(self.source) > 0)",message="backupID is only valid with source"
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable; create a new LogicalRestore"
type LogicalRestoreSpec struct {
	// Cluster is the running cluster to load into. The dump is loaded into its
	// current primary and reaches the replicas through replication.
	// +kubebuilder:validation:Required
	Cluster LocalObjectReference `json:"cluster"`

	// Backup references a completed logical Backup in this namespace. Mutually
	// exclusive with Source.
	// +optional
	Backup *LocalObjectReference `json:"backup,omitempty"`

	// Source names an entry of the target Cluster's spec.externalClusters whose
	// object store holds the dump. Mutually exclusive with Backup.
	// +optional
	Source string `json:"source,omitempty"`

	// BackupID selects a dump under Source. Empty picks the latest.
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// Databases are the schemas loaded from the dump. It is required: a restore
	// never loads a whole dump implicitly. Each must be in the dump.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="self.all(d, !(d.lowerAscii() in ['mysql', 'sys', 'performance_schema', 'information_schema']))",message="system schemas cannot be restored"
	// +listType=set
	Databases []string `json:"databases"`

	// Policy says what to do with a selected database that already holds
	// objects: FailIfExists refuses the restore, DropAndRecreate drops the
	// database first. It is required, so an overwrite is always explicit.
	// +kubebuilder:validation:Required
	Policy LogicalRestorePolicy `json:"policy"`

	// JobTemplate shapes the restore worker Job: resources, scheduling, extra
	// labels and annotations, the finished-Job TTL and the deadline. It
	// overrides the cluster-wide spec.backup.jobTemplate field by field, like a
	// Backup's.
	// +optional
	JobTemplate *BackupJobTemplate `json:"jobTemplate,omitempty"`
}

// LogicalRestoreStatus defines the observed state of LogicalRestore.
type LogicalRestoreStatus struct {
	// Phase is the current phase of the restore.
	// +optional
	Phase LogicalRestorePhase `json:"phase,omitempty"`

	// TargetInstance is the primary the dump is loaded into.
	// +optional
	TargetInstance string `json:"targetInstance,omitempty"`

	// JobName is the Kubernetes Job running the restore.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// BackupID identifies the dump in the object store.
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// SourcePath is the full object-store path of the dump.
	// +optional
	SourcePath string `json:"sourcePath,omitempty"`

	// Databases lists the schemas restored.
	// +optional
	Databases []string `json:"databases,omitempty"`

	// StartedAt/StoppedAt record the restore timing.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	StoppedAt *metav1.Time `json:"stoppedAt,omitempty"`

	// Error holds the error message if the restore failed, and says whether
	// the selected databases were changed.
	// +optional
	Error string `json:"error,omitempty"`

	// Conditions represent the latest observations of the restore state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=mylogicalrestore
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="Policy",type=string,JSONPath=`.spec.policy`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LogicalRestore loads selected databases from a logical backup into a running
// cluster's primary.
type LogicalRestore struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of LogicalRestore
	// +required
	Spec LogicalRestoreSpec `json:"spec"`

	// status defines the observed state of LogicalRestore
	// +optional
	Status LogicalRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LogicalRestoreList contains a list of LogicalRestore.
type LogicalRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LogicalRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LogicalRestore{}, &LogicalRestoreList{})
}
