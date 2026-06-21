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

// EnsureOption controls whether a declarative object must be present or absent.
// +kubebuilder:validation:Enum=present;absent
type EnsureOption string

const (
	// EnsurePresent means the object must exist.
	EnsurePresent EnsureOption = "present"
	// EnsureAbsent means the object must not exist.
	EnsureAbsent EnsureOption = "absent"
)

// DatabaseSpec defines the desired state of Database.
type DatabaseSpec struct {
	// Cluster references the MySQL cluster this database belongs to.
	// +kubebuilder:validation:Required
	Cluster LocalObjectReference `json:"cluster"`

	// Name is the name of the MySQL database (schema). Defaults to the resource
	// name if empty.
	// +optional
	Name string `json:"name,omitempty"`

	// Ensure controls whether the database is created or dropped.
	// +kubebuilder:default:=present
	// +optional
	Ensure EnsureOption `json:"ensure,omitempty"`

	// CharacterSet of the database (e.g. "utf8mb4").
	// +optional
	CharacterSet string `json:"characterSet,omitempty"`

	// Collation of the database (e.g. "utf8mb4_0900_ai_ci").
	// +optional
	Collation string `json:"collation,omitempty"`

	// Users is the list of users managed for this database.
	// +optional
	Users []DatabaseUser `json:"users,omitempty"`

	// ReclaimPolicy controls what happens to the MySQL database when the
	// Database object is deleted.
	// +kubebuilder:validation:Enum=delete;retain
	// +kubebuilder:default:=retain
	// +optional
	ReclaimPolicy string `json:"reclaimPolicy,omitempty"`
}

// DatabaseUser describes a MySQL user managed declaratively.
type DatabaseUser struct {
	// Name of the user.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Host the user connects from. Defaults to "%".
	// +kubebuilder:default:=`%`
	// +optional
	Host string `json:"host,omitempty"`

	// Ensure controls whether the user is created or dropped.
	// +kubebuilder:default:=present
	// +optional
	Ensure EnsureOption `json:"ensure,omitempty"`

	// PasswordSecret references the secret key holding the user's password.
	// +optional
	PasswordSecret *SecretKeySelector `json:"passwordSecret,omitempty"`

	// Grants is the list of grants applied to the user.
	// +optional
	Grants []DatabaseGrant `json:"grants,omitempty"`
}

// DatabaseGrant describes a single MySQL GRANT statement.
type DatabaseGrant struct {
	// Privileges is the list of privileges (e.g. "SELECT", "INSERT", "ALL").
	// +kubebuilder:validation:Required
	Privileges []string `json:"privileges"`

	// On is the target of the grant (e.g. "mydb.*"). Defaults to the managed
	// database.
	// +optional
	On string `json:"on,omitempty"`
}

// DatabaseStatus defines the observed state of Database.
type DatabaseStatus struct {
	// Applied is true once the desired state has been reconciled.
	// +optional
	Applied *bool `json:"applied,omitempty"`

	// Message provides additional detail, typically an error.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// PasswordStatus records, per managed user ("name@host"), the source Secret
	// resourceVersion last applied to MySQL. It lets the controller re-apply a
	// password only when its Secret changes.
	// +optional
	PasswordStatus map[string]string `json:"passwordStatus,omitempty"`

	// Conditions represent the latest observations of the database state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=mydatabase
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.name`
// +kubebuilder:printcolumn:name="Applied",type=boolean,JSONPath=`.status.applied`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Database is the Schema for the databases API.
type Database struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of Database
	// +required
	Spec DatabaseSpec `json:"spec"`

	// status defines the observed state of Database
	// +optional
	Status DatabaseStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatabaseList contains a list of Database.
type DatabaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Database `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Database{}, &DatabaseList{})
}
