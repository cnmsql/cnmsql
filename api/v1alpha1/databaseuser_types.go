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

// DatabaseUserAdoptAnnotation, when set to "true" on a DatabaseUser, tells the
// controller to adopt a MySQL account that already exists but was not created by
// this resource, instead of refusing with a UserConflict.
const DatabaseUserAdoptAnnotation = "mysql.cnmsql.co/adopt"

// DatabaseUserSpec defines the desired state of DatabaseUser. A DatabaseUser
// owns a single installation-wide MySQL account (name@host); unlike the inline
// users of a Database, it is not scoped to a single schema and its grants may
// target anything in the instance.
type DatabaseUserSpec struct {
	// Cluster references the MySQL cluster this user belongs to.
	// +kubebuilder:validation:Required
	Cluster LocalObjectReference `json:"cluster"`

	// Name is the MySQL user name. Defaults to the resource name if empty.
	// +kubebuilder:validation:MaxLength=32
	// +optional
	Name string `json:"name,omitempty"`

	// Host the user connects from. Defaults to "%".
	// +kubebuilder:default:=`%`
	// +optional
	Host string `json:"host,omitempty"`

	// Ensure controls whether the user is created or dropped.
	// +kubebuilder:default:=present
	// +optional
	Ensure EnsureOption `json:"ensure,omitempty"`

	// PasswordSecret references the secret key holding the user's password.
	// Required when Ensure=present.
	// +optional
	PasswordSecret *SecretKeySelector `json:"passwordSecret,omitempty"`

	// Superuser grants ALL PRIVILEGES on *.* WITH GRANT OPTION. It is unsafe for
	// multi-tenant use and is mutually exclusive with Grants. For a constrained
	// "DBaaS admin", use a Grants entry with ALL on *.* instead, which omits
	// WITH GRANT OPTION.
	// +kubebuilder:default:=false
	// +optional
	Superuser bool `json:"superuser,omitempty"`

	// MaxUserConnections resource limit. 0 = no limit.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxUserConnections int32 `json:"maxUserConnections,omitempty"`

	// MaxQueriesPerHour resource limit. 0 = no limit.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxQueriesPerHour int32 `json:"maxQueriesPerHour,omitempty"`

	// MaxUpdatesPerHour resource limit. 0 = no limit.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxUpdatesPerHour int32 `json:"maxUpdatesPerHour,omitempty"`

	// MaxConnectionsPerHour resource limit. 0 = no limit.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxConnectionsPerHour int32 `json:"maxConnectionsPerHour,omitempty"`

	// RequireTLS sets REQUIRE X509, REQUIRE SSL, or none.
	// +kubebuilder:validation:Enum=x509;ssl;none
	// +kubebuilder:default:=none
	// +optional
	RequireTLS string `json:"requireTLS,omitempty"`

	// Comment is an optional user comment.
	// +optional
	Comment string `json:"comment,omitempty"`

	// Grants is the list of grants applied to the user. Targets are
	// installation-wide and have no default schema (unlike Database.spec.users).
	// +optional
	Grants []DatabaseUserGrant `json:"grants,omitempty"`

	// Revokes lists privileges to revoke after Grants are applied, each scoped to
	// an explicit target (typically a system schema such as "mysql.*"). Combined
	// with partial_revokes=ON on the cluster, this carves the system schemas out
	// of a broad "*.*" grant so a cross-database admin cannot modify the grant
	// tables and self-escalate. Revokes are applied after Grants, in order.
	// +optional
	Revokes []DatabaseUserRevoke `json:"revokes,omitempty"`

	// ReclaimPolicy controls what happens to the MySQL user when the
	// DatabaseUser object is deleted.
	// +kubebuilder:validation:Enum=delete;retain
	// +kubebuilder:default:=retain
	// +optional
	ReclaimPolicy string `json:"reclaimPolicy,omitempty"`
}

// DatabaseUserGrant describes a single MySQL GRANT statement.
type DatabaseUserGrant struct {
	// Privileges is the list of privileges (e.g. "SELECT", "INSERT", "ALL").
	// +kubebuilder:validation:MinItems=1
	Privileges []string `json:"privileges"`

	// On is the target of the grant (e.g. "*.*", "mydb.*", "mydb.mytable").
	// Defaults to "*.*".
	// +kubebuilder:default:=`*.*`
	// +optional
	On string `json:"on,omitempty"`
}

// DatabaseUserRevoke describes a single MySQL REVOKE statement, applied after
// the grants. The target is required: a revoke must name the schema (or schema
// object) it carves out, so it cannot accidentally strip a global grant.
type DatabaseUserRevoke struct {
	// Privileges is the list of privileges to revoke (e.g. "INSERT", "UPDATE").
	// +kubebuilder:validation:MinItems=1
	Privileges []string `json:"privileges"`

	// On is the target to revoke from (e.g. "mysql.*", "sys.*").
	// +kubebuilder:validation:Required
	On string `json:"on"`
}

// DatabaseUserStatus defines the observed state of DatabaseUser.
type DatabaseUserStatus struct {
	// Applied is true once the desired state has been reconciled.
	// +optional
	Applied *bool `json:"applied,omitempty"`

	// Message provides additional detail, typically an error.
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// PasswordSecretResourceVersion records the source Secret resourceVersion
	// last applied to MySQL, so the password is re-applied only when it changes.
	// +optional
	PasswordSecretResourceVersion string `json:"passwordSecretResourceVersion,omitempty"`

	// Conditions represent the latest observations of the user state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=myuser
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster.name`
// +kubebuilder:printcolumn:name="User",type=string,JSONPath=`.spec.name`
// +kubebuilder:printcolumn:name="Applied",type=boolean,JSONPath=`.status.applied`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DatabaseUser is the Schema for the databaseusers API.
type DatabaseUser struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of DatabaseUser
	// +required
	Spec DatabaseUserSpec `json:"spec"`

	// status defines the observed state of DatabaseUser
	// +optional
	Status DatabaseUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatabaseUserList contains a list of DatabaseUser.
type DatabaseUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabaseUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DatabaseUser{}, &DatabaseUserList{})
}
