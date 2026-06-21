/*
Copyright 2026 The CloudNative MySQL Authors.

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

package webserver

// Role is the replication role reported by an instance.
type Role string

const (
	// RolePrimary is a read-write source instance.
	RolePrimary Role = "primary"
	// RoleReplica is a read-only replica instance.
	RoleReplica Role = "replica"
	// RoleUnknown is reported when the role cannot be determined.
	RoleUnknown Role = "unknown"
)

// Status is the JSON document the operator reads from an instance to drive
// reconciliation, switchover and failover decisions.
type Status struct {
	// InstanceName is the pod/instance name.
	InstanceName string `json:"instanceName"`
	// Role is the current replication role.
	Role Role `json:"role"`
	// Version is the mysqld server version.
	Version string `json:"version,omitempty"`
	// IsReady reflects whether the instance is ready to serve traffic.
	IsReady bool `json:"isReady"`
	// ReadOnly and SuperReadOnly mirror the server variables.
	ReadOnly      bool `json:"readOnly"`
	SuperReadOnly bool `json:"superReadOnly"`

	// GTIDExecuted and GTIDPurged are the gtid_executed / gtid_purged sets.
	GTIDExecuted string `json:"gtidExecuted,omitempty"`
	GTIDPurged   string `json:"gtidPurged,omitempty"`

	// Replication holds replica-side details; nil on a primary.
	Replication *ReplicationStatus `json:"replication,omitempty"`

	// SemiSync reports semi-synchronous replication state.
	SemiSync SemiSyncStatus `json:"semiSync"`

	// UptimeSeconds is the mysqld uptime in seconds.
	UptimeSeconds int64 `json:"uptimeSeconds,omitempty"`

	// ExecutableHash is the SHA-256 of the running instance manager binary.
	ExecutableHash string `json:"executableHash,omitempty"`

	// InPlaceUpgrading is true while an in-place manager re-exec is in flight,
	// and briefly after the new image adopts mysqld. The operator extends the
	// failover grace period when the primary reports this, so a transiently
	// unreachable control API during the re-exec window does not trigger a
	// spurious failover.
	InPlaceUpgrading bool `json:"inPlaceUpgrading,omitempty"`

	// Archiving reports continuous binlog archiving health; nil when archiving is
	// not enabled on this instance.
	Archiving *ArchivingStatus `json:"archiving,omitempty"`

	// GroupReplication reports this member's Group Replication state and its view
	// of the whole group; nil for async clusters or when the plugin is not
	// active. The operator aggregates each member's view in observe() exactly as
	// it does Replication, and cross-validates the group primary across the ONLINE
	// majority.
	GroupReplication *GroupReplicationMemberStatus `json:"groupReplication,omitempty"`
}

// GroupReplicationMemberStatus is one instance manager's Group Replication
// report: this member's own state plus its view of every member, read from
// performance_schema.replication_group_members and related tables.
type GroupReplicationMemberStatus struct {
	// MemberID is this member's server_uuid, the key the group uses to identify
	// it in replication_group_members.
	MemberID string `json:"memberId,omitempty"`
	// State is this member's group state: ONLINE, RECOVERING, OFFLINE, ERROR or
	// UNREACHABLE.
	State string `json:"state,omitempty"`
	// Role is this member's group role: PRIMARY or SECONDARY.
	Role string `json:"role,omitempty"`
	// GroupName is the active group_replication_group_name as this member sees it.
	GroupName string `json:"groupName,omitempty"`
	// ViewID is the current group view identifier; it changes on every membership
	// change and is one of the operator's reconcile signals.
	ViewID string `json:"viewId,omitempty"`
	// PrimaryMemberID is the server_uuid the group currently considers PRIMARY.
	PrimaryMemberID string `json:"primaryMemberId,omitempty"`
	// Members is this member's view of the whole group.
	Members []GroupReplicationMember `json:"members,omitempty"`
}

// GroupReplicationMember is one row of a member's view of the group.
type GroupReplicationMember struct {
	// MemberID is the member's server_uuid.
	MemberID string `json:"memberId,omitempty"`
	// Host and Port are the member's reported address.
	Host string `json:"host,omitempty"`
	Port int    `json:"port,omitempty"`
	// State is the member's group state (ONLINE/RECOVERING/OFFLINE/ERROR/UNREACHABLE).
	State string `json:"state,omitempty"`
	// Role is the member's group role (PRIMARY/SECONDARY).
	Role string `json:"role,omitempty"`
}

// ArchivingStatus reports the in-Pod continuous binlog archiver's frontier and
// health, which the operator mirrors into Cluster.status.
type ArchivingStatus struct {
	// Active is true while this instance is the writable primary and archiving.
	Active bool `json:"active"`
	// LastArchivedBinlog/GTID/Time describe the archive frontier.
	LastArchivedBinlog string `json:"lastArchivedBinlog,omitempty"`
	LastArchivedGTID   string `json:"lastArchivedGTID,omitempty"`
	LastArchivedTime   string `json:"lastArchivedTime,omitempty"`
	// PendingFiles is the number of rotated logs not yet shipped (archive lag).
	PendingFiles int `json:"pendingFiles,omitempty"`
	// LastError and LastErrorTime record the most recent archiving failure.
	LastError     string `json:"lastError,omitempty"`
	LastErrorTime string `json:"lastErrorTime,omitempty"`
}

// ReplicationStatus captures the replica-side replication state, derived from
// SHOW REPLICA/SLAVE STATUS.
type ReplicationStatus struct {
	// SourceHost is the configured replication source.
	SourceHost string `json:"sourceHost,omitempty"`
	// IORunning and SQLRunning reflect the replication thread states.
	IORunning  bool `json:"ioRunning"`
	SQLRunning bool `json:"sqlRunning"`
	// SecondsBehindSource is the replication lag; nil when not replicating.
	SecondsBehindSource *int64 `json:"secondsBehindSource,omitempty"`
	// LastError holds the last replication error, if any.
	LastError string `json:"lastError,omitempty"`
	// RetrievedGTIDSet and ExecutedGTIDSet from the replica's perspective.
	RetrievedGTIDSet string `json:"retrievedGtidSet,omitempty"`
}

// SemiSyncStatus reports the semi-synchronous replication state.
type SemiSyncStatus struct {
	SourceEnabled  bool `json:"sourceEnabled"`
	ReplicaEnabled bool `json:"replicaEnabled"`
}
