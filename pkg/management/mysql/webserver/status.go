/*
Copyright 2026 The CNMySQL Authors.

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

	// Archiving reports continuous binlog archiving health; nil when archiving is
	// not enabled on this instance.
	Archiving *ArchivingStatus `json:"archiving,omitempty"`
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
