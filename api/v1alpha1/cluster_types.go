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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Defaults used by the Cluster API.
const (
	// DefaultInstances is the default number of instances of a Cluster.
	DefaultInstances = 1

	// DefaultBinlogFormat is the default binary log format. ROW is required for
	// safe physical/logical replication and PITR.
	DefaultBinlogFormat = "ROW"

	// DefaultPrimaryUpdateStrategy is the default strategy for primary updates.
	DefaultPrimaryUpdateStrategy = PrimaryUpdateStrategyUnsupervised

	// DefaultPrimaryUpdateMethod is the default method for primary updates.
	DefaultPrimaryUpdateMethod = PrimaryUpdateMethodSwitchover

	// DefaultStartupDelay is the default maximum startup delay, in seconds.
	DefaultStartupDelay = 3600

	// DefaultShutdownDelay is the default maximum shutdown delay, in seconds.
	DefaultShutdownDelay = 1800

	// DefaultSmartShutdownTimeout is the default smart shutdown timeout, in seconds.
	DefaultSmartShutdownTimeout = 180

	// DefaultSwitchoverDelay is the default maximum switchover delay, in seconds.
	DefaultSwitchoverDelay = 3600
)

// PrimaryUpdateStrategy contains the strategy to follow when upgrading the
// primary server of the cluster as part of rolling updates.
// +kubebuilder:validation:Enum=unsupervised;supervised
type PrimaryUpdateStrategy string

const (
	// PrimaryUpdateStrategyUnsupervised means that the operator performs the
	// switchover/restart of the primary automatically.
	PrimaryUpdateStrategyUnsupervised PrimaryUpdateStrategy = "unsupervised"

	// PrimaryUpdateStrategySupervised means that the operator waits for the user
	// to manually trigger the primary update.
	PrimaryUpdateStrategySupervised PrimaryUpdateStrategy = "supervised"
)

// PrimaryUpdateMethod contains the method to use when upgrading the primary
// server of the cluster as part of rolling updates.
// +kubebuilder:validation:Enum=switchover;restart
type PrimaryUpdateMethod string

const (
	// PrimaryUpdateMethodSwitchover means the operator promotes a replica before
	// updating the former primary.
	PrimaryUpdateMethodSwitchover PrimaryUpdateMethod = "switchover"

	// PrimaryUpdateMethodRestart means the operator restarts the primary in
	// place.
	PrimaryUpdateMethodRestart PrimaryUpdateMethod = "restart"
)

// ClusterSpec defines the desired state of Cluster.
type ClusterSpec struct {
	// Description of this MySQL cluster.
	// +optional
	Description string `json:"description,omitempty"`

	// Metadata that will be inherited by all objects related to the Cluster.
	// +optional
	InheritedMetadata *EmbeddedObjectMetadata `json:"inheritedMetadata,omitempty"`

	// ImageName is the name of the Percona Server for MySQL container image to
	// use. Mutually exclusive with ImageCatalogRef.
	// +optional
	ImageName string `json:"imageName,omitempty"`

	// ImageCatalogRef resolves the image from an ImageCatalog or
	// ClusterImageCatalog based on the MySQL series. Mutually exclusive
	// with ImageName.
	// +optional
	ImageCatalogRef *ImageCatalogRef `json:"imageCatalogRef,omitempty"`

	// ImagePullPolicy is the policy used to pull the container image.
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets is the list of pull secrets used to pull the image.
	// +optional
	ImagePullSecrets []LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Instances is the number of MySQL instances (one primary + replicas).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=1
	// +optional
	Instances int `json:"instances,omitempty"`

	// MinSyncReplicas is the minimum number of semi-synchronous replicas that
	// must acknowledge a transaction before it is committed on the primary.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinSyncReplicas int `json:"minSyncReplicas,omitempty"`

	// MaxSyncReplicas is the maximum number of semi-synchronous replicas the
	// primary will wait for. Must be lower than the number of instances.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxSyncReplicas int `json:"maxSyncReplicas,omitempty"`

	// MySQL holds the engine configuration (my.cnf parameters, replication
	// options).
	// +optional
	MySQL MySQLConfiguration `json:"mysql,omitempty"`

	// Replication selects and tunes the replication topology (asynchronous /
	// semi-synchronous GTID replication, or quorum-based Group Replication). The
	// mode is immutable after creation; when omitted the cluster is async.
	// +optional
	Replication *ReplicationConfiguration `json:"replication,omitempty"`

	// Storage configuration for the instance data directory.
	// +kubebuilder:validation:Required
	Storage StorageConfiguration `json:"storage"`

	// BinlogStorage, when set, places the binary logs on a separate volume from
	// the data directory.
	// +optional
	BinlogStorage *StorageConfiguration `json:"binlogStorage,omitempty"`

	// Bootstrap describes how the cluster is initialised (fresh init or recovery
	// from a backup).
	// +optional
	Bootstrap *BootstrapConfiguration `json:"bootstrap,omitempty"`

	// RootPasswordSecret references a secret containing the password for the
	// MySQL root user. If not provided, a secret is generated.
	// +optional
	RootPasswordSecret *LocalObjectReference `json:"rootPasswordSecret,omitempty"`

	// EnableSuperuserAccess, when true, makes the root user reachable through the
	// generated/provided secret. Defaults to false.
	// +kubebuilder:default:=false
	// +optional
	EnableSuperuserAccess *bool `json:"enableSuperuserAccess,omitempty"`

	// Certificates configures the TLS/mTLS material used by the cluster.
	// +optional
	Certificates *CertificatesConfiguration `json:"certificates,omitempty"`

	// Resources describes the compute resource requirements of the instance
	// pods.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Affinity/anti-affinity rules for the instance pods.
	// +optional
	Affinity AffinityConfiguration `json:"affinity,omitempty"`

	// TopologySpreadConstraints describes how the instance pods should be spread
	// across topology domains.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// PriorityClassName for the instance pods.
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// SchedulerName to use for the instance pods.
	// +optional
	SchedulerName string `json:"schedulerName,omitempty"`

	// PrimaryUpdateStrategy controls whether the operator performs the primary
	// update automatically (unsupervised) or waits for the user (supervised).
	// +kubebuilder:default:=unsupervised
	// +optional
	PrimaryUpdateStrategy PrimaryUpdateStrategy `json:"primaryUpdateStrategy,omitempty"`

	// PrimaryUpdateMethod controls how the primary is updated: by switchover
	// (promoting a replica first) or by in-place restart.
	// +kubebuilder:default:=switchover
	// +optional
	PrimaryUpdateMethod PrimaryUpdateMethod `json:"primaryUpdateMethod,omitempty"`

	// InPlaceInstanceManagerUpdates, when true, rolls an operator upgrade out to
	// this cluster's instances by streaming the new instance-manager binary to each
	// Pod, which re-execs in place — no Pod restart and no switchover. When false
	// (the default) the operator instead deletes and recreates each Pod one at a
	// time (replicas first, primary last via switchover).
	// +optional
	InPlaceInstanceManagerUpdates bool `json:"inPlaceInstanceManagerUpdates,omitempty"`

	// Upgrade tunes MySQL server major-version upgrades.
	// +optional
	Upgrade *UpgradeConfiguration `json:"upgrade,omitempty"`

	// MaxStartDelay is the time in seconds allowed for an instance to start.
	// +kubebuilder:default:=3600
	// +optional
	MaxStartDelay int32 `json:"maxStartDelay,omitempty"`

	// MaxStopDelay is the time in seconds allowed for an instance to gracefully
	// shut down.
	// +kubebuilder:default:=1800
	// +optional
	MaxStopDelay int32 `json:"maxStopDelay,omitempty"`

	// SmartShutdownTimeout is the time in seconds reserved for a "smart"
	// (graceful) shutdown attempt before falling back to a "fast" shutdown.
	// Must be lower than maxStopDelay; the remaining time is used for the
	// fast/immediate fallback. Defaults to 180.
	// +kubebuilder:validation:Minimum=0
	// +optional
	SmartShutdownTimeout *int32 `json:"smartShutdownTimeout,omitempty"`

	// MaxSwitchoverDelay is the time in seconds allowed for a switchover to
	// complete before being considered failed.
	// +kubebuilder:default:=3600
	// +optional
	MaxSwitchoverDelay int32 `json:"maxSwitchoverDelay,omitempty"`

	// FailoverDelay is the amount of time in seconds the operator waits before
	// declaring an unreachable primary failed and triggering a failover.
	// +kubebuilder:default:=0
	// +optional
	FailoverDelay int32 `json:"failoverDelay,omitempty"`

	// EnablePrimaryLease, when true (default), makes the acting primary hold a
	// per-cluster Lease before accepting writes.
	// +kubebuilder:default:=true
	// +optional
	EnablePrimaryLease *bool `json:"enablePrimaryLease,omitempty"`

	// Backup configures continuous archiving and the object store target.
	// +optional
	Backup *BackupConfiguration `json:"backup,omitempty"`

	// Replica turns this cluster into a replica cluster that follows a source
	// defined in ExternalClusters.
	// +optional
	Replica *ReplicaClusterConfiguration `json:"replica,omitempty"`

	// ExternalClusters is the list of external clusters that can be used as a
	// replication source or a recovery origin.
	// +optional
	ExternalClusters []ExternalCluster `json:"externalClusters,omitempty"`

	// Managed describes the resources (roles, services) managed declaratively by
	// the operator.
	// +optional
	Managed *ManagedConfiguration `json:"managed,omitempty"`

	// Monitoring configuration.
	// +optional
	Monitoring *MonitoringConfiguration `json:"monitoring,omitempty"`

	// NodeMaintenanceWindow defines if the cluster is tolerant to node failures
	// during maintenance (e.g. PVC may be reused).
	// +optional
	NodeMaintenanceWindow *NodeMaintenanceWindow `json:"nodeMaintenanceWindow,omitempty"`

	// EnablePDB, when true (default), makes the operator create a
	// PodDisruptionBudget for the cluster.
	// +kubebuilder:default:=true
	// +optional
	EnablePDB *bool `json:"enablePDB,omitempty"`

	// EnableSwitchoverOnDrain, when true (default), makes the operator promote a
	// healthy replica via a planned switchover when the primary Pod is gracefully
	// terminated (e.g. a node drain or eviction), instead of waiting for the
	// primary to become unreachable and failing over. Falls back to failover when
	// no safe candidate exists or the handoff cannot complete in time.
	// +kubebuilder:default:=true
	// +optional
	EnableSwitchoverOnDrain *bool `json:"enableSwitchoverOnDrain,omitempty"`

	// ServiceAccountTemplate to use for the generated service account.
	// +optional
	ServiceAccountTemplate *ServiceAccountTemplate `json:"serviceAccountTemplate,omitempty"`

	// Env is a list of additional environment variables added to the instance
	// containers.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// EnvFrom is a list of sources to populate environment variables in the
	// instance containers.
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// PodSecurityContext applied to the instance pods.
	// +optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// SecurityContext applied to the instance containers.
	// +optional
	SecurityContext *corev1.SecurityContext `json:"securityContext,omitempty"`

	// LogLevel sets the operator-side log level for this cluster.
	// +kubebuilder:validation:Enum=error;warning;info;debug;trace
	// +kubebuilder:default:=info
	// +optional
	LogLevel string `json:"logLevel,omitempty"`
}

// MySQLConfiguration holds the MySQL engine configuration.
type MySQLConfiguration struct {
	// Parameters is a key/value map of my.cnf settings applied under the
	// [mysqld] section.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`

	// BinlogFormat is the binary log format. ROW is required for safe
	// replication and PITR and is the default.
	// +kubebuilder:validation:Enum=ROW;STATEMENT;MIXED
	// +kubebuilder:default:=ROW
	// +optional
	BinlogFormat string `json:"binlogFormat,omitempty"`

	// SemiSync configures semi-synchronous replication.
	// +optional
	SemiSync *SemiSyncConfiguration `json:"semiSync,omitempty"`

	// AdditionalConfigFiles are extra files dropped into the MySQL configuration
	// directory, keyed by file name.
	// +optional
	AdditionalConfigFiles map[string]string `json:"additionalConfigFiles,omitempty"`
}

// Data durability levels for semi-synchronous replication. They control how
// strictly the configured number of synchronous replicas is enforced when some
// replicas are unhealthy.
const (
	// DataDurabilityPreferred keeps the primary writable during a replica
	// outage by temporarily lowering the number of acknowledgements the primary
	// waits for to the number of healthy replicas. Availability is favoured over
	// strict durability. This is the default.
	DataDurabilityPreferred = "preferred"
	// DataDurabilityRequired strictly enforces minSyncReplicas: writes block
	// (until rpl_semi_sync_*_timeout) when fewer healthy replicas can
	// acknowledge. Durability is favoured over availability.
	DataDurabilityRequired = "required"
)

// SemiSyncConfiguration configures semi-synchronous replication.
type SemiSyncConfiguration struct {
	// Enabled turns on semi-synchronous replication.
	// +kubebuilder:default:=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// Timeout in milliseconds the primary waits for a replica acknowledgement
	// before falling back to asynchronous replication.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TimeoutMillis *int32 `json:"timeoutMillis,omitempty"`

	// DataDurability controls how strictly minSyncReplicas is enforced when
	// replicas are unhealthy. "preferred" (the default) keeps the primary
	// writable by self-healing the acknowledgement count down to the number of
	// healthy replicas; "required" leaves it fixed so writes block until enough
	// replicas acknowledge.
	// +kubebuilder:validation:Enum=preferred;required
	// +kubebuilder:default:=preferred
	// +optional
	DataDurability string `json:"dataDurability,omitempty"`
}

// ReplicationMode selects the cluster replication topology.
const (
	// ReplicationModeAsync is asynchronous / semi-synchronous GTID replication
	// with operator-driven primary election. This is the default.
	ReplicationModeAsync = "async"
	// ReplicationModeGroupReplication is quorum-based MySQL Group Replication in
	// single-primary mode. The group elects the primary; the operator observes.
	ReplicationModeGroupReplication = "groupReplication"
)

// ReplicationConfiguration selects and tunes the replication topology.
type ReplicationConfiguration struct {
	// Mode is the replication topology. Immutable after creation.
	// +kubebuilder:validation:Enum=async;groupReplication
	// +kubebuilder:default:=async
	// +optional
	Mode string `json:"mode,omitempty"`

	// GroupReplication tunes the group when Mode=groupReplication. It must not be
	// set unless Mode is groupReplication.
	// +optional
	GroupReplication *GroupReplicationConfiguration `json:"groupReplication,omitempty"`
}

// GroupReplicationConfiguration tunes MySQL Group Replication. All fields map to
// group_replication_* server variables; the operator owns the rest of the
// namespace (group name, addresses, seeds, start/bootstrap control).
type GroupReplicationConfiguration struct {
	// Consistency maps to group_replication_consistency, the transaction
	// consistency guarantee the group enforces.
	// +kubebuilder:validation:Enum=EVENTUAL;BEFORE_ON_PRIMARY_FAILOVER;BEFORE;AFTER;BEFORE_AND_AFTER
	// +kubebuilder:default:=BEFORE_ON_PRIMARY_FAILOVER
	// +optional
	Consistency string `json:"consistency,omitempty"`

	// ExitStateAction maps to group_replication_exit_state_action: what a member
	// does when it involuntarily leaves the group (e.g. on an unrecoverable error
	// or loss of quorum).
	// +kubebuilder:validation:Enum=READ_ONLY;OFFLINE_MODE;ABORT_SERVER
	// +kubebuilder:default:=READ_ONLY
	// +optional
	ExitStateAction string `json:"exitStateAction,omitempty"`

	// AutoRejoinTries maps to group_replication_autorejoin_tries: how many times a
	// member tries to automatically rejoin after being expelled before giving up.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default:=3
	// +optional
	AutoRejoinTries *int32 `json:"autoRejoinTries,omitempty"`

	// GroupName, when set, pins group_replication_group_name (a UUID). When unset
	// the operator generates one on first bootstrap and persists it to
	// status.groupReplication.groupName. Immutable once the group exists; a
	// changed group name fractures the group.
	// +optional
	GroupName string `json:"groupName,omitempty"`
}

// ImageCatalogRef references an ImageCatalog or ClusterImageCatalog entry to
// resolve a container image for a given MySQL series.
type ImageCatalogRef struct {
	// TypedLocalObjectReference points to the (Cluster)ImageCatalog.
	corev1.TypedLocalObjectReference `json:",inline"`

	// Series is the MySQL release series to resolve in the catalog, in
	// "major.minor" form (e.g. "8.0", "8.4", "9.0").
	// +kubebuilder:validation:Pattern=`^[0-9]+\.[0-9]+$`
	// +kubebuilder:validation:Required
	Series string `json:"series"`
}

// UpgradeConfiguration tunes MySQL server major-version upgrades.
type UpgradeConfiguration struct {
	// BackupBeforeUpgrade controls whether the operator takes a fresh backup
	// before starting a major-version upgrade and waits for it to succeed before
	// rolling any instance. Defaults to true. Set false to skip (e.g. when an
	// external backup process is in place). The data-dictionary upgrade is
	// irreversible, so the backup is the only rollback path.
	// +optional
	BackupBeforeUpgrade *bool `json:"backupBeforeUpgrade,omitempty"`
}

// BackupBeforeUpgradeEnabled reports the effective BackupBeforeUpgrade setting,
// defaulting to true when unset.
func (cluster *Cluster) BackupBeforeUpgradeEnabled() bool {
	if cluster.Spec.Upgrade == nil || cluster.Spec.Upgrade.BackupBeforeUpgrade == nil {
		return true
	}
	return *cluster.Spec.Upgrade.BackupBeforeUpgrade
}

// BootstrapConfiguration describes how the cluster is initialised.
type BootstrapConfiguration struct {
	// InitDB bootstraps a fresh, empty cluster.
	// +optional
	InitDB *BootstrapInitDB `json:"initdb,omitempty"`

	// Recovery bootstraps the cluster by restoring a physical backup.
	// +optional
	Recovery *BootstrapRecovery `json:"recovery,omitempty"`
}

// BootstrapInitDB configures a fresh cluster initialisation.
type BootstrapInitDB struct {
	// Database is the name of the application database to create.
	// +optional
	Database string `json:"database,omitempty"`

	// Owner is the name of the application user that owns the database.
	// +optional
	Owner string `json:"owner,omitempty"`

	// Secret references the credentials for the application user. If empty, a
	// secret is generated.
	// +optional
	Secret *LocalObjectReference `json:"secret,omitempty"`

	// PostInitSQL is a list of SQL statements run as root after the database is
	// created.
	// +optional
	PostInitSQL []string `json:"postInitSQL,omitempty"`

	// Encoding/charset of the application database.
	// +optional
	CharacterSet string `json:"characterSet,omitempty"`

	// Collation of the application database.
	// +optional
	Collation string `json:"collation,omitempty"`
}

// BootstrapRecovery configures bootstrapping from a physical backup.
type BootstrapRecovery struct {
	// Backup references a Backup object to recover from.
	// +optional
	Backup *LocalObjectReference `json:"backup,omitempty"`

	// Source is the name of an entry in ExternalClusters to recover from.
	// Mutually exclusive with Backup. The entry's objectStore holds the
	// backups and its name is the S3 key prefix to discover them under.
	// +optional
	Source string `json:"source,omitempty"`

	// BackupID narrows recovery to a specific base backup within the object
	// store. Only meaningful when Source is set; when empty, the latest
	// completed backup is selected. Ignored when Backup is set.
	// +optional
	BackupID string `json:"backupID,omitempty"`

	// RecoveryTarget describes the point-in-time recovery target. When omitted,
	// recovery proceeds to the latest available point.
	// +optional
	RecoveryTarget *RecoveryTarget `json:"recoveryTarget,omitempty"`
}

// RecoveryTarget allows to specify a point in time to recover to.
type RecoveryTarget struct {
	// TargetTime is an RFC3339 timestamp to recover to.
	// +optional
	TargetTime string `json:"targetTime,omitempty"`

	// TargetGTID is the GTID set to recover up to.
	// +optional
	TargetGTID string `json:"targetGTID,omitempty"`

	// TargetImmediate stops recovery as soon as a consistent state is reached.
	// +optional
	TargetImmediate *bool `json:"targetImmediate,omitempty"`
}

// BackupConfiguration describes the continuous archiving target for the
// cluster.
type BackupConfiguration struct {
	// ObjectStore is the S3-compatible destination for backups and binlog
	// archiving.
	// +optional
	ObjectStore *S3ObjectStore `json:"objectStore,omitempty"`

	// RetentionPolicy is a duration string (e.g. "30d", "8w") describing how
	// long to keep backups.
	// +optional
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]*[dwm]$`
	RetentionPolicy string `json:"retentionPolicy,omitempty"`

	// Target instance to take backups from, defaults to a standby if available.
	// +kubebuilder:validation:Enum=primary;prefer-standby
	// +kubebuilder:default:=prefer-standby
	// +optional
	Target BackupTarget `json:"target,omitempty"`

	// XtrabackupOptions are extra flags passed to xtrabackup.
	// +optional
	XtrabackupOptions []string `json:"xtrabackupOptions,omitempty"`

	// ContinuousArchiving configures continuous binary-log archiving to the
	// object store, the foundation for point-in-time recovery. Disabled by
	// default.
	// +optional
	ContinuousArchiving *ContinuousArchivingConfiguration `json:"continuousArchiving,omitempty"`
}

// ContinuousArchivingConfiguration configures continuous binary-log (binlog)
// archiving: the current primary's instance manager ships rotated binlog files
// to the object store so the cluster keeps a gapless, GTID-addressable change
// history.
type ContinuousArchivingConfiguration struct {
	// Enabled turns continuous binlog archiving on. Requires Backup.ObjectStore.
	// +kubebuilder:default:=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// TargetRPOSeconds bounds the recovery point objective: the primary forces a
	// binary-log rotation at least this often so a low-write cluster still
	// archives promptly. Defaults to 300 (5 minutes).
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:default:=300
	// +optional
	TargetRPOSeconds int32 `json:"targetRPOSeconds,omitempty"`

	// MaxBinlogSizeMB caps the active binary log before mysqld rotates it,
	// bounding the size-based RPO and per-object size. Defaults to 16 MiB.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=16
	// +optional
	MaxBinlogSizeMB int32 `json:"maxBinlogSizeMB,omitempty"`

	// BinlogExpireSeconds is the conservative backstop after which mysqld may
	// expire a binary log, applied under the active purge gate. Defaults to
	// 604800 (7 days).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default:=604800
	// +optional
	BinlogExpireSeconds int32 `json:"binlogExpireSeconds,omitempty"`
}

// BackupTarget describes which instance a backup is taken from.
// +kubebuilder:validation:Enum=primary;prefer-standby
type BackupTarget string

const (
	// BackupTargetPrimary takes backups from the primary instance.
	BackupTargetPrimary BackupTarget = "primary"

	// BackupTargetPreferStandby prefers a standby instance, falling back to the
	// primary if no standby is available.
	BackupTargetPreferStandby BackupTarget = "prefer-standby"
)

// ReplicaClusterConfiguration turns the cluster into a replica that follows an
// external source.
type ReplicaClusterConfiguration struct {
	// Enabled controls whether the cluster runs in replica mode.
	// +kubebuilder:default:=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Source is the name of the entry in ExternalClusters to replicate from.
	// +kubebuilder:validation:Required
	Source string `json:"source"`
}

// ExternalCluster describes a MySQL server external to this Cluster, used as a
// replication source or recovery origin.
type ExternalCluster struct {
	// Name of the external cluster, referenced by Replica.Source and
	// BootstrapRecovery.Source.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// ConnectionParameters is a key/value map of connection settings (host,
	// port, etc.).
	// +optional
	ConnectionParameters map[string]string `json:"connectionParameters,omitempty"`

	// Password references a secret key holding the password for the connection.
	// +optional
	Password *SecretKeySelector `json:"password,omitempty"`

	// SSLCert/SSLKey/SSLRootCert reference secret keys for mTLS to the source.
	// +optional
	SSLCert *SecretKeySelector `json:"sslCert,omitempty"`
	// +optional
	SSLKey *SecretKeySelector `json:"sslKey,omitempty"`
	// +optional
	SSLRootCert *SecretKeySelector `json:"sslRootCert,omitempty"`

	// ObjectStore allows recovering from a backup stored in an object store.
	// +optional
	ObjectStore *S3ObjectStore `json:"objectStore,omitempty"`
}

// ManagedConfiguration describes resources managed declaratively by the
// operator.
type ManagedConfiguration struct {
	// Services describes the services managed for the cluster.
	// +optional
	Services *ManagedServices `json:"services,omitempty"`

	// Roles is the list of MySQL users (roles) managed declaratively on the
	// primary instance.
	// +optional
	Roles []RoleConfiguration `json:"roles,omitempty"`
}

// RoleConfiguration describes a MySQL user managed declaratively against the
// primary instance.
type RoleConfiguration struct {
	// Name is the MySQL user name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=32
	Name string `json:"name"`

	// Host is the MySQL host part. Defaults to "%".
	// +kubebuilder:default:="%"
	// +optional
	Host string `json:"host,omitempty"`

	// Ensure controls whether the user should exist or be absent.
	// +kubebuilder:validation:Enum=present;absent
	// +kubebuilder:default:=present
	// +optional
	Ensure EnsureOption `json:"ensure,omitempty"`

	// PasswordSecret references a Secret key holding the user's password. When
	// unset, the operator generates a password and stores it in a Secret named
	// "<cluster>-<name>" with key "password".
	// +optional
	PasswordSecret *SecretKeySelector `json:"passwordSecret,omitempty"`

	// Superuser grants ALL PRIVILEGES on *.* WITH GRANT OPTION.
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

	// RequireTLS sets the connection TLS requirement: x509, ssl, or none.
	// +kubebuilder:validation:Enum=x509;ssl;none
	// +kubebuilder:default:=none
	// +optional
	RequireTLS string `json:"requireTLS,omitempty"`

	// Privileges are grants (global or per-database). Mutually exclusive with
	// Superuser.
	// +optional
	Privileges []RolePrivilege `json:"privileges,omitempty"`
}

// RolePrivilege is a grant of one or more privileges on a target.
type RolePrivilege struct {
	// Privileges is the grant list (SELECT, INSERT, ALL, etc.).
	// +kubebuilder:validation:MinItems=1
	Privileges []string `json:"privileges"`

	// On is the target (e.g. "*.*", "mydb.*"). Defaults to "*.*".
	// +optional
	On string `json:"on,omitempty"`
}

// ManagedServices controls the services generated for the cluster.
type ManagedServices struct {
	// DisabledDefaultServices is the list of default services (rw, ro, r) to
	// disable. The rw service cannot be disabled.
	// +optional
	DisabledDefaultServices []ServiceSelectorType `json:"disabledDefaultServices,omitempty"`

	// Template applies to the three default services (rw, ro, r). Fields set
	// here are merged into each default service. The operator still chooses the
	// selector and port based on the service role.
	// +optional
	Template *ServiceTemplateSpec `json:"template,omitempty"`

	// Additional is a list of additional managed services specified by the
	// user. Each entry declares a selectorType and an optional template to
	// overlay on top of the role-specific defaults.
	// +optional
	Additional []ManagedService `json:"additional,omitempty"`
}

// ManagedService describes a user-defined managed service.
type ManagedService struct {
	// SelectorType specifies the type of selectors the service will have.
	// Valid values are "rw", "r", and "ro".
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=rw;r;ro
	SelectorType ServiceSelectorType `json:"selectorType"`

	// Name is the name of the additional service. Must be unique among all
	// managed services and must not collide with the default service names
	// (<cluster>-rw, <cluster>-ro, <cluster>-r).
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// UpdateStrategy describes how the service template is reconciled with the
	// operator defaults.
	// +kubebuilder:default:="patch"
	// +optional
	UpdateStrategy ServiceUpdateStrategy `json:"updateStrategy,omitempty"`

	// ServiceTemplate is the template specification for the service. When
	// UpdateStrategy is "patch", fields here are merged on top of the
	// role-specific defaults. When "replace", they replace the defaults
	// entirely (except for selector and owner reference).
	// +optional
	ServiceTemplate ServiceTemplateSpec `json:"serviceTemplate,omitempty"`
}

// ServiceUpdateStrategy describes how the service template is reconciled.
// +kubebuilder:validation:Enum=patch;replace
type ServiceUpdateStrategy string

const (
	// ServiceUpdateStrategyPatch merges user fields onto operator defaults.
	ServiceUpdateStrategyPatch ServiceUpdateStrategy = "patch"
	// ServiceUpdateStrategyReplace replaces operator defaults with the user template.
	ServiceUpdateStrategyReplace ServiceUpdateStrategy = "replace"
)

// ServiceTemplateSpec describes the user-customisable parts of a managed
// Service.
type ServiceTemplateSpec struct {
	// Standard object's metadata applied to the Service.
	// +optional
	ObjectMeta *ObjectMetaTemplate `json:"metadata,omitempty"`

	// Specification of the desired behavior of the Service. The selector field
	// is operator-managed and cannot be overridden.
	// +optional
	Spec *ServiceTemplateServiceSpec `json:"spec,omitempty"`
}

// ObjectMetaTemplate carries the user-configurable metadata fields.
type ObjectMetaTemplate struct {
	// Labels added to the Service.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations added to the Service.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ServiceTemplateServiceSpec exposes the subset of corev1.ServiceSpec fields
// that users are allowed to customise. The operator retains control over the
// selector, ports, clusterIP, and owner reference.
type ServiceTemplateServiceSpec struct {
	// Type determines how the Service is exposed. Defaults to ClusterIP.
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer;ExternalName
	// +optional
	Type *corev1.ServiceType `json:"type,omitempty"`

	// ExternalTrafficPolicy describes how nodes distribute service traffic.
	// +optional
	ExternalTrafficPolicy *corev1.ServiceExternalTrafficPolicy `json:"externalTrafficPolicy,omitempty"`

	// SessionAffinity configures session affinity.
	// +optional
	SessionAffinity *corev1.ServiceAffinity `json:"sessionAffinity,omitempty"`

	// LoadBalancerSourceRanges restricts load balancer access.
	// +optional
	LoadBalancerSourceRanges []string `json:"loadBalancerSourceRanges,omitempty"`

	// ExternalName is the external reference for ExternalName services.
	// +optional
	ExternalName string `json:"externalName,omitempty"`

	// HealthCheckNodePort specifies the health check node port.
	// +optional
	HealthCheckNodePort *int32 `json:"healthCheckNodePort,omitempty"`
}

// ServiceSelectorType is the type of a default service.
// +kubebuilder:validation:Enum=rw;ro;r
type ServiceSelectorType string

const (
	// ServiceSelectorTypeRW selects the read-write (primary) service.
	ServiceSelectorTypeRW ServiceSelectorType = "rw"
	// ServiceSelectorTypeRO selects the read-only (replicas) service.
	ServiceSelectorTypeRO ServiceSelectorType = "ro"
	// ServiceSelectorTypeR selects the read (any instance) service.
	ServiceSelectorTypeR ServiceSelectorType = "r"
)

// MonitoringConfiguration configures cluster monitoring.
type MonitoringConfiguration struct {
	// EnablePodMonitor creates a PodMonitor for the cluster pods.
	// +kubebuilder:default:=false
	// +optional
	EnablePodMonitor bool `json:"enablePodMonitor,omitempty"`

	// CustomQueriesConfigMap references config maps holding custom monitoring
	// queries.
	// +optional
	CustomQueriesConfigMap []ConfigMapKeySelector `json:"customQueriesConfigMap,omitempty"`

	// CustomQueriesSecret references secrets holding custom monitoring queries.
	// +optional
	CustomQueriesSecret []SecretKeySelector `json:"customQueriesSecret,omitempty"`

	// DisableDefaultQueries disables the built-in monitoring query set.
	// +optional
	DisableDefaultQueries *bool `json:"disableDefaultQueries,omitempty"`

	// MetricsQueriesTTL is the minimum interval between executions of the
	// default and custom monitoring queries.
	// +optional
	MetricsQueriesTTL *metav1.Duration `json:"metricsQueriesTTL,omitempty"`

	// TLS configures TLS for the instance metrics endpoint.
	// +optional
	TLSConfig *ClusterMonitoringTLSConfig `json:"tls,omitempty"`
}

// ClusterMonitoringTLSConfig configures TLS for cluster metrics scraping.
type ClusterMonitoringTLSConfig struct {
	// Enabled serves metrics over TLS when true.
	// +kubebuilder:default:=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`
}

// NodeMaintenanceWindow contains information that the operator will use while
// upgrading the underlying nodes.
type NodeMaintenanceWindow struct {
	// ReusePVC, when true, reuses the existing PVC during a node maintenance
	// (instead of provisioning a fresh one).
	// +kubebuilder:default:=true
	// +optional
	ReusePVC *bool `json:"reusePVC,omitempty"`

	// InProgress signals that a node maintenance is in progress.
	// +kubebuilder:default:=false
	// +optional
	InProgress bool `json:"inProgress,omitempty"`
}

// ClusterStatus defines the observed state of Cluster.
type ClusterStatus struct {
	// Instances is the total number of instances reported.
	// +optional
	Instances int `json:"instances,omitempty"`

	// ReadyInstances is the number of ready instances.
	// +optional
	ReadyInstances int `json:"readyInstances,omitempty"`

	// InstanceNames is the list of instance (pod) names.
	// +optional
	InstanceNames []string `json:"instanceNames,omitempty"`

	// CurrentPrimary is the name of the instance currently acting as primary.
	// +optional
	CurrentPrimary string `json:"currentPrimary,omitempty"`

	// TargetPrimary is the name of the instance that should become primary (used
	// during switchover/failover).
	// +optional
	TargetPrimary string `json:"targetPrimary,omitempty"`

	// CurrentPrimaryTimestamp is when the current primary was elected.
	// +optional
	CurrentPrimaryTimestamp string `json:"currentPrimaryTimestamp,omitempty"`

	// TargetPrimaryTimestamp is when the current switchover request to
	// TargetPrimary was started. It bounds the switchover by spec.maxSwitchoverDelay.
	// +optional
	TargetPrimaryTimestamp string `json:"targetPrimaryTimestamp,omitempty"`

	// DivergedInstances are replicas whose executed GTID set is not contained in
	// the primary's (errant transactions). They cannot safely rejoin; their
	// in-Pod reconciler reads this list and refuses to self-configure as a
	// replica, leaving them read-only for an operator to resolve.
	// +optional
	DivergedInstances []string `json:"divergedInstances,omitempty"`

	// FencedInstances are instances the operator has fenced because their Pod
	// carries the fencing annotation. A fenced instance is pulled out of the
	// routing Services, kept read-only by its in-Pod reconciler (so it stops
	// archiving and refuses writes), and is excluded as a failover candidate.
	// Clearing the annotation removes it from this list and restores it.
	// +optional
	FencedInstances []string `json:"fencedInstances,omitempty"`

	// FailedInstances are instances whose Pod shows positive evidence of being
	// unable to run: a Failed Pod phase, or a container stuck in CrashLoopBackOff
	// after repeated restarts. Unlike a not-yet-ready instance (which is expected
	// during initial provisioning), a failed instance is a degradation regardless
	// of whether the cluster ever finished provisioning, so it is surfaced
	// independently of the phase. It is the cluster's "unhealthy" bucket.
	// +optional
	FailedInstances []string `json:"failedInstances,omitempty"`

	// ReplicationBrokenInstances are reachable replicas whose replication has
	// aborted with a recorded error — a stopped IO or SQL thread, e.g. a
	// duplicate-key conflict that halts replication. Unlike a diverged instance
	// (detected by comparing GTID sets and listed in DivergedInstances), this is
	// derived from the SQL-layer replication error the in-Pod reconciler reports,
	// so a replica that is Running but cannot replicate is surfaced as a
	// degradation rather than being mistaken for one still finishing provisioning.
	// +optional
	ReplicationBrokenInstances []string `json:"replicationBrokenInstances,omitempty"`

	// ResizingPVC lists the PVCs whose storage is currently being expanded — the
	// volume-level resize or the node-side filesystem grow has not yet completed.
	// For a backend that cannot expand a volume in use (storage.resizeInUseVolumes
	// false) a name lingers here until the operator recycles the owning Pod and the
	// fresh mount finishes the resize.
	// +optional
	ResizingPVC []string `json:"resizingPVC,omitempty"`

	// PrimaryFailingSince records when the current primary first became
	// unreachable. It is used to enforce spec.failoverDelay before an automatic
	// failover, and is cleared once the primary is healthy again.
	// +optional
	PrimaryFailingSince string `json:"primaryFailingSince,omitempty"`

	// LatestGeneratedNode is the serial of the latest generated instance.
	// +optional
	LatestGeneratedNode int `json:"latestGeneratedNode,omitempty"`

	// EstablishedAt records the first time the cluster reached full readiness
	// (every instance ready together), marking that it completed initial
	// provisioning at least once. It is sticky: once set it is never cleared, so a
	// later degradation cannot reset it. Its presence is what distinguishes a
	// cluster that is still being provisioned (a drop below readiness is expected)
	// from an established one (a drop below readiness is a degradation). It is
	// deliberately independent of Phase, which intermediate reconcile steps
	// re-stamp and which therefore cannot carry this fact reliably.
	// +optional
	EstablishedAt *metav1.Time `json:"establishedAt,omitempty"`

	// Phase is a high-level human-readable cluster phase.
	// +optional
	Phase string `json:"phase,omitempty"`

	// PhaseReason gives more detail about the current phase.
	// +optional
	PhaseReason string `json:"phaseReason,omitempty"`

	// Image is the resolved image currently in use.
	// +optional
	Image string `json:"image,omitempty"`

	// GTIDExecutedByInstance maps an instance name to its gtid_executed set.
	// +optional
	GTIDExecutedByInstance map[string]string `json:"gtidExecutedByInstance,omitempty"`

	// GTIDExecutedUpdatedAt records when GTIDExecutedByInstance was last
	// refreshed. Because gtid_executed advances on every write, the operator
	// throttles how often it persists the map; this timestamp marks the last
	// persisted snapshot.
	// +optional
	GTIDExecutedUpdatedAt *metav1.Time `json:"gtidExecutedUpdatedAt,omitempty"`

	// ExecutableHashByInstance maps an instance name to the SHA-256 hash of its
	// running instance manager binary, as reported by the in-Pod control API.
	// The operator uses it to detect stale instance managers when upgrading.
	// +optional
	ExecutableHashByInstance map[string]string `json:"executableHashByInstance,omitempty"`

	// OperatorExecutableHash is the SHA-256 hash of the running operator binary.
	// It is the target hash every instance manager should match after an upgrade.
	// +optional
	OperatorExecutableHash string `json:"operatorExecutableHash,omitempty"`

	// Certificates reports the status of the managed certificates.
	// +optional
	Certificates *CertificatesStatus `json:"certificates,omitempty"`

	// ContinuousArchiving reports the health of continuous binlog archiving when
	// it is enabled.
	// +optional
	ContinuousArchiving *ContinuousArchivingStatus `json:"continuousArchiving,omitempty"`

	// LastRetentionRunTime is when the operator last ran a backup-retention GC
	// pass against the object store. It throttles the periodic pass.
	// +optional
	LastRetentionRunTime *metav1.Time `json:"lastRetentionRunTime,omitempty"`

	// ObservedGeneration is the generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the cluster's
	// state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ManagedRolesStatus reports the reconciliation state of the declarative
	// managed roles.
	// +optional
	ManagedRolesStatus *ManagedRolesStatus `json:"managedRolesStatus,omitempty"`

	// GroupReplication reflects the live group membership and quorum, mirrored
	// from performance_schema.replication_group_members. Nil for async clusters.
	// The operator is the sole writer of this block.
	// +optional
	GroupReplication *GroupReplicationStatus `json:"groupReplication,omitempty"`
}

// GroupReplicationStatus is the operator's cross-validated view of the group,
// aggregated from every member's reported replication_group_members.
type GroupReplicationStatus struct {
	// GroupName is the pinned group_replication_group_name (a UUID). Sticky and
	// immutable once set: a changed group name fractures the group.
	// +optional
	GroupName string `json:"groupName,omitempty"`

	// Bootstrapped records that the group has been created at least once. Sticky:
	// false→true on first bootstrap, never auto-cleared. It makes group bootstrap
	// exactly-once across restarts and re-elections; re-arming it is the path to a
	// split-brain second group.
	// +optional
	Bootstrapped bool `json:"bootstrapped,omitempty"`

	// PrimaryMember is the pod name of the member the group elected PRIMARY.
	// status.currentPrimary is mirrored from this field.
	// +optional
	PrimaryMember string `json:"primaryMember,omitempty"`

	// Members is the per-member view of the group.
	// +optional
	Members []GroupMember `json:"members,omitempty"`

	// HasQuorum reports whether a majority of configured members is ONLINE and
	// reachable, i.e. the group can make progress.
	HasQuorum bool `json:"hasQuorum"`

	// ObservedViewMax is the largest group view size the operator has ever
	// observed for this group. Sticky: never decreases. Used as the quorum
	// denominator so that a group that loses members is recognised as
	// quorum-lost, while a bootstrapping group uses its current view size.
	// +optional
	ObservedViewMax int `json:"observedViewMax,omitempty"`

	// ObservedOnlineMax is the largest number of ONLINE members the operator
	// has ever observed for this group. Sticky, tracked alongside ViewMax.
	// +optional
	ObservedOnlineMax int `json:"observedOnlineMax,omitempty"`

	// ViewID is the current group view identifier. It changes on every membership
	// change and is one of the signals the operator reconciles on.
	// +optional
	ViewID string `json:"viewId,omitempty"`

	// CommunicationProtocol is the effective minimum-compatible protocol
	// reported by group_replication_get_communication_protocol(). It can differ
	// from the server version passed to the setter (MySQL 8.4 reports 8.0.27).
	// +optional
	CommunicationProtocol string `json:"communicationProtocol,omitempty"`

	// CommunicationProtocolTarget is the MySQL server version most recently
	// passed successfully to group_replication_set_communication_protocol(). It
	// is the idempotency marker for post-upgrade protocol finalization.
	// +optional
	CommunicationProtocolTarget string `json:"communicationProtocolTarget,omitempty"`
}

// GroupMember is one member's state within the group.
type GroupMember struct {
	// Instance is the pod name of the member.
	Instance string `json:"instance"`

	// State is the member's group state: ONLINE, RECOVERING, OFFLINE, ERROR or
	// UNREACHABLE.
	State string `json:"state"`

	// Role is the member's group role: PRIMARY or SECONDARY.
	Role string `json:"role"`

	// Reachable reports whether the group currently considers this member
	// reachable (not UNREACHABLE).
	Reachable bool `json:"reachable"`
}

// ManagedRolesStatus reports the reconciliation state of managed roles.
type ManagedRolesStatus struct {
	// ByStatus groups managed role names by their reconciliation status.
	// +optional
	ByStatus map[ManagedRoleStatus][]string `json:"byStatus,omitempty"`

	// CannotReconcile maps a role name to the reasons it could not be reconciled.
	// +optional
	CannotReconcile map[string][]string `json:"cannotReconcile,omitempty"`

	// PasswordStatus tracks the applied password Secret version per role.
	// +optional
	PasswordStatus map[string]RolePasswordState `json:"passwordStatus,omitempty"`
}

// ManagedRoleStatus is the reconciliation state of a single managed role.
type ManagedRoleStatus string

const (
	// ManagedRoleReconciled means the role matches its desired state.
	ManagedRoleReconciled ManagedRoleStatus = "reconciled"
	// ManagedRoleNotManaged means the MySQL user exists but is not managed.
	ManagedRoleNotManaged ManagedRoleStatus = "not-managed"
	// ManagedRolePendingReconciliation means the role still needs work.
	ManagedRolePendingReconciliation ManagedRoleStatus = "pending-reconciliation"
	// ManagedRoleReserved means the role name is reserved by the operator.
	ManagedRoleReserved ManagedRoleStatus = "reserved"
)

// RolePasswordState records the password Secret version last applied for a role.
type RolePasswordState struct {
	// SecretResourceVersion is the resourceVersion of the password Secret last
	// applied.
	// +optional
	SecretResourceVersion string `json:"secretResourceVersion,omitempty"`

	// LastApplied is when the password was last applied.
	// +optional
	LastApplied metav1.Time `json:"lastApplied,omitempty"`
}

// ContinuousArchivingStatus reports the health and frontier of continuous
// binary-log archiving.
type ContinuousArchivingStatus struct {
	// Enabled mirrors whether continuous archiving is configured on.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// LastArchivedBinlog is the most recent binary-log file shipped by the
	// current primary.
	// +optional
	LastArchivedBinlog string `json:"lastArchivedBinlog,omitempty"`

	// LastArchivedGTID is the last GTID covered by the archive.
	// +optional
	LastArchivedGTID string `json:"lastArchivedGTID,omitempty"`

	// LastArchivedTime is when the most recent file finished archiving.
	// +optional
	LastArchivedTime string `json:"lastArchivedTime,omitempty"`

	// PendingFiles is the number of rotated binary logs not yet archived
	// (archive lag). A growing value means the archiver is falling behind.
	// +optional
	PendingFiles int `json:"pendingFiles,omitempty"`

	// LastFailureReason and LastFailureTime record the most recent archiving
	// failure on the current primary, if any.
	// +optional
	LastFailureReason string `json:"lastFailureReason,omitempty"`
	// +optional
	LastFailureTime string `json:"lastFailureTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.instances,statuspath=.status.instances
// +kubebuilder:resource:scope=Namespaced,shortName=mysql;mysqlcluster,categories=all
// +kubebuilder:printcolumn:name="Instances",type=integer,JSONPath=`.status.instances`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyInstances`
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=`.status.currentPrimary`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Cluster is the Schema for the clusters API.
type Cluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of Cluster
	// +required
	Spec ClusterSpec `json:"spec"`

	// status defines the observed state of Cluster
	// +optional
	Status ClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterList contains a list of Cluster.
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
}
