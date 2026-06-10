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
	// ClusterImageCatalog based on the MySQL major version. Mutually exclusive
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

	// MaxStartDelay is the time in seconds allowed for an instance to start.
	// +kubebuilder:default:=3600
	// +optional
	MaxStartDelay int32 `json:"maxStartDelay,omitempty"`

	// MaxStopDelay is the time in seconds allowed for an instance to gracefully
	// shut down.
	// +kubebuilder:default:=1800
	// +optional
	MaxStopDelay int32 `json:"maxStopDelay,omitempty"`

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
}

// ImageCatalogRef references an ImageCatalog or ClusterImageCatalog entry to
// resolve a container image for a given major version.
type ImageCatalogRef struct {
	// TypedLocalObjectReference points to the (Cluster)ImageCatalog.
	corev1.TypedLocalObjectReference `json:",inline"`

	// Major is the MySQL major version to resolve in the catalog.
	// +kubebuilder:validation:Required
	Major int `json:"major"`
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
	// +optional
	Source string `json:"source,omitempty"`

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
}

// ManagedServices controls the services generated for the cluster.
type ManagedServices struct {
	// DisabledDefaultServices is the list of default services (rw, ro, r) to
	// disable.
	// +optional
	DisabledDefaultServices []ServiceSelectorType `json:"disabledDefaultServices,omitempty"`
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

	// LatestGeneratedNode is the serial of the latest generated instance.
	// +optional
	LatestGeneratedNode int `json:"latestGeneratedNode,omitempty"`

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

	// Certificates reports the status of the managed certificates.
	// +optional
	Certificates *CertificatesStatus `json:"certificates,omitempty"`

	// ObservedGeneration is the generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations of the cluster's
	// state.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
