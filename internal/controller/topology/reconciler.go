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

// Package topology defines the boundary between the common Cluster reconciler
// and replication-topology-specific reconciliation.
package topology

import (
	"context"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	mysqlconfig "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
)

// Cluster status phases recorded on the Cluster CRD.
const (
	PhasePending        = "Pending"
	PhaseProvisioning   = "Provisioning"
	PhaseReady          = "Ready"
	PhaseBlocked        = "Blocked"
	PhaseSwitchover     = "Switchover"
	PhaseDegraded       = "Degraded"
	PhaseFailingOver    = "FailingOver"
	PhaseUpgrading      = "Upgrading"
	PhaseWaitingForUser = "WaitingForUser"
)

// TLS paths inside the instance container.
const (
	ClientCAPath  = "/etc/cloudnative-mysql/tls/client-ca"
	ServerTLSPath = "/etc/cloudnative-mysql/tls/server"
)

// PatchClusterStatus fetches the latest Cluster, applies mutate and patches the
// status sub-resource, then copies the result back into cluster.
func PatchClusterStatus(
	ctx context.Context,
	c client.Client,
	cluster *mysqlv1alpha1.Cluster,
	mutate func(*mysqlv1alpha1.ClusterStatus),
) error {
	latest := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	if err := c.Get(ctx, key, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	mutate(&latest.Status)
	if err := c.Status().Patch(ctx, latest, client.MergeFrom(before)); err != nil {
		return err
	}
	latest.Status.DeepCopyInto(&cluster.Status)
	return nil
}

// InstanceIdentity describes one instance's topology-specific Kubernetes
// identity. Common ServiceAccount and RoleBinding lifecycle stays in the
// Cluster reconciler.
type InstanceIdentity struct {
	InstanceName       string
	ServiceAccountName string
	Labels             map[string]string
}

// PrimaryLeaseStatus is the topology's view of the async split-brain guard.
// RetryAfter is meaningful when Held is true.
type PrimaryLeaseStatus struct {
	Held       bool
	RetryAfter time.Duration
}

// InstanceAvailability is the topology-neutral health view of one instance.
type InstanceAvailability struct {
	Ready bool
}

// AvailabilityState contains the observed state needed for topology-specific
// degraded-cluster adjustments.
type AvailabilityState struct {
	PrimaryName       string
	Instances         map[string]InstanceAvailability
	DivergedInstances []string
	FencedInstances   []string
}

// FailoverInstance contains the async failover policy inputs for one instance.
type FailoverInstance struct {
	Ready            bool
	Primary          bool
	Replica          bool
	Role             string
	IORunning        bool
	SQLRunning       bool
	GTID             string
	InPlaceUpgrading bool
}

// FailoverState is the topology-neutral observed state used to choose whether
// and where an async failover can proceed.
type FailoverState struct {
	PrimaryName   string
	InstanceNames []string
	Instances     map[string]FailoverInstance
	Fenced        []string
	Diverged      []string
}

// OperationPhase requests a common observed-status phase patch from the root
// Cluster reconciler.
type OperationPhase struct {
	Phase       string
	Reason      string
	Ready       bool
	Progressing bool
}

// FailoverRequest contains common orchestration inputs for a topology-specific
// failover pass.
type FailoverRequest struct {
	Instances         int
	Observed          FailoverState
	RetryInterval     time.Duration
	ProvisioningRetry time.Duration
}

// FailoverResult tells the root reconciler whether failover owned this pass.
type FailoverResult struct {
	Handled      bool
	RequeueAfter time.Duration
	Phase        *OperationPhase
}

// ObservationInput contains the instance-manager reports used to derive
// topology-specific cluster health.
type ObservationInput struct {
	PrimaryName       string
	InstanceNames     []string
	StatusByInstance  map[string]*webserver.Status
	GTIDByInstance    map[string]string
	ConfiguredMembers int
	ObservedViewMax   int
}

// Observation is the topology-specific portion of the operator's observed
// Cluster state.
type Observation struct {
	PrimaryName                string
	PrimaryAuthoritative       bool
	DivergedInstances          []string
	ReplicationBrokenInstances []string
	GroupReplication           *mysqlv1alpha1.GroupReplicationStatus
}

// ServerConfigInput identifies one instance and all stable member names needed
// for topology-specific MySQL configuration.
type ServerConfigInput struct {
	InstanceName string
	MemberNames  []string
}

// PodPolicy contains topology-specific instance-manager command behavior.
type PodPolicy struct {
	InitializeReplica bool
	InitDBArgs        []string
	RunArgs           []string
}

// SemiSyncControl adjusts the acknowledgement count on an async primary.
type SemiSyncControl interface {
	SetSemiSyncWaitForReplicaCount(
		ctx context.Context,
		cluster *mysqlv1alpha1.Cluster,
		instanceName string,
		count int,
	) error
}

// QuorumResult reports whether a quorum-sensitive action is blocked.
type QuorumResult struct {
	Blocked     bool
	Reason      string
	CurrentSize int
	Quorum      int
}

// Quorum-recovery action kinds carried by ForceQuorumRecovery.Action.
const (
	// QuorumRecoveryForceMembers re-forms a group that still has at least one
	// ONLINE survivor below quorum, via group_replication_force_members.
	QuorumRecoveryForceMembers = "force_members"
	// QuorumRecoveryRebootstrap re-creates the group from scratch when no member
	// survived ONLINE (total outage), by bootstrapping the most-advanced member.
	QuorumRecoveryRebootstrap = "rebootstrap"
)

// ForceQuorumRecovery describes a guided quorum-recovery action computed by the
// operator. Two kinds exist (see Action): a force_members re-form from the
// most-advanced ONLINE survivor, and — when the group has no ONLINE survivor at
// all — a total-outage re-bootstrap from the most-advanced reachable member.
// ForceMembers is only set for the force_members action.
type ForceQuorumRecovery struct {
	Action       string
	Survivor     string
	ForceMembers string
}

// Reconciler owns behavior that differs between replication topologies. The
// interface starts with RBAC and will grow as failover, switchover, status, and
// topology configuration move out of the common Cluster reconciler.
type Reconciler interface {
	Name() string
	EnsureConfigured(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error
	ConfigureServer(
		cluster *mysqlv1alpha1.Cluster,
		input ServerConfigInput,
		config *mysqlconfig.ServerConfig,
	)
	DonorAvailable(observed Observation, failover FailoverState) bool
	PodPolicy(cluster *mysqlv1alpha1.Cluster) PodPolicy
	PublishNotReadyAddresses(role mysqlv1alpha1.ServiceSelectorType) bool
	InstancePolicyRules(cluster *mysqlv1alpha1.Cluster) []rbacv1.PolicyRule
	ReconcileInstanceRBAC(
		ctx context.Context,
		cluster *mysqlv1alpha1.Cluster,
		identity InstanceIdentity,
	) error
	EnsurePrimaryLease(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error
	PrimaryLeaseStatus(
		ctx context.Context,
		cluster *mysqlv1alpha1.Cluster,
		holder string,
	) (PrimaryLeaseStatus, error)
	ReconcileAvailability(
		ctx context.Context,
		cluster *mysqlv1alpha1.Cluster,
		observed AvailabilityState,
	) error
	ReconcileFailover(
		ctx context.Context,
		cluster *mysqlv1alpha1.Cluster,
		request FailoverRequest,
	) (FailoverResult, error)
	ReconcileSwitchover(
		ctx context.Context,
		cluster *mysqlv1alpha1.Cluster,
		observed FailoverState,
	) (FailoverResult, error)
	Observe(input ObservationInput) Observation
	MergeStatus(cluster *mysqlv1alpha1.Cluster, observed Observation)
	ObservedFailover(before, after *mysqlv1alpha1.Cluster) (string, string, bool)

	// FenceQuorumGuard returns a blocking reason if fencing any of the named
	// instances would drop the group below quorum (GR mode), or nil (always
	// allowed, async). The fenceSet is the full set of instances whose fencing
	// annotation is already set, so the guard can subtract all pending leaves
	// from the online count, not just the one being checked.
	FenceQuorumGuard(cluster *mysqlv1alpha1.Cluster, fenceSet []string) *QuorumResult

	// PDBMaxUnavailable returns the maxUnavailable count for topology-specific
	// PDBs. For GR this is N - quorum; for async it carries the current split.
	PDBMaxUnavailable(cluster *mysqlv1alpha1.Cluster) (primary, replica intstr.IntOrString)

	// ScaleDownQuorumGuard returns a blocking reason if removing instanceName
	// (typically the highest-ordinal member) would drop below quorum.
	ScaleDownQuorumGuard(cluster *mysqlv1alpha1.Cluster, instanceName string) *QuorumResult

	// ComputeForceQuorumRecovery computes the safe survivor for an opt-in
	// quorum-recovery action, using each instance's gtid_executed to pick the
	// most-advanced member. It returns nil when recovery is unprovably safe.
	ComputeForceQuorumRecovery(cluster *mysqlv1alpha1.Cluster, gtidByInstance map[string]string) *ForceQuorumRecovery
}
