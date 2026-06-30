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

package controller

import (
	"context"
	"io"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

const (
	// defaultInstanceImage is the published slim instance image, built and pushed
	// from the separate containers repo to GHCR. See docs/src/instance-images.md.
	defaultInstanceImage = "ghcr.io/cnmsql/cnmsql-instance:8.0"

	clusterLabel           = mysqlv1alpha1.ClusterLabelName
	podMonitorClusterLabel = "cnmsql.co/cluster"
	instanceLabel          = "mysql.cnmsql.co/instance"
	roleLabel              = "mysql.cnmsql.co/role"
	// routableLabel gates membership of the rw/ro/r routing Services. Every
	// instance Pod carries it set to "true"; fencing flips it to "false" so the
	// fenced Pod is dropped from all routing Services (Service selectors are
	// equality-only, so a positive gate is the way to exclude a member).
	routableLabel = "mysql.cnmsql.co/routable"

	rolePrimary = "primary"
	roleReplica = "replica"

	routableTrue  = "true"
	routableFalse = "false"

	// fencingAnnotation, when set to "true" on an instance Pod, fences that
	// instance: it is removed from routing, kept read-only, and not eligible as a
	// failover candidate. Clearing it restores the instance.
	fencingAnnotation = "cnmsql.cnmsql.co/fencing"
	// restartAnnotation, when set to an RFC3339 timestamp on the Cluster, triggers
	// a rolling restart of every instance: its value is folded into the Pod
	// template hash, so bumping it rolls the Pods one at a time (gated on the
	// previous instance becoming Ready) without otherwise changing the spec.
	restartAnnotation = "cnmsql.cnmsql.co/restart"
	// reloadAnnotation, when set to an RFC3339 timestamp on the Cluster, requests
	// that dynamic my.cnf parameters be re-applied to the running mysqld via the
	// instance manager control API, without restarting the process.
	reloadAnnotation = "cnmsql.cnmsql.co/reload"
	// reinitAnnotation, when set on the Cluster to a comma-separated list of
	// instance names, requests that each listed instance be re-initialised from
	// scratch: the operator deletes its Pod and PVC and lets the normal reconcile
	// recreate them empty, so the bootstrap init-container re-clones a fresh copy
	// from a backup and rejoins replication. It is the remediation for a diverged
	// or irrecoverably broken replica (MySQL has no pg_rewind), keeping the
	// instance's identity (name/ordinal, hence server_id) while discarding its
	// data. It lives on the Cluster, not the Pod, so the request survives the Pod
	// being deleted mid-flight; the operator clears each name once its teardown
	// completes. The current primary is never re-initialised this way.
	reinitAnnotation = "cnmsql.cnmsql.co/reinit"
	// reloadAppliedAnnotation records, on each instance Pod, the reload token that
	// was last applied to that instance. The reconciler compares it to the
	// Cluster's reloadAnnotation to decide whether a reload is still pending,
	// making the SET GLOBAL pass idempotent without a CRD status change.
	reloadAppliedAnnotation = "cnmsql.cnmsql.co/reload-applied"
	// unreachableSinceAnnotation records, on an instance Pod, the RFC3339 time the
	// operator first failed to reach that instance's control endpoint. Once an
	// established replica has been unreachable for deRouteGracePeriod it is pulled
	// out of the ro/r routing Services (routable=false) so reads stop being served
	// from a partitioned node; the annotation and routing are restored as soon as
	// the instance is reachable again. The grace period absorbs transient blips so
	// a single failed poll does not churn Service endpoints.
	unreachableSinceAnnotation = "cnmsql.cnmsql.co/unreachable-since"
	// forceQuorumRecoveryAnnotation, when set to "yes" on the Cluster, triggers
	// a guarded quorum recovery for a Group Replication cluster that has lost
	// quorum. The operator computes the safe survivor set, stamps the survivor
	// Pod with force-quorum-members, and clears this annotation. Recovery is
	// never automatic — it gate-checks that no quorum exists and a safe survivor
	// is provable, and refuses otherwise.
	forceQuorumRecoveryAnnotation = "cnmsql.cnmsql.co/force-quorum-recovery"
	// forceQuorumMembersAnnotation, when set on an instance Pod to a
	// comma-separated list of XCom addresses, instructs the in-Pod reconciler to
	// execute group_replication_force_members with that address set.
	forceQuorumMembersAnnotation = "cnmsql.cnmsql.co/force-quorum-members"
	// forceGroupRebootstrapAnnotation, when set to "yes" on an instance Pod,
	// instructs the in-Pod reconciler to re-bootstrap the group from that member
	// after a total outage (no member survived ONLINE). It is the operator's
	// guarded signal that this member holds every committed transaction.
	forceGroupRebootstrapAnnotation = "cnmsql.cnmsql.co/force-group-rebootstrap"

	// groupObservationAnnotation is published by the in-Pod reconciler as a
	// doorbell on the instance Pod whenever its locally observed Group Replication
	// snapshot changes. The operator must preserve it across ensurePod patches so
	// it is not lost between in-Pod manager updates.
	groupObservationAnnotation = "mysql.cnmsql.co/gr-observed"

	configMapAnnotation       = "cnmsql.cnmsql.co/config-map"
	configHashAnnotation      = "cnmsql.cnmsql.co/config-hash"
	podTemplateHashAnnotation = "cnmsql.cnmsql.co/pod-template-hash"

	conditionReady               = "Ready"
	conditionProgressing         = "Progressing"
	conditionContinuousArchiving = "ContinuousArchiving"
	conditionStoragePressure     = "StoragePressure"

	eventFailoverObserved        = "FailoverObserved"
	eventStoragePressure         = "StoragePressure"
	eventStoragePressureResolved = "StoragePressureResolved"

	// storagePressurePercent is the data-volume usage (percent of capacity) at or
	// above which an instance is considered under storage pressure. It is a fixed
	// default for now; promote to a storage.* field if clusters need to tune it.
	storagePressurePercent = 85

	dataDir       = "/var/lib/mysql"
	socketPath    = "/var/run/mysqld/mysqld.sock"
	configPath    = "/etc/mysql/my.cnf"
	joinBackupDir = "/backup"

	replicationUser = "cnmsql_repl"
	backupUser      = "cnmsql_backup"
	controlUser     = "cnmsql_control"
	metricsUser     = "cnmsql_metrics"
	mysqldBinary    = "/usr/sbin/mysqld"

	// switchoverHandoffSeconds bounds how long a draining primary's preStop hook
	// blocks waiting for the operator to switch its role away. A switchover
	// completes in seconds, so this is a small budget rather than the full
	// MaxStopDelay: it caps the worst case (a teardown, where no demotion ever
	// comes) instead of letting the Pod hang for the whole stop delay.
	switchoverHandoffSeconds = int64(30)

	// provisioningRequeue paces reconciles while the instance is still coming up.
	provisioningRequeue = 10 * time.Second
	// readyResync re-polls the instance manager once the cluster is ready so the
	// reported status (GTID, role, readiness) does not go stale between events.
	readyResync = 30 * time.Second
	// deRouteGracePeriod bounds how long an established replica may be unreachable
	// before the operator pulls it out of the ro/r routing Services.
	deRouteGracePeriod = 30 * time.Second
)

// InstanceControlClient reads instance state over the mTLS control API. Role
// changes are driven by each instance's in-Pod reconciler (CNPG pull-model), so
// the operator only needs to read status.
type InstanceControlClient interface {
	Status(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*webserver.Status, error)

	ListUsers(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*user.ListUsersResponse, error)
	CreateUser(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.CreateUserRequest) error
	AlterUser(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.AlterUserRequest) error
	DropUser(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.DropUserRequest) error

	CreateDatabase(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.CreateDatabaseRequest) error
	DropDatabase(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req user.DropDatabaseRequest) error
	ListDatabases(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*user.ListDatabasesResponse, error)

	SetSemiSyncWaitForReplicaCount(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, count int) error

	Reload(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, req webserver.ReloadRequest) (*webserver.ReloadResponse, error)

	// UpgradeInstanceManager streams a new instance-manager binary to the named
	// instance's control API, tagged with expectedHash, so the manager validates
	// and re-execs it in place without restarting mysqld.
	UpgradeInstanceManager(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string, binary io.Reader, expectedHash string) error
	// SetAsPrimary performs a planned Group Replication primary change on the
	// named instance, designating the member with the given server_uuid.
	SetAsPrimary(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName, memberUUID string) error
	// SetGroupCommunicationProtocol raises the GR communication protocol after a
	// completed major-version upgrade.
	SetGroupCommunicationProtocol(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName, targetVersion string) error
}

// ClusterReconciler reconciles a Cluster object.
type ClusterReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	ControlClient InstanceControlClient
	// APIReader bypasses the controller-runtime cache for narrow reads that
	// should not start informers, such as checking namespace deletion state.
	APIReader client.Reader
	// OperatorImageName is the image name the operator controller runs as. It is
	// injected into instance pods as the bootstrap-controller init container so the
	// operator and instance manager binaries are always the same version.
	OperatorImageName string
	// OperatorExecutableHash is the SHA-256 of the running operator binary. It is
	// compared against each instance's reported executable hash to detect stale
	// instance managers that need upgrade.
	OperatorExecutableHash string
	// openOperatorBinary returns a reader over the operator's own manager binary,
	// streamed to instances during an in-place upgrade. It defaults to opening
	// os.Executable() and is overridable in tests.
	openOperatorBinary func() (io.ReadCloser, error)
	// podMonitorAvailable records whether the Prometheus Operator PodMonitor CRD
	// is installed. PodMonitor support is fully opt-in: when the CRD is absent we
	// neither watch nor reconcile PodMonitors, so the operator runs without the
	// Prometheus Operator present. Set in SetupWithManager.
	podMonitorAvailable bool
}

// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=imagecatalogs,verbs=get;list;watch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusterimagecatalogs,verbs=get;list;watch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=backups,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;pods;pods/status;persistentvolumeclaims;secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile creates the first primary instance for a fresh single-instance
// Cluster. Replicas, traffic services, backup and failover are intentionally
// deferred to later milestones.
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("Reconciling Cluster")

	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	cluster.SetDefaults()

	// Nothing to do while the Cluster is being deleted: owned resources are
	// garbage-collected via owner references.
	if !cluster.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if reason := unsupportedReason(cluster); reason != "" {
		log.Info("Cluster shape is not supported by M3", "reason", reason)
		return ctrl.Result{}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       topology.PhaseBlocked,
			PhaseReason: reason,
			Ready:       false,
			Progressing: false,
		})
	}

	r.warnDeprecatedParameters(cluster)

	plan, err := r.buildPlan(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       topology.PhaseBlocked,
			PhaseReason: err.Error(),
			Ready:       false,
			Progressing: false,
		})
	}

	// Elect the bootstrap primary by pointing targetPrimary at the first
	// instance. From here on, role is driven by each instance's in-Pod
	// reconciler: it promotes itself when it is the target and follows the
	// current primary otherwise.
	if cluster.Status.TargetPrimary == "" {
		bootstrapPrimary := instanceName(cluster, 1)
		log.Info("Electing bootstrap primary", "primary", bootstrapPrimary)
		if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
			s.TargetPrimary = bootstrapPrimary
			s.TargetPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.topologyReconciler(cluster).EnsureConfigured(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	if result, err, handled := r.ensureInfrastructure(ctx, cluster, plan); handled {
		return result, err
	}

	certsReady, err := r.certSecretsReady(ctx, cluster, plan)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !certsReady {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       topology.PhaseProvisioning,
			PhaseReason: "Waiting for cert-manager certificates",
			Ready:       false,
			Progressing: true,
			Plan:        plan,
		})
	}

	// Guard against a fresh cluster adopting (and overwriting) an object-store
	// destination that already holds another cluster's backups. Block before any
	// instance is provisioned so the primary never archives over existing data.
	if result, err, handled := r.handleBackupCheck(ctx, cluster, plan); handled {
		return result, err
	}

	// For a point-in-time recovery, fail fast with a clear condition when the
	// target is obviously unsatisfiable (e.g. a targetGTID beyond the archive)
	// rather than provisioning a primary whose init container will CrashLoop.
	if result, err, handled := r.handleRecoveryCheck(ctx, cluster, plan); handled {
		return result, err
	}

	// Remove replicas above the desired count (highest ordinal first), then
	// observe surviving instances. Observation runs before provisioning so
	// that when the current primary Pod is gone, the operator can select a
	// failover candidate and update targetPrimary before reconcileInstances
	// recreates the Pod. Otherwise the recreated Pod re-establishes itself
	// as primary (targetPrimary still points to it from the bootstrap) and
	// failover never fires.
	if err := r.scaleDownReplicas(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	observed, err := r.observe(ctx, cluster, plan)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Under GR, refuse to fence a member when doing so would break quorum.
	// Remove the instance from the fenced set and surface Blocked instead
	// so the in-Pod reconciler never executes STOP GROUP_REPLICATION.
	if blockedReason := r.checkFenceQuorumGuard(ctx, cluster, &observed); blockedReason != "" {
		return ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, observed)
	}
	// Guarded quorum recovery is opt-in via annotation. The operator computes
	// the safe survivor set, re-arms bootstrap, and lets the designated member
	// re-form the group. Only runs when quorum is provably lost.
	if result, err, handled := r.handleQuorumRecovery(ctx, cluster, observed); handled {
		return result, err
	}
	// When the primary Pod is gracefully terminating (e.g. a node drain or
	// eviction) and the cluster is otherwise healthy, prefer a planned switchover
	// to a GTID-safe replica over waiting for the primary to vanish and failing
	// over. Best-effort: when no safe candidate exists this is a no-op and the
	// failover path below handles the primary once it becomes unreachable.
	if result, err, handled := r.reconcileDrainSwitchover(ctx, cluster, observed); handled {
		return result, err
	}
	// An unreachable primary takes precedence over a manual switchover: drive
	// automatic failover (bounded by spec.failoverDelay) before anything else.
	failoverHandled, failoverResult, err := r.reconcileFailover(ctx, cluster, plan, observed)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failoverHandled {
		return failoverResult, nil
	}
	// Before rolling a MySQL major-version upgrade, hold until a pre-upgrade
	// backup has completed (the data-dictionary upgrade is irreversible). No-op
	// unless a major upgrade is pending and backupBeforeUpgrade is enabled.
	if result, err, handled := r.reconcileUpgradeBackupGate(ctx, cluster, plan, observed); handled {
		return result, err
	}
	provisioned, err := r.reconcileInstances(ctx, cluster, plan, observed)
	if err != nil {
		return ctrl.Result{}, err
	}
	switched, err := r.reconcileSwitchover(ctx, cluster, observed)
	if err != nil {
		return ctrl.Result{}, err
	}
	if switched {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
	}
	// Keep rw/ro/r routing in step with the current primary (set by whichever
	// instance promoted itself).
	if err := r.reconcileRoleLabels(ctx, cluster, observed); err != nil {
		return ctrl.Result{}, err
	}
	// Pull fenced instances out of routing (routable=false) and restore unfenced
	// ones, keeping the routing Services in step with the fencing annotations.
	if err := r.reconcileFencing(ctx, cluster, observed); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.patchStatus(ctx, cluster, observed); err != nil {
		return ctrl.Result{}, err
	}
	if !provisioned {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
	}
	// Finish operator upgrades before finalizing the Group Replication
	// communication protocol for a completed MySQL major-version upgrade.
	// Run only after the cluster is fully provisioned so the observation
	// (especially instance-manager hashes) reflects the current state.
	if result, err, handled := r.reconcileUpgradeSteps(ctx, cluster, plan, observed); handled {
		return result, err
	}
	r.reconcileAvailability(ctx, cluster, observed)
	if !observed.Ready {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
	}
	r.reconcileSteadyState(ctx, cluster)
	// Keep re-polling the instance managers so status (GTID, roles, readiness)
	// stays fresh even when no Kubernetes event triggers a reconcile.
	return ctrl.Result{RequeueAfter: readyResync}, nil
}

// ensureInfrastructure provisions the supporting resources a Cluster needs
// before any instance Pod is created: credentials, RBAC, the primary lease,
// certificates, routing Services, the PodMonitor and the PDB. The returned bool
// is true when the caller should stop reconciliation (an error occurred, or the
// cluster is blocked waiting on a valid certificate).
func (r *ClusterReconciler) ensureInfrastructure(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (ctrl.Result, error, bool) {
	if err := r.ensureCredentials(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err, true
	}
	if err := r.ensureInstanceRBAC(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err, true
	}
	if err := r.topologyReconciler(cluster).EnsurePrimaryLease(ctx, cluster); err != nil {
		return ctrl.Result{}, err, true
	}
	if ok, result, err := r.blockOnInvalidCertificate(ctx, cluster, plan); ok {
		return result, err, true
	}
	if err := r.ensureCertificates(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err, true
	}
	if err := r.ensureDefaultServices(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err, true
	}
	if err := r.reconcilePodMonitor(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err, true
	}
	if err := r.reconcilePDB(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err, true
	}
	return ctrl.Result{}, nil, false
}

// handleBackupCheck guards a fresh cluster from adopting a non-empty backup
// destination. Returns a result to return and a boolean indicating the caller
// should stop reconciliation.
func (r *ClusterReconciler) handleBackupCheck(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (ctrl.Result, error, bool) {
	log := logf.FromContext(ctx)
	check := r.checkBackupDestination(ctx, cluster)
	if check.Retry != nil {
		log.Info("Could not verify backup destination, will retry", "error", check.Retry.Error())
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       topology.PhaseProvisioning,
			PhaseReason: "Verifying backup destination is empty",
			Ready:       false,
			Progressing: true,
			Plan:        plan,
		}), true
	}
	if check.Blocked != "" {
		log.Info("Blocking cluster: backup destination is not empty", "reason", check.Blocked)
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "BackupDestinationNotEmpty", check.Blocked)
		return ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       topology.PhaseBlocked,
			PhaseReason: check.Blocked,
			Ready:       false,
			Progressing: false,
			Plan:        plan,
		}), true
	}
	return ctrl.Result{}, nil, false
}

// handleRecoveryCheck validates that a point-in-time recovery target is
// satisfiable before provisioning. Returns a result to return and a boolean
// indicating the caller should stop reconciliation.
func (r *ClusterReconciler) handleRecoveryCheck(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (ctrl.Result, error, bool) {
	log := logf.FromContext(ctx)
	check := r.checkRecoveryTarget(ctx, cluster, plan)
	if check.Retry != nil {
		log.Info("Could not verify recovery target, will retry", "error", check.Retry.Error())
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       topology.PhaseProvisioning,
			PhaseReason: "Verifying recovery target is reachable from the archive",
			Ready:       false,
			Progressing: true,
			Plan:        plan,
		}), true
	}
	if check.Blocked != "" {
		log.Info("Blocking cluster: recovery target is unsatisfiable", "reason", check.Blocked)
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "RecoveryTargetUnsatisfiable", check.Blocked)
		return ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       topology.PhaseBlocked,
			PhaseReason: check.Blocked,
			Ready:       false,
			Progressing: false,
			Plan:        plan,
		}), true
	}
	return ctrl.Result{}, nil, false
}

// blockOnInvalidCertificate checks whether user-provided TLS certificate
// Secrets are valid. When invalid the cluster is blocked until the user fixes
// them, avoiding instance provisioning with broken TLS configuration.
func (r *ClusterReconciler) blockOnInvalidCertificate(ctx context.Context, cluster *mysqlv1alpha1.Cluster, plan clusterPlan) (bool, ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if err := r.validateUserCertificates(ctx, cluster); err != nil {
		log.Info("Blocking cluster: user-provided certificate secret is invalid", "reason", err.Error())
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, "InvalidUserCertificate", err.Error())
		}
		return true, ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       topology.PhaseBlocked,
			PhaseReason: err.Error(),
			Ready:       false,
			Progressing: false,
			Plan:        plan,
		})
	}
	return false, ctrl.Result{}, nil
}

// reconcileAvailability runs best-effort availability adjustments that must
// happen while the cluster is degraded, not only after it returns to Ready.
func (r *ClusterReconciler) reconcileAvailability(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) {
	log := logf.FromContext(ctx)
	if err := r.topologyReconciler(cluster).ReconcileAvailability(ctx, cluster, topologyAvailabilityState(observed)); err != nil {
		log.Info("Semi-sync self-healing pass failed, will retry", "error", err.Error())
	}
}

// reconcileSteadyState runs the best-effort, post-Ready reconciliations:
// declarative managed roles and backup retention. Failures here are logged and
// retried on the next resync rather than failing the whole reconcile.
func (r *ClusterReconciler) reconcileSteadyState(ctx context.Context, cluster *mysqlv1alpha1.Cluster) {
	log := logf.FromContext(ctx)
	if err := r.reconcileManagedRoles(ctx, cluster); err != nil {
		log.Info("Managed roles reconciliation failed, will retry", "error", err.Error())
	}
	if err := r.reconcileRetention(ctx, cluster); err != nil {
		log.Info("Backup retention pass failed, will retry", "error", err.Error())
	}
	if err := r.reconcileReload(ctx, cluster); err != nil {
		log.Info("Configuration reload pass failed, will retry", "error", err.Error())
	}
}

func (r *ClusterReconciler) instanceControlClient() InstanceControlClient {
	if r.ControlClient == nil {
		r.ControlClient = &HTTPControlClient{Client: r.Client}
	}
	return r.ControlClient
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControlClient == nil {
		r.ControlClient = &HTTPControlClient{Client: r.Client}
	}
	r.podMonitorAvailable = podMonitorCRDInstalled(mgr.GetRESTMapper())

	builder := ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.Cluster{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Owns(&policyv1.PodDisruptionBudget{})
	// Only watch PodMonitors when the Prometheus Operator CRD is installed;
	// otherwise the informer fails to start with a no-matches error.
	if r.podMonitorAvailable {
		builder = builder.Owns(&monitoringv1.PodMonitor{})
	} else {
		mgr.GetLogger().Info("PodMonitor CRD not installed; PodMonitor reconciliation disabled")
	}
	return builder.Named("cluster").Complete(r)
}

// podMonitorCRDInstalled reports whether the PodMonitor CRD is registered in the
// cluster, via the manager's RESTMapper.
func podMonitorCRDInstalled(mapper meta.RESTMapper) bool {
	_, err := mapper.RESTMapping(schema.GroupKind{
		Group: monitoringv1.SchemeGroupVersion.Group,
		Kind:  monitoringv1.PodMonitorsKind,
	}, monitoringv1.SchemeGroupVersion.Version)
	return err == nil
}
