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

package controller

import (
	"context"
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

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/user"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

const (
	defaultInstanceImage = "cnmysql-instance:8.0"

	clusterLabel           = "mysql.cloudnative-mysql.io/cluster"
	podMonitorClusterLabel = "cnmysql.io/cluster"
	instanceLabel          = "mysql.cloudnative-mysql.io/instance"
	roleLabel              = "mysql.cloudnative-mysql.io/role"

	rolePrimary = "primary"
	roleReplica = "replica"

	configMapAnnotation       = "cnmysql.cloudnative-mysql.io/config-map"
	configHashAnnotation      = "cnmysql.cloudnative-mysql.io/config-hash"
	podTemplateHashAnnotation = "cnmysql.cloudnative-mysql.io/pod-template-hash"

	conditionReady               = "Ready"
	conditionProgressing         = "Progressing"
	conditionContinuousArchiving = "ContinuousArchiving"

	phasePending      = "Pending"
	phaseProvisioning = "Provisioning"
	phaseReady        = "Ready"
	phaseBlocked      = "Blocked"
	phaseSwitchover   = "Switchover"
	phaseDegraded     = "Degraded"
	phaseFailingOver  = "FailingOver"

	dataDir       = "/var/lib/mysql"
	socketPath    = "/var/run/mysqld/mysqld.sock"
	configPath    = "/etc/mysql/my.cnf"
	serverTLSPath = "/etc/cnmysql/tls/server"
	clientCAPath  = "/etc/cnmysql/tls/client-ca"
	joinBackupDir = "/backup"

	replicationUser = "cnmysql_repl"
	backupUser      = "cnmysql_backup"
	controlUser     = "cnmysql_control"
	metricsUser     = "cnmysql_metrics_exporter"
	mysqldBinary    = "/usr/sbin/mysqld"

	// provisioningRequeue paces reconciles while the instance is still coming up.
	provisioningRequeue = 10 * time.Second
	// readyResync re-polls the instance manager once the cluster is ready so the
	// reported status (GTID, role, readiness) does not go stale between events.
	readyResync = 30 * time.Second
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
}

// ClusterReconciler reconciles a Cluster object.
type ClusterReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	ControlClient InstanceControlClient
	// podMonitorAvailable records whether the Prometheus Operator PodMonitor CRD
	// is installed. PodMonitor support is fully opt-in: when the CRD is absent we
	// neither watch nor reconcile PodMonitors, so the operator runs without the
	// Prometheus Operator present. Set in SetupWithManager.
	podMonitorAvailable bool
}

// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=imagecatalogs,verbs=get;list;watch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusterimagecatalogs,verbs=get;list;watch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=backups,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;pods;pods/status;persistentvolumeclaims;secrets;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=podmonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile creates the first primary instance for a fresh single-instance
// Cluster. Replicas, traffic services, backup and failover are intentionally
// deferred to later milestones.
func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	cluster.SetDefaults()

	if reason := unsupportedReason(cluster); reason != "" {
		log.Info("Cluster shape is not supported by M3", "reason", reason)
		return ctrl.Result{}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseBlocked,
			PhaseReason: reason,
			Ready:       false,
			Progressing: false,
		})
	}

	r.warnDeprecatedParameters(cluster)

	plan, err := r.buildPlan(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseBlocked,
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
		if err := r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
			s.TargetPrimary = instanceName(cluster, 1)
			s.TargetPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
		}); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.ensureCredentials(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureInstanceRBAC(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.validateUserCertificates(ctx, cluster); err != nil {
		log.Info("Blocking cluster: user-provided certificate secret is invalid", "reason", err.Error())
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, "InvalidUserCertificate", err.Error())
		}
		return ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseBlocked,
			PhaseReason: err.Error(),
			Ready:       false,
			Progressing: false,
			Plan:        plan,
		})
	}
	if err := r.ensureCertificates(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureDefaultServices(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePodMonitor(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcilePDB(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}

	certsReady, err := r.certSecretsReady(ctx, cluster, plan)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !certsReady {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseProvisioning,
			PhaseReason: "Waiting for cert-manager certificates",
			Ready:       false,
			Progressing: true,
			Plan:        plan,
		})
	}

	// Guard against a fresh cluster adopting (and overwriting) an object-store
	// destination that already holds another cluster's backups. Block before any
	// instance is provisioned so the primary never archives over existing data.
	if check := r.checkBackupDestination(ctx, cluster); check.Retry != nil {
		log.Info("Could not verify backup destination, will retry", "error", check.Retry.Error())
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseProvisioning,
			PhaseReason: "Verifying backup destination is empty",
			Ready:       false,
			Progressing: true,
			Plan:        plan,
		})
	} else if check.Blocked != "" {
		log.Info("Blocking cluster: backup destination is not empty", "reason", check.Blocked)
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "BackupDestinationNotEmpty", check.Blocked)
		return ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseBlocked,
			PhaseReason: check.Blocked,
			Ready:       false,
			Progressing: false,
			Plan:        plan,
		})
	}

	// For a point-in-time recovery, fail fast with a clear condition when the
	// target is obviously unsatisfiable (e.g. a targetGTID beyond the archive)
	// rather than provisioning a primary whose init container will CrashLoop.
	if check := r.checkRecoveryTarget(ctx, cluster, plan); check.Retry != nil {
		log.Info("Could not verify recovery target, will retry", "error", check.Retry.Error())
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseProvisioning,
			PhaseReason: "Verifying recovery target is reachable from the archive",
			Ready:       false,
			Progressing: true,
			Plan:        plan,
		})
	} else if check.Blocked != "" {
		log.Info("Blocking cluster: recovery target is unsatisfiable", "reason", check.Blocked)
		r.Recorder.Event(cluster, corev1.EventTypeWarning, "RecoveryTargetUnsatisfiable", check.Blocked)
		return ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseBlocked,
			PhaseReason: check.Blocked,
			Ready:       false,
			Progressing: false,
			Plan:        plan,
		})
	}

	// Remove replicas above the desired count (highest ordinal first), then
	// provision instances in order, ramping up one replica at a time.
	if err := r.scaleDownReplicas(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	provisioned, err := r.reconcileInstances(ctx, cluster, plan)
	if err != nil {
		return ctrl.Result{}, err
	}

	observed, err := r.observe(ctx, cluster, plan)
	if err != nil {
		return ctrl.Result{}, err
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
	switched, err := r.reconcileSwitchover(ctx, cluster, plan, observed)
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
	if err := r.patchStatus(ctx, cluster, observed); err != nil {
		return ctrl.Result{}, err
	}
	if !provisioned || !observed.Ready {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
	}
	r.reconcileSteadyState(ctx, cluster, observed)
	// Keep re-polling the instance managers so status (GTID, roles, readiness)
	// stays fresh even when no Kubernetes event triggers a reconcile.
	return ctrl.Result{RequeueAfter: readyResync}, nil
}

// reconcileSteadyState runs the best-effort, post-Ready reconciliations:
// declarative managed roles, backup retention and semi-sync self-healing.
// Failures here are logged and retried on the next resync rather than failing
// the whole reconcile.
func (r *ClusterReconciler) reconcileSteadyState(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) {
	log := logf.FromContext(ctx)
	if err := r.reconcileManagedRoles(ctx, cluster); err != nil {
		log.Info("Managed roles reconciliation failed, will retry", "error", err.Error())
	}
	if err := r.reconcileRetention(ctx, cluster); err != nil {
		log.Info("Backup retention pass failed, will retry", "error", err.Error())
	}
	if err := r.reconcileSemiSync(ctx, cluster, observed); err != nil {
		log.Info("Semi-sync self-healing pass failed, will retry", "error", err.Error())
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
