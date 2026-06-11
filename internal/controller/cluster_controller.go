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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
)

const (
	defaultInstanceImage = "cnmysql-instance:8.0"

	clusterLabel  = "mysql.cloudnative-mysql.io/cluster"
	instanceLabel = "mysql.cloudnative-mysql.io/instance"
	roleLabel     = "mysql.cloudnative-mysql.io/role"

	rolePrimary = "primary"
	roleReplica = "replica"

	configMapAnnotation       = "cnmysql.cloudnative-mysql.io/config-map"
	configHashAnnotation      = "cnmysql.cloudnative-mysql.io/config-hash"
	podTemplateHashAnnotation = "cnmysql.cloudnative-mysql.io/pod-template-hash"

	conditionReady       = "Ready"
	conditionProgressing = "Progressing"

	phasePending      = "Pending"
	phaseProvisioning = "Provisioning"
	phaseReady        = "Ready"
	phaseBlocked      = "Blocked"

	dataDir       = "/var/lib/mysql"
	socketPath    = "/var/run/mysqld/mysqld.sock"
	configPath    = "/etc/mysql/my.cnf"
	serverTLSPath = "/etc/cnmysql/tls/server"
	clientCAPath  = "/etc/cnmysql/tls/client-ca"
	joinBackupDir = "/backup"

	replicationUser = "cnmysql_repl"
	backupUser      = "cnmysql_backup"
	controlUser     = "cnmysql_control"
	mysqldBinary    = "/usr/sbin/mysqld"

	// provisioningRequeue paces reconciles while the instance is still coming up.
	provisioningRequeue = 10 * time.Second
	// readyResync re-polls the instance manager once the cluster is ready so the
	// reported status (GTID, role, readiness) does not go stale between events.
	readyResync = 30 * time.Second
)

// InstanceStatusClient reads the status served by an instance manager.
type InstanceStatusClient interface {
	Status(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instanceName string) (*webserver.Status, error)
}

// ClusterReconciler reconciles a Cluster object.
type ClusterReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Recorder     record.EventRecorder
	StatusClient InstanceStatusClient
}

// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=imagecatalogs,verbs=get;list;watch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusterimagecatalogs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps;pods;pods/status;persistentvolumeclaims;secrets;services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=issuers;certificates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

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

	plan, err := r.buildPlan(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, r.patchStatus(ctx, cluster, observedCluster{
			Phase:       phaseBlocked,
			PhaseReason: err.Error(),
			Ready:       false,
			Progressing: false,
		})
	}

	if err := r.ensureCredentials(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureCertificates(ctx, cluster, plan); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureDefaultServices(ctx, cluster, plan); err != nil {
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
	if err := r.patchStatus(ctx, cluster, observed); err != nil {
		return ctrl.Result{}, err
	}
	if !provisioned || !observed.Ready {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
	}
	// Keep re-polling the instance managers so status (GTID, roles, readiness)
	// stays fresh even when no Kubernetes event triggers a reconcile.
	return ctrl.Result{RequeueAfter: readyResync}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.Cluster{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.Service{}).
		Named("cluster").
		Complete(r)
}
