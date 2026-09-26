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
	"errors"
	"fmt"
	"slices"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
)

// restoreWorkerContainer is the name of the worker container in a restore Job.
const restoreWorkerContainer = "restore"

// Reasons a LogicalRestore ends in or waits on, besides the ones its worker
// reports (webserver.LoadReason*, backupworker.Reason*).
const (
	restoreReasonClusterNotFound   = "ClusterNotFound"
	restoreReasonInvalidSpec       = "InvalidSpec"
	restoreReasonSourceNotReady    = "SourceNotReady"
	restoreReasonPhysicalBackup    = "PhysicalBackupNotRestorable"
	restoreReasonPrimaryNotReady   = "PrimaryNotReady"
	restoreReasonJobMissing        = "JobMissing"
	restoreReasonJobConflict       = "JobConflict"
	restoreReasonRestoreCompleted  = "Completed"
	restoreReasonRestoreInProgress = "Running"
)

// Suffixes of a failed restore's message, saying whether the data was touched
// (design 029 LR9).
const (
	restoreUnchangedNote = "Nothing was changed on the cluster."
	restorePartialNote   = "The selected databases may be partly restored."
)

// LogicalRestoreReconciler runs one-shot LogicalRestore objects: it locates the
// dump, waits for a ready primary, and runs a worker Job that streams the dump
// to the primary's instance manager, which loads it (design 029).
type LogicalRestoreReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// APIReader reads past the cache where a stale read would end a restore
	// for good. Falls back to the client when nil.
	APIReader client.Reader
	// OperatorImageName is the image the manager binary is copied from into
	// the worker Pod, as for backup workers. Falls back to the instance image.
	OperatorImageName string
}

// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=logicalrestores,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=logicalrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=logicalrestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters;backups,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a LogicalRestore from pending to completed or failed.
func (r *LogicalRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	restore := &mysqlv1alpha1.LogicalRestore{}
	if err := r.Get(ctx, req.NamespacedName, restore); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	switch restore.Status.Phase {
	case mysqlv1alpha1.LogicalRestorePhaseCompleted, mysqlv1alpha1.LogicalRestorePhaseFailed:
		return ctrl.Result{}, nil
	}
	// Once the Job exists the restore is only tracked: the dump and the target
	// were settled when it was created, and must not move under a running load.
	if restore.Status.JobName != "" {
		return r.trackJob(ctx, restore)
	}
	// A Job from an earlier pass whose status write failed may be loading
	// already: record it before anything that could fail the restore for good
	// (its Backup deleted, say) while the load still runs.
	job, err := r.ownedJob(ctx, restore)
	if err != nil {
		return ctrl.Result{}, err
	}
	if job != nil {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.markRunning(ctx, restore, job)
	}
	return r.start(ctx, restore)
}

// ownedJob returns the restore's worker Job if it exists and belongs to it. It
// reads past the cache: a Job created just before a failed status write may
// not be in it yet.
func (r *LogicalRestoreReconciler) ownedJob(
	ctx context.Context,
	restore *mysqlv1alpha1.LogicalRestore,
) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	key := types.NamespacedName{Namespace: restore.Namespace, Name: logicalRestoreJobName(restore)}
	if err := r.reader().Get(ctx, key, job); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(job, restore) {
		return nil, nil
	}
	return job, nil
}

// start resolves the dump and the target primary and creates the worker Job.
func (r *LogicalRestoreReconciler) start(ctx context.Context, restore *mysqlv1alpha1.LogicalRestore) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: restore.Namespace, Name: restore.Spec.Cluster.Name}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.fail(ctx, restore, restoreReasonClusterNotFound,
				fmt.Sprintf("Cluster %q was not found", restore.Spec.Cluster.Name), true)
		}
		return ctrl.Result{}, err
	}
	cluster.SetDefaults()

	// Admission enforces both rules; this covers objects admitted before it did.
	hasBackup, hasSource := restore.Spec.Backup != nil, restore.Spec.Source != ""
	switch {
	case hasBackup == hasSource:
		return ctrl.Result{}, r.fail(ctx, restore, restoreReasonInvalidSpec, "Set exactly one of backup or source", true)
	case len(restore.Spec.Databases) == 0:
		return ctrl.Result{}, r.fail(ctx, restore, restoreReasonInvalidSpec, "Select at least one database", true)
	}

	dump, err := r.resolveDump(ctx, restore, cluster)
	if err != nil {
		var de *dumpError
		if !errors.As(err, &de) {
			return ctrl.Result{}, err
		}
		switch de.kind {
		case dumpNotReady:
			return ctrl.Result{RequeueAfter: provisioningRequeue},
				r.markPending(ctx, restore, restoreReasonSourceNotReady, de.msg)
		case dumpPhysical:
			return ctrl.Result{}, r.fail(ctx, restore, restoreReasonPhysicalBackup,
				de.msg+", and a LogicalRestore loads a logical backup", true)
		default:
			return ctrl.Result{}, r.fail(ctx, restore, backupworker.ReasonIncompatible, de.msg, true)
		}
	}
	if err := dump.Meta.CheckImportable(string(cluster.ResolvedFlavor()), restore.Spec.Databases); err != nil {
		return ctrl.Result{}, r.fail(ctx, restore, backupworker.ReasonIncompatible, err.Error(), true)
	}

	// Warn as soon as the dump is resolved, so a restore waiting for its
	// primary already shows it. Repeats are aggregated by the recorder.
	r.warnNewerSeries(restore, cluster, dump.Meta.ServerVersion)

	primary, reason := r.targetPrimary(ctx, cluster)
	if primary == "" {
		return ctrl.Result{RequeueAfter: provisioningRequeue},
			r.markPending(ctx, restore, restoreReasonPrimaryNotReady, reason)
	}

	image := backupWorkerImage(cluster)
	operatorImage := r.OperatorImageName
	if operatorImage == "" {
		operatorImage = image
	}
	tpl := resolveRestoreJobTemplate(restore, cluster)
	job := logicalRestoreJob(restore, cluster, dump, primary, image, operatorImage, tpl)
	if err := controllerutil.SetControllerReference(restore, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, job); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		// Ours from an earlier pass whose status patch failed, or someone
		// else's (a Job left by a deleted restore of the same name). Never
		// adopt the latter: it may be loading another dump.
		existing := &batchv1.Job{}
		if err := r.reader().Get(ctx, client.ObjectKeyFromObject(job), existing); err != nil {
			return ctrl.Result{}, err
		}
		if !metav1.IsControlledBy(existing, restore) {
			return ctrl.Result{}, r.fail(ctx, restore, restoreReasonJobConflict,
				fmt.Sprintf("A Job named %s already exists and does not belong to this restore", job.Name), true)
		}
		job = existing
	}
	log.Info("Started logical restore worker Job", "job", job.Name, "target", primary,
		"backupID", dump.Meta.BackupID, "databases", restore.Spec.Databases, "policy", restore.Spec.Policy)
	if r.Recorder != nil {
		r.Recorder.Eventf(restore, corev1.EventTypeNormal, "Started",
			"Loading %v into %s with policy %s", restore.Spec.Databases, primary, restore.Spec.Policy)
	}
	return ctrl.Result{RequeueAfter: provisioningRequeue}, r.markRunning(ctx, restore, job)
}

// markRunning records a started worker Job. What the Job was started with is
// read back from its annotations, so a Job found on a later pass is recorded
// the same way.
func (r *LogicalRestoreReconciler) markRunning(
	ctx context.Context,
	restore *mysqlv1alpha1.LogicalRestore,
	job *batchv1.Job,
) error {
	target := job.Annotations[restoreTargetAnnotation]
	message := fmt.Sprintf("Loading into %s", target)
	return r.patchStatus(ctx, restore, func(s *mysqlv1alpha1.LogicalRestoreStatus) {
		now := metav1.Now()
		s.Phase = mysqlv1alpha1.LogicalRestorePhaseRunning
		s.StartedAt = &now
		s.TargetInstance = target
		s.JobName = job.Name
		s.BackupID = job.Annotations[restoreBackupIDAnnotation]
		s.SourcePath = job.Annotations[restoreSourceAnnotation]
		s.Databases = slices.Clone(restore.Spec.Databases)
		s.Error = ""
		setRestoreCondition(s, mysqlv1alpha1.ConditionProgressing, metav1.ConditionTrue, restoreReasonRestoreInProgress, message, restore.Generation)
		setRestoreCondition(s, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, restoreReasonRestoreInProgress, message, restore.Generation)
	})
}

func (r *LogicalRestoreReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// resolveDump locates the dump with the resolver bootstrap import uses.
func (r *LogicalRestoreReconciler) resolveDump(
	ctx context.Context,
	restore *mysqlv1alpha1.LogicalRestore,
	cluster *mysqlv1alpha1.Cluster,
) (*logicalDump, error) {
	resolver := logicalDumpResolver{
		client:     r.Client,
		backupNoun: "backup",
		sourceNoun: "source",
		target:     fmt.Sprintf("cluster %q", cluster.Name),
	}
	if restore.Spec.Source != "" {
		return resolver.fromSource(ctx, restore.Namespace, restore.Spec.Source,
			cluster.Spec.FindExternalCluster(restore.Spec.Source), restore.Spec.BackupID)
	}
	var fallback *mysqlv1alpha1.S3ObjectStore
	if cluster.Spec.Backup != nil {
		fallback = cluster.Spec.Backup.ObjectStore
	}
	return resolver.fromBackup(ctx, restore.Namespace, restore.Spec.Backup.Name, fallback)
}

// targetPrimary returns the primary to load into, or why there is none yet: no
// current primary, a switchover or failover in flight, or a primary Pod that
// is not ready.
func (r *LogicalRestoreReconciler) targetPrimary(ctx context.Context, cluster *mysqlv1alpha1.Cluster) (string, string) {
	primary := cluster.Status.CurrentPrimary
	switch {
	case primary == "":
		return "", fmt.Sprintf("Cluster %q has no current primary", cluster.Name)
	case cluster.Status.TargetPrimary != "" && cluster.Status.TargetPrimary != primary:
		return "", fmt.Sprintf("Cluster %q is moving its primary from %s to %s",
			cluster.Name, primary, cluster.Status.TargetPrimary)
	}
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: primary}, pod); err != nil {
		return "", fmt.Sprintf("Reading primary Pod %s: %v", primary, err)
	}
	if !podReady(pod) {
		return "", fmt.Sprintf("Primary Pod %s is not ready", primary)
	}
	return primary, ""
}

// warnNewerSeries emits a Warning when the dump comes from a newer server
// series than the cluster runs, as bootstrap import does.
func (r *LogicalRestoreReconciler) warnNewerSeries(
	restore *mysqlv1alpha1.LogicalRestore, cluster *mysqlv1alpha1.Cluster, source string,
) {
	if r.Recorder == nil {
		return
	}
	eng := engine.MustForFlavor(engine.Flavor(cluster.ResolvedFlavor()))
	target, err := resolveServerVersion(backupWorkerImage(cluster), eng)
	if err != nil || !dumpFromNewerSeries(source, target) {
		return
	}
	r.Recorder.Eventf(restore, corev1.EventTypeWarning, "RestoreFromNewerServer",
		"The dump was taken on %s, a newer series than cluster %q's %s. Loading it is allowed but not tested",
		source, cluster.Name, target)
}

// trackJob mirrors the worker Job's outcome into the status.
func (r *LogicalRestoreReconciler) trackJob(ctx context.Context, restore *mysqlv1alpha1.LogicalRestore) (ctrl.Result, error) {
	job := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: restore.Namespace, Name: restore.Status.JobName}, job); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		// Just created and not in the cache yet, or deleted under a running
		// restore. Failing is final, so ask the API server, not the cache.
		err := r.reader().Get(ctx, types.NamespacedName{Namespace: restore.Namespace, Name: restore.Status.JobName}, job)
		switch {
		case err == nil:
			// The cache is behind; track the Job on the next pass.
			return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
		case !apierrors.IsNotFound(err):
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.fail(ctx, restore, restoreReasonJobMissing,
			fmt.Sprintf("The restore worker Job %s is gone", restore.Status.JobName), false)
	}

	switch {
	case job.Status.Succeeded > 0:
		logf.FromContext(ctx).Info("Logical restore completed", "job", job.Name, "target", restore.Status.TargetInstance)
		if r.Recorder != nil {
			r.Recorder.Eventf(restore, corev1.EventTypeNormal, restoreReasonRestoreCompleted,
				"Restored %v into %s", restore.Status.Databases, restore.Status.TargetInstance)
		}
		return ctrl.Result{}, r.patchStatus(ctx, restore, func(s *mysqlv1alpha1.LogicalRestoreStatus) {
			now := metav1.Now()
			s.Phase = mysqlv1alpha1.LogicalRestorePhaseCompleted
			s.StoppedAt = &now
			s.Error = ""
			msg := "Restore completed"
			setRestoreCondition(s, mysqlv1alpha1.ConditionProgressing, metav1.ConditionFalse, restoreReasonRestoreCompleted, msg, restore.Generation)
			setRestoreCondition(s, mysqlv1alpha1.ConditionReady, metav1.ConditionTrue, restoreReasonRestoreCompleted, msg, restore.Generation)
		})
	case jobFinished(job, batchv1.JobFailed):
		reason, message := workerJobFailure(job, "Restore worker")
		unchangedData := false
		if msg, ok := readWorkerTermination(ctx, r.Client, job, restoreWorkerContainer); ok {
			reason, message, unchangedData = msg.Reason, msg.Message, msg.Unchanged
		}
		return ctrl.Result{}, r.fail(ctx, restore, reason, message, unchangedData)
	default:
		return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *LogicalRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.LogicalRestore{}).
		Owns(&batchv1.Job{}).
		Named("logicalrestore").
		Complete(r)
}

// Annotations on a restore worker Job recording what it was started with.
const (
	restoreTargetAnnotation   = "mysql.cnmsql.co/restore-target"
	restoreBackupIDAnnotation = "mysql.cnmsql.co/restore-backup-id"
	restoreSourceAnnotation   = "mysql.cnmsql.co/restore-source"
)

func logicalRestoreJobName(restore *mysqlv1alpha1.LogicalRestore) string {
	return restore.Name + "-restore"
}

// resolveRestoreJobTemplate merges the restore's job template over the
// cluster-wide backup worker template, like a Backup's.
func resolveRestoreJobTemplate(
	restore *mysqlv1alpha1.LogicalRestore, cluster *mysqlv1alpha1.Cluster,
) mysqlv1alpha1.BackupJobTemplate {
	levels := []*mysqlv1alpha1.BackupJobTemplate{restore.Spec.JobTemplate}
	if cluster.Spec.Backup != nil {
		levels = append(levels, cluster.Spec.Backup.JobTemplate)
	}
	return mergeJobTemplates(levels...)
}

// logicalRestoreJob renders the worker Job. It has the backup worker's shape
// (D11: the cluster's instance image, with the manager copied in from the
// operator image, and the client TLS identity), the resolved store's
// credentials, and a single attempt: a retry after a partial load would hit
// FailIfExists or silently redo a DropAndRecreate (design 029 LR7).
func logicalRestoreJob(
	restore *mysqlv1alpha1.LogicalRestore,
	cluster *mysqlv1alpha1.Cluster,
	dump *logicalDump,
	primary string,
	image string,
	operatorImage string,
	tpl mysqlv1alpha1.BackupJobTemplate,
) *batchv1.Job {
	backoffLimit := int32(0)
	ttl := backupJobTTLSeconds(tpl)
	targetHost := primary + "." + restore.Namespace + ".svc"
	args := make([]string, 0, 13+len(restore.Spec.Databases))
	args = append(args,
		managerInstanceCmd, "logical-restore",
		"--target-manager-url=https://"+targetHost+":8080/cluster/load",
		"--target-manager-server-name="+targetHost,
		"--instance-name="+primary,
		"--tls-cert="+topology.ServerTLSPath+"/tls.crt",
		"--tls-key="+topology.ServerTLSPath+"/tls.key",
		"--tls-ca="+topology.ClientCAPath+"/ca.crt",
		"--bucket="+dump.Store.Bucket,
		"--dump-key="+dump.DumpKey,
		"--manifest-key="+dump.ManifestKey,
		"--policy="+string(restore.Spec.Policy),
		"--flavor="+string(cluster.ResolvedFlavor()),
	)
	for _, db := range restore.Spec.Databases {
		args = append(args, "--database="+escapeArgVars(db))
	}

	jobLabels := combineStringMaps(tpl.Labels, workerJobLabels(cluster.Name, logicalRestoreLabel, restore.Name))
	jobAnnotations := combineStringMaps(tpl.Annotations, map[string]string{
		restoreTargetAnnotation:   primary,
		restoreBackupIDAnnotation: dump.Meta.BackupID,
		restoreSourceAnnotation:   fmt.Sprintf("s3://%s/%s", dump.Store.Bucket, dump.DumpKey),
	})
	podLabels := combineStringMaps(tpl.Labels, map[string]string{clusterLabel: cluster.Name})

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        logicalRestoreJobName(restore),
			Namespace:   restore.Namespace,
			Labels:      jobLabels,
			Annotations: jobAnnotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   backupJobActiveDeadlineSeconds(tpl),
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: jobAnnotations,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:     corev1.RestartPolicyNever,
					NodeSelector:      tpl.NodeSelector,
					Tolerations:       tpl.Tolerations,
					Affinity:          tpl.Affinity,
					PriorityClassName: tpl.PriorityClassName,
					InitContainers:    []corev1.Container{workerBootstrapContainer(operatorImage)},
					Containers: []corev1.Container{{
						Name:         restoreWorkerContainer,
						Image:        image,
						Command:      []string{managerBinary},
						Args:         args,
						Env:          backupObjectStoreEnv(*dump.Store),
						VolumeMounts: backupWorkerVolumeMounts(),
						Resources:    tpl.Resources,
					}},
					Volumes: backupWorkerVolumes(cluster.Name),
				},
			},
		},
	}
}

// logicalRestoreLabel names the LogicalRestore a worker Job belongs to.
const logicalRestoreLabel = "mysql.cnmsql.co/logicalrestore"

// markPending records why a restore has not started yet.
func (r *LogicalRestoreReconciler) markPending(
	ctx context.Context, restore *mysqlv1alpha1.LogicalRestore, reason, message string,
) error {
	if restore.Status.Phase == mysqlv1alpha1.LogicalRestorePhasePending {
		if c := apimeta.FindStatusCondition(restore.Status.Conditions, mysqlv1alpha1.ConditionProgressing); c != nil &&
			c.Reason == reason && c.Message == message {
			return nil
		}
	}
	return r.patchStatus(ctx, restore, func(s *mysqlv1alpha1.LogicalRestoreStatus) {
		s.Phase = mysqlv1alpha1.LogicalRestorePhasePending
		setRestoreCondition(s, mysqlv1alpha1.ConditionProgressing, metav1.ConditionFalse, reason, message, restore.Generation)
		setRestoreCondition(s, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, reason, message, restore.Generation)
	})
}

// fail ends the restore. unchangedData says the cluster was not touched; the
// message says so either way (design 029 LR9).
func (r *LogicalRestoreReconciler) fail(
	ctx context.Context, restore *mysqlv1alpha1.LogicalRestore, reason, message string, unchangedData bool,
) error {
	note := restorePartialNote
	if unchangedData {
		note = restoreUnchangedNote
	}
	message = strings.TrimRight(message, ". ") + ". " + note
	logf.FromContext(ctx).Info("Logical restore failed", "reason", reason, "message", message)
	if r.Recorder != nil {
		r.Recorder.Event(restore, corev1.EventTypeWarning, reason, message)
	}
	return r.patchStatus(ctx, restore, func(s *mysqlv1alpha1.LogicalRestoreStatus) {
		now := metav1.Now()
		s.Phase = mysqlv1alpha1.LogicalRestorePhaseFailed
		s.StoppedAt = &now
		s.Error = message
		setRestoreCondition(s, mysqlv1alpha1.ConditionProgressing, metav1.ConditionFalse, reason, message, restore.Generation)
		setRestoreCondition(s, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, reason, message, restore.Generation)
		setRestoreCondition(s, mysqlv1alpha1.ConditionDegraded, metav1.ConditionTrue, reason, message, restore.Generation)
	})
}

func (r *LogicalRestoreReconciler) patchStatus(
	ctx context.Context,
	restore *mysqlv1alpha1.LogicalRestore,
	mutate func(*mysqlv1alpha1.LogicalRestoreStatus),
) error {
	latest := &mysqlv1alpha1.LogicalRestore{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: restore.Namespace, Name: restore.Name}, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	mutate(&latest.Status)
	return r.Status().Patch(ctx, latest, client.MergeFrom(before))
}

func setRestoreCondition(
	status *mysqlv1alpha1.LogicalRestoreStatus,
	conditionType string, conditionStatus metav1.ConditionStatus,
	reason, message string, generation int64,
) {
	apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}
