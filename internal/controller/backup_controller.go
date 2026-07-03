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
	"fmt"
	"sort"
	"strings"
	"time"

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
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

const (
	backupPhasePending   = string(mysqlv1alpha1.BackupPhasePending)
	backupPhaseRunning   = string(mysqlv1alpha1.BackupPhaseRunning)
	backupPhaseCompleted = string(mysqlv1alpha1.BackupPhaseCompleted)
	backupPhaseFailed    = string(mysqlv1alpha1.BackupPhaseFailed)
)

// backupFinalizer triggers object-store cleanup of a Backup's archive when the
// Backup object is deleted, so removing the Kubernetes object also reclaims the
// remote backup.xbstream and metadata.json.
//
// It is opt-in: the operator only adds it when the Backup's reclaim policy is
// Delete, because removing the only copy of a recovery point by default is
// dangerous (accidental deletes, namespace teardown, owner-ref cascades, GitOps
// prunes). A user opts in with spec.reclaimPolicy: Delete on the Backup, or via
// a ScheduledBackup's spec.reclaimPolicy, which the generated Backups inherit.
const backupFinalizer = mysqlv1alpha1.BackupCleanupFinalizer

// BackupReconciler reconciles one-shot physical Backup objects.
type BackupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// OperatorImageName is the image the operator controller runs as. It is
	// used for the backup worker's bootstrap init container, which copies the
	// manager binary into a shared volume (the instance image no longer ships
	// it). Falls back to the instance image when empty.
	OperatorImageName string
}

// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=backups,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=backups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=backups/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods;secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile creates and tracks the backup worker Job for a Backup.
func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	backup := &mysqlv1alpha1.Backup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	backup.SetDefaults()

	if !backup.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, backup)
	}

	// Keep the cleanup finalizer in sync with the reclaim policy before anything
	// else, so a backup that opts in (even after it completed) is protected and
	// one that opts back out drops the finalizer.
	if changed, err := r.reconcileFinalizer(ctx, backup); err != nil || changed {
		return ctrl.Result{}, err
	}

	if backup.Status.Phase == mysqlv1alpha1.BackupPhaseCompleted {
		return ctrl.Result{}, nil
	}

	cluster := &mysqlv1alpha1.Cluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.Cluster.Name}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.failBackup(ctx, backup, "ClusterNotFound", fmt.Sprintf("Cluster %q was not found", backup.Spec.Cluster.Name))
		}
		return ctrl.Result{}, err
	}
	cluster.SetDefaults()

	method := backup.Spec.Method
	if method == "" {
		method = mysqlv1alpha1.BackupMethodXtrabackup
	}
	if method != mysqlv1alpha1.BackupMethodXtrabackup {
		return ctrl.Result{}, r.failBackup(ctx, backup, "UnsupportedMethod", fmt.Sprintf("Backup method %q is not supported in M6", method))
	}

	store, err := backupObjectStore(backup, cluster)
	if err != nil {
		return ctrl.Result{}, r.failBackup(ctx, backup, "ObjectStoreNotConfigured", err.Error())
	}
	backupID := backup.Status.BackupID
	if backupID == "" {
		backupID = defaultBackupID(backup)
	}
	keys, err := objectstore.BuildBackupKeys(*store, cluster.Name, backup.Name, backupID)
	if err != nil {
		return ctrl.Result{}, r.failBackup(ctx, backup, "InvalidObjectStore", err.Error())
	}

	sourceInstance, err := r.selectBackupSource(ctx, backup, cluster)
	if err != nil {
		return ctrl.Result{}, r.failBackup(ctx, backup, "NoBackupSource", err.Error())
	}
	jobName := backupJobName(backup)
	image := backupWorkerImage(cluster)
	operatorImage := r.OperatorImageName
	if operatorImage == "" {
		operatorImage = image
	}
	jobTTL := resolveBackupJobTTL(backup, cluster)
	job := backupJob(backup, cluster, *store, keys, backupID, sourceInstance, image, operatorImage, jobName, jobTTL)
	if err := controllerutil.SetControllerReference(backup, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.createBackupJob(ctx, job); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.patchBackupStatus(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
		if status.StartedAt == nil {
			now := metav1.Now()
			status.StartedAt = &now
		}
		status.Phase = mysqlv1alpha1.BackupPhaseRunning
		status.Method = method
		status.BackupID = backupID
		status.JobName = jobName
		status.InstanceName = sourceInstance
		status.DestinationPath = keys.ArchiveURI
		status.ObjectStore = store
		status.Error = ""
		setBackupCondition(status, mysqlv1alpha1.ConditionProgressing, metav1.ConditionTrue, backupPhaseRunning, "Backup worker Job is running", backup.Generation)
		setBackupCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, backupPhaseRunning, "Backup worker Job is running", backup.Generation)
	}); err != nil {
		return ctrl.Result{}, err
	}

	latestJob := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: jobName}, latestJob); err != nil {
		return ctrl.Result{}, err
	}
	switch {
	case latestJob.Status.Succeeded > 0:
		log.Info("Backup completed", "backup", backup.Name, "job", jobName)
		return ctrl.Result{}, r.patchBackupStatus(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
			now := metav1.Now()
			status.Phase = mysqlv1alpha1.BackupPhaseCompleted
			status.StoppedAt = &now
			status.Error = ""
			setBackupCondition(status, mysqlv1alpha1.ConditionProgressing, metav1.ConditionFalse, backupPhaseCompleted, "Backup completed", backup.Generation)
			setBackupCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionTrue, backupPhaseCompleted, "Backup completed", backup.Generation)
		})
	case latestJob.Status.Failed > 0 && jobFinished(latestJob, batchv1.JobFailed):
		return ctrl.Result{}, r.failBackup(ctx, backup, "JobFailed", "Backup worker Job failed")
	default:
		return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.Backup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

func backupObjectStore(backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster) (*mysqlv1alpha1.S3ObjectStore, error) {
	if backup.Spec.ObjectStore != nil {
		return backup.Spec.ObjectStore, nil
	}
	if cluster.Spec.Backup != nil && cluster.Spec.Backup.ObjectStore != nil {
		return cluster.Spec.Backup.ObjectStore, nil
	}
	return nil, fmt.Errorf("backup requires spec.objectStore or cluster spec.backup.objectStore")
}

func defaultBackupID(backup *mysqlv1alpha1.Backup) string {
	if backup.UID != "" {
		return backup.Name + "-" + string(backup.UID)
	}
	return fmt.Sprintf("%s-%d", backup.Name, backup.CreationTimestamp.Unix())
}

func backupJobName(backup *mysqlv1alpha1.Backup) string {
	return backup.Name + "-backup"
}

// defaultBackupJobTTL is how long a finished backup worker Job is kept when
// neither the Backup nor the cluster overrides it.
const defaultBackupJobTTL = 24 * time.Hour

// resolveBackupJobTTL resolves the finished-Job retention (ttlSecondsAfterFinished)
// in seconds: the Backup's own spec.jobTTL wins, then the cluster-wide
// spec.backup.jobTTL, then the 24h default. A negative duration is invalid and
// falls back to the default; a zero duration is honoured (delete immediately).
func resolveBackupJobTTL(backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster) int32 {
	if d := backup.Spec.JobTTL; d != nil && d.Duration >= 0 {
		return int32(d.Seconds())
	}
	if cluster.Spec.Backup != nil {
		if d := cluster.Spec.Backup.JobTTL; d != nil && d.Duration >= 0 {
			return int32(d.Seconds())
		}
	}
	return int32(defaultBackupJobTTL.Seconds())
}

func backupWorkerImage(cluster *mysqlv1alpha1.Cluster) string {
	switch {
	case cluster.Status.Image != "":
		return cluster.Status.Image
	case cluster.Spec.ImageName != "":
		return cluster.Spec.ImageName
	default:
		return defaultInstanceImage
	}
}

func (r *BackupReconciler) selectBackupSource(ctx context.Context, backup *mysqlv1alpha1.Backup, cluster *mysqlv1alpha1.Cluster) (string, error) {
	target := backup.Spec.Target
	if target == "" && cluster.Spec.Backup != nil {
		target = cluster.Spec.Backup.Target
	}
	if target == "" {
		target = mysqlv1alpha1.BackupTargetPreferStandby
	}
	if target == mysqlv1alpha1.BackupTargetPrimary {
		if cluster.Status.CurrentPrimary == "" {
			return "", fmt.Errorf("cluster currentPrimary is not set")
		}
		return cluster.Status.CurrentPrimary, nil
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{clusterLabel: cluster.Name},
	); err != nil {
		return "", err
	}
	var replicas []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Labels[roleLabel] != roleReplica || !podReady(pod) {
			continue
		}
		replicas = append(replicas, pod.Name)
	}
	sort.Strings(replicas)
	if len(replicas) > 0 {
		return replicas[0], nil
	}
	if cluster.Status.CurrentPrimary == "" {
		return "", fmt.Errorf("no healthy replica and cluster currentPrimary is not set")
	}
	return cluster.Status.CurrentPrimary, nil
}

func backupJob(
	backup *mysqlv1alpha1.Backup,
	cluster *mysqlv1alpha1.Cluster,
	store mysqlv1alpha1.S3ObjectStore,
	keys objectstore.BackupKeys,
	backupID string,
	sourceInstance string,
	image string,
	operatorImage string,
	jobName string,
	ttl int32,
) *batchv1.Job {
	backoffLimit := int32(1)
	sourceHost := sourceInstance + "." + backup.Namespace + ".svc"
	env := backupObjectStoreEnv(store)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: backup.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "cnmsql",
				"app.kubernetes.io/managed-by": "cnmsql",
				clusterLabel:                   cluster.Name,
				"mysql.cnmsql.co/backup":       backup.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					clusterLabel: cluster.Name,
				}},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					InitContainers: []corev1.Container{{
						// Copy the manager binary out of the operator image into
						// the shared scratch volume; the instance image no longer
						// ships it, so the worker runs /controller/manager.
						Name:         "bootstrap-controller",
						Image:        operatorImage,
						Command:      []string{"/manager"},
						Args:         []string{"bootstrap", "/controller/manager"},
						VolumeMounts: backupWorkerVolumeMounts(),
					}},
					Containers: []corev1.Container{{
						Name:    "backup",
						Image:   image,
						Command: []string{"/controller/manager"},
						Args: []string{
							"instance", "backup", "upload",
							"--source-manager-url=https://" + sourceHost + ":8080/cluster/backup",
							"--source-manager-server-name=" + sourceHost,
							"--bucket=" + store.Bucket,
							"--archive-key=" + keys.ArchiveKey,
							"--metadata-key=" + keys.MetadataKey,
							"--backup-id=" + backupID,
							"--backup-name=" + backup.Name,
							"--cluster-name=" + cluster.Name,
							"--instance-name=" + sourceInstance,
							"--sha256",
							"--tls-cert=" + topology.ServerTLSPath + "/tls.crt",
							"--tls-key=" + topology.ServerTLSPath + "/tls.key",
							"--tls-ca=" + topology.ClientCAPath + "/ca.crt",
						},
						Env:          env,
						VolumeMounts: backupWorkerVolumeMounts(),
					}},
					Volumes: backupWorkerVolumes(cluster.Name),
				},
			},
		},
	}
}

func backupObjectStoreEnv(store mysqlv1alpha1.S3ObjectStore) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "cnmsql_S3_ENDPOINT", Value: store.Endpoint},
		{Name: "cnmsql_S3_REGION", Value: store.Region},
		{Name: "cnmsql_S3_SIGNATURE_VERSION", Value: string(store.SignatureVersion)},
	}
	if store.ForcePathStyle != nil {
		env = append(env, corev1.EnvVar{Name: "cnmsql_S3_FORCE_PATH_STYLE", Value: fmt.Sprintf("%t", *store.ForcePathStyle)})
	}
	if store.Credentials.AccessKeyID != nil {
		env = append(env, secretKeyEnv("cnmsql_S3_ACCESS_KEY_ID", *store.Credentials.AccessKeyID))
	}
	if store.Credentials.SecretAccessKey != nil {
		env = append(env, secretKeyEnv("cnmsql_S3_SECRET_ACCESS_KEY", *store.Credentials.SecretAccessKey))
	}
	if store.Credentials.SessionToken != nil {
		env = append(env, secretKeyEnv("cnmsql_S3_SESSION_TOKEN", *store.Credentials.SessionToken))
	}
	return env
}

func secretKeyEnv(name string, selector mysqlv1alpha1.SecretKeySelector) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: selector.Name},
			Key:                  selector.Key,
		}},
	}
}

func backupWorkerVolumes(clusterName string) []corev1.Volume {
	return []corev1.Volume{
		{Name: "scratch-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "client-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: clusterName + "-client-tls"}}},
		{Name: "client-ca", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: clusterName + "-ca"}}},
	}
}

func backupWorkerVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{Name: "scratch-data", MountPath: "/controller"},
		{Name: "client-tls", MountPath: topology.ServerTLSPath, ReadOnly: true},
		{Name: "client-ca", MountPath: topology.ClientCAPath, ReadOnly: true},
	}
}

func (r *BackupReconciler) createBackupJob(ctx context.Context, job *batchv1.Job) error {
	current := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, current)
	if apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Info("Starting backup worker Job", "job", job.Name)
		return r.Create(ctx, job)
	}
	return err
}

// reconcileFinalizer adds or removes the cleanup finalizer so it matches the
// Backup's reclaim policy. It reports whether it changed the object (in which
// case the caller should stop this pass and let the update re-trigger reconcile).
func (r *BackupReconciler) reconcileFinalizer(ctx context.Context, backup *mysqlv1alpha1.Backup) (bool, error) {
	has := controllerutil.ContainsFinalizer(backup, backupFinalizer)
	switch {
	case backup.WantsObjectStoreCleanup() && !has:
		controllerutil.AddFinalizer(backup, backupFinalizer)
		return true, r.Update(ctx, backup)
	case !backup.WantsObjectStoreCleanup() && has:
		controllerutil.RemoveFinalizer(backup, backupFinalizer)
		return true, r.Update(ctx, backup)
	}
	return false, nil
}

// reconcileDelete cleans up the Backup's object-store artifacts and then
// releases the finalizer so the Kubernetes object can be removed. A cleanup
// failure requeues (via the returned error) so deletion never silently leaves
// half-cleaned remote state.
func (r *BackupReconciler) reconcileDelete(ctx context.Context, backup *mysqlv1alpha1.Backup) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(backup, backupFinalizer) {
		return ctrl.Result{}, nil
	}
	if err := r.cleanupObjectStore(ctx, backup); err != nil {
		if r.Recorder != nil {
			r.Recorder.Event(backup, corev1.EventTypeWarning, "CleanupFailed", err.Error())
		}
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(backup, backupFinalizer)
	return ctrl.Result{}, r.Update(ctx, backup)
}

// cleanupObjectStore removes the backup's archive directory (backup.xbstream +
// metadata.json) from the object store. It is a no-op when the backup never
// uploaded anything, and it skips (without blocking deletion) when the
// destination object store can no longer be resolved — e.g. the cluster that
// held the configuration is already gone and the Backup did not override it.
func (r *BackupReconciler) cleanupObjectStore(ctx context.Context, backup *mysqlv1alpha1.Backup) error {
	log := logf.FromContext(ctx)

	backupID := backup.Status.BackupID
	if backupID == "" {
		return nil
	}

	store, err := r.resolveBackupStore(ctx, backup)
	if err != nil {
		log.Info("Skipping backup object-store cleanup: object store not resolvable",
			"backup", backup.Name, "reason", err.Error())
		return nil
	}

	keys, err := objectstore.BuildBackupKeys(*store, backup.Spec.Cluster.Name, backup.Name, backupID)
	if err != nil {
		return err
	}
	cfg, err := resolveObjectStoreConfig(ctx, r.Client, backup.Namespace, store)
	if err != nil {
		return err
	}
	osClient, err := objectstore.NewClient(cfg)
	if err != nil {
		return err
	}

	// Delete the whole backup directory wholesale (archive + metadata), matching
	// the retention GC's per-backup removal.
	prefix := strings.TrimSuffix(keys.MetadataKey, objectstore.BackupMetadataName)
	if err := osClient.RemovePrefix(ctx, store.Bucket, prefix); err != nil {
		return err
	}
	log.Info("Removed backup object-store artifacts", "backup", backup.Name, "bucket", store.Bucket, "prefix", prefix)
	if r.Recorder != nil {
		r.Recorder.Event(backup, corev1.EventTypeNormal, "Cleanup",
			fmt.Sprintf("Removed object-store artifacts under s3://%s/%s", store.Bucket, prefix))
	}
	return nil
}

// resolveBackupStore resolves the Backup's destination object store. It prefers
// the destination snapshotted onto status at backup time (which survives the
// referenced Cluster being deleted), then the spec override, and finally reads
// it from the referenced Cluster.
func (r *BackupReconciler) resolveBackupStore(
	ctx context.Context,
	backup *mysqlv1alpha1.Backup,
) (*mysqlv1alpha1.S3ObjectStore, error) {
	if backup.Status.ObjectStore != nil {
		return backup.Status.ObjectStore, nil
	}
	if backup.Spec.ObjectStore != nil {
		return backup.Spec.ObjectStore, nil
	}
	cluster := &mysqlv1alpha1.Cluster{}
	key := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.Cluster.Name}
	if err := r.Get(ctx, key, cluster); err != nil {
		return nil, err
	}
	return backupObjectStore(backup, cluster)
}

func (r *BackupReconciler) failBackup(ctx context.Context, backup *mysqlv1alpha1.Backup, reason, message string) error {
	return r.patchBackupStatus(ctx, backup, func(status *mysqlv1alpha1.BackupStatus) {
		now := metav1.Now()
		status.Phase = mysqlv1alpha1.BackupPhaseFailed
		status.StoppedAt = &now
		status.Error = message
		setBackupCondition(status, mysqlv1alpha1.ConditionProgressing, metav1.ConditionFalse, reason, message, backup.Generation)
		setBackupCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, reason, message, backup.Generation)
		setBackupCondition(status, mysqlv1alpha1.ConditionDegraded, metav1.ConditionTrue, reason, message, backup.Generation)
	})
}

func (r *BackupReconciler) patchBackupStatus(
	ctx context.Context,
	backup *mysqlv1alpha1.Backup,
	mutate func(*mysqlv1alpha1.BackupStatus),
) error {
	latest := &mysqlv1alpha1.Backup{}
	key := types.NamespacedName{Namespace: backup.Namespace, Name: backup.Name}
	if err := r.Get(ctx, key, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	mutate(&latest.Status)
	return r.Status().Patch(ctx, latest, client.MergeFrom(before))
}

func setBackupCondition(status *mysqlv1alpha1.BackupStatus, conditionType string, conditionStatus metav1.ConditionStatus, reason, message string, generation int64) {
	apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

func jobFinished(job *batchv1.Job, conditionType batchv1.JobConditionType) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == conditionType && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
