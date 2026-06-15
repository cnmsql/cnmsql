/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

const (
	// parentScheduledBackupLabel ties a Backup back to the ScheduledBackup that
	// created it, regardless of the backupOwnerReference mode.
	parentScheduledBackupLabel = "mysql.cloudnative-mysql.io/scheduled-backup"
	// immediateBackupLabel marks Backups created by the immediate-on-create path.
	immediateBackupLabel = "mysql.cloudnative-mysql.io/immediate-backup"

	// backupParentScheduledBackupIndex is the field indexer key over the parent
	// label, used for efficient child-Backup lookups.
	backupParentScheduledBackupIndex = "metadata.labels." + parentScheduledBackupLabel
)

// ScheduledBackupReconciler creates Backup objects on a cron schedule.
type ScheduledBackupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=scheduledbackups,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=scheduledbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=backups,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile evaluates the schedule and creates Backups when due, never letting
// two backups for the same ScheduledBackup overlap.
func (r *ScheduledBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	scheduledBackup := &mysqlv1alpha1.ScheduledBackup{}
	if err := r.Get(ctx, req.NamespacedName, scheduledBackup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	scheduledBackup.SetDefaults()

	if scheduledBackup.IsSuspended() {
		log.Info("Skipping ScheduledBackup as it is suspended")
		return ctrl.Result{}, nil
	}

	// Concurrency guard: never overlap backups for the same ScheduledBackup.
	children, err := r.getChildBackups(ctx, scheduledBackup)
	if err != nil {
		return ctrl.Result{}, err
	}
	for i := range children {
		if !backupIsDone(&children[i]) {
			log.Info("A child Backup is still running, retrying in 60 seconds",
				"backup", children[i].Name, "phase", children[i].Status.Phase)
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
	}

	return r.reconcileScheduledBackup(ctx, scheduledBackup)
}

// reconcileScheduledBackup is the core scheduling logic once the concurrency
// guard has passed.
func (r *ScheduledBackupReconciler) reconcileScheduledBackup(
	ctx context.Context,
	scheduledBackup *mysqlv1alpha1.ScheduledBackup,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	schedule, err := mysqlv1alpha1.ParseSchedule(scheduledBackup.GetSchedule())
	if err != nil {
		log.Info("Detected an invalid cron schedule", "schedule", scheduledBackup.GetSchedule())
		r.Recorder.Eventf(scheduledBackup, corev1.EventTypeWarning, "InvalidSchedule",
			"Invalid cron schedule %q: %v", scheduledBackup.GetSchedule(), err)
		return ctrl.Result{}, nil
	}

	now := time.Now()
	if schedule.Next(now).IsZero() {
		// No future time satisfies the schedule; nothing to do.
		r.Recorder.Eventf(scheduledBackup, corev1.EventTypeWarning, "NoSchedule",
			"No time satisfying the schedule %q was found", scheduledBackup.GetSchedule())
		return ctrl.Result{}, nil
	}

	// Immediate-on-create: take a backup now, on top of the schedule.
	if scheduledBackup.Status.LastCheckTime == nil && scheduledBackup.IsImmediate() {
		// Operator-restart guard: a prior reconcile may have created the
		// immediate Backup but failed to land the status patch. time.Now()
		// differs on retry, so a name lookup would miss it; list by
		// parent + immediate label to adopt any existing one instead of
		// double-firing.
		var existingImmediate mysqlv1alpha1.BackupList
		if err := r.List(ctx, &existingImmediate,
			client.InNamespace(scheduledBackup.Namespace),
			client.MatchingLabels{
				parentScheduledBackupLabel: scheduledBackup.Name,
				immediateBackupLabel:       "true",
			},
		); err != nil {
			return ctrl.Result{}, err
		}
		if len(existingImmediate.Items) > 0 {
			// Pick the oldest so LastScheduleTime is stable across reconciles.
			sort.Slice(existingImmediate.Items, func(i, j int) bool {
				return existingImmediate.Items[i].CreationTimestamp.Before(&existingImmediate.Items[j].CreationTimestamp)
			})
			adopted := existingImmediate.Items[0].CreationTimestamp.Time
			return r.advanceScheduledBackupStatus(ctx, scheduledBackup, adopted, now, schedule.Next(now))
		}

		r.Recorder.Eventf(scheduledBackup, corev1.EventTypeNormal, "BackupSchedule",
			"Scheduling immediate backup now: %v", now)
		return r.createBackup(ctx, scheduledBackup, now, now, schedule, true)
	}

	// First check: stamp lastCheckTime and wait for the first scheduled slot.
	if scheduledBackup.Status.LastCheckTime == nil {
		orig := scheduledBackup.DeepCopy()
		scheduledBackup.Status.LastCheckTime = &metav1.Time{Time: now}
		if err := r.Status().Patch(ctx, scheduledBackup, client.MergeFrom(orig)); err != nil {
			return ctrl.Result{}, err
		}
		next := schedule.Next(now)
		log.Info("Scheduled first backup", "next", next)
		r.Recorder.Eventf(scheduledBackup, corev1.EventTypeNormal, "BackupSchedule",
			"Scheduled first backup by %v", next)
		return ctrl.Result{RequeueAfter: next.Sub(now)}, nil
	}

	// Steady state: is a new slot due?
	next := schedule.Next(scheduledBackup.Status.LastCheckTime.Time)
	if now.Before(next) {
		return ctrl.Result{RequeueAfter: next.Sub(now)}, nil
	}

	// The Backup name is deterministic, so observe the apiserver before acting.
	// If a Backup with this iteration's name already exists and is ours, a prior
	// reconcile created it but missed the status patch: adopt and advance.
	existing := &mysqlv1alpha1.Backup{}
	switch err := r.Get(ctx, types.NamespacedName{
		Name:      scheduledBackup.BackupName(next),
		Namespace: scheduledBackup.Namespace,
	}, existing); {
	case apierrors.IsNotFound(err):
		return r.createBackup(ctx, scheduledBackup, next, now, schedule, false)
	case err != nil:
		return ctrl.Result{}, err
	default:
		if existing.Labels[parentScheduledBackupLabel] != scheduledBackup.Name {
			return r.skipIterationOnNameCollision(ctx, scheduledBackup, existing, next, now, schedule.Next(now))
		}
		return r.advanceScheduledBackupStatus(ctx, scheduledBackup, next, now, schedule.Next(now))
	}
}

// skipIterationOnNameCollision handles a Backup occupying this iteration's
// deterministic name that we did not create. Adopting it would advance the
// schedule over a backup we did not run; creating ours would loop on
// AlreadyExists. We skip the slot and resume at the next one (cron semantics:
// missed slots are not retroactively run). LastScheduleTime is left untouched so
// the operator can see we did not run this iteration.
func (r *ScheduledBackupReconciler) skipIterationOnNameCollision(
	ctx context.Context,
	scheduledBackup *mysqlv1alpha1.ScheduledBackup,
	existing *mysqlv1alpha1.Backup,
	iteration time.Time,
	now time.Time,
	nextBackupTime time.Time,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	log.Info("Backup name collision; not adopting",
		"backup", existing.Name, "iteration", iteration)
	r.Recorder.Eventf(scheduledBackup, corev1.EventTypeWarning, "BackupAdoptionRefused",
		"Backup %q exists but is not owned by this ScheduledBackup; skipping iteration %s",
		existing.Name, iteration.Format(time.RFC3339))

	orig := scheduledBackup.DeepCopy()
	scheduledBackup.Status.LastCheckTime = &metav1.Time{Time: now}
	scheduledBackup.Status.NextScheduleTime = &metav1.Time{Time: nextBackupTime}
	if err := r.Status().Patch(ctx, scheduledBackup, client.MergeFrom(orig)); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: nextBackupTime.Sub(now)}, nil
}

// createBackup creates the Backup for a scheduled time and advances the status.
func (r *ScheduledBackupReconciler) createBackup(
	ctx context.Context,
	scheduledBackup *mysqlv1alpha1.ScheduledBackup,
	backupTime time.Time,
	now time.Time,
	schedule cron.Schedule,
	immediate bool,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	backup := scheduledBackup.CreateBackup(scheduledBackup.BackupName(backupTime))
	if backup.Labels == nil {
		backup.Labels = make(map[string]string)
	}
	backup.Labels[clusterLabel] = scheduledBackup.Spec.Cluster.Name
	backup.Labels[immediateBackupLabel] = strconv.FormatBool(immediate)
	backup.Labels[parentScheduledBackupLabel] = scheduledBackup.Name

	switch scheduledBackup.Spec.BackupOwnerReference {
	case "cluster":
		cluster := &mysqlv1alpha1.Cluster{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      scheduledBackup.Spec.Cluster.Name,
			Namespace: scheduledBackup.Namespace,
		}, cluster); err != nil {
			return ctrl.Result{}, err
		}
		if err := controllerutil.SetControllerReference(cluster, backup, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
	case "self":
		if err := controllerutil.SetControllerReference(scheduledBackup, backup, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
	default:
		// "none": standalone Backup, no owner reference.
	}

	log.Info("Creating scheduled backup", "backup", backup.Name)
	if err := r.Create(ctx, backup); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Stale cache at the Get-first observation, or a racing reconcile won.
			// Requeue so the next pass observes the Backup and advances from there.
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		r.Recorder.Event(scheduledBackup, corev1.EventTypeWarning, "BackupCreation",
			"Error while creating backup object")
		return ctrl.Result{}, err
	}

	return r.advanceScheduledBackupStatus(ctx, scheduledBackup, backupTime, now, schedule.Next(now))
}

// advanceScheduledBackupStatus records that a Backup for backupTime exists and
// requeues for the next iteration. Both the create path and the adopt path
// funnel through here so the status invariants stay aligned.
func (r *ScheduledBackupReconciler) advanceScheduledBackupStatus(
	ctx context.Context,
	scheduledBackup *mysqlv1alpha1.ScheduledBackup,
	backupTime time.Time,
	now time.Time,
	nextBackupTime time.Time,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	orig := scheduledBackup.DeepCopy()
	scheduledBackup.Status.LastCheckTime = &metav1.Time{Time: now}
	scheduledBackup.Status.LastScheduleTime = &metav1.Time{Time: backupTime}
	scheduledBackup.Status.NextScheduleTime = &metav1.Time{Time: nextBackupTime}

	if err := r.Status().Patch(ctx, scheduledBackup, client.MergeFrom(orig)); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Scheduled next backup", "next", nextBackupTime)
	r.Recorder.Eventf(scheduledBackup, corev1.EventTypeNormal, "BackupSchedule",
		"Next backup scheduled by %v", nextBackupTime)
	return ctrl.Result{RequeueAfter: nextBackupTime.Sub(now)}, nil
}

// getChildBackups lists all Backups created by a ScheduledBackup via the parent
// label field indexer, regardless of the backupOwnerReference mode.
func (r *ScheduledBackupReconciler) getChildBackups(
	ctx context.Context,
	scheduledBackup *mysqlv1alpha1.ScheduledBackup,
) ([]mysqlv1alpha1.Backup, error) {
	var childBackups mysqlv1alpha1.BackupList
	if err := r.List(ctx, &childBackups,
		client.InNamespace(scheduledBackup.Namespace),
		client.MatchingFields{backupParentScheduledBackupIndex: scheduledBackup.Name},
	); err != nil {
		return nil, fmt.Errorf("unable to list child backups: %w", err)
	}
	return childBackups.Items, nil
}

// backupIsDone reports whether a Backup has reached a terminal phase.
func backupIsDone(backup *mysqlv1alpha1.Backup) bool {
	return backup.Status.Phase == mysqlv1alpha1.BackupPhaseCompleted ||
		backup.Status.Phase == mysqlv1alpha1.BackupPhaseFailed
}

// SetupWithManager sets up the controller with the Manager.
func (r *ScheduledBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&mysqlv1alpha1.Backup{},
		backupParentScheduledBackupIndex,
		func(rawObj client.Object) []string {
			backup := rawObj.(*mysqlv1alpha1.Backup)
			if parent, ok := backup.Labels[parentScheduledBackupLabel]; ok {
				return []string{parent}
			}
			return nil
		},
	); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.ScheduledBackup{}).
		Owns(&mysqlv1alpha1.Backup{}).
		Named("scheduled-backup").
		Complete(r)
}
