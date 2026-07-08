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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// planBackupGC decides which of a ScheduledBackup's child Backups to
// garbage-collect. It is pure so the keep/prune policy is exhaustively
// unit-testable, mirroring objectstore.PlanRetention.
//
// Policy (opt-in on every axis; an unset axis is disabled):
//   - Only terminal Backups (completed/failed) are ever candidates; pending and
//     running Backups are never deleted.
//   - Count: keep the newest succLimit completed and failLimit failed Backups;
//     older terminal Backups of that phase are candidates. A nil limit disables
//     the count axis for that phase.
//   - Time: a terminal Backup whose CreationTimestamp predates now-window is a
//     candidate. A zero window disables the time axis.
//   - Floor: the single newest completed Backup is always retained, even if it
//     ages past the window, so a schedule never prunes its last recovery point
//     (parity with objectstore.PlanRetention always keeping the newest base
//     backup).
//
// The returned slice holds the Backups to delete, newest-first ordering not
// guaranteed.
func planBackupGC(
	children []mysqlv1alpha1.Backup,
	succLimit, failLimit *int32,
	window time.Duration,
	now time.Time,
) []mysqlv1alpha1.Backup {
	var completed, failed []mysqlv1alpha1.Backup
	for i := range children {
		switch children[i].Status.Phase {
		case mysqlv1alpha1.BackupPhaseCompleted:
			completed = append(completed, children[i])
		case mysqlv1alpha1.BackupPhaseFailed:
			failed = append(failed, children[i])
		default:
			// pending/running: never a GC candidate.
		}
	}

	// Newest first, so index >= limit is the "older than the newest N" tail.
	sortNewestFirst(completed)
	sortNewestFirst(failed)

	// The newest completed Backup is the retention floor; never prune it.
	var floor *mysqlv1alpha1.Backup
	if len(completed) > 0 {
		floor = &completed[0]
	}

	candidates := map[string]mysqlv1alpha1.Backup{}
	collect := func(entries []mysqlv1alpha1.Backup, limit *int32) {
		for i := range entries {
			entry := entries[i]
			overCount := limit != nil && i >= int(*limit)
			overAge := window > 0 && entry.CreationTimestamp.Time.Before(now.Add(-window))
			if overCount || overAge {
				candidates[entry.Name] = entry
			}
		}
	}
	collect(completed, succLimit)
	collect(failed, failLimit)

	if floor != nil {
		delete(candidates, floor.Name)
	}

	out := make([]mysqlv1alpha1.Backup, 0, len(candidates))
	for _, entry := range candidates {
		out = append(out, entry)
	}
	return out
}

// sortNewestFirst orders Backups by CreationTimestamp descending (newest first),
// breaking ties by name for a deterministic order.
func sortNewestFirst(entries []mysqlv1alpha1.Backup) {
	sort.Slice(entries, func(i, j int) bool {
		ti, tj := entries[i].CreationTimestamp.Time, entries[j].CreationTimestamp.Time
		if ti.Equal(tj) {
			return entries[i].Name > entries[j].Name
		}
		return ti.After(tj)
	})
}

// reconcileBackupGC garbage-collects the terminal Backups a ScheduledBackup owns
// according to its retention knobs. It is best-effort: a delete error is returned
// so the reconcile retries, and the run is a no-op (no extra API cost) when no
// retention knob is set. Deleting a Backup honours its reclaimPolicy via the
// BackupReconciler finalizer, so a Delete-policy Backup also reclaims its archive.
func (r *ScheduledBackupReconciler) reconcileBackupGC(
	ctx context.Context,
	scheduledBackup *mysqlv1alpha1.ScheduledBackup,
	children []mysqlv1alpha1.Backup,
) error {
	if !scheduledBackup.HasRetention() {
		return nil
	}
	log := logf.FromContext(ctx)

	plan := planBackupGC(
		children,
		scheduledBackup.Spec.SuccessfulBackupsHistoryLimit,
		scheduledBackup.Spec.FailedBackupsHistoryLimit,
		scheduledBackup.RetentionWindow(),
		time.Now(),
	)
	if len(plan) == 0 {
		return nil
	}

	deleted := 0
	for i := range plan {
		if err := r.Delete(ctx, &plan[i]); err != nil {
			if apierrors.IsNotFound(err) {
				continue // already gone; nothing to do
			}
			return fmt.Errorf("garbage-collecting backup %q: %w", plan[i].Name, err)
		}
		deleted++
		log.Info("Garbage-collected expired scheduled backup", "backup", plan[i].Name,
			"phase", plan[i].Status.Phase)
	}
	if deleted > 0 {
		r.Recorder.Eventf(scheduledBackup, corev1.EventTypeNormal, "BackupRetention",
			"Garbage-collected %d expired Backup(s)", deleted)
	}
	return nil
}
