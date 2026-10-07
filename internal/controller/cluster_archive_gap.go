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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/binlog"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// archiveGapBackupLabel marks the Backups the operator takes to anchor
// point-in-time recovery past an archive gap.
const archiveGapBackupLabel = "mysql.cnmsql.co/archive-gap-backup"

// newestPhysicalBackup returns the most recently completed physical Backup of
// the cluster, or nil when there is none.
func (r *ClusterReconciler) newestPhysicalBackup(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster,
) (*mysqlv1alpha1.Backup, error) {
	var backups mysqlv1alpha1.BackupList
	if err := r.List(ctx, &backups, client.InNamespace(cluster.Namespace)); err != nil {
		return nil, err
	}
	var newest *mysqlv1alpha1.Backup
	for i := range backups.Items {
		b := &backups.Items[i]
		if b.Spec.Cluster.Name != cluster.Name || b.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted ||
			isLogicalBackup(b) || b.Status.StoppedAt == nil {
			continue
		}
		if newest == nil || b.Status.StoppedAt.After(newest.Status.StoppedAt.Time) {
			newest = b
		}
	}
	return newest, nil
}

func isLogicalBackup(b *mysqlv1alpha1.Backup) bool {
	return b.Spec.Method == mysqlv1alpha1.BackupMethodLogical || b.Status.Method == mysqlv1alpha1.BackupMethodLogical
}

// openArchiveGaps returns the gaps a base backup with the given anchor does not
// hold: recovery from it to the latest point would have to cross them. An
// unknown anchor holds none.
func openArchiveGaps(gaps []string, anchor string) []string {
	var open []string
	for _, gap := range gaps {
		if !anchorCoversGap(anchor, gap) {
			open = append(open, gap)
		}
	}
	return open
}

// anchorCoversGap reports whether a base backup anchor already holds a gap: a
// MySQL set it contains, or a MariaDB "domain-first..last" range it is past.
func anchorCoversGap(anchor, gap string) bool {
	if anchor == "" {
		return false
	}
	if domain, rng, ok := strings.Cut(gap, "-"); ok && !strings.Contains(gap, ":") {
		d, err := strconv.ParseUint(domain, 10, 32)
		if err != nil {
			return false
		}
		_, last, ok := strings.Cut(rng, "..")
		if !ok {
			return false
		}
		through, err := strconv.ParseUint(last, 10, 64)
		if err != nil {
			return false
		}
		return binlog.MariaSeqForDomain(anchor, uint32(d)) >= through
	}
	contains, err := replication.GTIDContains(anchor, gap)
	return err == nil && contains
}

// applyArchiveGapCondition reports whether the archive is missing a stretch of
// the timeline that recovery to the latest point, from the newest base backup,
// would have to cross. A base backup taken after the gap anchors past it, which
// clears the condition; recovery to a point inside the gap stays impossible.
func (r *ClusterReconciler) applyArchiveGapCondition(ctx context.Context, latest *mysqlv1alpha1.Cluster) {
	if !latest.IsArchivingEnabled() {
		apimeta.RemoveStatusCondition(&latest.Status.Conditions, mysqlv1alpha1.ConditionArchiveGap)
		return
	}
	ca := latest.Status.ContinuousArchiving
	if ca == nil {
		return
	}
	condition := metav1.Condition{
		Type:               mysqlv1alpha1.ConditionArchiveGap,
		Status:             metav1.ConditionFalse,
		Reason:             "NoGap",
		Message:            "The binary-log archive holds every transaction after the newest base backup",
		ObservedGeneration: latest.Generation,
	}
	if len(ca.Gaps) > 0 {
		backup, err := r.newestPhysicalBackup(ctx, latest)
		if err != nil {
			logf.FromContext(ctx).Error(err, "Could not list Backups to judge archive gaps")
			return
		}
		anchor, name := "", "none"
		if backup != nil {
			anchor, name = backup.Status.EndGTID, backup.Name
		}
		if open := openArchiveGaps(ca.Gaps, anchor); len(open) > 0 {
			condition.Status = metav1.ConditionTrue
			condition.Reason = "TransactionsMissingFromArchive"
			condition.Message = fmt.Sprintf(
				"The binary-log archive is missing %s, which the newest base backup (%s) does not hold: "+
					"point-in-time recovery cannot cross them until a base backup taken after them completes",
				strings.Join(open, "; "), name)
		} else {
			condition.Reason = "GapBehindNewestBackup"
			condition.Message = fmt.Sprintf(
				"The binary-log archive is missing %s, before the newest base backup (%s): "+
					"recovery to a point before that backup cannot cross them", strings.Join(ca.Gaps, "; "), name)
		}
	}
	apimeta.SetStatusCondition(&latest.Status.Conditions, condition)
}

// onArchiveGap warns when the ArchiveGap condition turns True and takes a base
// backup, so recovery to the latest point works again as soon as it completes.
func (r *ClusterReconciler) onArchiveGap(ctx context.Context, latest *mysqlv1alpha1.Cluster, wasGapped bool) {
	condition := apimeta.FindStatusCondition(latest.Status.Conditions, mysqlv1alpha1.ConditionArchiveGap)
	if wasGapped || condition == nil || condition.Status != metav1.ConditionTrue {
		return
	}
	if r.Recorder != nil {
		r.Recorder.Event(latest, corev1.EventTypeWarning, mysqlv1alpha1.ConditionArchiveGap, condition.Message)
	}
	if err := r.ensureArchiveGapBackup(ctx, latest); err != nil {
		logf.FromContext(ctx).Error(err, "Could not create a base backup past the archive gap")
	}
}

// ensureArchiveGapBackup creates, once per set of gaps, a physical Backup owned
// by the cluster. Clusters without a backup object store get only the event.
func (r *ClusterReconciler) ensureArchiveGapBackup(ctx context.Context, cluster *mysqlv1alpha1.Cluster) error {
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.ObjectStore == nil || cluster.Status.ContinuousArchiving == nil {
		return nil
	}
	sum := sha256.Sum256([]byte(strings.Join(cluster.Status.ContinuousArchiving.Gaps, ";")))
	backup := &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-archive-gap-%s", cluster.Name, hex.EncodeToString(sum[:])[:10]),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				clusterLabel:          cluster.Name,
				archiveGapBackupLabel: "true",
			},
		},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: cluster.Name},
			Method:  mysqlv1alpha1.BackupMethodXtrabackup,
		},
	}
	if err := controllerutil.SetControllerReference(cluster, backup, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, backup); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	logf.FromContext(ctx).Info("Created a base backup past the archive gap", "backup", backup.Name)
	return nil
}
