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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// deadBranch is what a base backup holds that the surviving timeline
// disowned: a MySQL GTID set, or MariaDB ranges.
type deadBranch struct {
	gtidSet string
	ranges  []objectstore.ArchiveDisownedRange
}

func (d deadBranch) empty() bool { return d.gtidSet == "" && len(d.ranges) == 0 }

func (d deadBranch) String() string {
	if d.gtidSet != "" {
		return d.gtidSet
	}
	parts := make([]string, 0, len(d.ranges))
	for _, r := range d.ranges {
		parts = append(parts, fmt.Sprintf("%d-%d-%d..%d-%d-%d", r.Domain, r.Server, r.After+1, r.Domain, r.Server, r.Through))
	}
	return strings.Join(parts, ",")
}

// judgeAnchor returns the part of a base backup's anchor the surviving
// timeline does not hold. On MySQL that is the anchor less the primary's
// gtid_executed: whatever the chain received is in it, so what remains was
// committed on a branch the timeline left. On MariaDB the primary timeline
// names the author of each stretch of sequence numbers.
func judgeAnchor(anchor, primaryGTID string, timeline engine.MariaDBTimeline, mariadb bool) (deadBranch, error) {
	if !mariadb {
		dead, err := replication.DifferenceGTIDStrings(anchor, primaryGTID)
		return deadBranch{gtidSet: dead}, err
	}
	var out deadBranch
	if len(timeline) == 0 {
		return out, nil
	}
	gtids, err := engine.ParseMariaDBPosition(anchor)
	if err != nil {
		return out, err
	}
	for _, g := range gtids {
		if after, ok := timeline.DeadAfter(g); ok && after < g.Seq {
			out.ranges = append(out.ranges, objectstore.ArchiveDisownedRange{
				Domain: g.Domain, Server: g.Server, After: after, Through: g.Seq,
			})
		}
	}
	return out, nil
}

// judgeBackups marks the cluster's completed physical Backups whose anchor the
// surviving timeline disowned, and records what they hold in the archive
// index, where recovery from raw S3 sees it too. A backup taken on a dead
// branch cannot recover a time or the latest point: replay cannot remove what
// the backup already holds.
//
// The authority is the writable primary's gtid_executed read this pass, and
// only Backups that completed before it was read are judged, so a backup can
// never hold a transaction the primary has not executed yet.
func (r *ClusterReconciler) judgeBackups(ctx context.Context, cluster *mysqlv1alpha1.Cluster, observed observedCluster) {
	if !cluster.IsArchivingEnabled() || observed.PrimaryGTIDReadAt.IsZero() {
		return
	}
	status, ok := observed.StatusByInstance[observed.PrimaryName]
	if !ok || !writableStatus(status) {
		return
	}
	primaryGTID := observed.GTIDByInstance[observed.PrimaryName]
	if primaryGTID == "" {
		return
	}
	mariadb := cluster.ResolvedFlavor() == mysqlv1alpha1.FlavorMariaDB
	timeline := observed.MariaDBTimeline.engineTimeline()
	log := logf.FromContext(ctx)

	var backups mysqlv1alpha1.BackupList
	if err := r.List(ctx, &backups, client.InNamespace(cluster.Namespace)); err != nil {
		log.Error(err, "Could not list Backups to judge their anchors")
		return
	}
	type verdict struct {
		backup *mysqlv1alpha1.Backup
		dead   deadBranch
	}
	var verdicts []verdict
	var newlyDead []verdict
	for i := range backups.Items {
		b := &backups.Items[i]
		if b.Spec.Cluster.Name != cluster.Name || b.Status.Phase != mysqlv1alpha1.BackupPhaseCompleted ||
			isLogicalBackup(b) || b.Status.EndGTID == "" || b.Status.StoppedAt == nil ||
			!b.Status.StoppedAt.Time.Before(observed.PrimaryGTIDReadAt) {
			continue
		}
		dead, err := judgeAnchor(b.Status.EndGTID, primaryGTID, timeline, mariadb)
		if err != nil {
			log.Error(err, "Could not judge the anchor of a Backup", "backup", b.Name)
			continue
		}
		verdicts = append(verdicts, verdict{b, dead})
		if !dead.empty() && !apimeta.IsStatusConditionTrue(b.Status.Conditions, mysqlv1alpha1.ConditionDeadBranch) {
			newlyDead = append(newlyDead, verdict{b, dead})
		}
	}
	if len(newlyDead) > 0 {
		var merged deadBranch
		var sets []string
		for _, v := range newlyDead {
			sets = append(sets, v.dead.gtidSet)
			merged.ranges = append(merged.ranges, v.dead.ranges...)
		}
		union, err := replication.UnionGTIDStrings(sets...)
		if err != nil {
			log.Error(err, "Could not merge dead-branch anchors")
			return
		}
		merged.gtidSet = union
		if err := r.recordDisowned(ctx, cluster, merged); err != nil {
			// The Backups are marked only once the archive knows, so the next pass
			// retries both.
			log.Error(err, "Could not record dead-branch backups in the archive index")
			return
		}
	}
	for _, v := range verdicts {
		r.setDeadBranchCondition(ctx, cluster, v.backup, v.dead)
	}
}

// writableStatus reports whether an instance status shows a writable primary.
func writableStatus(status *webserver.Status) bool {
	return status != nil && status.Role == webserver.RolePrimary && !status.ReadOnly && !status.SuperReadOnly
}

// recordDisowned folds a dead branch into the archive index's disowned record.
// An archive with no index yet has nothing to recover from and is left alone.
func (r *ClusterReconciler) recordDisowned(ctx context.Context, cluster *mysqlv1alpha1.Cluster, dead deadBranch) error {
	if r.updateArchiveIndex != nil {
		return r.updateArchiveIndex(ctx, cluster, func(idx *objectstore.ArchiveIndex, exists bool) (bool, error) {
			return foldDeadBranch(idx, exists, dead)
		})
	}
	store := cluster.BinlogObjectStore()
	osClient, err := r.objectStoreClient(ctx, cluster.Namespace, store)
	if err != nil {
		return err
	}
	return objectstore.UpdateArchiveIndex(ctx, osClient, store.Bucket, objectstore.ArchiveIndexKey(*store, cluster.Name),
		func(idx *objectstore.ArchiveIndex, exists bool) (bool, error) {
			return foldDeadBranch(idx, exists, dead)
		})
}

// foldDeadBranch adds a dead branch to an index's disowned record.
func foldDeadBranch(idx *objectstore.ArchiveIndex, exists bool, dead deadBranch) (bool, error) {
	if !exists {
		return false, nil
	}
	if idx.Disowned == nil {
		idx.Disowned = &objectstore.ArchiveDisowned{}
	}
	changed := false
	if dead.gtidSet != "" {
		union, err := replication.UnionGTIDStrings(idx.Disowned.GTIDSet, dead.gtidSet)
		if err != nil {
			return false, err
		}
		if contains, err := replication.GTIDContains(idx.Disowned.GTIDSet, dead.gtidSet); err != nil || !contains {
			changed = true
		}
		idx.Disowned.GTIDSet = union
	}
	for _, rng := range dead.ranges {
		if idx.Disowned.AddRange(rng) {
			changed = true
		}
	}
	if changed {
		idx.UpdatedAt = time.Now().UTC()
	}
	return changed, nil
}

// setDeadBranchCondition records a Backup's verdict, warning on the Cluster
// when it turns True.
func (r *ClusterReconciler) setDeadBranchCondition(
	ctx context.Context, cluster *mysqlv1alpha1.Cluster, backup *mysqlv1alpha1.Backup, dead deadBranch,
) {
	condition := metav1.Condition{
		Type:               mysqlv1alpha1.ConditionDeadBranch,
		Status:             metav1.ConditionFalse,
		Reason:             "OnSurvivingTimeline",
		Message:            "The surviving timeline holds every transaction of the backup's anchor",
		ObservedGeneration: backup.Generation,
	}
	if !dead.empty() {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "AnchorHoldsDisownedTransactions"
		condition.Message = fmt.Sprintf(
			"The backup holds %s, which the surviving timeline never executed: it cannot recover a time "+
				"or the latest point, only a targetGTID that names that branch", dead)
	}
	existing := apimeta.FindStatusCondition(backup.Status.Conditions, mysqlv1alpha1.ConditionDeadBranch)
	if existing == nil && dead.empty() {
		return
	}
	if existing != nil && existing.Status == condition.Status && existing.Message == condition.Message {
		return
	}
	before := backup.DeepCopy()
	apimeta.SetStatusCondition(&backup.Status.Conditions, condition)
	if err := r.Status().Patch(ctx, backup, client.MergeFrom(before)); err != nil {
		logf.FromContext(ctx).Error(err, "Could not record the dead-branch verdict of a Backup", "backup", backup.Name)
		return
	}
	if !dead.empty() && r.Recorder != nil {
		r.Recorder.Eventf(cluster, corev1.EventTypeWarning, mysqlv1alpha1.ConditionDeadBranch,
			"Backup %s was taken on a dead branch: %s", backup.Name, condition.Message)
	}
}
