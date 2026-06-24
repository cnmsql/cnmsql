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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/groupreplication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// majorUpgradePending reports whether a MySQL major-version upgrade is in
// progress: at least one observed instance reports a live server version on an
// older series than the target image. It returns the target version so callers
// can name the pre-upgrade backup deterministically. Patch-level differences
// within the same series are not upgrades.
func majorUpgradePending(plan clusterPlan, observed observedCluster) (version.Version, bool) {
	target, err := version.Parse(plan.ServerVersion)
	if err != nil {
		return version.Version{}, false
	}
	for _, name := range observed.InstanceNames {
		status, ok := observed.StatusByInstance[name]
		if !ok || status.Version == "" {
			continue
		}
		running, err := version.Parse(status.Version)
		if err != nil {
			continue
		}
		// An instance still on an older series than the target means the upgrade
		// has not finished rolling.
		if !running.AtLeast(target.Major, target.Minor, 0) {
			return target, true
		}
	}
	return target, false
}

// reconcileUpgradeSteps keeps the main reconcile loop's upgrade branch narrow:
// first finish any operator/instance-manager rollout, then finalize the GR
// communication protocol after a completed MySQL major-version rollout.
func (r *ClusterReconciler) reconcileUpgradeSteps(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (ctrl.Result, error, bool) {
	upgrading, result, err := r.reconcileUpgrade(ctx, cluster, plan, observed)
	if err != nil || upgrading {
		return result, err, true
	}
	return r.reconcileGroupCommunicationProtocol(ctx, cluster, plan, observed)
}

// reconcileGroupCommunicationProtocol completes a Group Replication major
// upgrade by raising the group's pinned communication protocol. MySQL permits
// this only after every member is on the target series and ONLINE. The primary's
// observed protocol makes the action idempotent across reconciles.
func (r *ClusterReconciler) reconcileGroupCommunicationProtocol(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (ctrl.Result, error, bool) {
	if !cluster.IsGroupReplication() || observed.PrimaryName == "" {
		return ctrl.Result{}, nil, false
	}

	target, pending := majorUpgradePending(plan, observed)
	if pending || !allGroupMembersUpgradedAndOnline(target, observed) {
		return ctrl.Result{}, nil, false
	}

	primaryStatus := observed.StatusByInstance[observed.PrimaryName]
	if primaryStatus == nil || primaryStatus.GroupReplication == nil {
		return ctrl.Result{}, nil, false
	}
	if cluster.Status.GroupReplication != nil {
		finalized, err := version.Parse(cluster.Status.GroupReplication.CommunicationProtocolTarget)
		if err == nil && finalized.Series() == target.Series() {
			return ctrl.Result{}, nil, false
		}
	}

	logf.FromContext(ctx).Info("Finalizing group communication protocol",
		"primary", observed.PrimaryName,
		"currentVersion", primaryStatus.GroupReplication.CommunicationProtocol,
		"targetVersion", plan.ServerVersion)
	if err := r.ControlClient.SetGroupCommunicationProtocol(
		ctx, cluster, observed.PrimaryName, plan.ServerVersion,
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("set group communication protocol: %w", err), true
	}
	if observed.GroupReplication == nil {
		observed.GroupReplication = &mysqlv1alpha1.GroupReplicationStatus{}
	}
	observed.GroupReplication.CommunicationProtocolTarget = plan.ServerVersion

	reason := fmt.Sprintf("Finalizing Group Replication communication protocol at %s", plan.ServerVersion)
	return ctrl.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster,
		upgradeGateStatus(topology.PhaseUpgrading, reason, plan, observed)), true
}

func allGroupMembersUpgradedAndOnline(target version.Version, observed observedCluster) bool {
	if len(observed.InstanceNames) == 0 {
		return false
	}
	for _, name := range observed.InstanceNames {
		status := observed.StatusByInstance[name]
		if status == nil || status.GroupReplication == nil ||
			status.GroupReplication.State != groupreplication.MemberStateOnline {
			return false
		}
		running, err := version.Parse(status.Version)
		if err != nil || !running.AtLeast(target.Major, target.Minor, 0) {
			return false
		}
	}
	return true
}

// preUpgradeBackupName is the deterministic name of the backup taken before a
// major upgrade, keyed by the target series so each hop in the chain has its own.
func preUpgradeBackupName(cluster *mysqlv1alpha1.Cluster, target version.Version) string {
	return fmt.Sprintf("%s-preupgrade-%d-%d", cluster.Name, target.Major, target.Minor)
}

// reconcileUpgradeBackupGate holds a MySQL major-version upgrade until a fresh
// backup has completed, when spec.upgrade.backupBeforeUpgrade is enabled (the
// default). It is a no-op unless a major upgrade is actually pending. When a
// backup is required but no object store is configured it hard-fails with a
// Blocked status rather than rolling unprotected, since the data-dictionary
// upgrade is irreversible. It follows the reconcile "handled" convention:
// handled=true means the caller must return the given result/error and stop
// this pass; handled=false means the gate is satisfied and the roll may proceed.
func (r *ClusterReconciler) reconcileUpgradeBackupGate(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
) (ctrl.Result, error, bool) {
	target, pending := majorUpgradePending(plan, observed)
	if !pending || !cluster.BackupBeforeUpgradeEnabled() {
		return ctrl.Result{}, nil, false
	}

	log := logf.FromContext(ctx)

	if cluster.Spec.Backup == nil || cluster.Spec.Backup.ObjectStore == nil {
		log.Info("Pre-upgrade backup required but no object store is configured; blocking upgrade")
		if r.Recorder != nil {
			r.Recorder.Event(cluster, "Warning", "BackupRequired",
				"MySQL major upgrade blocked: spec.upgrade.backupBeforeUpgrade is enabled but no backup object store is configured. "+
					"Configure spec.backup.objectStore or set spec.upgrade.backupBeforeUpgrade=false.")
		}
		return ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, upgradeGateStatus(
			topology.PhaseBlocked,
			"MySQL major upgrade blocked: a pre-upgrade backup is required but no object store is configured",
			plan, observed)), true
	}

	backupName := preUpgradeBackupName(cluster, target)
	backup := &mysqlv1alpha1.Backup{}
	switch err := r.Get(ctx, types.NamespacedName{Name: backupName, Namespace: cluster.Namespace}, backup); {
	case apierrors.IsNotFound(err):
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.createPreUpgradeBackup(ctx, cluster, plan, observed, backupName), true
	case err != nil:
		return ctrl.Result{}, err, true
	}

	switch backup.Status.Phase {
	case mysqlv1alpha1.BackupPhaseCompleted:
		return ctrl.Result{}, nil, false
	case mysqlv1alpha1.BackupPhaseFailed:
		log.Info("Pre-upgrade backup failed; blocking upgrade", "backup", backupName)
		return ctrl.Result{RequeueAfter: readyResync}, r.patchStatus(ctx, cluster, upgradeGateStatus(
			topology.PhaseBlocked,
			fmt.Sprintf("MySQL major upgrade blocked: pre-upgrade backup %s failed", backupName),
			plan, observed)), true
	default:
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.patchStatus(ctx, cluster, upgradeGateStatus(
			topology.PhaseUpgrading,
			fmt.Sprintf("Waiting for pre-upgrade backup %s to complete", backupName),
			plan, observed)), true
	}
}

// createPreUpgradeBackup creates the cluster-owned Backup that gates the upgrade.
func (r *ClusterReconciler) createPreUpgradeBackup(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	plan clusterPlan,
	observed observedCluster,
	backupName string,
) error {
	backup := &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupName,
			Namespace: cluster.Namespace,
			Labels:    map[string]string{clusterLabel: cluster.Name},
		},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: cluster.Name},
		},
	}
	if err := controllerutil.SetControllerReference(cluster, backup, r.Scheme); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("Creating pre-upgrade backup", "backup", backupName)
	if err := r.Create(ctx, backup); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return r.patchStatus(ctx, cluster, upgradeGateStatus(
		topology.PhaseUpgrading,
		fmt.Sprintf("Taking pre-upgrade backup %s before the MySQL major upgrade", backupName),
		plan, observed))
}

// upgradeGateStatus builds the status patch for the backup gate, carrying the
// observed instance facts through unchanged so only the phase/reason differ.
func upgradeGateStatus(phase, reason string, plan clusterPlan, observed observedCluster) observedCluster {
	out := observed
	out.Phase = phase
	out.PhaseReason = reason
	out.Ready = false
	out.Progressing = true
	out.Plan = plan
	return out
}
