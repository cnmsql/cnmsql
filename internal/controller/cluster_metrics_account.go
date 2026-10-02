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

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// Reasons of the MetricsAccountReady condition.
const (
	metricsAccountReasonApplied         = "Applied"
	metricsAccountReasonPrimaryNotReady = "PrimaryNotReady"
	metricsAccountReasonApplyFailed     = "ApplyFailed"
)

// reconcileMetricsAccount keeps the cnmsql_metrics account on the primary at
// its base grants plus spec.monitoring.privileges, creating it when missing
// (clusters bootstrapped before it existed, or an account dropped by hand).
// The instance manager diffs SHOW GRANTS and only writes on a difference, so
// running this on every resync restores revoked grants and removes extra ones
// without churning the binlog. The change reaches replicas through
// replication.
func (r *ClusterReconciler) reconcileMetricsAccount(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) error {
	primary := observed.PrimaryName
	primaryStatus := observed.StatusByInstance[primary]
	if primary == "" || primaryStatus == nil || !primaryStatus.IsReady || primaryStatus.Role != webserver.RolePrimary {
		return r.setMetricsAccountCondition(ctx, cluster, metav1.ConditionFalse, metricsAccountReasonPrimaryNotReady,
			"Waiting for a ready primary to reconcile the cnmsql_metrics account")
	}

	var privileges []mysqlv1alpha1.RolePrivilege
	if cluster.Spec.Monitoring != nil {
		privileges = cluster.Spec.Monitoring.Privileges
	}
	resp, err := r.instanceControlClient().EnsureMetricsAccount(ctx, cluster, primary,
		user.MetricsAccountRequest{Privileges: toPrivileges(privileges)})
	if err != nil {
		msg := fmt.Sprintf("Could not reconcile the cnmsql_metrics account on %s: %v", primary, err)
		if cerr := r.setMetricsAccountCondition(ctx, cluster, metav1.ConditionFalse,
			metricsAccountReasonApplyFailed, msg); cerr != nil {
			return cerr
		}
		return err
	}

	if change := describeMetricsAccountChange(resp); change != "" {
		logf.FromContext(ctx).Info("Updated the metrics account", "primary", primary,
			"created", resp.Created, "granted", resp.Granted, "revoked", resp.Revoked)
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeNormal, "MetricsAccountUpdated",
				fmt.Sprintf("Updated the cnmsql_metrics account on %s: %s", primary, change))
		}
	}
	return r.setMetricsAccountCondition(ctx, cluster, metav1.ConditionTrue, metricsAccountReasonApplied,
		"The cnmsql_metrics account holds its built-in grants and spec.monitoring.privileges")
}

// reconcileMetricsAccountBestEffort runs reconcileMetricsAccount and reports a
// failure for the next pass instead of failing the Cluster reconcile.
func (r *ClusterReconciler) reconcileMetricsAccountBestEffort(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) {
	if err := r.reconcileMetricsAccount(ctx, cluster, observed); err != nil {
		logf.FromContext(ctx).Error(err, "Could not reconcile the metrics account")
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, "MetricsAccountFailed", err.Error())
		}
	}
}

// describeMetricsAccountChange summarizes a response for an event, or returns
// "" when nothing changed.
func describeMetricsAccountChange(resp *user.MetricsAccountResponse) string {
	var parts []string
	if resp.Created {
		parts = append(parts, "created the account")
	}
	if len(resp.Granted) > 0 {
		parts = append(parts, "granted "+strings.Join(resp.Granted, "; "))
	}
	if len(resp.Revoked) > 0 {
		parts = append(parts, "revoked "+strings.Join(resp.Revoked, "; "))
	}
	return strings.Join(parts, ", ")
}

// setMetricsAccountCondition writes the MetricsAccountReady condition. It
// skips the write when nothing would change, including the generation, so a
// spec edit is acknowledged even when the outcome reads the same.
func (r *ClusterReconciler) setMetricsAccountCondition(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	status metav1.ConditionStatus,
	reason, message string,
) error {
	current := apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionMetricsAccountReady)
	if current != nil && current.Status == status && current.Reason == reason &&
		current.Message == message && current.ObservedGeneration == cluster.Generation {
		return nil
	}
	return r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		apimeta.SetStatusCondition(&s.Conditions, metav1.Condition{
			Type:               mysqlv1alpha1.ConditionMetricsAccountReady,
			Status:             status,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: cluster.Generation,
		})
	})
}
