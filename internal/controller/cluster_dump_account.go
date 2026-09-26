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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// dumpAccountSecretName is the Secret holding the cnmsql_dump password.
func dumpAccountSecretName(cluster *mysqlv1alpha1.Cluster) string {
	return cluster.DumpSecretName()
}

// Reasons of the DumpAccountReady condition.
const (
	dumpAccountReasonApplied         = "Applied"
	dumpAccountReasonPrimaryNotReady = "PrimaryNotReady"
	dumpAccountReasonApplyFailed     = "ApplyFailed"
	dumpAccountReasonInvalidSecret   = "InvalidSecret"
)

// reconcileDumpAccount makes sure the read-only cnmsql_dump@localhost account
// exists on the primary with the password in the <cluster>-dump Secret and the
// engine's grants. It is the same path for new clusters and for clusters
// created before logical backups (the migration): the account is created
// through the instance manager's user API, which every manager version has, and
// reaches replicas through replication. The Pod spec never changes, so no
// instance restarts.
//
// It only talks to the primary when the Secret's resourceVersion differs from
// status.dumpAccountSecretVersion or the condition is not true, so the steady
// state costs one Secret read and no SQL.
func (r *ClusterReconciler) reconcileDumpAccount(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) error {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: cluster.Namespace, Name: dumpAccountSecretName(cluster)}
	if err := r.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			// ensureCredentials creates it; the next reconcile picks it up.
			return nil
		}
		return err
	}
	primary := observed.PrimaryName
	primaryStatus := observed.StatusByInstance[primary]
	// The grants depend on the server version, so an in-place upgrade of the
	// primary re-applies them. An unobserved primary is no reason to.
	if cluster.Status.DumpAccountSecretVersion == secret.ResourceVersion &&
		apimeta.IsStatusConditionTrue(cluster.Status.Conditions, mysqlv1alpha1.ConditionDumpAccountReady) &&
		(primaryStatus == nil || primaryStatus.Version == "" ||
			primaryStatus.Version == cluster.Status.DumpAccountServerVersion) {
		return nil
	}

	if primary == "" || primaryStatus == nil || !primaryStatus.IsReady || primaryStatus.Role != webserver.RolePrimary {
		return r.setDumpAccountCondition(ctx, cluster, metav1.ConditionFalse, dumpAccountReasonPrimaryNotReady,
			"Waiting for a ready primary to create the cnmsql_dump account", nil)
	}
	password := string(secret.Data["password"])
	if password == "" {
		// Demote the condition before returning, so a stale True from an
		// earlier apply cannot hide that every dump would fail.
		if err := r.setDumpAccountCondition(ctx, cluster, metav1.ConditionFalse, dumpAccountReasonInvalidSecret,
			fmt.Sprintf("The %s Secret has no password", key.Name), nil); err != nil {
			return err
		}
		return fmt.Errorf("secret %s has no password", key.Name)
	}

	eng := engine.MustForFlavor(engine.Flavor(cluster.ResolvedFlavor()))
	serverVersion, err := eng.ParseServerVersion(primaryStatus.Version)
	if err != nil {
		logf.FromContext(ctx).Info("Could not parse the server version, falling back to the base dump grants",
			"version", primaryStatus.Version, "error", err.Error())
		serverVersion = version.Version{}
	}
	logical := eng.Logical()
	control := r.instanceControlClient()

	// CREATE USER IF NOT EXISTS is a no-op for an account that is already there
	// (a recovered cluster, or an earlier partial pass), so the ALTER after it
	// is what sets the password in every case.
	applyErr := control.CreateUser(ctx, cluster, primary, user.CreateUserRequest{
		Name:       engine.DumpAccountName,
		Host:       engine.DumpAccountHost,
		Password:   password,
		RequireTLS: requireTLSNone,
		Privileges: toUserPrivileges(logical.DumpAccountGrants(serverVersion)),
		Revokes:    toUserPrivileges(logical.DumpAccountRevokes(serverVersion)),
	})
	if applyErr == nil {
		applyErr = control.AlterUser(ctx, cluster, primary, user.AlterUserRequest{
			Name:     engine.DumpAccountName,
			Host:     engine.DumpAccountHost,
			Password: &password,
		})
	}
	if applyErr != nil {
		msg := fmt.Sprintf("Could not apply the cnmsql_dump account on %s: %v", primary, applyErr)
		if err := r.setDumpAccountCondition(ctx, cluster, metav1.ConditionFalse, dumpAccountReasonApplyFailed, msg, nil); err != nil {
			return err
		}
		return applyErr
	}

	logf.FromContext(ctx).Info("Applied the dump account", "primary", primary,
		"secretVersion", secret.ResourceVersion, "serverVersion", primaryStatus.Version)
	if r.Recorder != nil {
		r.Recorder.Event(cluster, corev1.EventTypeNormal, "DumpAccountReady",
			fmt.Sprintf("Applied the cnmsql_dump account on %s", primary))
	}
	return r.setDumpAccountCondition(ctx, cluster, metav1.ConditionTrue, dumpAccountReasonApplied,
		"The cnmsql_dump account matches the "+key.Name+" Secret",
		&appliedDumpAccount{secretVersion: secret.ResourceVersion, serverVersion: primaryStatus.Version})
}

// reconcileDumpAccountBestEffort runs reconcileDumpAccount and reports a
// failure for the next pass instead of failing the Cluster reconcile.
func (r *ClusterReconciler) reconcileDumpAccountBestEffort(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	observed observedCluster,
) {
	if err := r.reconcileDumpAccount(ctx, cluster, observed); err != nil {
		logf.FromContext(ctx).Error(err, "Could not reconcile the dump account")
		if r.Recorder != nil {
			r.Recorder.Event(cluster, corev1.EventTypeWarning, "DumpAccountFailed", err.Error())
		}
	}
}

// appliedDumpAccount is what the dump account was last applied from.
type appliedDumpAccount struct {
	secretVersion string
	serverVersion string
}

// setDumpAccountCondition writes the DumpAccountReady condition, and on success
// what the account was applied from. It skips the write when nothing would
// change.
func (r *ClusterReconciler) setDumpAccountCondition(
	ctx context.Context,
	cluster *mysqlv1alpha1.Cluster,
	status metav1.ConditionStatus,
	reason, message string,
	applied *appliedDumpAccount,
) error {
	current := apimeta.FindStatusCondition(cluster.Status.Conditions, mysqlv1alpha1.ConditionDumpAccountReady)
	if current != nil && current.Status == status && current.Reason == reason && current.Message == message &&
		(applied == nil || (cluster.Status.DumpAccountSecretVersion == applied.secretVersion &&
			cluster.Status.DumpAccountServerVersion == applied.serverVersion)) {
		return nil
	}
	return r.updateStatus(ctx, cluster, func(s *mysqlv1alpha1.ClusterStatus) {
		if applied != nil {
			s.DumpAccountSecretVersion = applied.secretVersion
			s.DumpAccountServerVersion = applied.serverVersion
		}
		apimeta.SetStatusCondition(&s.Conditions, metav1.Condition{
			Type:               mysqlv1alpha1.ConditionDumpAccountReady,
			Status:             status,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: cluster.Generation,
		})
	})
}

func toUserPrivileges(grants []engine.AccountGrant) []user.Privilege {
	if len(grants) == 0 {
		return nil
	}
	out := make([]user.Privilege, 0, len(grants))
	for _, g := range grants {
		out = append(out, user.Privilege{Privileges: g.Privileges, On: g.On})
	}
	return out
}
