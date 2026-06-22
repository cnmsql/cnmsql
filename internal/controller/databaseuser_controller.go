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
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
)

// databaseUserFinalizer guards the MySQL account so the reclaim policy can run
// before the DatabaseUser object is removed.
const databaseUserFinalizer = "mysql.cnmsql.co/databaseuser"

// grantTargetAll is the default grant target (all schemas, all tables).
const grantTargetAll = "*.*"

// DatabaseUserReconciler reconciles a DatabaseUser object: a standalone,
// installation-wide MySQL account managed against the referenced cluster's
// primary. It owns one account (name@host) and its grants, honouring the
// reclaim policy on deletion. Accounts it did not create are not touched unless
// the adopt annotation opts in.
type DatabaseUserReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	ControlClient InstanceControlClient
}

// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databaseusers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databaseusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cnmsql.co,resources=databaseusers/finalizers,verbs=update

// Reconcile drives a DatabaseUser towards its desired state.
func (r *DatabaseUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	du := &mysqlv1alpha1.DatabaseUser{}
	if err := r.Get(ctx, req.NamespacedName, du); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	du.SetDefaults()

	cluster := &mysqlv1alpha1.Cluster{}
	err := r.Get(ctx, types.NamespacedName{Namespace: du.Namespace, Name: du.Spec.Cluster.Name}, cluster)
	if apierrors.IsNotFound(err) {
		if !du.DeletionTimestamp.IsZero() {
			return ctrl.Result{}, r.removeUserFinalizer(ctx, du)
		}
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.markUserNotApplied(ctx, du,
			"ClusterNotFound", fmt.Sprintf("Cluster %q was not found", du.Spec.Cluster.Name))
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	cluster.SetDefaults()
	primary := cluster.Status.CurrentPrimary

	if !du.DeletionTimestamp.IsZero() {
		return r.finalizeUser(ctx, du, cluster, primary)
	}

	if !controllerutil.ContainsFinalizer(du, databaseUserFinalizer) {
		controllerutil.AddFinalizer(du, databaseUserFinalizer)
		if err := r.Update(ctx, du); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if errs := du.Validate(); len(errs) > 0 {
		return ctrl.Result{}, r.markUserNotApplied(ctx, du, "Invalid", errs.ToAggregate().Error())
	}

	if primary == "" {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.markUserNotApplied(ctx, du,
			"PrimaryNotReady", "Waiting for the cluster primary to be available")
	}

	secretRV, err := r.applyUser(ctx, du, cluster, primary)
	if err != nil {
		if conflict, ok := err.(userConflictError); ok {
			r.recordUserEvent(du, corev1.EventTypeWarning, "UserConflict", conflict.Error())
			return ctrl.Result{RequeueAfter: readyResync}, r.markUserNotApplied(ctx, du, "UserConflict", conflict.Error())
		}
		log.Info("DatabaseUser reconciliation failed, will retry", "user", du.UserName(), "error", err.Error())
		r.recordUserEvent(du, corev1.EventTypeWarning, "ReconcileError", err.Error())
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.markUserNotApplied(ctx, du, "ReconcileError", err.Error())
	}

	return ctrl.Result{RequeueAfter: readyResync}, r.markUserApplied(ctx, du, secretRV)
}

// userConflictError signals that the MySQL account already exists but was not
// created by this DatabaseUser; the controller refuses to mutate it unless the
// adopt annotation is set.
type userConflictError struct{ msg string }

func (e userConflictError) Error() string { return e.msg }

// applyUser reconciles the single account and returns the source Secret
// resourceVersion that was applied (empty when the user was dropped).
func (r *DatabaseUserReconciler) applyUser(
	ctx context.Context,
	du *mysqlv1alpha1.DatabaseUser,
	cluster *mysqlv1alpha1.Cluster,
	primary string,
) (string, error) {
	name := du.UserName()
	host := defaultHost(du.Spec.Host)

	listed, err := r.ControlClient.ListUsers(ctx, cluster, primary)
	if err != nil {
		return "", fmt.Errorf("listing users on %s: %w", primary, err)
	}
	var current user.UserInfo
	exists := false
	for _, u := range listed.Users {
		if roleKey(u.Name, u.Host) == roleKey(name, host) {
			current, exists = u, true
			break
		}
	}

	if du.Spec.Ensure == mysqlv1alpha1.EnsureAbsent {
		if exists {
			if err := r.ControlClient.DropUser(ctx, cluster, primary, user.DropUserRequest{Name: name, Host: host}); err != nil {
				return "", err
			}
			r.recordUserEvent(du, corev1.EventTypeNormal, "UserDropped", name)
		}
		return "", nil
	}

	// owned is true once we have successfully applied this account at least once
	// (markUserApplied is the only writer of Applied). If the account exists but
	// we have never applied it, it belongs to something else: refuse to clobber
	// it unless adoption is explicitly requested.
	owned := du.Status.Applied != nil
	if exists && !owned && !du.AdoptRequested() {
		return "", userConflictError{msg: fmt.Sprintf(
			"MySQL account %s@%s already exists and is not managed by this DatabaseUser; "+
				"set annotation %s=true to adopt it", name, host, mysqlv1alpha1.DatabaseUserAdoptAnnotation)}
	}

	password, secretRV, err := r.resolveDatabaseUserPassword(ctx, du)
	if err != nil {
		return "", err
	}

	if !exists {
		if err := r.ControlClient.CreateUser(ctx, cluster, primary, user.CreateUserRequest{
			Name:                  name,
			Host:                  host,
			Password:              password,
			Superuser:             du.Spec.Superuser,
			MaxUserConnections:    du.Spec.MaxUserConnections,
			MaxQueriesPerHour:     du.Spec.MaxQueriesPerHour,
			MaxUpdatesPerHour:     du.Spec.MaxUpdatesPerHour,
			MaxConnectionsPerHour: du.Spec.MaxConnectionsPerHour,
			RequireTLS:            normalizeRequireTLS(du.Spec.RequireTLS),
			Privileges:            duPrivileges(du.Spec.Grants),
			Revokes:               duRevokes(du.Spec.Revokes),
		}); err != nil {
			return "", err
		}
		r.recordUserEvent(du, corev1.EventTypeNormal, "UserCreated", name)
		return secretRV, nil
	}

	// Adopting a pre-existing account: take ownership and reconcile its state.
	if !owned {
		r.recordUserEvent(du, corev1.EventTypeNormal, "Adopted", name)
	}

	alter, changed := r.diffDatabaseUser(du, name, host, current)
	if secretRV != du.Status.PasswordSecretResourceVersion {
		alter.Password = &password
		changed = true
	}
	if changed {
		if err := r.ControlClient.AlterUser(ctx, cluster, primary, alter); err != nil {
			return "", err
		}
		r.recordUserEvent(du, corev1.EventTypeNormal, "UserUpdated", name)
	}
	return secretRV, nil
}

// diffDatabaseUser builds a minimal ALTER USER from the attributes that differ
// between the desired user and the observed account (password handled by caller).
func (r *DatabaseUserReconciler) diffDatabaseUser(
	du *mysqlv1alpha1.DatabaseUser, name, host string, current user.UserInfo,
) (user.AlterUserRequest, bool) {
	alter := user.AlterUserRequest{Name: name, Host: host}
	changed := false
	if du.Spec.MaxUserConnections != current.MaxUserConnections {
		v := du.Spec.MaxUserConnections
		alter.MaxUserConnections = &v
		changed = true
	}
	if du.Spec.MaxQueriesPerHour != current.MaxQueriesPerHour {
		v := du.Spec.MaxQueriesPerHour
		alter.MaxQueriesPerHour = &v
		changed = true
	}
	if du.Spec.MaxUpdatesPerHour != current.MaxUpdatesPerHour {
		v := du.Spec.MaxUpdatesPerHour
		alter.MaxUpdatesPerHour = &v
		changed = true
	}
	if du.Spec.MaxConnectionsPerHour != current.MaxConnectionsPerHour {
		v := du.Spec.MaxConnectionsPerHour
		alter.MaxConnectionsPerHour = &v
		changed = true
	}
	if normalizeRequireTLS(du.Spec.RequireTLS) != normalizeRequireTLS(current.RequireTLS) {
		v := normalizeRequireTLS(du.Spec.RequireTLS)
		alter.RequireTLS = &v
		changed = true
	}
	grantsChanged := !duGrantsSatisfied(current.Grants, du)
	revokesChanged := !duRevokesSatisfied(current.Grants, du)
	if grantsChanged || revokesChanged {
		su := du.Spec.Superuser
		alter.Superuser = &su
		privs := duPrivileges(du.Spec.Grants)
		alter.Privileges = &privs
		// Re-apply revokes whenever grants change (a GRANT on *.* re-widens any
		// system-schema carve-out) or when the revoke spec itself differs from
		// what is already in place in MySQL.
		if revokes := duRevokes(du.Spec.Revokes); revokes != nil || revokesChanged {
			if revokes == nil {
				revokes = []user.Privilege{}
			}
			alter.Revokes = &revokes
		}
		changed = true
	}
	return alter, changed
}

// finalizeUser honours the reclaim policy and releases the finalizer.
func (r *DatabaseUserReconciler) finalizeUser(
	ctx context.Context,
	du *mysqlv1alpha1.DatabaseUser,
	cluster *mysqlv1alpha1.Cluster,
	primary string,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(du, databaseUserFinalizer) {
		return ctrl.Result{}, nil
	}
	if strings.EqualFold(du.Spec.ReclaimPolicy, reclaimDelete) {
		if primary == "" {
			return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
		}
		if err := r.ControlClient.DropUser(ctx, cluster, primary, user.DropUserRequest{
			Name: du.UserName(), Host: defaultHost(du.Spec.Host),
		}); err != nil {
			return ctrl.Result{}, err
		}
		r.recordUserEvent(du, corev1.EventTypeNormal, "UserReclaimed", du.UserName())
	}
	return ctrl.Result{}, r.removeUserFinalizer(ctx, du)
}

func (r *DatabaseUserReconciler) removeUserFinalizer(ctx context.Context, du *mysqlv1alpha1.DatabaseUser) error {
	if !controllerutil.ContainsFinalizer(du, databaseUserFinalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(du, databaseUserFinalizer)
	return r.Update(ctx, du)
}

// resolveDatabaseUserPassword reads the user's password from the referenced
// Secret. A user with Ensure=present must reference a Secret.
func (r *DatabaseUserReconciler) resolveDatabaseUserPassword(
	ctx context.Context, du *mysqlv1alpha1.DatabaseUser,
) (string, string, error) {
	if du.Spec.PasswordSecret == nil {
		return "", "", fmt.Errorf("user %s has no passwordSecret", du.UserName())
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: du.Namespace, Name: du.Spec.PasswordSecret.Name}, secret); err != nil {
		return "", "", fmt.Errorf("reading password secret %s: %w", du.Spec.PasswordSecret.Name, err)
	}
	data, ok := secret.Data[du.Spec.PasswordSecret.Key]
	if !ok {
		return "", "", fmt.Errorf("password secret %s missing key %s", du.Spec.PasswordSecret.Name, du.Spec.PasswordSecret.Key)
	}
	return string(data), secret.ResourceVersion, nil
}

// duPrivileges converts declared grants into control-API privileges. Targets are
// used verbatim (defaulted to "*.*" by SetDefaults), with no schema defaulting.
func duPrivileges(grants []mysqlv1alpha1.DatabaseUserGrant) []user.Privilege {
	if len(grants) == 0 {
		return nil
	}
	out := make([]user.Privilege, 0, len(grants))
	for _, g := range grants {
		on := g.On
		if on == "" {
			on = grantTargetAll
		}
		out = append(out, user.Privilege{Privileges: g.Privileges, On: on})
	}
	return out
}

// duRevokes converts declared revokes into control-API privileges. Targets are
// used verbatim (a revoke must name its target; validation rejects an empty one).
func duRevokes(revokes []mysqlv1alpha1.DatabaseUserRevoke) []user.Privilege {
	if len(revokes) == 0 {
		return nil
	}
	out := make([]user.Privilege, 0, len(revokes))
	for _, r := range revokes {
		out = append(out, user.Privilege{Privileges: r.Privileges, On: r.On})
	}
	return out
}

// duGrantsSatisfied reports whether every declared grant is already present in
// the observed SHOW GRANTS output (case- and quoting-insensitive).
func duGrantsSatisfied(observed []string, du *mysqlv1alpha1.DatabaseUser) bool {
	have := map[string]bool{}
	for _, g := range observed {
		for _, tok := range parseGrantTokens(g) {
			have[tok] = true
		}
	}
	if du.Spec.Superuser {
		return have["all privileges@"+grantTargetAll]
	}
	for _, g := range du.Spec.Grants {
		on := g.On
		if on == "" {
			on = grantTargetAll
		}
		target := normalizeGrantTarget(on)
		for _, priv := range g.Privileges {
			if !have[strings.ToLower(strings.TrimSpace(priv))+"@"+target] {
				return false
			}
		}
	}
	return true
}

// duRevokesSatisfied reports whether every declared revoke is already present
// in the observed SHOW GRANTS output. REVOKE lines appear in SHOW GRANTS only
// when partial_revokes is ON.
func duRevokesSatisfied(observed []string, du *mysqlv1alpha1.DatabaseUser) bool {
	if len(du.Spec.Revokes) == 0 {
		return true
	}
	have := make(map[string]bool, len(observed))
	for _, g := range observed {
		for _, tok := range parseRevokeTokens(g) {
			have[tok] = true
		}
	}
	for _, r := range du.Spec.Revokes {
		target := normalizeGrantTarget(r.On)
		for _, priv := range r.Privileges {
			if !have[strings.ToLower(strings.TrimSpace(priv))+"@"+target] {
				return false
			}
		}
	}
	return true
}

// parseRevokeTokens turns a SHOW GRANTS REVOKE line into "priv@target" tokens.
// It handles REVOKE … ON … FROM …, matching MySQL 8.0 SHOW GRANTS output when
// partial_revokes is ON. Non-REVOKE lines return nil.
func parseRevokeTokens(line string) []string {
	upper := strings.ToUpper(strings.TrimSpace(line))
	if !strings.HasPrefix(upper, "REVOKE ") {
		return nil
	}
	idx := strings.Index(strings.ToUpper(upper), " ON ")
	if idx < 0 {
		return nil
	}
	privPart := strings.TrimSpace(upper[len("REVOKE "):idx])
	rest := strings.TrimSpace(upper[idx+len(" ON "):])
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return nil
	}
	target := normalizeGrantTarget(strings.TrimSpace(rest[:fromIdx]))
	var tokens []string
	for p := range strings.SplitSeq(privPart, ",") {
		tokens = append(tokens, strings.ToLower(strings.TrimSpace(p))+"@"+target)
	}
	return tokens
}

func (r *DatabaseUserReconciler) recordUserEvent(du *mysqlv1alpha1.DatabaseUser, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(du, eventType, reason, message)
	}
}

func (r *DatabaseUserReconciler) markUserApplied(ctx context.Context, du *mysqlv1alpha1.DatabaseUser, secretRV string) error {
	return r.patchUserStatus(ctx, du, func(status *mysqlv1alpha1.DatabaseUserStatus) {
		applied := true
		status.Applied = &applied
		status.Message = ""
		status.ObservedGeneration = du.Generation
		status.PasswordSecretResourceVersion = secretRV
		setUserCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionTrue, "Reconciled", "DatabaseUser reconciled", du.Generation)
	})
}

// markUserNotApplied records a failure on the Ready condition and Message but
// deliberately leaves status.Applied untouched. Applied is set non-nil only on a
// successful apply (markUserApplied); keeping it nil until then preserves the
// "never applied by us" signal that drives conflict detection, so a pre-existing
// account is never silently adopted just because an earlier reconcile failed.
func (r *DatabaseUserReconciler) markUserNotApplied(ctx context.Context, du *mysqlv1alpha1.DatabaseUser, reason, message string) error {
	return r.patchUserStatus(ctx, du, func(status *mysqlv1alpha1.DatabaseUserStatus) {
		status.Message = message
		status.ObservedGeneration = du.Generation
		setUserCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, reason, message, du.Generation)
	})
}

func (r *DatabaseUserReconciler) patchUserStatus(
	ctx context.Context,
	du *mysqlv1alpha1.DatabaseUser,
	mutate func(*mysqlv1alpha1.DatabaseUserStatus),
) error {
	latest := &mysqlv1alpha1.DatabaseUser{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: du.Namespace, Name: du.Name}, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	mutate(&latest.Status)
	du.Status = latest.Status
	return r.Status().Patch(ctx, latest, client.MergeFrom(before))
}

func setUserCondition(status *mysqlv1alpha1.DatabaseUserStatus, conditionType string, conditionStatus metav1.ConditionStatus, reason, message string, generation int64) {
	apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControlClient == nil {
		r.ControlClient = &HTTPControlClient{Client: r.Client}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.DatabaseUser{}).
		Complete(r)
}
