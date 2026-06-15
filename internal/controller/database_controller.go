/*
Copyright 2026 The cloudnative-mysql Authors.

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

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
)

// databaseFinalizer guards the MySQL schema so the reclaim policy can run before
// the Database object is removed.
const databaseFinalizer = "mysql.cloudnative-mysql.io/database"

const reclaimDelete = "delete"

// DatabaseReconciler reconciles a Database object against the referenced
// cluster's primary instance. It owns the schema and its declared users,
// honouring the reclaim policy on deletion.
type DatabaseReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Recorder      record.EventRecorder
	ControlClient InstanceControlClient
}

// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=databases,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=databases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=databases/finalizers,verbs=update
// +kubebuilder:rbac:groups=mysql.cloudnative-mysql.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives a Database towards its desired state.
func (r *DatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	db := &mysqlv1alpha1.Database{}
	if err := r.Get(ctx, req.NamespacedName, db); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	cluster := &mysqlv1alpha1.Cluster{}
	err := r.Get(ctx, types.NamespacedName{Namespace: db.Namespace, Name: db.Spec.Cluster.Name}, cluster)
	if apierrors.IsNotFound(err) {
		// Without the cluster we cannot touch MySQL. If the object is being
		// deleted there is nothing left to reclaim, so release the finalizer.
		if !db.DeletionTimestamp.IsZero() {
			return ctrl.Result{}, r.removeFinalizer(ctx, db)
		}
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.markNotApplied(ctx, db,
			"ClusterNotFound", fmt.Sprintf("Cluster %q was not found", db.Spec.Cluster.Name))
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	cluster.SetDefaults()
	primary := cluster.Status.CurrentPrimary

	if !db.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, db, cluster, primary)
	}

	if !controllerutil.ContainsFinalizer(db, databaseFinalizer) {
		controllerutil.AddFinalizer(db, databaseFinalizer)
		if err := r.Update(ctx, db); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if primary == "" {
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.markNotApplied(ctx, db,
			"PrimaryNotReady", "Waiting for the cluster primary to be available")
	}

	passwords, err := r.applyDatabase(ctx, db, cluster, primary)
	if err != nil {
		log.Info("Database reconciliation failed, will retry", "database", db.Name, "error", err.Error())
		if r.Recorder != nil {
			r.Recorder.Event(db, corev1.EventTypeWarning, "ReconcileError", err.Error())
		}
		return ctrl.Result{RequeueAfter: provisioningRequeue}, r.markNotApplied(ctx, db, "ReconcileError", err.Error())
	}

	return ctrl.Result{RequeueAfter: readyResync}, r.markApplied(ctx, db, passwords)
}

// applyDatabase reconciles the schema and its declared users, returning the
// per-user applied password versions to record in status.
func (r *DatabaseReconciler) applyDatabase(
	ctx context.Context,
	db *mysqlv1alpha1.Database,
	cluster *mysqlv1alpha1.Cluster,
	primary string,
) (map[string]string, error) {
	name := databaseName(db)

	if db.Spec.Ensure == mysqlv1alpha1.EnsureAbsent {
		if err := r.ControlClient.DropDatabase(ctx, cluster, primary, user.DropDatabaseRequest{Name: name}); err != nil {
			return nil, fmt.Errorf("dropping database %s: %w", name, err)
		}
		r.recordDBEvent(db, "DatabaseDropped", name)
		return nil, nil
	}

	if err := r.ControlClient.CreateDatabase(ctx, cluster, primary, user.CreateDatabaseRequest{
		Name:         name,
		CharacterSet: db.Spec.CharacterSet,
		Collation:    db.Spec.Collation,
	}); err != nil {
		return nil, fmt.Errorf("creating database %s: %w", name, err)
	}
	r.recordDBEvent(db, "DatabaseReconciled", name)

	return r.reconcileDatabaseUsers(ctx, db, cluster, primary, name)
}

// reconcileDatabaseUsers diffs the declared users against the live MySQL state
// and issues the minimal CREATE/ALTER/DROP, defaulting grant targets to the
// managed schema.
func (r *DatabaseReconciler) reconcileDatabaseUsers(
	ctx context.Context,
	db *mysqlv1alpha1.Database,
	cluster *mysqlv1alpha1.Cluster,
	primary, dbName string,
) (map[string]string, error) {
	if len(db.Spec.Users) == 0 {
		return nil, nil
	}
	listed, err := r.ControlClient.ListUsers(ctx, cluster, primary)
	if err != nil {
		return nil, fmt.Errorf("listing users on %s: %w", primary, err)
	}
	observed := make(map[string]user.UserInfo, len(listed.Users))
	for _, u := range listed.Users {
		observed[roleKey(u.Name, u.Host)] = u
	}

	passwords := map[string]string{}
	for i := range db.Spec.Users {
		du := &db.Spec.Users[i]
		key := roleKey(du.Name, du.Host)
		rv, err := r.reconcileDatabaseUser(ctx, db, cluster, primary, dbName, du, observed, prevPassword(db, key))
		if err != nil {
			return nil, fmt.Errorf("user %s: %w", du.Name, err)
		}
		if rv != "" {
			passwords[key] = rv
		}
	}
	return passwords, nil
}

// reconcileDatabaseUser reconciles a single declared user and returns the source
// Secret resourceVersion that was applied (empty when the user was dropped).
func (r *DatabaseReconciler) reconcileDatabaseUser(
	ctx context.Context,
	db *mysqlv1alpha1.Database,
	cluster *mysqlv1alpha1.Cluster,
	primary, dbName string,
	du *mysqlv1alpha1.DatabaseUser,
	observed map[string]user.UserInfo,
	prevRV string,
) (string, error) {
	host := defaultHost(du.Host)
	_, exists := observed[roleKey(du.Name, host)]

	if du.Ensure == mysqlv1alpha1.EnsureAbsent {
		if exists {
			if err := r.ControlClient.DropUser(ctx, cluster, primary, user.DropUserRequest{Name: du.Name, Host: host}); err != nil {
				return "", err
			}
			r.recordDBEvent(db, "UserDropped", du.Name)
		}
		return "", nil
	}

	password, secretRV, err := r.resolveUserPassword(ctx, db.Namespace, du)
	if err != nil {
		return "", err
	}
	privs := dbPrivileges(du.Grants, dbName)

	if !exists {
		if err := r.ControlClient.CreateUser(ctx, cluster, primary, user.CreateUserRequest{
			Name:       du.Name,
			Host:       host,
			Password:   password,
			RequireTLS: requireTLSNone,
			Privileges: privs,
		}); err != nil {
			return "", err
		}
		r.recordDBEvent(db, "UserCreated", du.Name)
		return secretRV, nil
	}

	// The user exists: re-apply password only when its Secret changed, and
	// re-grant when the desired grants are not already present.
	alter := user.AlterUserRequest{Name: du.Name, Host: host}
	changed := false
	if secretRV != prevRV {
		alter.Password = &password
		changed = true
	}
	if !dbGrantsSatisfied(observed[roleKey(du.Name, host)].Grants, du.Grants, dbName) {
		alter.Privileges = &privs
		changed = true
	}
	if changed {
		if err := r.ControlClient.AlterUser(ctx, cluster, primary, alter); err != nil {
			return "", err
		}
		r.recordDBEvent(db, "UserUpdated", du.Name)
	}
	return secretRV, nil
}

// finalize honours the reclaim policy and releases the finalizer.
func (r *DatabaseReconciler) finalize(
	ctx context.Context,
	db *mysqlv1alpha1.Database,
	cluster *mysqlv1alpha1.Cluster,
	primary string,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(db, databaseFinalizer) {
		return ctrl.Result{}, nil
	}
	if strings.EqualFold(db.Spec.ReclaimPolicy, reclaimDelete) {
		if primary == "" {
			// Cannot reclaim without a primary; wait for one rather than orphan.
			return ctrl.Result{RequeueAfter: provisioningRequeue}, nil
		}
		if err := r.ControlClient.DropDatabase(ctx, cluster, primary, user.DropDatabaseRequest{Name: databaseName(db)}); err != nil {
			return ctrl.Result{}, err
		}
		r.recordDBEvent(db, "DatabaseReclaimed", databaseName(db))
	}
	return ctrl.Result{}, r.removeFinalizer(ctx, db)
}

func (r *DatabaseReconciler) removeFinalizer(ctx context.Context, db *mysqlv1alpha1.Database) error {
	if !controllerutil.ContainsFinalizer(db, databaseFinalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(db, databaseFinalizer)
	return r.Update(ctx, db)
}

// resolveUserPassword reads the user's password from the referenced Secret. A
// declared user must reference a Secret to be created.
func (r *DatabaseReconciler) resolveUserPassword(
	ctx context.Context,
	namespace string,
	du *mysqlv1alpha1.DatabaseUser,
) (string, string, error) {
	if du.PasswordSecret == nil {
		return "", "", fmt.Errorf("user %s has no passwordSecret", du.Name)
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: du.PasswordSecret.Name}, secret); err != nil {
		return "", "", fmt.Errorf("reading password secret %s: %w", du.PasswordSecret.Name, err)
	}
	data, ok := secret.Data[du.PasswordSecret.Key]
	if !ok {
		return "", "", fmt.Errorf("password secret %s missing key %s", du.PasswordSecret.Name, du.PasswordSecret.Key)
	}
	return string(data), secret.ResourceVersion, nil
}

// dbPrivileges converts declared grants into control-API privileges, defaulting
// the target to the managed schema's tables.
func dbPrivileges(grants []mysqlv1alpha1.DatabaseGrant, dbName string) []user.Privilege {
	if len(grants) == 0 {
		return nil
	}
	out := make([]user.Privilege, 0, len(grants))
	for _, g := range grants {
		out = append(out, user.Privilege{Privileges: g.Privileges, On: defaultGrantTarget(g.On, dbName)})
	}
	return out
}

// dbGrantsSatisfied reports whether every declared grant is already present in
// the observed SHOW GRANTS output (case- and quoting-insensitive).
func dbGrantsSatisfied(observed []string, grants []mysqlv1alpha1.DatabaseGrant, dbName string) bool {
	have := map[string]bool{}
	for _, g := range observed {
		for _, tok := range parseGrantTokens(g) {
			have[tok] = true
		}
	}
	for _, g := range grants {
		target := normalizeGrantTarget(defaultGrantTarget(g.On, dbName))
		for _, priv := range g.Privileges {
			if !have[strings.ToLower(strings.TrimSpace(priv))+"@"+target] {
				return false
			}
		}
	}
	return true
}

func defaultGrantTarget(on, dbName string) string {
	if on == "" {
		return dbName + ".*"
	}
	return on
}

func databaseName(db *mysqlv1alpha1.Database) string {
	if db.Spec.Name != "" {
		return db.Spec.Name
	}
	return db.Name
}

func prevPassword(db *mysqlv1alpha1.Database, key string) string {
	if db.Status.PasswordStatus == nil {
		return ""
	}
	return db.Status.PasswordStatus[key]
}

func (r *DatabaseReconciler) recordDBEvent(db *mysqlv1alpha1.Database, reason, name string) {
	if r.Recorder != nil {
		r.Recorder.Event(db, corev1.EventTypeNormal, reason, name)
	}
}

func (r *DatabaseReconciler) markApplied(ctx context.Context, db *mysqlv1alpha1.Database, passwords map[string]string) error {
	return r.patchDatabaseStatus(ctx, db, func(status *mysqlv1alpha1.DatabaseStatus) {
		applied := true
		status.Applied = &applied
		status.Message = ""
		status.ObservedGeneration = db.Generation
		status.PasswordStatus = passwords
		setDatabaseCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionTrue, "Reconciled", "Database reconciled", db.Generation)
	})
}

func (r *DatabaseReconciler) markNotApplied(ctx context.Context, db *mysqlv1alpha1.Database, reason, message string) error {
	return r.patchDatabaseStatus(ctx, db, func(status *mysqlv1alpha1.DatabaseStatus) {
		applied := false
		status.Applied = &applied
		status.Message = message
		status.ObservedGeneration = db.Generation
		setDatabaseCondition(status, mysqlv1alpha1.ConditionReady, metav1.ConditionFalse, reason, message, db.Generation)
	})
}

func (r *DatabaseReconciler) patchDatabaseStatus(
	ctx context.Context,
	db *mysqlv1alpha1.Database,
	mutate func(*mysqlv1alpha1.DatabaseStatus),
) error {
	latest := &mysqlv1alpha1.Database{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: db.Namespace, Name: db.Name}, latest); err != nil {
		return err
	}
	before := latest.DeepCopy()
	mutate(&latest.Status)
	db.Status = latest.Status
	if !appliedEqual(before.Status.Applied, latest.Status.Applied) {
		logf.FromContext(ctx).Info("Database applied state changed",
			"database", latest.Name, "applied", boolValue(latest.Status.Applied), "message", latest.Status.Message)
	}
	return r.Status().Patch(ctx, latest, client.MergeFrom(before))
}

// appliedEqual reports whether two *bool Applied states are equivalent, treating
// nil (not yet observed) as distinct from both true and false.
func appliedEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func boolValue(b *bool) bool {
	return b != nil && *b
}

func setDatabaseCondition(status *mysqlv1alpha1.DatabaseStatus, conditionType string, conditionStatus metav1.ConditionStatus, reason, message string, generation int64) {
	apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ControlClient == nil {
		r.ControlClient = &HTTPControlClient{Client: r.Client}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&mysqlv1alpha1.Database{}).
		Complete(r)
}
