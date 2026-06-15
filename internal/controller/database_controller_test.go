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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
)

func databaseReconciler(t *testing.T, control *recordingControlClient, objs ...client.Object) *DatabaseReconciler {
	t.Helper()
	scheme := testScheme(t)
	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Database{}).
		WithObjects(objs...)
	return &DatabaseReconciler{
		Client:        builder.Build(),
		Scheme:        scheme,
		Recorder:      record.NewFakeRecorder(20),
		ControlClient: control,
	}
}

func readyClusterForDB() *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Status.CurrentPrimary = testPrimary
	return cluster
}

func newDatabase(mutate func(*mysqlv1alpha1.Database)) *mysqlv1alpha1.Database {
	db := &mysqlv1alpha1.Database{
		ObjectMeta: metav1.ObjectMeta{Name: "appdb", Namespace: "default"},
		Spec: mysqlv1alpha1.DatabaseSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: "demo"},
			Name:    appName,
			Ensure:  mysqlv1alpha1.EnsurePresent,
		},
	}
	if mutate != nil {
		mutate(db)
	}
	return db
}

func dbRequest(db *mysqlv1alpha1.Database) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: db.Namespace, Name: db.Name}}
}

func reconcileToApplied(t *testing.T, r *DatabaseReconciler, db *mysqlv1alpha1.Database) *mysqlv1alpha1.Database {
	t.Helper()
	// First pass adds the finalizer and requeues.
	if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	// Second pass performs the actual reconciliation.
	if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	got := &mysqlv1alpha1.Database{}
	if err := r.Get(context.Background(), dbRequest(db).NamespacedName, got); err != nil {
		t.Fatalf("get database: %v", err)
	}
	return got
}

func TestDatabaseReconcileCreatesSchema(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	db := newDatabase(nil)
	r := databaseReconciler(t, control, readyClusterForDB(), db)

	got := reconcileToApplied(t, r, db)

	if len(control.createdDatabase) != 1 || control.createdDatabase[0].Name != appName {
		t.Fatalf("createdDatabase = %+v, want [app]", control.createdDatabase)
	}
	if got.Status.Applied == nil || !*got.Status.Applied {
		t.Errorf("status.applied = %+v, want true", got.Status.Applied)
	}
	if !controllerutil.ContainsFinalizer(got, databaseFinalizer) {
		t.Errorf("finalizer not set: %+v", got.Finalizers)
	}
}

func TestDatabaseReconcileBlocksWhenClusterMissing(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	db := newDatabase(nil)
	r := databaseReconciler(t, control, db) // no cluster

	if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := &mysqlv1alpha1.Database{}
	if err := r.Get(context.Background(), dbRequest(db).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Applied == nil || *got.Status.Applied {
		t.Errorf("status.applied = %+v, want false", got.Status.Applied)
	}
	if len(control.createdDatabase) != 0 {
		t.Errorf("createdDatabase = %+v, want none", control.createdDatabase)
	}
}

func TestDatabaseReconcileCreatesUserWithGrants(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-pw", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("s3cret")},
	}
	db := newDatabase(func(d *mysqlv1alpha1.Database) {
		d.Spec.Users = []mysqlv1alpha1.DatabaseUser{{
			Name:           appName,
			Host:           "%",
			Ensure:         mysqlv1alpha1.EnsurePresent,
			PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "app-pw", Key: "password"},
			Grants:         []mysqlv1alpha1.DatabaseGrant{{Privileges: []string{"SELECT"}}},
		}}
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db, secret)

	got := reconcileToApplied(t, r, db)

	if len(control.created) != 1 {
		t.Fatalf("created users = %d, want 1", len(control.created))
	}
	cu := control.created[0]
	if cu.Name != appName || cu.Password != "s3cret" {
		t.Errorf("unexpected create user: %+v", cu)
	}
	// Grant target defaulted to the managed schema.
	if len(cu.Privileges) != 1 || cu.Privileges[0].On != "app.*" {
		t.Errorf("grant target not defaulted to app.*: %+v", cu.Privileges)
	}
	if got.Status.PasswordStatus["app@%"] == "" {
		t.Errorf("password version not recorded: %+v", got.Status.PasswordStatus)
	}
}

func TestDatabaseReconcileNoUserChangeWhenMatching(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{
			Name: appName, Host: "%",
			Grants: []string{"GRANT SELECT ON `app`.* TO `app`@`%`"},
		}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-pw", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("s3cret")},
	}
	db := newDatabase(func(d *mysqlv1alpha1.Database) {
		d.Spec.Users = []mysqlv1alpha1.DatabaseUser{{
			Name:           appName,
			Host:           "%",
			Ensure:         mysqlv1alpha1.EnsurePresent,
			PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "app-pw", Key: "password"},
			Grants:         []mysqlv1alpha1.DatabaseGrant{{Privileges: []string{"SELECT"}}},
		}}
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db, secret)

	// Two full reconciles; after the password is recorded, the second pass must
	// not re-alter the matching user.
	reconcileToApplied(t, r, db)
	control.altered = nil
	if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
		t.Fatal(err)
	}
	if len(control.altered) != 0 {
		t.Fatalf("altered = %+v, want none on steady state", control.altered)
	}
	if len(control.created) != 0 {
		t.Fatalf("created = %+v, want none (user already exists)", control.created)
	}
}

func TestDatabaseReconcileReclaimDeleteDropsSchema(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	now := metav1.Now()
	db := newDatabase(func(d *mysqlv1alpha1.Database) {
		d.Spec.ReclaimPolicy = "delete"
		d.Finalizers = []string{databaseFinalizer}
		d.DeletionTimestamp = &now
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db)

	if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.droppedDatabase) != 1 || control.droppedDatabase[0].Name != appName {
		t.Fatalf("droppedDatabase = %+v, want [app]", control.droppedDatabase)
	}
}

func TestDatabaseReconcileReclaimRetainKeepsSchema(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	now := metav1.Now()
	db := newDatabase(func(d *mysqlv1alpha1.Database) {
		d.Spec.ReclaimPolicy = "retain"
		d.Finalizers = []string{databaseFinalizer}
		d.DeletionTimestamp = &now
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db)

	if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.droppedDatabase) != 0 {
		t.Fatalf("droppedDatabase = %+v, want none on retain", control.droppedDatabase)
	}
}

func TestDatabaseReconcileEnsureAbsentDropsSchema(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	db := newDatabase(func(d *mysqlv1alpha1.Database) {
		d.Spec.Ensure = mysqlv1alpha1.EnsureAbsent
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db)

	reconcileToApplied(t, r, db)

	if len(control.droppedDatabase) != 1 || control.droppedDatabase[0].Name != appName {
		t.Fatalf("droppedDatabase = %+v, want [app]", control.droppedDatabase)
	}
	if len(control.createdDatabase) != 0 {
		t.Fatalf("createdDatabase = %+v, want none", control.createdDatabase)
	}
}
