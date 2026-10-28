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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
)

func databaseReconciler(t *testing.T, control InstanceControlClient, objs ...client.Object) *DatabaseReconciler {
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
		d.Spec.Users = []mysqlv1alpha1.InlineUser{{
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
		d.Spec.Users = []mysqlv1alpha1.InlineUser{{
			Name:           appName,
			Host:           "%",
			Ensure:         mysqlv1alpha1.EnsurePresent,
			PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "app-pw", Key: "password"},
			Grants:         []mysqlv1alpha1.DatabaseGrant{{Privileges: []string{"SELECT"}}},
		}}
		// The account is this Database's own, its password not applied yet.
		d.Status.PasswordStatus = map[string]string{"app@%": ""}
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db, secret)

	// Two full reconciles; after the password is recorded, the second pass must
	// not re-alter the matching user.
	if got := reconcileToApplied(t, r, db); got.Status.Applied == nil || !*got.Status.Applied {
		t.Fatalf("status.applied = %+v, want true; message %q", got.Status.Applied, got.Status.Message)
	}
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

func TestDatabaseDriftDetectionDefaultReapplies(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	db := newDatabase(nil) // DriftDetection unset == enabled
	r := databaseReconciler(t, control, readyClusterForDB(), db)

	reconcileToApplied(t, r, db) // createdDatabase: 1

	// A subsequent reconcile with drift detection on re-asserts the schema and
	// keeps self-requeuing on the resync timer.
	result, err := r.Reconcile(context.Background(), dbRequest(db))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.createdDatabase) != 2 {
		t.Fatalf("createdDatabase = %d, want 2 (re-applied)", len(control.createdDatabase))
	}
	if result.RequeueAfter != readyResync {
		t.Fatalf("RequeueAfter = %s, want %s", result.RequeueAfter, readyResync)
	}
}

func TestDatabaseDriftDetectionDisabledStopsReapply(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	db := newDatabase(func(d *mysqlv1alpha1.Database) {
		d.Spec.DriftDetection = new(false)
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db)

	reconcileToApplied(t, r, db) // createdDatabase: 1

	// Nothing changed: the reconcile must be a no-op and must not self-requeue.
	result, err := r.Reconcile(context.Background(), dbRequest(db))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.createdDatabase) != 1 {
		t.Fatalf("createdDatabase = %d, want 1 (no re-apply)", len(control.createdDatabase))
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %s, want 0 (no timed requeue)", result.RequeueAfter)
	}
}

func TestDatabaseDriftDetectionDisabledReappliesOnSecretChange(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-pw", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("s3cret")},
	}
	db := newDatabase(func(d *mysqlv1alpha1.Database) {
		d.Spec.DriftDetection = new(false)
		d.Spec.Users = []mysqlv1alpha1.InlineUser{{
			Name:           appName,
			Host:           "%",
			Ensure:         mysqlv1alpha1.EnsurePresent,
			PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "app-pw", Key: "password"},
		}}
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db, secret)

	reconcileToApplied(t, r, db) // createdDatabase: 1

	// Rotate the password Secret: the recorded resourceVersion no longer matches,
	// so the database must be re-applied even with drift detection disabled.
	secret.Data["password"] = []byte("rotated")
	if err := r.Update(context.Background(), secret); err != nil {
		t.Fatalf("update secret: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.createdDatabase) != 2 {
		t.Fatalf("createdDatabase = %d, want 2 (re-applied on secret change)", len(control.createdDatabase))
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

// A Database stored before the CRD rejected reserved inline user names must
// still never reach the operator's own accounts.
func TestDatabaseReconcileRefusesReservedInlineUser(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{Name: "cnmsql_dump", Host: "localhost"}},
	}
	db := newDatabase(func(d *mysqlv1alpha1.Database) {
		d.Spec.Users = []mysqlv1alpha1.InlineUser{{
			Name:   "cnmsql_dump",
			Host:   "localhost",
			Ensure: mysqlv1alpha1.EnsureAbsent,
		}}
	})
	r := databaseReconciler(t, control, readyClusterForDB(), db)

	for range 2 {
		if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	if len(control.dropped)+len(control.created)+len(control.altered) != 0 {
		t.Fatalf("reserved user touched: dropped=%+v created=%+v altered=%+v",
			control.dropped, control.created, control.altered)
	}
	got := &mysqlv1alpha1.Database{}
	if err := r.Get(context.Background(), dbRequest(db).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Applied == nil || *got.Status.Applied {
		t.Errorf("status.applied = %+v, want false", got.Status.Applied)
	}
	if !strings.Contains(got.Status.Message, "reserved") {
		t.Errorf("status.message = %q, want it to mention the reserved name", got.Status.Message)
	}
}

// A Database stored before the webhook check (or admitted with webhooks off)
// must not reach MySQL with grants outside its schema (GHSA-jpxg-rv6j-7mf4).
func TestDatabaseReconcileRefusesInvalidGrants(t *testing.T) {
	t.Parallel()
	for name, grant := range map[string]mysqlv1alpha1.DatabaseGrant{
		"ALL on *.*":       {Privileges: []string{"ALL"}, On: "*.*"},
		"injected grantee": {Privileges: []string{"SELECT"}, On: "app.* TO 'other'@'%' WITH GRANT OPTION #"},
		"dynamic":          {Privileges: []string{"SYSTEM_VARIABLES_ADMIN"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			control := &recordingControlClient{}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "app-pw", Namespace: "default"},
				Data:       map[string][]byte{"password": []byte("s3cret")},
			}
			db := newDatabase(func(d *mysqlv1alpha1.Database) {
				d.Spec.Users = []mysqlv1alpha1.InlineUser{{
					Name:           appName,
					Ensure:         mysqlv1alpha1.EnsurePresent,
					PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "app-pw", Key: "password"},
					Grants:         []mysqlv1alpha1.DatabaseGrant{grant},
				}}
			})
			r := databaseReconciler(t, control, readyClusterForDB(), db, secret)

			got := reconcileToApplied(t, r, db)

			if len(control.createdDatabase)+len(control.created)+len(control.altered) != 0 {
				t.Fatalf("invalid Database applied: databases=%+v created=%+v altered=%+v",
					control.createdDatabase, control.created, control.altered)
			}
			if got.Status.Applied == nil || *got.Status.Applied {
				t.Errorf("status.applied = %+v, want false", got.Status.Applied)
			}
			if !strings.Contains(got.Status.Message, "spec.users[0].grants[0]") {
				t.Errorf("status.message = %q, want it to name the grant", got.Status.Message)
			}
		})
	}
}

func inlineAppUser(ensure mysqlv1alpha1.EnsureOption) func(*mysqlv1alpha1.Database) {
	return func(d *mysqlv1alpha1.Database) {
		d.Spec.Users = []mysqlv1alpha1.InlineUser{{
			Name:           appName,
			Ensure:         ensure,
			PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "app-pw", Key: "password"},
			Grants:         []mysqlv1alpha1.DatabaseGrant{{Privileges: []string{"SELECT"}}},
		}}
	}
}

func appPasswordSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-pw", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("s3cret")},
	}
}

// An inline user naming an existing account this Database did not create
// (another tenant's, a DatabaseUser's, a managed role's) must neither alter
// nor drop it (GHSA-rfv8-v7q9-cp69).
func TestDatabaseReconcileRefusesUnownedAccount(t *testing.T) {
	t.Parallel()
	for name, ensure := range map[string]mysqlv1alpha1.EnsureOption{
		"present": mysqlv1alpha1.EnsurePresent,
		"absent":  mysqlv1alpha1.EnsureAbsent,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			control := &recordingControlClient{users: []user.UserInfo{{Name: appName, Host: "%"}}}
			rec := record.NewFakeRecorder(20)
			db := newDatabase(inlineAppUser(ensure))
			r := databaseReconciler(t, control, readyClusterForDB(), db, appPasswordSecret())
			r.Recorder = rec

			got := reconcileToApplied(t, r, db)

			if len(control.created)+len(control.altered)+len(control.dropped) != 0 {
				t.Fatalf("unowned account touched: created=%+v altered=%+v dropped=%+v",
					control.created, control.altered, control.dropped)
			}
			if got.Status.Applied == nil || *got.Status.Applied {
				t.Errorf("status.applied = %+v, want false", got.Status.Applied)
			}
			if c := apimeta.FindStatusCondition(got.Status.Conditions, mysqlv1alpha1.ConditionReady); c == nil || c.Reason != "UserConflict" {
				t.Errorf("ready condition = %+v, want reason UserConflict", c)
			}
			if !drainEvents(rec.Events, "UserConflict") {
				t.Errorf("expected a UserConflict event")
			}
		})
	}
}

// The account is claimed before the create is sent, so a create that errors
// after MySQL ran it is recovered as this Database's own on the next pass.
func TestDatabaseReconcileRecoversAccountFromTimedOutCreate(t *testing.T) {
	t.Parallel()
	control := &lostCreateControlClient{recordingControlClient: &recordingControlClient{}}
	db := newDatabase(inlineAppUser(mysqlv1alpha1.EnsurePresent))
	r := databaseReconciler(t, control, readyClusterForDB(), db, appPasswordSecret())
	control.onCreate = func() {
		got := &mysqlv1alpha1.Database{}
		if err := r.Get(context.Background(), dbRequest(db).NamespacedName, got); err != nil {
			t.Fatal(err)
		}
		if _, ok := got.Status.PasswordStatus["app@%"]; !ok {
			t.Errorf("status.passwordStatus at create time = %+v, want app@%% claimed", got.Status.PasswordStatus)
		}
	}

	reconcileToApplied(t, r, db) // finalizer, then the create that times out
	if _, err := r.Reconcile(context.Background(), dbRequest(db)); err != nil {
		t.Fatal(err)
	}

	got := &mysqlv1alpha1.Database{}
	if err := r.Get(context.Background(), dbRequest(db).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Applied == nil || !*got.Status.Applied {
		t.Fatalf("status.applied = %+v, want true; message %q", got.Status.Applied, got.Status.Message)
	}
	if len(control.created) != 1 {
		t.Errorf("created = %d, want no second create", len(control.created))
	}
	if len(control.altered) != 1 || control.altered[0].Password == nil || *control.altered[0].Password != "s3cret" {
		t.Errorf("altered = %+v, want the password re-applied once", control.altered)
	}
	if got.Status.PasswordStatus["app@%"] == "" {
		t.Errorf("password version not recorded: %+v", got.Status.PasswordStatus)
	}
}
