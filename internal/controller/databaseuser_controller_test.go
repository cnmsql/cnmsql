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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
)

const testUserName = "tenant"

func databaseUserReconciler(t *testing.T, control *recordingControlClient, recorder record.EventRecorder, objs ...client.Object) *DatabaseUserReconciler {
	t.Helper()
	scheme := testScheme(t)
	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.DatabaseUser{}).
		WithObjects(objs...)
	return &DatabaseUserReconciler{
		Client:        builder.Build(),
		Scheme:        scheme,
		Recorder:      recorder,
		ControlClient: control,
	}
}

func userPasswordSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "user-pw", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("s3cret")},
	}
}

func newDatabaseUser(mutate func(*mysqlv1alpha1.DatabaseUser)) *mysqlv1alpha1.DatabaseUser {
	du := &mysqlv1alpha1.DatabaseUser{
		ObjectMeta: metav1.ObjectMeta{Name: testUserName, Namespace: "default"},
		Spec: mysqlv1alpha1.DatabaseUserSpec{
			Cluster:        mysqlv1alpha1.LocalObjectReference{Name: scheduledTestCluster},
			Name:           testUserName,
			Ensure:         mysqlv1alpha1.EnsurePresent,
			PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "user-pw", Key: "password"},
			Grants:         []mysqlv1alpha1.DatabaseUserGrant{{Privileges: []string{"SELECT"}, On: "app.*"}},
		},
	}
	if mutate != nil {
		mutate(du)
	}
	return du
}

func userRequest(du *mysqlv1alpha1.DatabaseUser) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: du.Namespace, Name: du.Name}}
}

func getDatabaseUser(t *testing.T, r *DatabaseUserReconciler, du *mysqlv1alpha1.DatabaseUser) *mysqlv1alpha1.DatabaseUser {
	t.Helper()
	got := &mysqlv1alpha1.DatabaseUser{}
	if err := r.Get(context.Background(), userRequest(du).NamespacedName, got); err != nil {
		t.Fatalf("get databaseuser: %v", err)
	}
	return got
}

// reconcileUserToApplied runs the finalizer pass and the work pass.
func reconcileUserToApplied(t *testing.T, r *DatabaseUserReconciler, du *mysqlv1alpha1.DatabaseUser) *mysqlv1alpha1.DatabaseUser {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	return getDatabaseUser(t, r, du)
}

func TestDatabaseUserCreates(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	du := newDatabaseUser(nil)
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	got := reconcileUserToApplied(t, r, du)

	if len(control.created) != 1 {
		t.Fatalf("created users = %d, want 1", len(control.created))
	}
	cu := control.created[0]
	if cu.Name != testUserName || cu.Host != "%" || cu.Password != "s3cret" {
		t.Errorf("unexpected create: %+v", cu)
	}
	if len(cu.Privileges) != 1 || cu.Privileges[0].On != "app.*" {
		t.Errorf("grant target not preserved: %+v", cu.Privileges)
	}
	if got.Status.Applied == nil || !*got.Status.Applied {
		t.Errorf("status.applied = %+v, want true", got.Status.Applied)
	}
	if got.Status.PasswordSecretResourceVersion == "" {
		t.Errorf("password version not recorded")
	}
}

func TestDatabaseUserCreatesWithRevokes(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.Grants = []mysqlv1alpha1.DatabaseUserGrant{
			{Privileges: mysqlv1alpha1.SafeDBaaSAdminPrivileges(), On: "*.*"},
		}
		d.Spec.Revokes = mysqlv1alpha1.SafeDBaaSAdminRevokes()
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	reconcileUserToApplied(t, r, du)

	if len(control.created) != 1 {
		t.Fatalf("created = %d, want 1", len(control.created))
	}
	cu := control.created[0]
	if len(cu.Revokes) == 0 {
		t.Fatalf("revokes not forwarded to control client: %+v", cu)
	}
	if cu.Revokes[0].On != "mysql.*" {
		t.Errorf("first revoke target = %q, want mysql.*", cu.Revokes[0].On)
	}
}

func TestDatabaseUserNoChangeWhenMatching(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{
			Name: testUserName, Host: "%",
			Grants: []string{"GRANT SELECT ON `app`.* TO `tenant`@`%`"},
		}},
	}
	du := newDatabaseUser(nil)
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	// Adopt path is not relevant here: the user pre-exists, but we steer the test
	// through adoption by annotating, then assert steady state makes no changes.
	du.Annotations = map[string]string{mysqlv1alpha1.DatabaseUserAdoptAnnotation: "true"}
	if err := r.Update(context.Background(), du); err != nil {
		t.Fatal(err)
	}
	reconcileUserToApplied(t, r, du)
	control.altered = nil
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if len(control.altered) != 0 {
		t.Fatalf("altered = %+v, want none on steady state", control.altered)
	}
	if len(control.created) != 0 {
		t.Fatalf("created = %+v, want none (user already exists)", control.created)
	}
}

func TestDatabaseUserRevokeOnlyChangeDetected(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{
			Name: testUserName,
			Host: "%",
			Grants: []string{
				"GRANT SELECT, INSERT, UPDATE ON *.* TO `tenant`@`%`",
				"REVOKE INSERT ON `mysql`.* FROM `tenant`@`%`",
			},
		}},
	}
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.Grants = []mysqlv1alpha1.DatabaseUserGrant{
			{Privileges: []string{"SELECT", "INSERT", "UPDATE"}, On: "*.*"},
		}
		d.Spec.Revokes = []mysqlv1alpha1.DatabaseUserRevoke{
			{Privileges: []string{"INSERT"}, On: "mysql.*"},
		}
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	// Adopt + first reconcile to get applied status.
	du.Annotations = map[string]string{mysqlv1alpha1.DatabaseUserAdoptAnnotation: "true"}
	if err := r.Update(context.Background(), du); err != nil {
		t.Fatal(err)
	}
	reconcileUserToApplied(t, r, du)
	control.altered = nil

	// Re-fetch after the status patch bumped the resourceVersion.
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: du.Namespace, Name: du.Name}, du); err != nil {
		t.Fatal(err)
	}

	// Now change the revokes to add another system-schema carve-out (sys.*).
	du.Spec.Revokes = append(du.Spec.Revokes, mysqlv1alpha1.DatabaseUserRevoke{
		Privileges: []string{"INSERT", "UPDATE"}, On: "sys.*",
	})
	if err := r.Update(context.Background(), du); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}

	if len(control.altered) == 0 {
		t.Fatal("no alter produced when revokes changed")
	}
	alt := control.altered[0]
	if alt.Revokes == nil {
		t.Fatal("revokes not sent in alter")
	}
	if len(*alt.Revokes) < 2 {
		t.Fatalf("revokes = %d entries, want at least 2", len(*alt.Revokes))
	}
}

func TestDatabaseUserPasswordRotation(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{
			Name: testUserName, Host: "%",
			Grants: []string{"GRANT SELECT ON `app`.* TO `tenant`@`%`"},
		}},
	}
	secret := userPasswordSecret()
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Annotations = map[string]string{mysqlv1alpha1.DatabaseUserAdoptAnnotation: "true"}
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, secret)

	reconcileUserToApplied(t, r, du)
	control.altered = nil

	// Bump the Secret resourceVersion; the next reconcile must re-apply the password.
	latest := &corev1.Secret{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "user-pw"}, latest); err != nil {
		t.Fatal(err)
	}
	latest.Data["password"] = []byte("rotated")
	if err := r.Update(context.Background(), latest); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if len(control.altered) != 1 || control.altered[0].Password == nil || *control.altered[0].Password != "rotated" {
		t.Fatalf("altered = %+v, want one password rotation", control.altered)
	}
}

func TestDatabaseUserEnsureAbsentDrops(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{Name: testUserName, Host: "%"}},
	}
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.Ensure = mysqlv1alpha1.EnsureAbsent
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	reconcileUserToApplied(t, r, du)

	if len(control.dropped) != 1 || control.dropped[0].Name != testUserName {
		t.Fatalf("dropped = %+v, want [tenant]", control.dropped)
	}
	if len(control.created) != 0 {
		t.Fatalf("created = %+v, want none", control.created)
	}
}

func TestDatabaseUserReclaimDeleteDropsUser(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	now := metav1.Now()
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.ReclaimPolicy = "delete"
		d.Finalizers = []string{databaseUserFinalizer}
		d.DeletionTimestamp = &now
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.dropped) != 1 || control.dropped[0].Name != testUserName {
		t.Fatalf("dropped = %+v, want [tenant]", control.dropped)
	}
}

func TestDatabaseUserReclaimRetainKeepsUser(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	now := metav1.Now()
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.ReclaimPolicy = "retain"
		d.Finalizers = []string{databaseUserFinalizer}
		d.DeletionTimestamp = &now
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.dropped) != 0 {
		t.Fatalf("dropped = %+v, want none on retain", control.dropped)
	}
}

func TestDatabaseUserClusterMissing(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	du := newDatabaseUser(nil)
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), du, userPasswordSecret()) // no cluster

	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := getDatabaseUser(t, r, du)
	if got.Status.Applied != nil {
		t.Errorf("status.applied = %+v, want nil (never applied)", got.Status.Applied)
	}
	if len(control.created) != 0 {
		t.Errorf("created = %+v, want none", control.created)
	}
}

func TestDatabaseUserConflictRefusesUnownedAccount(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{Name: testUserName, Host: "%"}}, // pre-existing, not ours
	}
	rec := record.NewFakeRecorder(20)
	du := newDatabaseUser(nil)
	r := databaseUserReconciler(t, control, rec, readyClusterForDB(), du, userPasswordSecret())

	// finalizer pass, then work pass that should detect the conflict.
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 0 || len(control.altered) != 0 {
		t.Fatalf("MySQL was mutated on conflict: created=%+v altered=%+v", control.created, control.altered)
	}
	got := getDatabaseUser(t, r, du)
	if got.Status.Applied != nil {
		t.Errorf("status.applied = %+v, want nil on conflict", got.Status.Applied)
	}
	if !drainEvents(rec.Events, "UserConflict") {
		t.Errorf("expected a UserConflict event")
	}
}

func TestDatabaseUserAdoptsWithAnnotation(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{Name: testUserName, Host: "%"}}, // pre-existing
	}
	rec := record.NewFakeRecorder(20)
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Annotations = map[string]string{mysqlv1alpha1.DatabaseUserAdoptAnnotation: "true"}
	})
	r := databaseUserReconciler(t, control, rec, readyClusterForDB(), du, userPasswordSecret())

	got := reconcileUserToApplied(t, r, du)

	if len(control.created) != 0 {
		t.Fatalf("created = %+v, want none (adopted existing)", control.created)
	}
	if len(control.altered) != 1 {
		t.Fatalf("altered = %+v, want one (adopt reconciles grants/password)", control.altered)
	}
	if got.Status.Applied == nil || !*got.Status.Applied {
		t.Errorf("status.applied = %+v, want true after adoption", got.Status.Applied)
	}
	if !drainEvents(rec.Events, "Adopted") {
		t.Errorf("expected an Adopted event")
	}
}

func TestDatabaseUserValidationRejectsDeniedPrivilege(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.Grants = []mysqlv1alpha1.DatabaseUserGrant{{
			Privileges: []string{"REPLICATION_SLAVE_ADMIN"}, On: "*.*",
		}}
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	reconcileUserToApplied(t, r, du)

	if len(control.created) != 0 {
		t.Fatalf("created = %+v, want none for invalid user", control.created)
	}
	got := getDatabaseUser(t, r, du)
	if got.Status.Applied != nil {
		t.Errorf("status.applied = %+v, want nil for invalid user", got.Status.Applied)
	}
}
