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

func databaseUserReconciler(t *testing.T, control InstanceControlClient, recorder record.EventRecorder, objs ...client.Object) *DatabaseUserReconciler {
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

func TestDatabaseUserDriftDetectionDisabledStopsReapply(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.DriftDetection = new(false)
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	reconcileUserToApplied(t, r, du) // created: 1

	// Nothing changed: the reconcile must be a no-op and must not self-requeue.
	result, err := r.Reconcile(context.Background(), userRequest(du))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.created) != 1 {
		t.Fatalf("created = %d, want 1 (no re-apply)", len(control.created))
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %s, want 0 (no timed requeue)", result.RequeueAfter)
	}
}

func TestDatabaseUserDriftDetectionDisabledReappliesOnSecretChange(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	secret := userPasswordSecret()
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.DriftDetection = new(false)
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, secret)

	reconcileUserToApplied(t, r, du) // created: 1

	// Rotate the password Secret: the recorded resourceVersion no longer matches,
	// so the user must be re-applied even with drift detection disabled.
	secret.Data["password"] = []byte("rotated")
	if err := r.Update(context.Background(), secret); err != nil {
		t.Fatalf("update secret: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.created) != 2 {
		t.Fatalf("created = %d, want 2 (re-applied on secret change)", len(control.created))
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

// An account can already hold ALL PRIVILEGES on *.* from a plain grant, so the
// grants diff sees nothing to do; the missing grant option still has to be
// applied.
func TestDatabaseUserSuperuserAppliedWhenGrantsUnchanged(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{
			Name: testUserName, Host: "%",
			Grants: []string{"GRANT ALL PRIVILEGES ON *.* TO `tenant`@`%`"},
		}},
	}
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.Superuser = true
		d.Spec.Grants = nil
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	du.Annotations = map[string]string{mysqlv1alpha1.DatabaseUserAdoptAnnotation: "true"}
	if err := r.Update(context.Background(), du); err != nil {
		t.Fatal(err)
	}
	reconcileUserToApplied(t, r, du)
	// Drop the alters driven by the initial password apply: the next pass has to
	// stand on the superuser drift alone.
	control.altered = nil
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}

	if len(control.altered) == 0 {
		t.Fatal("no alter produced when the superuser bit flipped")
	}
	alt := control.altered[0]
	if alt.Superuser == nil || !*alt.Superuser {
		t.Fatalf("alter.Superuser = %v, want true", alt.Superuser)
	}
}

// The counterpart: an account that already carries the superuser grant is left
// alone, so the fix does not turn steady state into an alter loop.
func TestDatabaseUserSuperuserNoChangeWhenGrantOptionPresent(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{
			Name: testUserName, Host: "%",
			Grants: []string{"GRANT ALL PRIVILEGES ON *.* TO `tenant`@`%` WITH GRANT OPTION"},
		}},
	}
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.Superuser = true
		d.Spec.Grants = nil
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

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
}

// SHOW GRANTS renders the superuser line differently per flavour: MySQL puts the
// grant option straight after the account, MariaDB wedges the authentication
// clause in between, and MySQL 8 prints the expansion of ALL PRIVILEGES rather
// than the token itself.
func TestDuSuperuserSatisfiedAcrossFlavours(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		observed []string
		want     bool
	}{
		{"mysql superuser", []string{"GRANT ALL PRIVILEGES ON *.* TO `tenant`@`%` WITH GRANT OPTION"}, true},
		{"mariadb superuser", []string{
			"GRANT ALL PRIVILEGES ON *.* TO `tenant`@`%` IDENTIFIED VIA mysql_native_password " +
				"USING '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9' WITH GRANT OPTION",
		}, true},
		{"mariadb pre-10.4 superuser", []string{
			"GRANT ALL PRIVILEGES ON *.* TO 'tenant'@'%' IDENTIFIED BY PASSWORD " +
				"'*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9' WITH GRANT OPTION",
		}, true},
		{"mysql 8+ expanded superuser", []string{
			"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, " +
				"FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, " +
				"LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, " +
				"SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, " +
				"CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `tenant`@`%` WITH GRANT OPTION",
		}, true},
		{"mysql 8+ expanded without grant option", []string{
			"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, " +
				"FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, " +
				"LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, " +
				"SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, " +
				"CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `tenant`@`%`",
		}, false},
		// A partial out-of-band revoke leaves most of the expansion in place. It
		// still has to read as drift, so the whole static set is required and no
		// single privilege can stand in for it.
		{"mysql 8+ expanded minus one privilege", []string{
			"GRANT INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, " +
				"FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, " +
				"LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, " +
				"SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, " +
				"CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `tenant`@`%` WITH GRANT OPTION",
		}, false},
		{"super alone with grant option", []string{
			"GRANT SUPER ON *.* TO `tenant`@`%` WITH GRANT OPTION",
		}, false},
		// MySQL 8 prints the dynamic privileges on a second line; neither line on
		// its own is the whole grant, and the static one is what settles it.
		{"mysql 8+ expanded with dynamic privileges line", []string{
			"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, " +
				"FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, " +
				"LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, " +
				"SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, " +
				"CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `tenant`@`%` WITH GRANT OPTION",
			"GRANT APPLICATION_PASSWORD_ADMIN,AUDIT_ABORT_EXEMPT,AUDIT_ADMIN,BACKUP_ADMIN," +
				"BINLOG_ADMIN,CLONE_ADMIN,CONNECTION_ADMIN,SYSTEM_USER,SYSTEM_VARIABLES_ADMIN " +
				"ON *.* TO `tenant`@`%` WITH GRANT OPTION",
		}, true},
		{"all privileges without grant option", []string{"GRANT ALL PRIVILEGES ON *.* TO `tenant`@`%`"}, false},
		{"grant option on a schema only", []string{"GRANT SELECT ON `app`.* TO `tenant`@`%` WITH GRANT OPTION"}, false},
		{"usage only", []string{"GRANT USAGE ON *.* TO `tenant`@`%`"}, false},
		{"no grants", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := duSuperuserSatisfied(tc.observed); got != tc.want {
				t.Fatalf("duSuperuserSatisfied = %v, want %v", got, tc.want)
			}
		})
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
		d.Status.Account = "tenant@%"
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
		d.Status.Account = "tenant@%"
	})
	r := databaseUserReconciler(t, control, record.NewFakeRecorder(20), readyClusterForDB(), du, userPasswordSecret())

	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.dropped) != 1 || control.dropped[0].Name != testUserName {
		t.Fatalf("dropped = %+v, want [tenant]", control.dropped)
	}
}

// Reclaim drops only the account this DatabaseUser owns: one renamed onto
// another account before it was ever applied must not drop that account
// (GHSA-rfv8-v7q9-cp69).
func TestDatabaseUserReclaimDeleteKeepsUnownedAccount(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{users: []user.UserInfo{{Name: "victim", Host: "%"}}}
	rec := record.NewFakeRecorder(20)
	now := metav1.Now()
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.Name = "victim"
		d.Spec.ReclaimPolicy = "delete"
		d.Finalizers = []string{databaseUserFinalizer}
		d.DeletionTimestamp = &now
		d.Status.Account = "tenant@%"
	})
	r := databaseUserReconciler(t, control, rec, readyClusterForDB(), du, userPasswordSecret())

	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(control.dropped) != 0 {
		t.Fatalf("dropped = %+v, want none for an unowned account", control.dropped)
	}
	if !drainEvents(rec.Events, "UserNotReclaimed") {
		t.Errorf("expected a UserNotReclaimed event")
	}
}

// ensure: absent must not drop an account the DatabaseUser does not own.
func TestDatabaseUserEnsureAbsentRefusesUnownedAccount(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{users: []user.UserInfo{{Name: testUserName, Host: "%"}}}
	rec := record.NewFakeRecorder(20)
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Spec.Ensure = mysqlv1alpha1.EnsureAbsent
	})
	r := databaseUserReconciler(t, control, rec, readyClusterForDB(), du, userPasswordSecret())

	reconcileUserToApplied(t, r, du)

	if len(control.dropped) != 0 {
		t.Fatalf("dropped = %+v, want none for an unowned account", control.dropped)
	}
	if !drainEvents(rec.Events, "UserConflict") {
		t.Errorf("expected a UserConflict event")
	}
}

// An applied DatabaseUser renamed onto another existing account must not take
// it over: ownership stays with the account it applied (GHSA-rfv8-v7q9-cp69).
func TestDatabaseUserRenameDoesNotTakeOverAccount(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{users: []user.UserInfo{
		{Name: testUserName, Host: "%"},
		{Name: "victim", Host: "%"},
	}}
	rec := record.NewFakeRecorder(20)
	applied := true
	du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
		d.Finalizers = []string{databaseUserFinalizer}
		d.Status.Applied = &applied
		d.Status.Account = "tenant@%"
		d.Spec.Name = "victim"
	})
	r := databaseUserReconciler(t, control, rec, readyClusterForDB(), du, userPasswordSecret())

	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if len(control.altered)+len(control.created)+len(control.dropped) != 0 {
		t.Fatalf("renamed DatabaseUser touched MySQL: altered=%+v created=%+v dropped=%+v",
			control.altered, control.created, control.dropped)
	}
	if !drainEvents(rec.Events, "UserConflict") {
		t.Errorf("expected a UserConflict event")
	}
}

// A DatabaseUser applied before status.account existed keeps its account on
// upgrade, but only when its Ready condition covers the current generation.
func TestDatabaseUserRecordsLegacyAccount(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		readyGeneration int64
		wantOwned       bool
	}{
		"applied at current generation": {readyGeneration: 2, wantOwned: true},
		"spec changed since last apply": {readyGeneration: 1, wantOwned: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			control := &recordingControlClient{users: []user.UserInfo{{Name: testUserName, Host: "%"}}}
			rec := record.NewFakeRecorder(20)
			applied := true
			du := newDatabaseUser(func(d *mysqlv1alpha1.DatabaseUser) {
				d.Generation = 2
				d.Finalizers = []string{databaseUserFinalizer}
				d.Status.Applied = &applied
				d.Status.Conditions = []metav1.Condition{{
					Type: mysqlv1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "Reconciled",
					ObservedGeneration: tc.readyGeneration, LastTransitionTime: metav1.Now(),
				}}
			})
			r := databaseUserReconciler(t, control, rec, readyClusterForDB(), du, userPasswordSecret())

			if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
				t.Fatal(err)
			}
			got := getDatabaseUser(t, r, du)
			if owned := got.Status.Account == "tenant@%"; owned != tc.wantOwned {
				t.Fatalf("status.account = %q, want owned=%v", got.Status.Account, tc.wantOwned)
			}
			if conflict := drainEvents(rec.Events, "UserConflict"); conflict == tc.wantOwned {
				t.Errorf("UserConflict event = %v, want %v", conflict, !tc.wantOwned)
			}
		})
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

// lostCreateControlClient creates the account on the instance but reports the
// create as failed, like a /user/create that times out after MySQL ran it.
type lostCreateControlClient struct {
	*recordingControlClient
	onCreate func()
}

func (c *lostCreateControlClient) CreateUser(ctx context.Context, cluster *mysqlv1alpha1.Cluster, instance string, req user.CreateUserRequest) error {
	if c.onCreate != nil {
		c.onCreate()
	}
	_ = c.recordingControlClient.CreateUser(ctx, cluster, instance, req)
	c.users = append(c.users, user.UserInfo{Name: req.Name, Host: req.Host})
	return context.DeadlineExceeded
}

func TestDatabaseUserRecoversAccountFromTimedOutCreate(t *testing.T) {
	t.Parallel()
	control := &lostCreateControlClient{recordingControlClient: &recordingControlClient{}}
	rec := record.NewFakeRecorder(20)
	du := newDatabaseUser(nil)
	r := databaseUserReconciler(t, control, rec, readyClusterForDB(), du, userPasswordSecret())

	// The marker must be persisted before the request leaves, or a crash or
	// timeout right after sending it would lose it.
	control.onCreate = func() {
		if got := getDatabaseUser(t, r, du).Status.PendingCreate; got != "tenant@%" {
			t.Errorf("status.pendingCreate at create time = %q, want %q", got, "tenant@%")
		}
	}

	// Finalizer pass, then the create that times out after landing.
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 1 {
		t.Fatalf("created = %d, want 1", len(control.created))
	}
	got := getDatabaseUser(t, r, du)
	if got.Status.Applied != nil {
		t.Fatalf("status.applied = %v after a failed create, want nil", *got.Status.Applied)
	}
	if got.Status.PendingCreate != "tenant@%" {
		t.Fatalf("status.pendingCreate = %q after a failed create, want it kept", got.Status.PendingCreate)
	}
	drainEvents(rec.Events, "")

	// The next pass finds the account and must take it as its own.
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	got = getDatabaseUser(t, r, du)
	if got.Status.Applied == nil || !*got.Status.Applied {
		t.Fatalf("status.applied = %+v, want true; ready condition: %+v", got.Status.Applied, got.Status.Conditions)
	}
	if got.Status.PendingCreate != "" {
		t.Errorf("status.pendingCreate = %q after apply, want cleared", got.Status.PendingCreate)
	}
	if got.Status.Account != "tenant@%" {
		t.Errorf("status.account = %q after apply, want %q", got.Status.Account, "tenant@%")
	}
	if got.Status.PasswordSecretResourceVersion == "" {
		t.Errorf("password version not recorded")
	}
	if len(control.created) != 1 {
		t.Errorf("created = %d, want no second create", len(control.created))
	}
	// The password and grants of an unconfirmed create are re-applied.
	if len(control.altered) != 1 {
		t.Fatalf("altered = %+v, want one", control.altered)
	}
	if a := control.altered[0]; a.Password == nil || *a.Password != "s3cret" || a.Privileges == nil {
		t.Errorf("alter did not re-apply password and grants: %+v", a)
	}
	events := []string{}
	for len(rec.Events) > 0 {
		events = append(events, <-rec.Events)
	}
	for _, e := range events {
		if strings.Contains(e, "UserConflict") || strings.Contains(e, "Adopted") {
			t.Errorf("unexpected event %q for an account we created", e)
		}
	}
}

func TestDatabaseUserPendingCreateForOtherAccountStillConflicts(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{Name: testUserName, Host: "%"}}, // pre-existing, not ours
	}
	rec := record.NewFakeRecorder(20)
	du := newDatabaseUser(nil)
	// A create was once sent for another host (spec.host changed since).
	du.Status.PendingCreate = testUserName + "@10.0.0.%"
	r := databaseUserReconciler(t, control, rec, readyClusterForDB(), du, userPasswordSecret())

	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), userRequest(du)); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 0 || len(control.altered) != 0 {
		t.Fatalf("MySQL was mutated on conflict: created=%+v altered=%+v", control.created, control.altered)
	}
	if got := getDatabaseUser(t, r, du); got.Status.Applied != nil {
		t.Errorf("status.applied = %+v, want nil on conflict", got.Status.Applied)
	}
	if !drainEvents(rec.Events, "UserConflict") {
		t.Errorf("expected a UserConflict event")
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
