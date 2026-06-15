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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
)

func managedRolesReconciler(t *testing.T, control *recordingControlClient, objs ...any) (*ClusterReconciler, *mysqlv1alpha1.Cluster) {
	t.Helper()
	scheme := testScheme(t)
	cluster := baseCluster()
	cluster.Status.CurrentPrimary = testPrimary

	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster)
	for _, o := range objs {
		if secret, ok := o.(*corev1.Secret); ok {
			builder = builder.WithObjects(secret)
		}
	}
	return &ClusterReconciler{
		Client:        builder.Build(),
		Scheme:        scheme,
		Recorder:      record.NewFakeRecorder(20),
		ControlClient: control,
	}, cluster
}

func TestReconcileManagedRolesCreatesMissingUserWithGeneratedPassword(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	r, cluster := managedRolesReconciler(t, control)
	cluster.Spec.Managed = &mysqlv1alpha1.ManagedConfiguration{Roles: []mysqlv1alpha1.RoleConfiguration{
		{Name: appName, Host: "%", Ensure: mysqlv1alpha1.EnsurePresent,
			Privileges: []mysqlv1alpha1.RolePrivilege{{Privileges: []string{"SELECT"}, On: "app.*"}}},
	}}

	if err := r.reconcileManagedRoles(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 1 {
		t.Fatalf("created = %d, want 1", len(control.created))
	}
	if control.created[0].Name != appName || control.created[0].Password == "" {
		t.Errorf("unexpected create request: %+v", control.created[0])
	}

	// The operator generated and persisted a password Secret.
	secret := &corev1.Secret{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-app"}, secret); err != nil {
		t.Fatalf("generated secret not found: %v", err)
	}

	if cluster.Status.ManagedRolesStatus == nil ||
		len(cluster.Status.ManagedRolesStatus.ByStatus[mysqlv1alpha1.ManagedRoleReconciled]) != 1 {
		t.Errorf("role not marked reconciled: %+v", cluster.Status.ManagedRolesStatus)
	}
}

func TestReconcileManagedRolesDefaultsControlClient(t *testing.T) {
	t.Parallel()
	scheme := testScheme(t)
	cluster := baseCluster()
	cluster.Status.CurrentPrimary = testPrimary
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
			WithObjects(cluster).
			Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
	}
	cluster.Spec.Managed = &mysqlv1alpha1.ManagedConfiguration{Roles: []mysqlv1alpha1.RoleConfiguration{
		{Name: appName, Host: "%", Ensure: mysqlv1alpha1.EnsurePresent},
	}}

	if err := r.reconcileManagedRoles(context.Background(), cluster); err == nil {
		t.Fatal("reconcileManagedRoles returned nil error with no reachable control API")
	}
	if r.ControlClient == nil {
		t.Fatal("ControlClient was not defaulted")
	}
}

func TestReconcileManagedRolesDropsAbsentUser(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{Name: "old", Host: "%"}},
	}
	r, cluster := managedRolesReconciler(t, control)
	cluster.Spec.Managed = &mysqlv1alpha1.ManagedConfiguration{Roles: []mysqlv1alpha1.RoleConfiguration{
		{Name: "old", Host: "%", Ensure: mysqlv1alpha1.EnsureAbsent},
	}}

	if err := r.reconcileManagedRoles(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if len(control.dropped) != 1 || control.dropped[0].Name != "old" {
		t.Fatalf("dropped = %+v, want [old]", control.dropped)
	}
}

func TestReconcileManagedRolesNoChangeWhenMatching(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{
			Name: appName, Host: "%", RequireTLS: "none",
			Grants: []string{"GRANT SELECT ON `app`.* TO `app`@`%`"},
		}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pw", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("s3cret")},
	}
	r, cluster := managedRolesReconciler(t, control, secret)
	cluster.Spec.Managed = &mysqlv1alpha1.ManagedConfiguration{Roles: []mysqlv1alpha1.RoleConfiguration{
		{Name: appName, Host: "%", Ensure: mysqlv1alpha1.EnsurePresent,
			PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "pw", Key: "password"},
			Privileges:     []mysqlv1alpha1.RolePrivilege{{Privileges: []string{"SELECT"}, On: "app.*"}}},
	}}
	// Pretend the password was already applied at the secret's current version.
	cluster.Status.ManagedRolesStatus = &mysqlv1alpha1.ManagedRolesStatus{
		PasswordStatus: map[string]mysqlv1alpha1.RolePasswordState{},
	}

	// First pass applies the password (version unknown), recording the version.
	if err := r.reconcileManagedRoles(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	control.altered = nil

	// Second pass: nothing changed, no alter should be issued.
	if err := r.reconcileManagedRoles(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if len(control.altered) != 0 {
		t.Fatalf("altered = %+v, want none on steady state", control.altered)
	}
	if len(control.created) != 0 {
		t.Fatalf("created = %+v, want none", control.created)
	}
}

func TestReconcileManagedRolesAltersChangedAttributes(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{
		users: []user.UserInfo{{
			Name: appName, Host: "%", MaxUserConnections: 0, RequireTLS: "none",
			Grants: []string{"GRANT SELECT ON `app`.* TO `app`@`%`"},
		}},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pw", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("s3cret")},
	}
	r, cluster := managedRolesReconciler(t, control, secret)
	cluster.Spec.Managed = &mysqlv1alpha1.ManagedConfiguration{Roles: []mysqlv1alpha1.RoleConfiguration{
		{Name: appName, Host: "%", Ensure: mysqlv1alpha1.EnsurePresent,
			MaxUserConnections: 10, RequireTLS: "x509",
			PasswordSecret: &mysqlv1alpha1.SecretKeySelector{Name: "pw", Key: "password"},
			Privileges:     []mysqlv1alpha1.RolePrivilege{{Privileges: []string{"SELECT"}, On: "app.*"}}},
	}}

	if err := r.reconcileManagedRoles(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if len(control.altered) != 1 {
		t.Fatalf("altered = %d, want 1", len(control.altered))
	}
	a := control.altered[0]
	if a.MaxUserConnections == nil || *a.MaxUserConnections != 10 {
		t.Errorf("MaxUserConnections not altered: %+v", a)
	}
	if a.RequireTLS == nil || *a.RequireTLS != "x509" {
		t.Errorf("RequireTLS not altered: %+v", a)
	}
}
