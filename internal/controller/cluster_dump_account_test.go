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
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/user"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

type failingCreateControlClient struct {
	*recordingControlClient
}

func (c failingCreateControlClient) CreateUser(context.Context, *mysqlv1alpha1.Cluster, string, user.CreateUserRequest) error {
	return errors.New("connection refused")
}

func dumpSecret(cluster *mysqlv1alpha1.Cluster, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-dump", Namespace: cluster.Namespace},
		Data:       map[string][]byte{"username": []byte("cnmsql_dump"), "password": []byte(password)},
	}
}

func dumpAccountReconciler(t *testing.T, control InstanceControlClient, cluster *mysqlv1alpha1.Cluster, objs ...*corev1.Secret) *ClusterReconciler {
	t.Helper()
	scheme := testScheme(t)
	builder := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&mysqlv1alpha1.Cluster{}).
		WithObjects(cluster)
	for _, o := range objs {
		builder = builder.WithObjects(o)
	}
	return &ClusterReconciler{
		Client:        builder.Build(),
		Scheme:        scheme,
		Recorder:      record.NewFakeRecorder(20),
		ControlClient: control,
	}
}

func observedWithPrimary(ready bool, version string) observedCluster {
	return observedCluster{
		PrimaryName: testPrimary,
		StatusByInstance: map[string]*webserver.Status{
			testPrimary: {Role: webserver.RolePrimary, IsReady: ready, Version: version},
		},
	}
}

func dumpCondition(t *testing.T, r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster) (*metav1.Condition, string) {
	t.Helper()
	latest := &mysqlv1alpha1.Cluster{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, latest); err != nil {
		t.Fatal(err)
	}
	return apimeta.FindStatusCondition(latest.Status.Conditions, mysqlv1alpha1.ConditionDumpAccountReady),
		latest.Status.DumpAccountSecretVersion
}

func secretVersion(t *testing.T, r *ClusterReconciler, cluster *mysqlv1alpha1.Cluster) string {
	t.Helper()
	s := &corev1.Secret{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + "-dump"}, s); err != nil {
		t.Fatal(err)
	}
	return s.ResourceVersion
}

func TestReconcileDumpAccountCreatesAccountOnPrimary(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	cluster := baseCluster()
	r := dumpAccountReconciler(t, control, cluster, dumpSecret(cluster, "pw1"))

	if err := r.reconcileDumpAccount(context.Background(), cluster, observedWithPrimary(true, "8.4.11-11")); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 1 || len(control.altered) != 1 {
		t.Fatalf("created=%d altered=%d, want 1/1", len(control.created), len(control.altered))
	}
	created := control.created[0]
	if created.Name != "cnmsql_dump" || created.Host != "localhost" || created.Password != "pw1" || created.Superuser {
		t.Fatalf("create = %+v", created)
	}
	if len(created.Privileges) != 1 || created.Privileges[0].On != "*.*" ||
		!slices.Contains(created.Privileges[0].Privileges, "REPLICATION CLIENT") ||
		slices.Contains(created.Privileges[0].Privileges, "INSERT") {
		t.Fatalf("grants = %+v", created.Privileges)
	}
	if len(created.Revokes) != 1 || created.Revokes[0].On != "mysql.*" {
		t.Fatalf("revokes = %+v", created.Revokes)
	}
	if altered := control.altered[0]; altered.Password == nil || *altered.Password != "pw1" || altered.Host != "localhost" {
		t.Fatalf("alter = %+v", altered)
	}

	cond, version := dumpCondition(t, r, cluster)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != dumpAccountReasonApplied {
		t.Fatalf("condition = %+v", cond)
	}
	if version == "" || version != secretVersion(t, r, cluster) {
		t.Fatalf("dumpAccountSecretVersion = %q", version)
	}

	// Steady state: nothing changed, so no SQL.
	if err := r.reconcileDumpAccount(context.Background(), cluster, observedWithPrimary(true, "8.4.11-11")); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 1 || len(control.altered) != 1 {
		t.Fatalf("steady state issued SQL: created=%d altered=%d", len(control.created), len(control.altered))
	}
}

func TestReconcileDumpAccountReappliesAfterPasswordRotation(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	cluster := baseCluster()
	r := dumpAccountReconciler(t, control, cluster, dumpSecret(cluster, "pw1"))
	ctx := context.Background()
	if err := r.reconcileDumpAccount(ctx, cluster, observedWithPrimary(true, "8.4.11-11")); err != nil {
		t.Fatal(err)
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "demo-dump"}, secret); err != nil {
		t.Fatal(err)
	}
	secret.Data["password"] = []byte("pw2")
	if err := r.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileDumpAccount(ctx, cluster, observedWithPrimary(true, "8.4.11-11")); err != nil {
		t.Fatal(err)
	}
	if len(control.altered) != 2 || *control.altered[1].Password != "pw2" {
		t.Fatalf("password not re-applied: %+v", control.altered)
	}
	if _, version := dumpCondition(t, r, cluster); version != secretVersion(t, r, cluster) {
		t.Fatalf("status version %q does not follow the Secret", version)
	}
}

func TestReconcileDumpAccountResetsAccountOfRecoveredCluster(t *testing.T) {
	t.Parallel()
	// A cluster recovered from another cluster's backup may carry that cluster's
	// cnmsql_dump. Its own status is empty, so the password is reset to its own
	// Secret even though the account already exists.
	control := &recordingControlClient{users: []user.UserInfo{{Name: "cnmsql_dump", Host: "localhost"}}}
	cluster := baseCluster()
	r := dumpAccountReconciler(t, control, cluster, dumpSecret(cluster, "mine"))
	if err := r.reconcileDumpAccount(context.Background(), cluster, observedWithPrimary(true, "8.0.46-37")); err != nil {
		t.Fatal(err)
	}
	if len(control.altered) != 1 || *control.altered[0].Password != "mine" {
		t.Fatalf("alter = %+v", control.altered)
	}
}

func TestReconcileDumpAccountWaitsForReadyPrimary(t *testing.T) {
	t.Parallel()
	for name, observed := range map[string]observedCluster{
		"no primary":        {},
		"primary not ready": observedWithPrimary(false, "8.4.11-11"),
		"primary unreachable": {
			PrimaryName: testPrimary, StatusByInstance: map[string]*webserver.Status{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			control := &recordingControlClient{}
			cluster := baseCluster()
			r := dumpAccountReconciler(t, control, cluster, dumpSecret(cluster, "pw"))
			if err := r.reconcileDumpAccount(context.Background(), cluster, observed); err != nil {
				t.Fatal(err)
			}
			if len(control.created)+len(control.altered) != 0 {
				t.Fatal("no SQL may run without a ready primary")
			}
			cond, version := dumpCondition(t, r, cluster)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != dumpAccountReasonPrimaryNotReady || version != "" {
				t.Fatalf("condition = %+v, version = %q", cond, version)
			}
		})
	}
}

func TestReconcileDumpAccountKeepsReadyConditionWhilePrimaryIsDown(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	cluster := baseCluster()
	r := dumpAccountReconciler(t, control, cluster, dumpSecret(cluster, "pw"))
	ctx := context.Background()
	if err := r.reconcileDumpAccount(ctx, cluster, observedWithPrimary(true, "8.4.11-11")); err != nil {
		t.Fatal(err)
	}
	// A failover in progress does not undo an applied account.
	if err := r.reconcileDumpAccount(ctx, cluster, observedCluster{}); err != nil {
		t.Fatal(err)
	}
	if cond, _ := dumpCondition(t, r, cluster); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("condition = %+v", cond)
	}
}

func TestReconcileDumpAccountReportsApplyFailure(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	r := dumpAccountReconciler(t, failingCreateControlClient{&recordingControlClient{}}, cluster, dumpSecret(cluster, "pw"))
	if err := r.reconcileDumpAccount(context.Background(), cluster, observedWithPrimary(true, "8.4.11-11")); err == nil {
		t.Fatal("expected the apply error to be returned for a retry")
	}
	cond, version := dumpCondition(t, r, cluster)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != dumpAccountReasonApplyFailed || version != "" {
		t.Fatalf("condition = %+v, version = %q", cond, version)
	}
}

func TestReconcileDumpAccountEmptyPasswordFailsTheCondition(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	cluster := baseCluster()
	r := dumpAccountReconciler(t, control, cluster, dumpSecret(cluster, ""))
	if err := r.reconcileDumpAccount(context.Background(), cluster, observedWithPrimary(true, "8.4.11-11")); err == nil {
		t.Fatal("expected the empty password to be an error")
	}
	if len(control.created)+len(control.altered) != 0 {
		t.Fatal("no SQL may run without a password")
	}
	cond, version := dumpCondition(t, r, cluster)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != dumpAccountReasonInvalidSecret || version != "" {
		t.Fatalf("condition = %+v, version = %q", cond, version)
	}
	if !strings.Contains(cond.Message, "no password") {
		t.Fatalf("message = %q, want it to name the missing password", cond.Message)
	}
}

func TestReconcileDumpAccountEmptiedPasswordDemotesReadyCondition(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	cluster := baseCluster()
	r := dumpAccountReconciler(t, control, cluster, dumpSecret(cluster, "pw"))
	ctx := context.Background()
	if err := r.reconcileDumpAccount(ctx, cluster, observedWithPrimary(true, "8.4.11-11")); err != nil {
		t.Fatal(err)
	}
	if cond, _ := dumpCondition(t, r, cluster); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("condition = %+v, want True after the first apply", cond)
	}

	// Emptying the password rotates the Secret's resourceVersion, so the next
	// pass re-applies it — and must demote the stale True condition.
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name + "-dump"}, secret); err != nil {
		t.Fatal(err)
	}
	secret.Data["password"] = nil
	if err := r.Update(ctx, secret); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcileDumpAccount(ctx, cluster, observedWithPrimary(true, "8.4.11-11")); err == nil {
		t.Fatal("expected the emptied password to be an error")
	}
	if len(control.created) != 1 || len(control.altered) != 1 {
		t.Fatalf("the emptied password must not re-apply the account: created=%d altered=%d", len(control.created), len(control.altered))
	}
	cond, _ := dumpCondition(t, r, cluster)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != dumpAccountReasonInvalidSecret {
		t.Fatalf("condition = %+v, want False with %s", cond, dumpAccountReasonInvalidSecret)
	}
}

func TestReconcileDumpAccountMariaDBGrants(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	cluster := baseCluster()
	cluster.Spec.Flavor = mysqlv1alpha1.FlavorMariaDB
	r := dumpAccountReconciler(t, control, cluster, dumpSecret(cluster, "pw"))
	if err := r.reconcileDumpAccount(context.Background(), cluster, observedWithPrimary(true, "11.4.13-MariaDB-deb12-log")); err != nil {
		t.Fatal(err)
	}
	created := control.created[0]
	privs := created.Privileges[0].Privileges
	if !slices.Contains(privs, "BINLOG MONITOR") || !slices.Contains(privs, "SHOW CREATE ROUTINE") ||
		slices.Contains(privs, "REPLICATION CLIENT") {
		t.Fatalf("mariadb grants = %v", privs)
	}
	if len(created.Revokes) != 0 {
		t.Fatalf("mariadb has no partial revokes, got %+v", created.Revokes)
	}
}

func TestReconcileDumpAccountWithoutSecretIsANoOp(t *testing.T) {
	t.Parallel()
	control := &recordingControlClient{}
	cluster := baseCluster()
	r := dumpAccountReconciler(t, control, cluster)
	if err := r.reconcileDumpAccount(context.Background(), cluster, observedWithPrimary(true, "8.4.11-11")); err != nil {
		t.Fatal(err)
	}
	if len(control.created) != 0 {
		t.Fatal("no account without its Secret")
	}
}
