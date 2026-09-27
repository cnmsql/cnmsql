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
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/internal/controller/topology"
)

// bootstrappedDataPVC builds the data PVC of the cluster's first instance whose
// bootstrap Job succeeded, so the volume holds a bootstrapped data directory.
func bootstrappedDataPVC(cluster *mysqlv1alpha1.Cluster, annotations map[string]string) *corev1.PersistentVolumeClaim {
	if annotations == nil {
		annotations = map[string]string{pvcStatusAnnotation: pvcStatusReady}
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "demo-1",
			Namespace:   cluster.Namespace,
			Labels:      map[string]string{clusterLabel: cluster.Name},
			Annotations: annotations,
		},
	}
}

// seededCredentialSecrets creates every operator-generated credential Secret
// except the named ones, so a test can delete one and check the guard.
func seededCredentialSecrets(cluster *mysqlv1alpha1.Cluster, plan clusterPlan, missing ...string) []client.Object {
	names := []string{plan.RootSecretName, plan.AppSecretName, plan.ReplicationSecret,
		plan.BackupSecretName, cluster.DumpSecretName(), plan.ControlSecretName}
	out := make([]client.Object, 0, len(names))
	for _, name := range names {
		if slices.Contains(missing, name) {
			continue
		}
		out = append(out, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace},
			Data:       map[string][]byte{"password": []byte("keep-" + name)},
		})
	}
	return out
}

// blockedClusterObjects builds a bootstrapped cluster whose credential Secrets
// exist except the named ones.
func blockedClusterObjects(t *testing.T, cluster *mysqlv1alpha1.Cluster, plan clusterPlan, missing ...string) []client.Object {
	t.Helper()
	objs := append([]client.Object{cluster, bootstrappedDataPVC(cluster, nil)},
		seededCredentialSecrets(cluster, plan, missing...)...)
	return objs
}

func TestHasBootstrappedInstances(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	scheme := testScheme(t)

	build := func(objs ...client.Object) *ClusterReconciler {
		return &ClusterReconciler{
			Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
			Scheme: scheme,
		}
	}
	r := build(cluster)
	if got, err := r.hasBootstrappedInstances(ctx, cluster); err != nil || got {
		t.Fatalf("no PVCs: bootstrapped = %t, %v; want false", got, err)
	}

	// A volume still being initialized holds no accounts yet.
	initializing := build(cluster, bootstrappedDataPVC(cluster,
		map[string]string{pvcStatusAnnotation: pvcStatusInitializing}))
	if got, err := initializing.hasBootstrappedInstances(ctx, cluster); err != nil || got {
		t.Fatalf("initializing PVC: bootstrapped = %t, %v; want false", got, err)
	}

	ready := build(cluster, bootstrappedDataPVC(cluster, nil))
	if got, err := ready.hasBootstrappedInstances(ctx, cluster); err != nil || !got {
		t.Fatalf("ready PVC: bootstrapped = %t, %v; want true", got, err)
	}

	// A volume without the annotation predates bootstrap Jobs and was
	// bootstrapped by its Pod's init container.
	legacy := build(cluster, bootstrappedDataPVC(cluster, map[string]string{}))
	if got, err := legacy.hasBootstrappedInstances(ctx, cluster); err != nil || !got {
		t.Fatalf("legacy PVC: bootstrapped = %t, %v; want true", got, err)
	}
}

// On a cluster whose instances are already bootstrapped, a deleted credential
// Secret must not be regenerated: the MySQL accounts still hold its previous
// password, and a fresh one would take every instance out of service (issue
// #140). It must not stop the reconcile either — the instance managers keep the
// password they last read, and failover must keep running — so it is skipped
// with a warning Event while the other Secrets are still ensured.
func TestEnsureCredentialsSkipsDeletedSecretOnBootstrappedCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	scheme := testScheme(t)

	recorder := record.NewFakeRecorder(10)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(blockedClusterObjects(t, cluster, plan, plan.ControlSecretName, plan.ReplicationSecret)...).
			Build(),
		Scheme:   scheme,
		Recorder: recorder,
	}

	if err := r.ensureCredentials(ctx, cluster, plan); err != nil {
		t.Fatalf("ensureCredentials: %v", err)
	}

	got := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: plan.ControlSecretName}, got); err == nil {
		t.Fatalf("the deleted Secret was regenerated with data %v", got.Data)
	}
	// The unguarded replication Secret is still healed.
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: plan.ReplicationSecret}, got); err != nil {
		t.Fatalf("replication Secret: %v", err)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "CredentialSecretMissing") || !strings.Contains(event, plan.ControlSecretName) {
			t.Fatalf("event = %q, want a CredentialSecretMissing warning naming %s", event, plan.ControlSecretName)
		}
	default:
		t.Fatal("no event was recorded")
	}
}

// Every guarded Secret is reported; spot-check the backup one, which the issue
// calls out alongside control and root.
func TestMissingCredentialSecretsReportsDeletedBackupSecret(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	scheme := testScheme(t)

	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(blockedClusterObjects(t, cluster, plan, plan.BackupSecretName)...).
			Build(),
		Scheme: scheme,
	}

	missing, err := r.missingCredentialSecrets(ctx, cluster, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(missing, []string{plan.BackupSecretName}) {
		t.Fatalf("missing = %v, want [%s]", missing, plan.BackupSecretName)
	}
}

// A cluster that has not bootstrapped any volume yet must still get its
// Secrets generated.
func TestEnsureCredentialsGeneratesSecretsForNewCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	scheme := testScheme(t)
	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(),
		Scheme: scheme,
	}

	if err := r.ensureCredentials(ctx, cluster, plan); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: plan.ControlSecretName}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Data["password"]) == 0 {
		t.Fatal("control Secret password was not generated")
	}
}

// The dump Secret stays on the self-healing path: the operator re-applies the
// cnmsql_dump account from its Secret, so it is still regenerated while the
// guarded Secrets are not.
func TestEnsureCredentialsStillGeneratesDumpSecretOnBootstrappedCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := baseCluster()
	plan := testPlan()
	scheme := testScheme(t)

	r := &ClusterReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(blockedClusterObjects(t, cluster, plan, cluster.DumpSecretName())...).
			Build(),
		Scheme: scheme,
	}

	if err := r.ensureCredentials(ctx, cluster, plan); err != nil {
		t.Fatalf("ensureCredentials: %v", err)
	}
	got := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.DumpSecretName()}, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Data["password"]) == 0 {
		t.Fatal("dump Secret password was not generated")
	}
}

// The Cluster reports the missing Secret as Degraded, naming it, even while
// every instance is still serving.
func TestComputeClusterPhaseReportsMissingCredentialSecrets(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Status.EstablishedAt = &metav1.Time{Time: metav1.Now().Time}
	o := observedCluster{Ready: true, MissingCredentialSecrets: []string{"demo-control"}}
	o.computeClusterPhase(cluster, testPlan())
	if o.Phase != topology.PhaseDegraded || !strings.Contains(o.PhaseReason, "demo-control") {
		t.Fatalf("phase %q reason %q, want Degraded naming demo-control", o.Phase, o.PhaseReason)
	}
}
