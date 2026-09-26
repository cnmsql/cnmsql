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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// TestInstancePodCarriesNoPasswordEnv pins the credential contract of the
// instance Pod: no container carries a MYSQL_*_PASSWORD env var, and the only
// Secret references a container may hold are the object-store credentials,
// which the workers read on each use. Every other credential reaches the
// instance commands from the Secrets through the Kubernetes API.
func TestInstancePodCarriesNoPasswordEnv(t *testing.T) {
	t.Parallel()

	check := func(t *testing.T, spec corev1.PodSpec) {
		t.Helper()
		containers := append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...)
		for _, c := range containers {
			for _, env := range c.Env {
				if strings.HasSuffix(env.Name, "_PASSWORD") {
					t.Errorf("container %s still carries %s", c.Name, env.Name)
				}
				if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil &&
					!strings.HasPrefix(env.Name, "cnmsql_S3_") {
					t.Errorf("container %s reads secret %s through %s", c.Name, env.ValueFrom.SecretKeyRef.Name, env.Name)
				}
			}
		}
	}

	t.Run("fresh cluster", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster()
		plan := testPlan()
		check(t, (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1)))
	})

	// An import cluster's "import" init container gets the same init env plus
	// the resolved dump's store env, which carries the source object store's
	// secret-backed credentials.
	t.Run("import cluster", func(t *testing.T) {
		t.Parallel()
		cluster := importCluster("http://127.0.0.1:1", &mysqlv1alpha1.BootstrapImport{Source: "prod"})
		// Established, so importTestPlan skips resolving the import against
		// the object store; the StoreEnv below is what resolveImport attaches.
		cluster.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
		plan := importTestPlan(t, cluster, importDumpKey)
		plan.Import.StoreEnv = append(backupObjectStoreEnv(*importStore("http://127.0.0.1:1")),
			corev1.EnvVar{Name: objectstore.EnvBucket, Value: "backups"},
			corev1.EnvVar{Name: objectstore.EnvPath, Value: "clusters"},
		)
		check(t, (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1)))
	})

	// A recovering primary's init container gets the restore store env, and an
	// archiving cluster's run container gets the archiving store env: both are
	// the allowed secret-backed cnmsql_S3_* variables.
	t.Run("recovery and archiving", func(t *testing.T) {
		t.Parallel()
		cluster := baseCluster()
		cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{
			ObjectStore:         importStore("http://127.0.0.1:1"),
			ContinuousArchiving: &mysqlv1alpha1.ContinuousArchivingConfiguration{Enabled: true},
		}
		plan := testPlan()
		plan.Recovery = &recoveryPlan{
			Bucket:     "backups",
			ArchiveKey: "clusters/demo/backup-sample/backup-sample-123/backup.xbstream",
			StoreEnv:   backupObjectStoreEnv(*importStore("http://127.0.0.1:1")),
		}
		check(t, (&ClusterReconciler{}).podSpec(cluster, plan, plan.instanceFor(cluster, 1)))
	})
}
