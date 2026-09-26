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
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore/objectstoretest"
)

// importManifest is a logical.json the fake store serves.
func importManifest(t *testing.T, mutate func(*objectstore.LogicalBackupMetadata)) []byte {
	t.Helper()
	meta := objectstore.LogicalBackupMetadata{
		FormatVersion: objectstore.LogicalFormatVersion,
		BackupID:      "b-1",
		ClusterName:   "prod",
		BackupName:    "nightly",
		Method:        "logical",
		Flavor:        "mysql",
		ServerVersion: "8.4.11-11",
		Compression:   objectstore.LogicalCompressionZstd,
		Databases:     []string{"billing", "shop"},
		CompletedAt:   time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC),
	}
	if mutate != nil {
		mutate(&meta)
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func importStore(endpoint string) *mysqlv1alpha1.S3ObjectStore {
	return &mysqlv1alpha1.S3ObjectStore{
		Bucket:   "backups",
		Path:     "clusters",
		Endpoint: endpoint,
		Credentials: mysqlv1alpha1.S3Credentials{
			AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "access"},
			SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "cluster-s3", Key: "secret"},
		},
	}
}

// importCluster is a fresh cluster importing imp, with an externalClusters
// entry "prod" on the fake store.
func importCluster(endpoint string, imp *mysqlv1alpha1.BootstrapImport) *mysqlv1alpha1.Cluster {
	cluster := &mysqlv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "restored", Namespace: "default"},
		Spec: mysqlv1alpha1.ClusterSpec{
			Instances: 3,
			Storage:   mysqlv1alpha1.StorageConfiguration{Size: "1Gi"},
			Bootstrap: &mysqlv1alpha1.BootstrapConfiguration{
				InitDB: &mysqlv1alpha1.BootstrapInitDB{Database: "app", Owner: "app", Import: imp},
			},
			ExternalClusters: []mysqlv1alpha1.ExternalCluster{{Name: "prod", ObjectStore: importStore(endpoint)}},
		},
	}
	cluster.SetDefaults()
	return cluster
}

// importBackup is a Backup of cluster "prod" whose store comes from that
// cluster.
func importBackup(phase mysqlv1alpha1.BackupPhase) *mysqlv1alpha1.Backup {
	return &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: "prod"},
			Method:  mysqlv1alpha1.BackupMethodLogical,
		},
		Status: mysqlv1alpha1.BackupStatus{Phase: phase, BackupID: "b-1"},
	}
}

func sourceCluster(endpoint string) *mysqlv1alpha1.Cluster {
	cluster := &mysqlv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
		Spec: mysqlv1alpha1.ClusterSpec{
			Instances: 1,
			Storage:   mysqlv1alpha1.StorageConfiguration{Size: "1Gi"},
			Backup:    &mysqlv1alpha1.BackupConfiguration{ObjectStore: importStore(endpoint)},
		},
	}
	cluster.SetDefaults()
	return cluster
}

func importReconciler(t *testing.T, objs ...client.Object) (*ClusterReconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := testScheme(t)
	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(append(objs, s3CredentialsSecret())...).Build()
	return &ClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}, recorder
}

// A dump of cluster "prod", Backup "nightly", backupID b-1.
const (
	importDumpKey     = "clusters/prod/nightly/b-1/dump.sql.zst"
	importManifestKey = "clusters/prod/nightly/b-1/logical.json"
)

func TestResolveImportFromBackup(t *testing.T) {
	t.Parallel()
	srv, _ := objectstoretest.NewServer(t, "backups", map[string][]byte{
		importManifestKey: importManifest(t, nil),
	})
	cluster := importCluster(srv.URL, &mysqlv1alpha1.BootstrapImport{
		Backup:        &mysqlv1alpha1.LocalObjectReference{Name: "nightly"},
		Databases:     []string{"shop"},
		PostImportSQL: []string{"SELECT 1"},
	})
	r, recorder := importReconciler(t, importBackup(mysqlv1alpha1.BackupPhaseCompleted), sourceCluster(srv.URL))

	plan, err := r.resolveImport(context.Background(), cluster, "8.4.11-11")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Bucket != "backups" || plan.DumpKey != importDumpKey || plan.ManifestKey != importManifestKey {
		t.Errorf("plan keys = %s %s %s", plan.Bucket, plan.DumpKey, plan.ManifestKey)
	}
	if !slices.Equal(plan.Databases, []string{"shop"}) || !slices.Equal(plan.PostImportSQL, []string{"SELECT 1"}) {
		t.Errorf("plan selection = %v %v", plan.Databases, plan.PostImportSQL)
	}
	env := map[string]string{}
	for _, e := range plan.StoreEnv {
		env[e.Name] = e.Value
	}
	if env[objectstore.EnvBucket] != "backups" || env[objectstore.EnvPath] != "clusters" {
		t.Errorf("store env = %v", env)
	}
	// Same series: no warning.
	select {
	case e := <-recorder.Events:
		t.Errorf("unexpected event %q", e)
	default:
	}
}

func TestResolveImportFromSource(t *testing.T) {
	t.Parallel()
	srv, _ := objectstoretest.NewServer(t, "backups", map[string][]byte{
		"clusters/prod/nightly/old/logical.json": importManifest(t, func(m *objectstore.LogicalBackupMetadata) {
			m.BackupID = "old"
			m.CompletedAt = m.CompletedAt.Add(-time.Hour)
		}),
		"clusters/prod/nightly/new/logical.json": importManifest(t, func(m *objectstore.LogicalBackupMetadata) {
			m.BackupID = "new"
		}),
		// A physical backup under the same prefix is never picked.
		"clusters/prod/base/x/metadata.json": []byte(`{"backupID":"x","completedAt":"2027-01-01T00:00:00Z"}`),
	})
	r, _ := importReconciler(t)

	for backupID, wantPrefix := range map[string]string{"": "clusters/prod/nightly/new/", "old": "clusters/prod/nightly/old/"} {
		cluster := importCluster(srv.URL, &mysqlv1alpha1.BootstrapImport{Source: "prod", BackupID: backupID})
		plan, err := r.resolveImport(context.Background(), cluster, "8.4.11-11")
		if err != nil {
			t.Fatalf("backupID %q: %v", backupID, err)
		}
		if plan.DumpKey != wantPrefix+"dump.sql.zst" || plan.ManifestKey != wantPrefix+"logical.json" {
			t.Errorf("backupID %q: keys %s %s", backupID, plan.DumpKey, plan.ManifestKey)
		}
	}
}

func TestResolveImportFailures(t *testing.T) {
	t.Parallel()
	physical := importBackup(mysqlv1alpha1.BackupPhaseCompleted)
	physical.Spec.Method = mysqlv1alpha1.BackupMethodXtrabackup

	for _, tc := range []struct {
		name         string
		imp          *mysqlv1alpha1.BootstrapImport
		objs         []client.Object
		manifest     []byte
		wantNotReady bool
		wantErr      string
	}{
		{
			name:         "the Backup does not exist yet",
			imp:          &mysqlv1alpha1.BootstrapImport{Backup: &mysqlv1alpha1.LocalObjectReference{Name: "nightly"}},
			wantNotReady: true,
			wantErr:      `import backup "nightly" does not exist`,
		},
		{
			name:         "the Backup is still running",
			imp:          &mysqlv1alpha1.BootstrapImport{Backup: &mysqlv1alpha1.LocalObjectReference{Name: "nightly"}},
			objs:         []client.Object{importBackup(mysqlv1alpha1.BackupPhaseRunning)},
			wantNotReady: true,
			wantErr:      "is not completed yet",
		},
		{
			name:    "the Backup failed",
			imp:     &mysqlv1alpha1.BootstrapImport{Backup: &mysqlv1alpha1.LocalObjectReference{Name: "nightly"}},
			objs:    []client.Object{importBackup(mysqlv1alpha1.BackupPhaseFailed)},
			wantErr: reasonImportIncompatible + `: import backup "nightly" failed`,
		},
		{
			name:    "a physical Backup",
			imp:     &mysqlv1alpha1.BootstrapImport{Backup: &mysqlv1alpha1.LocalObjectReference{Name: "nightly"}},
			objs:    []client.Object{physical},
			wantErr: reasonPhysicalBackupNotImportable,
		},
		{
			name:    "no manifest in the store",
			imp:     &mysqlv1alpha1.BootstrapImport{Backup: &mysqlv1alpha1.LocalObjectReference{Name: "nightly"}},
			objs:    []client.Object{importBackup(mysqlv1alpha1.BackupPhaseCompleted)},
			wantErr: reasonImportIncompatible + `: import backup "nightly" has no manifest`,
		},
		{
			name: "a MariaDB dump",
			imp:  &mysqlv1alpha1.BootstrapImport{Backup: &mysqlv1alpha1.LocalObjectReference{Name: "nightly"}},
			objs: []client.Object{importBackup(mysqlv1alpha1.BackupPhaseCompleted)},
			manifest: importManifest(t, func(m *objectstore.LogicalBackupMetadata) {
				m.Flavor = "mariadb"
			}),
			wantErr: reasonImportIncompatible + ": the dump was taken on a mariadb server",
		},
		{
			name: "a database not in the dump",
			imp: &mysqlv1alpha1.BootstrapImport{
				Backup:    &mysqlv1alpha1.LocalObjectReference{Name: "nightly"},
				Databases: []string{"crm"},
			},
			objs:    []client.Object{importBackup(mysqlv1alpha1.BackupPhaseCompleted)},
			wantErr: reasonImportIncompatible + ": databases crm are not in the dump",
		},
		{
			name:         "no dump under the source yet",
			imp:          &mysqlv1alpha1.BootstrapImport{Source: "prod"},
			wantNotReady: true,
			wantErr:      "no logical backups found",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			objects := map[string][]byte{}
			if tc.manifest != nil {
				objects[importManifestKey] = tc.manifest
			} else if !strings.Contains(tc.name, "no manifest") && tc.imp.Source == "" {
				objects[importManifestKey] = importManifest(t, nil)
			}
			srv, _ := objectstoretest.NewServer(t, "backups", objects)
			r, _ := importReconciler(t, append(tc.objs, sourceCluster(srv.URL))...)
			_, err := r.resolveImport(context.Background(), importCluster(srv.URL, tc.imp), "8.4.11-11")
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
			var notReady *importNotReadyError
			if got := errors.As(err, &notReady); got != tc.wantNotReady {
				t.Errorf("retryable = %v, want %v (%v)", got, tc.wantNotReady, err)
			}
		})
	}
}

// Once the cluster is established the import is not resolved again: its
// Backup may be gone and its store unreachable, and neither matters any more.
func TestResolveImportSkippedOnceEstablished(t *testing.T) {
	t.Parallel()
	cluster := importCluster("http://127.0.0.1:1", &mysqlv1alpha1.BootstrapImport{Source: "prod"})
	cluster.Status.EstablishedAt = &metav1.Time{Time: time.Now()}
	r, _ := importReconciler(t)
	plan, err := r.resolveImport(context.Background(), cluster, "8.4.11-11")
	if err != nil || plan != nil {
		t.Fatalf("plan = %v, err = %v; want neither", plan, err)
	}
}

func TestResolveImportWarnsOnNewerSeries(t *testing.T) {
	t.Parallel()
	srv, _ := objectstoretest.NewServer(t, "backups", map[string][]byte{
		importManifestKey: importManifest(t, func(m *objectstore.LogicalBackupMetadata) { m.ServerVersion = "9.6.0" }),
	})
	cluster := importCluster(srv.URL, &mysqlv1alpha1.BootstrapImport{
		Backup: &mysqlv1alpha1.LocalObjectReference{Name: "nightly"},
	})
	r, recorder := importReconciler(t, importBackup(mysqlv1alpha1.BackupPhaseCompleted), sourceCluster(srv.URL))
	if _, err := r.resolveImport(context.Background(), cluster, "8.4.11-11"); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-recorder.Events:
		if !strings.Contains(e, "ImportFromNewerServer") || !strings.Contains(e, "9.6.0") {
			t.Errorf("event = %q", e)
		}
	default:
		t.Error("want an ImportFromNewerServer warning")
	}
}

func importTestPlan(t *testing.T, cluster *mysqlv1alpha1.Cluster, dumpKey string) clusterPlan {
	t.Helper()
	r := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build()}
	plan, err := r.buildPlan(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if dumpKey != "" {
		plan.Import = &importPlan{
			Bucket:        "backups",
			DumpKey:       dumpKey,
			ManifestKey:   strings.TrimSuffix(dumpKey, "dump.sql.zst") + "logical.json",
			StoreEnv:      []corev1.EnvVar{{Name: objectstore.EnvBucket, Value: "backups"}},
			Databases:     []string{"shop"},
			PostImportSQL: []string{"UPDATE t SET price = '$(POD_NAME)'"},
		}
	}
	return plan
}

func TestImportContainerOnlyOnTheBootstrapPrimary(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	cluster.Spec.Instances = 2
	plan := importTestPlan(t, cluster, importDumpKey)
	r := &ClusterReconciler{Scheme: testScheme(t)}

	primary := r.podSpec(cluster, plan, plan.instanceFor(cluster, 1))
	if got := containerNames(primary.InitContainers); !slices.Equal(got, []string{bootstrapControllerName}) {
		t.Fatalf("primary init containers = %v, want only %s", got, bootstrapControllerName)
	}
	inst := plan.instanceFor(cluster, 1)
	job, err := r.bootstrapJob(cluster, plan, inst, r.bootstrapModeFor(cluster, plan, inst), instancePVC(cluster, inst.PVCName))
	if err != nil {
		t.Fatal(err)
	}
	jobSpec := job.Spec.Template.Spec
	if got := containerNames(jobSpec.InitContainers); !slices.Equal(got, []string{"bootstrap-controller", "initdb"}) {
		t.Fatalf("import job init containers = %v", got)
	}
	imp := jobSpec.Containers[0]
	if imp.Name != "import" {
		t.Fatalf("import job main container = %q, want import", imp.Name)
	}
	for _, want := range []string{
		"import", "--dump-key=" + importDumpKey, "--database=shop",
		// $ is escaped so the kubelet does not expand it.
		"--post-import-sql=UPDATE t SET price = '$$(POD_NAME)'",
	} {
		if !slices.Contains(imp.Args, want) {
			t.Errorf("import args lack %q: %v", want, imp.Args)
		}
	}
	env := map[string]bool{}
	for _, e := range imp.Env {
		env[e.Name] = true
	}
	for _, want := range []string{"CNMSQL_FLAVOR", objectstore.EnvBucket} {
		if !env[want] {
			t.Errorf("import env lacks %s", want)
		}
	}

	replica := plan.instanceFor(cluster, 2)
	if mode := r.bootstrapModeFor(cluster, plan, replica); mode != bootstrapModeJoin {
		t.Errorf("replica bootstrap mode = %q, want join", mode)
	}
}

func TestImportPhaseReason(t *testing.T) {
	t.Parallel()
	cluster := baseCluster()
	o := observedCluster{}
	o.computeClusterPhase(cluster, clusterPlan{Instances: 1, Import: &importPlan{}})
	if !strings.Contains(o.PhaseReason, "import the logical backup") {
		t.Errorf("phase reason = %q", o.PhaseReason)
	}
}
