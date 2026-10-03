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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// recoveryBackupFixture builds a completed physical Backup of cluster "shop"
// whose status and spec object stores can differ, mirroring a Backup taken
// against one store and recovered by a cluster configured with another.
func recoveryBackupFixture(statusStore, specStore *mysqlv1alpha1.S3ObjectStore) *mysqlv1alpha1.Backup {
	return &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "shop-bk-primary", Namespace: "default"},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster:     mysqlv1alpha1.LocalObjectReference{Name: "shop"},
			ObjectStore: specStore,
		},
		Status: mysqlv1alpha1.BackupStatus{
			Phase:       mysqlv1alpha1.BackupPhaseCompleted,
			BackupID:    "bk-1",
			ObjectStore: statusStore,
		},
	}
}

// recoveryTargetClusterFixture builds the recovering cluster referencing
// recoveryBackupFixture, optionally with its own spec.backup.objectStore.
func recoveryTargetClusterFixture(ownStore *mysqlv1alpha1.S3ObjectStore) *mysqlv1alpha1.Cluster {
	cluster := baseCluster()
	cluster.Spec.Bootstrap = &mysqlv1alpha1.BootstrapConfiguration{
		Recovery: &mysqlv1alpha1.BootstrapRecovery{
			Backup: &mysqlv1alpha1.LocalObjectReference{Name: "shop-bk-primary"},
		},
	}
	if ownStore != nil {
		cluster.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{ObjectStore: ownStore}
	}
	cluster.SetDefaults()
	return cluster
}

// sourceClusterFixture builds the cluster a Backup was taken from, with or
// without a backup object store.
func sourceClusterFixture(store *mysqlv1alpha1.S3ObjectStore) *mysqlv1alpha1.Cluster {
	source := &mysqlv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "shop", Namespace: "default"},
		Spec:       mysqlv1alpha1.ClusterSpec{Instances: 1},
	}
	if store != nil {
		source.Spec.Backup = &mysqlv1alpha1.BackupConfiguration{ObjectStore: store}
	}
	return source
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

// TestResolveRecoveryBackupObjectStore checks that recovery reads the backup
// from where the Backup was actually written: the store recorded in its
// status, then the Backup's own spec, then the source cluster's store, and
// only then the recovering cluster's store. The issue scenario recovers a new
// cluster whose own backup path differs from the Backup's.
func TestResolveRecoveryBackupObjectStore(t *testing.T) {
	t.Parallel()

	archiveKeyFor := func(path string) string {
		return path + "/shop/shop-bk-primary/bk-1/" + objectstore.BackupArchiveName
	}
	tests := []struct {
		name          string
		statusStore   *mysqlv1alpha1.S3ObjectStore
		specStore     *mysqlv1alpha1.S3ObjectStore
		sourceCluster *mysqlv1alpha1.Cluster
		ownStore      *mysqlv1alpha1.S3ObjectStore
		wantBucket    string
		wantPath      string
		wantErr       string
	}{
		{
			name:          "prefers the object store recorded in the backup status",
			statusStore:   &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "v076"},
			specStore:     &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "spec"},
			sourceCluster: sourceClusterFixture(&mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "source"}),
			ownStore:      &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "p2-own/shop"},
			wantBucket:    "backups",
			wantPath:      "v076",
		},
		{
			name:        "resolves the backup status store without a recovering-cluster store",
			statusStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "v076"},
			wantBucket:  "backups",
			wantPath:    "v076",
		},
		{
			name:          "prefers the backup spec object store over the source cluster's",
			specStore:     &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "spec"},
			sourceCluster: sourceClusterFixture(&mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "source"}),
			ownStore:      &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "p2-own/shop"},
			wantBucket:    "backups",
			wantPath:      "spec",
		},
		{
			name:          "uses the source cluster's object store",
			sourceCluster: sourceClusterFixture(&mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "source"}),
			ownStore:      &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "p2-own/shop"},
			wantBucket:    "backups",
			wantPath:      "source",
		},
		{
			name:          "falls back to the recovering cluster's object store",
			sourceCluster: sourceClusterFixture(nil),
			ownStore:      &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "p2-own/shop"},
			wantBucket:    "backups",
			wantPath:      "p2-own/shop",
		},
		{
			name:       "falls back to the recovering cluster's store when the source cluster is gone",
			ownStore:   &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "p2-own/shop"},
			wantBucket: "backups",
			wantPath:   "p2-own/shop",
		},
		{
			name:    "blocks when nothing names an object store",
			wantErr: "has no object store",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			objs := []client.Object{recoveryBackupFixture(tc.statusStore, tc.specStore)}
			if tc.sourceCluster != nil {
				objs = append(objs, tc.sourceCluster)
			}
			scheme := testScheme(t)
			reconciler := &ClusterReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
				Scheme: scheme,
			}
			cluster := recoveryTargetClusterFixture(tc.ownStore)

			plan, err := reconciler.resolveRecovery(context.Background(), cluster)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got plan %+v", tc.wantErr, plan)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.Bucket != tc.wantBucket {
				t.Errorf("bucket = %q, want %q", plan.Bucket, tc.wantBucket)
			}
			if want := archiveKeyFor(tc.wantPath); plan.ArchiveKey != want {
				t.Errorf("archiveKey = %q, want %q", plan.ArchiveKey, want)
			}
			if got := envValue(plan.StoreEnv, objectstore.EnvBucket); got != tc.wantBucket {
				t.Errorf("storeEnv bucket = %q, want %q", got, tc.wantBucket)
			}
			if got := envValue(plan.StoreEnv, objectstore.EnvPath); got != tc.wantPath {
				t.Errorf("storeEnv path = %q, want %q", got, tc.wantPath)
			}
		})
	}
}

func TestResolveRecoveryBinlogStore(t *testing.T) {
	t.Parallel()

	base := &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "base"}
	otherCreds := mysqlv1alpha1.S3Credentials{
		AccessKeyID: &mysqlv1alpha1.SecretKeySelector{Name: "other-s3", Key: "access"},
	}
	tests := []struct {
		name        string
		binlogStore *mysqlv1alpha1.S3ObjectStore
		noTarget    bool
		wantBucket  string
		wantTwinEnv bool
	}{
		{name: "backup without a recorded archive store uses the base store", wantBucket: "backups"},
		{
			name:        "same location with other credentials is one store",
			binlogStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "/base/", Credentials: otherCreds},
			wantBucket:  "backups",
		},
		{
			name:        "recorded archive store wins",
			binlogStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"},
			wantBucket:  "binlogs",
			wantTwinEnv: true,
		},
		{
			name:        "a restore without a target does not need the archive store",
			binlogStore: &mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"},
			noTarget:    true,
			wantBucket:  "binlogs",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backup := recoveryBackupFixture(base.DeepCopy(), nil)
			backup.Status.BinlogObjectStore = tc.binlogStore
			cluster := recoveryTargetClusterFixture(nil)
			if !tc.noTarget {
				cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &mysqlv1alpha1.RecoveryTarget{
					TargetGTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
				}
			}
			scheme := testScheme(t)
			r := &ClusterReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(backup).Build(),
				Scheme: scheme,
			}
			plan, err := r.resolveRecovery(context.Background(), cluster)
			if err != nil {
				t.Fatal(err)
			}
			if plan.BinlogStore.Bucket != tc.wantBucket {
				t.Fatalf("binlog store bucket = %q, want %q", plan.BinlogStore.Bucket, tc.wantBucket)
			}
			if got := envValue(plan.StoreEnv, "cnmsql_S3_BUCKET"); got != "backups" {
				t.Fatalf("base bucket env = %q", got)
			}
			twin := envValue(plan.StoreEnv, "cnmsql_BINLOG_S3_BUCKET")
			if tc.wantTwinEnv && twin != tc.wantBucket {
				t.Fatalf("binlog bucket env = %q, want %q", twin, tc.wantBucket)
			}
			if !tc.wantTwinEnv && twin != "" {
				t.Fatalf("binlog env rendered for a single store: %q", twin)
			}
		})
	}
}
