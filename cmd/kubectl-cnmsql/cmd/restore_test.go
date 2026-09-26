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

package cmd

import (
	"context"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func TestBuildLogicalRestore(t *testing.T) {
	cluster := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "shop", Namespace: "prod"}}

	restore, err := buildLogicalRestore(cluster, restoreOptions{
		name: "r1", backup: "nightly", databases: []string{" billing", "shop", "billing", ""},
		policy: string(mysqlv1alpha1.LogicalRestoreDropAndRecreate),
	})
	if err != nil {
		t.Fatal(err)
	}
	if restore.Namespace != "prod" || restore.Spec.Cluster.Name != "shop" || restore.Spec.Backup.Name != "nightly" ||
		!slices.Equal(restore.Spec.Databases, []string{"billing", "shop"}) ||
		restore.Spec.Policy != mysqlv1alpha1.LogicalRestoreDropAndRecreate {
		t.Errorf("restore = %+v", restore.Spec)
	}

	fromSource, err := buildLogicalRestore(cluster, restoreOptions{
		name: "r2", source: "prod", backupID: "b-1", databases: []string{"billing"},
		policy: string(mysqlv1alpha1.LogicalRestoreFailIfExists),
	})
	if err != nil {
		t.Fatal(err)
	}
	if fromSource.Spec.Backup != nil || fromSource.Spec.Source != "prod" || fromSource.Spec.BackupID != "b-1" {
		t.Errorf("restore = %+v", fromSource.Spec)
	}
}

func TestBuildLogicalRestoreRejects(t *testing.T) {
	cluster := &mysqlv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "shop", Namespace: "prod"}}
	ok := restoreOptions{backup: "nightly", databases: []string{"billing"}, policy: "FailIfExists"}
	for _, tc := range []struct {
		name   string
		mutate func(*restoreOptions)
		want   string
	}{
		{"both sources", func(o *restoreOptions) { o.source = "prod" }, "exactly one of --backup or --source"},
		{"no source", func(o *restoreOptions) { o.backup = "" }, "exactly one of --backup or --source"},
		{"backup-id without source", func(o *restoreOptions) { o.backupID = "b-1" }, "--backup-id needs --source"},
		{"no databases", func(o *restoreOptions) { o.databases = []string{" "} }, "--databases is required"},
		{"unknown policy", func(o *restoreOptions) { o.policy = "Merge" }, "--policy must be"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := ok
			tc.mutate(&opts)
			if _, err := buildLogicalRestore(cluster, opts); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCheckRestoreBackup(t *testing.T) {
	backup := func(name string, method mysqlv1alpha1.BackupMethod, phase mysqlv1alpha1.BackupPhase) *mysqlv1alpha1.Backup {
		return &mysqlv1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       mysqlv1alpha1.BackupSpec{Method: method},
			Status:     mysqlv1alpha1.BackupStatus{Phase: phase},
		}
	}
	c := clientfake.NewClientBuilder().WithScheme(plugin.Scheme).WithObjects(
		backup("done", mysqlv1alpha1.BackupMethodLogical, mysqlv1alpha1.BackupPhaseCompleted),
		backup("running", mysqlv1alpha1.BackupMethodLogical, mysqlv1alpha1.BackupPhaseRunning),
		backup("failed", mysqlv1alpha1.BackupMethodLogical, mysqlv1alpha1.BackupPhaseFailed),
		backup("physical", mysqlv1alpha1.BackupMethodXtrabackup, mysqlv1alpha1.BackupPhaseCompleted),
	).Build()
	for _, tc := range []struct {
		name, note, err string
	}{
		{name: "done"},
		{name: "running", note: "not completed yet"},
		{name: "failed", err: "failed"},
		{name: "physical", err: "logical backup"},
		{name: "nightyl", err: "not found"},
	} {
		note, err := checkRestoreBackup(context.Background(), c, "default", tc.name)
		switch {
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.err)
		case tc.err == "" && err != nil:
			t.Errorf("%s: err = %v", tc.name, err)
		case !strings.Contains(note, tc.note) || (tc.note == "" && note != ""):
			t.Errorf("%s: note = %q, want %q", tc.name, note, tc.note)
		}
	}
}
