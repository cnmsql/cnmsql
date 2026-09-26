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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// The LogicalRestore rules live in the CRD schema (CEL), so they are checked
// against a real API server. The reconcile logic is tested with a fake client
// in logicalrestore_test.go.
var _ = Describe("LogicalRestore admission", func() {
	ctx := context.Background()

	restore := func(name string, mutate func(*mysqlv1alpha1.LogicalRestoreSpec)) *mysqlv1alpha1.LogicalRestore {
		r := &mysqlv1alpha1.LogicalRestore{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: mysqlv1alpha1.LogicalRestoreSpec{
				Cluster:   mysqlv1alpha1.LocalObjectReference{Name: "demo"},
				Backup:    &mysqlv1alpha1.LocalObjectReference{Name: "nightly"},
				Databases: []string{"shop"},
				Policy:    mysqlv1alpha1.LogicalRestoreFailIfExists,
			},
		}
		if mutate != nil {
			mutate(&r.Spec)
		}
		return r
	}
	expectRejected := func(obj client.Object, message string) {
		err := k8sClient.Create(ctx, obj)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got %v", err)
		Expect(err.Error()).To(ContainSubstring(message))
	}

	It("accepts a restore from a Backup and one from a source", func() {
		fromBackup := restore("restore-from-backup", nil)
		Expect(k8sClient.Create(ctx, fromBackup)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, fromBackup) })

		fromSource := restore("restore-from-source", func(s *mysqlv1alpha1.LogicalRestoreSpec) {
			s.Backup = nil
			s.Source = "prod"
			s.BackupID = "nightly-1"
			s.Policy = mysqlv1alpha1.LogicalRestoreDropAndRecreate
		})
		Expect(k8sClient.Create(ctx, fromSource)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, fromSource) })
	})

	It("rejects both backup and source, or neither", func() {
		expectRejected(restore("restore-both", func(s *mysqlv1alpha1.LogicalRestoreSpec) {
			s.Source = "prod"
		}), "set exactly one of backup or source")
		expectRejected(restore("restore-neither", func(s *mysqlv1alpha1.LogicalRestoreSpec) {
			s.Backup = nil
		}), "set exactly one of backup or source")
	})

	It("rejects backupID without source", func() {
		expectRejected(restore("restore-backupid", func(s *mysqlv1alpha1.LogicalRestoreSpec) {
			s.BackupID = "nightly-1"
		}), "backupID is only valid with source")
	})

	It("requires databases and a policy", func() {
		expectRejected(restore("restore-no-databases", func(s *mysqlv1alpha1.LogicalRestoreSpec) {
			s.Databases = nil
		}), "spec.databases")
		expectRejected(restore("restore-no-policy", func(s *mysqlv1alpha1.LogicalRestoreSpec) {
			s.Policy = ""
		}), "spec.policy")
		expectRejected(restore("restore-bad-policy", func(s *mysqlv1alpha1.LogicalRestoreSpec) {
			s.Policy = "Merge"
		}), "spec.policy")
	})

	It("rejects system schemas", func() {
		expectRejected(restore("restore-system", func(s *mysqlv1alpha1.LogicalRestoreSpec) {
			s.Databases = []string{"shop", "Performance_Schema"}
		}), "system schemas cannot be restored")
	})

	It("keeps the spec immutable", func() {
		r := restore("restore-immutable", nil)
		Expect(k8sClient.Create(ctx, r)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, r) })

		r.Spec.Policy = mysqlv1alpha1.LogicalRestoreDropAndRecreate
		err := k8sClient.Update(ctx, r)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got %v", err)
		Expect(err.Error()).To(ContainSubstring("spec is immutable"))
	})
})
