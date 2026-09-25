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

// The logical-backup rules live in the CRD schema (CEL), so they are checked
// against a real API server.
var _ = Describe("Logical backup admission", func() {
	ctx := context.Background()

	backup := func(name string, mutate func(*mysqlv1alpha1.BackupSpec)) *mysqlv1alpha1.Backup {
		b := &mysqlv1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       mysqlv1alpha1.BackupSpec{Cluster: mysqlv1alpha1.LocalObjectReference{Name: "demo"}},
		}
		mutate(&b.Spec)
		return b
	}
	expectRejected := func(obj client.Object, message string) {
		err := k8sClient.Create(ctx, obj)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected Invalid, got %v", err)
		Expect(err.Error()).To(ContainSubstring(message))
	}

	It("accepts a logical backup with a database list", func() {
		b := backup("logical-ok", func(s *mysqlv1alpha1.BackupSpec) {
			s.Method = mysqlv1alpha1.BackupMethodLogical
			s.Logical = &mysqlv1alpha1.LogicalBackupOptions{Databases: []string{"billing", "shop"}}
		})
		Expect(k8sClient.Create(ctx, b)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, b) })
		// online keeps its default, which a logical backup honours.
		Expect(b.Spec.Online).To(Equal(new(true)))
	})

	It("rejects logical options on a physical backup", func() {
		expectRejected(backup("logical-on-physical", func(s *mysqlv1alpha1.BackupSpec) {
			s.Logical = &mysqlv1alpha1.LogicalBackupOptions{Databases: []string{"shop"}}
		}), "logical is only valid with method: logical")
	})

	It("rejects an offline logical backup", func() {
		expectRejected(backup("logical-offline", func(s *mysqlv1alpha1.BackupSpec) {
			s.Method = mysqlv1alpha1.BackupMethodLogical
			s.Online = new(false)
		}), "a logical backup is always online")
	})

	It("rejects system schemas", func() {
		expectRejected(backup("logical-system", func(s *mysqlv1alpha1.BackupSpec) {
			s.Method = mysqlv1alpha1.BackupMethodLogical
			s.Logical = &mysqlv1alpha1.LogicalBackupOptions{Databases: []string{"shop", "MySQL"}}
		}), "system schemas cannot be dumped")
	})

	It("applies the same rules to ScheduledBackups", func() {
		expectRejected(&mysqlv1alpha1.ScheduledBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "sched-logical-on-physical", Namespace: "default"},
			Spec: mysqlv1alpha1.ScheduledBackupSpec{
				Schedule: "0 0 3 * * *",
				Cluster:  mysqlv1alpha1.LocalObjectReference{Name: "demo"},
				Logical:  &mysqlv1alpha1.LogicalBackupOptions{Databases: []string{"shop"}},
			},
		}, "logical is only valid with method: logical")

		s := &mysqlv1alpha1.ScheduledBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "sched-logical-ok", Namespace: "default"},
			Spec: mysqlv1alpha1.ScheduledBackupSpec{
				Schedule: "0 0 3 * * *",
				Cluster:  mysqlv1alpha1.LocalObjectReference{Name: "demo"},
				Method:   mysqlv1alpha1.BackupMethodLogical,
				Logical:  &mysqlv1alpha1.LogicalBackupOptions{Databases: []string{"shop"}},
			},
		}
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, s) })
	})
})
