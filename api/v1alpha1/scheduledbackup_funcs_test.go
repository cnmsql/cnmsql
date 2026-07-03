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

package v1alpha1

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

var _ = Describe("ScheduledBackup defaulting", func() {
	It("applies defaults to an empty spec", func() {
		sb := &ScheduledBackup{}
		sb.SetDefaults()

		Expect(sb.Spec.Suspend).To(HaveValue(BeFalse()))
		Expect(sb.Spec.Immediate).To(HaveValue(BeFalse()))
		Expect(sb.Spec.BackupOwnerReference).To(Equal("self"))
		Expect(sb.Spec.Method).To(Equal(BackupMethodXtrabackup))
		Expect(sb.Spec.Target).To(Equal(BackupTargetPreferStandby))
		Expect(sb.Spec.Online).To(HaveValue(BeTrue()))
		Expect(sb.Spec.ReclaimPolicy).To(Equal(BackupReclaimRetain))
	})

	It("does not override explicitly set values", func() {
		sb := &ScheduledBackup{
			Spec: ScheduledBackupSpec{
				Suspend:              ptr.To(true),
				Immediate:            ptr.To(true),
				BackupOwnerReference: "cluster",
				Method:               BackupMethodXtrabackup,
				Target:               BackupTargetPrimary,
				Online:               ptr.To(false),
			},
		}
		sb.SetDefaults()

		Expect(sb.Spec.Suspend).To(HaveValue(BeTrue()))
		Expect(sb.Spec.Immediate).To(HaveValue(BeTrue()))
		Expect(sb.Spec.BackupOwnerReference).To(Equal("cluster"))
		Expect(sb.Spec.Target).To(Equal(BackupTargetPrimary))
		Expect(sb.Spec.Online).To(HaveValue(BeFalse()))
	})

	It("is idempotent", func() {
		sb := &ScheduledBackup{}
		sb.SetDefaults()
		first := sb.DeepCopy()
		sb.SetDefaults()
		Expect(sb.Spec).To(Equal(first.Spec))
	})
})

var _ = Describe("ScheduledBackup accessors", func() {
	It("resolves suspend and immediate through their pointers", func() {
		sb := &ScheduledBackup{}
		Expect(sb.IsSuspended()).To(BeFalse())
		Expect(sb.IsImmediate()).To(BeFalse())

		sb.Spec.Suspend = ptr.To(true)
		sb.Spec.Immediate = ptr.To(true)
		Expect(sb.IsSuspended()).To(BeTrue())
		Expect(sb.IsImmediate()).To(BeTrue())
	})

	It("returns the configured schedule", func() {
		sb := &ScheduledBackup{Spec: ScheduledBackupSpec{Schedule: "0 0 * * * *"}}
		Expect(sb.GetSchedule()).To(Equal("0 0 * * * *"))
	})
})

var _ = Describe("ScheduledBackup BackupName", func() {
	It("is deterministic for a given time and UTC-normalised", func() {
		sb := &ScheduledBackup{ObjectMeta: metav1.ObjectMeta{Name: "nightly"}}
		t := time.Date(2026, 6, 13, 1, 2, 3, 0, time.UTC)

		name := sb.BackupName(t)
		Expect(name).To(Equal("nightly-20260613010203"))
		// Calling again with the same instant yields the same name.
		Expect(sb.BackupName(t)).To(Equal(name))

		// The same instant in a different zone produces the same name.
		loc := time.FixedZone("UTC+2", 2*60*60)
		Expect(sb.BackupName(t.In(loc))).To(Equal(name))
	})

	It("differs for different times", func() {
		sb := &ScheduledBackup{ObjectMeta: metav1.ObjectMeta{Name: "nightly"}}
		t1 := time.Date(2026, 6, 13, 1, 2, 3, 0, time.UTC)
		t2 := t1.Add(time.Second)
		Expect(sb.BackupName(t1)).NotTo(Equal(sb.BackupName(t2)))
	})
})

var _ = Describe("ScheduledBackup CreateBackup", func() {
	It("propagates the cluster, method, target and online settings", func() {
		online := ptr.To(false)
		sb := &ScheduledBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "prod"},
			Spec: ScheduledBackupSpec{
				Cluster: LocalObjectReference{Name: "demo"},
				Method:  BackupMethodXtrabackup,
				Target:  BackupTargetPrimary,
				Online:  online,
			},
		}

		backup := sb.CreateBackup("nightly-20260613010203")
		Expect(backup.Name).To(Equal("nightly-20260613010203"))
		Expect(backup.Namespace).To(Equal("prod"))
		Expect(backup.Spec.Cluster.Name).To(Equal("demo"))
		Expect(backup.Spec.Method).To(Equal(BackupMethodXtrabackup))
		Expect(backup.Spec.Target).To(Equal(BackupTargetPrimary))
		Expect(backup.Spec.Online).To(HaveValue(BeFalse()))
	})

	It("defaults the generated backup to Retain and never stamps the finalizer", func() {
		sb := &ScheduledBackup{ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "prod"}}
		backup := sb.CreateBackup("nightly-1")
		Expect(backup.Spec.ReclaimPolicy).To(Equal(BackupReclaimRetain))
		// The finalizer is stamped later by the reconciler, not at creation.
		Expect(backup.Finalizers).NotTo(ContainElement(BackupCleanupFinalizer))
	})

	It("propagates reclaimPolicy Delete to the generated backup", func() {
		sb := &ScheduledBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "prod"},
			Spec:       ScheduledBackupSpec{ReclaimPolicy: BackupReclaimDelete},
		}
		backup := sb.CreateBackup("nightly-1")
		Expect(backup.Spec.ReclaimPolicy).To(Equal(BackupReclaimDelete))
		Expect(backup.WantsObjectStoreCleanup()).To(BeTrue())
	})
})

var _ = Describe("ScheduledBackup schedule validation", func() {
	It("accepts a valid 6-field (seconds) cron expression", func() {
		sb := &ScheduledBackup{Spec: ScheduledBackupSpec{Schedule: "0 0 0 * * *"}}
		Expect(sb.Validate()).To(BeEmpty())
	})

	It("rejects a 5-field expression (missing seconds)", func() {
		sb := &ScheduledBackup{Spec: ScheduledBackupSpec{Schedule: "0 0 * * *"}}
		Expect(sb.Validate()).NotTo(BeEmpty())
	})

	It("rejects garbage", func() {
		sb := &ScheduledBackup{Spec: ScheduledBackupSpec{Schedule: "not-a-cron"}}
		Expect(sb.Validate()).NotTo(BeEmpty())
	})

	It("rejects an empty schedule", func() {
		sb := &ScheduledBackup{}
		Expect(sb.Validate()).NotTo(BeEmpty())
	})
})
