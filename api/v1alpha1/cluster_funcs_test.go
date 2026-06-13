/*
Copyright 2026 The CNMySQL Authors.

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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster defaulting", func() {
	It("applies defaults to an empty spec", func() {
		cluster := &Cluster{}
		cluster.SetDefaults()

		Expect(cluster.Spec.Instances).To(Equal(DefaultInstances))
		Expect(cluster.Spec.MySQL.BinlogFormat).To(Equal(DefaultBinlogFormat))
		Expect(cluster.Spec.PrimaryUpdateStrategy).To(Equal(PrimaryUpdateStrategyUnsupervised))
		Expect(cluster.Spec.PrimaryUpdateMethod).To(Equal(PrimaryUpdateMethodSwitchover))
		Expect(cluster.Spec.MaxStartDelay).To(Equal(int32(DefaultStartupDelay)))
		Expect(cluster.Spec.MaxStopDelay).To(Equal(int32(DefaultShutdownDelay)))
		Expect(cluster.Spec.MaxSwitchoverDelay).To(Equal(int32(DefaultSwitchoverDelay)))
		Expect(cluster.Spec.EnablePDB).To(HaveValue(BeTrue()))
		Expect(cluster.Spec.EnableSuperuserAccess).To(HaveValue(BeFalse()))
		Expect(cluster.Spec.Storage.ResizeInUseVolumes).To(HaveValue(BeTrue()))
	})

	It("does not override explicitly set values", func() {
		cluster := &Cluster{
			Spec: ClusterSpec{
				Instances:             3,
				EnableSuperuserAccess: ptrTo(true),
				MySQL:                 MySQLConfiguration{BinlogFormat: "MIXED"},
			},
		}
		cluster.SetDefaults()

		Expect(cluster.Spec.Instances).To(Equal(3))
		Expect(cluster.Spec.EnableSuperuserAccess).To(HaveValue(BeTrue()))
		Expect(cluster.Spec.MySQL.BinlogFormat).To(Equal("MIXED"))
	})

	It("is idempotent", func() {
		cluster := &Cluster{}
		cluster.SetDefaults()
		first := cluster.DeepCopy()
		cluster.SetDefaults()
		Expect(cluster).To(Equal(first))
	})

	It("defaults the object store fields", func() {
		cluster := &Cluster{
			Spec: ClusterSpec{
				Backup: &BackupConfiguration{
					ObjectStore: &S3ObjectStore{Bucket: "backups"},
				},
			},
		}
		cluster.SetDefaults()

		Expect(cluster.Spec.Backup.ObjectStore.ForcePathStyle).To(HaveValue(BeTrue()))
		Expect(cluster.Spec.Backup.ObjectStore.SignatureVersion).To(Equal(SignatureVersionV4))
	})
})

var _ = Describe("Cluster validation", func() {
	newValidCluster := func() *Cluster {
		cluster := &Cluster{
			Spec: ClusterSpec{
				ImageName: "percona/percona-server:8.0",
				Instances: 3,
				Storage:   StorageConfiguration{Size: "10Gi"},
			},
		}
		cluster.SetDefaults()
		return cluster
	}

	It("accepts a valid cluster", func() {
		Expect(newValidCluster().Validate()).To(BeEmpty())
	})

	It("rejects setting both imageName and imageCatalogRef", func() {
		cluster := newValidCluster()
		cluster.Spec.ImageCatalogRef = &ImageCatalogRef{Major: 8}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects zero instances", func() {
		cluster := newValidCluster()
		cluster.Spec.Instances = 0
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects maxSyncReplicas >= instances", func() {
		cluster := newValidCluster()
		cluster.Spec.Instances = 3
		cluster.Spec.MaxSyncReplicas = 3
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects maxSyncReplicas lower than minSyncReplicas", func() {
		cluster := newValidCluster()
		cluster.Spec.MinSyncReplicas = 2
		cluster.Spec.MaxSyncReplicas = 1
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects initdb and recovery set together", func() {
		cluster := newValidCluster()
		cluster.Spec.Bootstrap = &BootstrapConfiguration{
			InitDB:   &BootstrapInitDB{},
			Recovery: &BootstrapRecovery{},
		}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	recoveryCluster := func() *Cluster {
		cluster := newValidCluster()
		cluster.Spec.Backup = &BackupConfiguration{ObjectStore: &S3ObjectStore{Bucket: "backups"}}
		cluster.Spec.Bootstrap = &BootstrapConfiguration{
			Recovery: &BootstrapRecovery{Backup: &LocalObjectReference{Name: "base"}},
		}
		return cluster
	}

	It("accepts a recovery with a valid targetTime", func() {
		cluster := recoveryCluster()
		cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &RecoveryTarget{TargetTime: "2026-06-12T10:30:00Z"}
		Expect(cluster.Validate()).To(BeEmpty())
	})

	It("accepts a recovery with a valid targetGTID", func() {
		cluster := recoveryCluster()
		cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &RecoveryTarget{
			TargetGTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
		}
		Expect(cluster.Validate()).To(BeEmpty())
	})

	It("rejects a malformed targetTime", func() {
		cluster := recoveryCluster()
		cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &RecoveryTarget{TargetTime: "yesterday"}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects a malformed targetGTID", func() {
		cluster := recoveryCluster()
		cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &RecoveryTarget{TargetGTID: "not-a-gtid"}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects more than one recovery target dimension", func() {
		cluster := recoveryCluster()
		cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &RecoveryTarget{
			TargetTime: "2026-06-12T10:30:00Z",
			TargetGTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
		}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects a recovery target without an object store", func() {
		cluster := recoveryCluster()
		cluster.Spec.Backup = nil
		cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &RecoveryTarget{TargetImmediate: ptrTo(true)}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects a replica source missing from externalClusters", func() {
		cluster := newValidCluster()
		cluster.Spec.Replica = &ReplicaClusterConfiguration{Source: "origin"}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("accepts a replica source present in externalClusters", func() {
		cluster := newValidCluster()
		cluster.Spec.Replica = &ReplicaClusterConfiguration{Source: "origin"}
		cluster.Spec.ExternalClusters = []ExternalCluster{{Name: "origin"}}
		Expect(cluster.Validate()).To(BeEmpty())
	})
})

var _ = Describe("Cluster helpers", func() {
	It("reports replica mode correctly", func() {
		cluster := &Cluster{}
		Expect(cluster.IsReplica()).To(BeFalse())

		cluster.Spec.Replica = &ReplicaClusterConfiguration{Source: "origin"}
		Expect(cluster.IsReplica()).To(BeTrue())

		cluster.Spec.Replica.Enabled = ptrTo(false)
		Expect(cluster.IsReplica()).To(BeFalse())
	})

	It("resolves superuser access default", func() {
		cluster := &Cluster{}
		Expect(cluster.GetEnableSuperuserAccess()).To(BeFalse())
		cluster.Spec.EnableSuperuserAccess = ptrTo(true)
		Expect(cluster.GetEnableSuperuserAccess()).To(BeTrue())
	})
})
