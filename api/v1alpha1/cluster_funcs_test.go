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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
)

var _ = Describe("Cluster instance selector", func() {
	It("exposes a serialized selector keyed on the cluster label", func() {
		cluster := &Cluster{}
		cluster.Name = "demo"
		cluster.Namespace = "default"

		selector := cluster.GetInstancesSelector()

		parsed, err := labels.Parse(selector)
		Expect(err).NotTo(HaveOccurred())
		Expect(parsed.Matches(labels.Set{ClusterLabelName: "demo"})).To(BeTrue())
		Expect(parsed.Matches(labels.Set{ClusterLabelName: "other"})).To(BeFalse())
	})

	It("matches the label the controller stamps on instance Pods", func() {
		cluster := &Cluster{}
		cluster.Name = "demo"

		selector := cluster.GetInstancesSelector()
		parsed, err := labels.Parse(selector)
		Expect(err).NotTo(HaveOccurred())

		// The operator labels every instance Pod with the cluster label set to
		// the Cluster name (see internal/controller labelsFor); the scale
		// sub-resource selector must select exactly those Pods.
		instancePodLabels := labels.Set{"mysql.cnmsql.co/cluster": "demo", "mysql.cnmsql.co/role": "replica"}
		Expect(parsed.Matches(instancePodLabels)).To(BeTrue())
	})
})

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
				EnableSuperuserAccess: ptr.To(true),
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
		cluster.Spec.ImageCatalogRef = &ImageCatalogRef{Series: "8.0"}
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
		cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &RecoveryTarget{TargetImmediate: ptr.To(true)}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("accepts a valid retention policy with an object store", func() {
		cluster := newValidCluster()
		cluster.Spec.Backup = &BackupConfiguration{
			ObjectStore:     &S3ObjectStore{Bucket: "backups"},
			RetentionPolicy: "30d",
		}
		Expect(cluster.Validate()).To(BeEmpty())
	})

	It("rejects a malformed retention policy", func() {
		cluster := newValidCluster()
		cluster.Spec.Backup = &BackupConfiguration{
			ObjectStore:     &S3ObjectStore{Bucket: "backups"},
			RetentionPolicy: "30x",
		}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects a retention policy without an object store", func() {
		cluster := newValidCluster()
		cluster.Spec.Backup = &BackupConfiguration{RetentionPolicy: "30d"}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("accepts a source-based recovery referencing an objectStore externalCluster", func() {
		cluster := newValidCluster()
		cluster.Spec.Bootstrap = &BootstrapConfiguration{
			Recovery: &BootstrapRecovery{Source: "prod"},
		}
		cluster.Spec.ExternalClusters = []ExternalCluster{
			{Name: "prod", ObjectStore: &S3ObjectStore{Bucket: "backups"}},
		}
		Expect(cluster.Validate()).To(BeEmpty())
	})

	It("rejects source and backup set together", func() {
		cluster := newValidCluster()
		cluster.Spec.Bootstrap = &BootstrapConfiguration{
			Recovery: &BootstrapRecovery{
				Source: "prod",
				Backup: &LocalObjectReference{Name: "base"},
			},
		}
		cluster.Spec.ExternalClusters = []ExternalCluster{
			{Name: "prod", ObjectStore: &S3ObjectStore{Bucket: "backups"}},
		}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects a source missing from externalClusters", func() {
		cluster := newValidCluster()
		cluster.Spec.Bootstrap = &BootstrapConfiguration{
			Recovery: &BootstrapRecovery{Source: "prod"},
		}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects a source whose externalCluster has no objectStore", func() {
		cluster := newValidCluster()
		cluster.Spec.Bootstrap = &BootstrapConfiguration{
			Recovery: &BootstrapRecovery{Source: "prod"},
		}
		cluster.Spec.ExternalClusters = []ExternalCluster{{Name: "prod"}}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("accepts a source-based recovery with a backupID", func() {
		cluster := newValidCluster()
		cluster.Spec.Bootstrap = &BootstrapConfiguration{
			Recovery: &BootstrapRecovery{Source: "prod", BackupID: "20260612T100000"},
		}
		cluster.Spec.ExternalClusters = []ExternalCluster{
			{Name: "prod", ObjectStore: &S3ObjectStore{Bucket: "backups"}},
		}
		Expect(cluster.Validate()).To(BeEmpty())
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

	It("rejects disabling the rw service", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Services: &ManagedServices{
			DisabledDefaultServices: []ServiceSelectorType{ServiceSelectorTypeRW},
		}}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("accepts disabling the ro service", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Services: &ManagedServices{
			DisabledDefaultServices: []ServiceSelectorType{ServiceSelectorTypeRO},
		}}
		Expect(cluster.Validate()).To(BeEmpty())
	})

	It("rejects duplicate additional service names", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Services: &ManagedServices{
			Additional: []ManagedService{
				{Name: "lb", SelectorType: ServiceSelectorTypeRW},
				{Name: "lb", SelectorType: ServiceSelectorTypeRO},
			},
		}}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects an additional service named after a default suffix", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Services: &ManagedServices{
			Additional: []ManagedService{{Name: "rw", SelectorType: ServiceSelectorTypeRW}},
		}}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("accepts a valid additional service", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Services: &ManagedServices{
			Additional: []ManagedService{{Name: "mysql-lb", SelectorType: ServiceSelectorTypeRW}},
		}}
		Expect(cluster.Validate()).To(BeEmpty())
	})

	It("accepts a valid managed role", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Roles: []RoleConfiguration{
			{Name: "app", Host: "%", RequireTLS: "x509",
				Privileges: []RolePrivilege{{Privileges: []string{"SELECT"}, On: "app.*"}}},
		}}
		Expect(cluster.Validate()).To(BeEmpty())
	})

	It("rejects a reserved managed role name", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Roles: []RoleConfiguration{
			{Name: "cnmsql_repl", Host: "%"},
		}}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects duplicate managed role name+host", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Roles: []RoleConfiguration{
			{Name: "app", Host: "%"},
			{Name: "app", Host: "%"},
		}}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects superuser combined with explicit privileges", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Roles: []RoleConfiguration{
			{Name: "app", Host: "%", Superuser: true,
				Privileges: []RolePrivilege{{Privileges: []string{"SELECT"}}}},
		}}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects an invalid requireTLS value", func() {
		cluster := newValidCluster()
		cluster.Spec.Managed = &ManagedConfiguration{Roles: []RoleConfiguration{
			{Name: "app", Host: "%", RequireTLS: "bogus"},
		}}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})
})

var _ = Describe("Cluster helpers", func() {
	It("reports replica mode correctly", func() {
		cluster := &Cluster{}
		Expect(cluster.IsReplica()).To(BeFalse())

		cluster.Spec.Replica = &ReplicaClusterConfiguration{Source: "origin"}
		Expect(cluster.IsReplica()).To(BeTrue())

		cluster.Spec.Replica.Enabled = ptr.To(false)
		Expect(cluster.IsReplica()).To(BeFalse())
	})

	It("resolves superuser access default", func() {
		cluster := &Cluster{}
		Expect(cluster.GetEnableSuperuserAccess()).To(BeFalse())
		cluster.Spec.EnableSuperuserAccess = ptr.To(true)
		Expect(cluster.GetEnableSuperuserAccess()).To(BeTrue())
	})

	It("parses retention policies into durations", func() {
		d, err := ParseRetentionPolicy("30d")
		Expect(err).NotTo(HaveOccurred())
		Expect(d).To(Equal(30 * 24 * time.Hour))

		w, err := ParseRetentionPolicy("8w")
		Expect(err).NotTo(HaveOccurred())
		Expect(w).To(Equal(8 * 7 * 24 * time.Hour))

		m, err := ParseRetentionPolicy("3m")
		Expect(err).NotTo(HaveOccurred())
		Expect(m).To(Equal(3 * 30 * 24 * time.Hour))

		_, err = ParseRetentionPolicy("0d")
		Expect(err).To(HaveOccurred())
		_, err = ParseRetentionPolicy("garbage")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("Group Replication validation", func() {
	newGRCluster := func() *Cluster {
		cluster := &Cluster{
			Spec: ClusterSpec{
				ImageName:   "percona/percona-server:8.0",
				Instances:   3,
				Storage:     StorageConfiguration{Size: "10Gi"},
				Replication: &ReplicationConfiguration{Mode: ReplicationModeGroupReplication},
			},
		}
		cluster.SetDefaults()
		return cluster
	}

	It("accepts a valid group replication cluster", func() {
		Expect(newGRCluster().Validate()).To(BeEmpty())
	})

	It("rejects group replication combined with semi-sync", func() {
		cluster := newGRCluster()
		cluster.Spec.MySQL.SemiSync = &SemiSyncConfiguration{Enabled: true}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("accepts a valid pinned group name", func() {
		cluster := newGRCluster()
		cluster.Spec.Replication.GroupReplication = &GroupReplicationConfiguration{
			GroupName: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		}
		Expect(cluster.Validate()).To(BeEmpty())
	})

	It("rejects a non-UUID group name", func() {
		cluster := newGRCluster()
		cluster.Spec.Replication.GroupReplication = &GroupReplicationConfiguration{
			GroupName: "not-a-uuid",
		}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects a groupReplication block on an async cluster", func() {
		cluster := newGRCluster()
		cluster.Spec.Replication.Mode = ReplicationModeAsync
		cluster.Spec.Replication.GroupReplication = &GroupReplicationConfiguration{}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects group replication on a pre-8.0 image catalog", func() {
		cluster := newGRCluster()
		cluster.Spec.ImageName = ""
		cluster.Spec.ImageCatalogRef = &ImageCatalogRef{Series: "5.7"}
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects changing replication.mode on update", func() {
		oldCluster := newGRCluster()
		newCluster := newGRCluster()
		newCluster.Spec.Replication.Mode = ReplicationModeAsync
		Expect(newCluster.ValidateUpdate(oldCluster)).NotTo(BeEmpty())
	})

	It("allows an unchanged mode on update", func() {
		oldCluster := newGRCluster()
		newCluster := newGRCluster()
		Expect(newCluster.ValidateUpdate(oldCluster)).To(BeEmpty())
	})

	It("rejects changing a pinned group name on update", func() {
		oldCluster := newGRCluster()
		oldCluster.Spec.Replication.GroupReplication = &GroupReplicationConfiguration{
			GroupName: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		}
		newCluster := newGRCluster()
		newCluster.Spec.Replication.GroupReplication = &GroupReplicationConfiguration{
			GroupName: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
		}
		Expect(newCluster.ValidateUpdate(oldCluster)).NotTo(BeEmpty())
	})
})

var _ = Describe("Series upgrade validation", func() {
	catalogCluster := func(series string) *Cluster {
		cluster := &Cluster{
			Spec: ClusterSpec{
				ImageCatalogRef: &ImageCatalogRef{Series: series},
				Instances:       3,
				Storage:         StorageConfiguration{Size: "10Gi"},
			},
		}
		cluster.SetDefaults()
		return cluster
	}
	imageCluster := func(image string) *Cluster {
		cluster := &Cluster{
			Spec: ClusterSpec{
				ImageName: image,
				Instances: 3,
				Storage:   StorageConfiguration{Size: "10Gi"},
			},
		}
		cluster.SetDefaults()
		return cluster
	}

	It("allows a single supported hop via catalog", func() {
		Expect(catalogCluster("8.4").ValidateUpdate(catalogCluster("8.0"))).To(BeEmpty())
	})

	It("rejects skipping a series via catalog", func() {
		Expect(catalogCluster("9.0").ValidateUpdate(catalogCluster("8.0"))).NotTo(BeEmpty())
	})

	It("rejects a downgrade via catalog", func() {
		Expect(catalogCluster("8.0").ValidateUpdate(catalogCluster("8.4"))).NotTo(BeEmpty())
	})

	It("allows an unchanged series via catalog", func() {
		Expect(catalogCluster("8.0").ValidateUpdate(catalogCluster("8.0"))).To(BeEmpty())
	})

	It("rejects a series change expressed through imageName", func() {
		old := imageCluster("percona/percona-server:8.0")
		updated := imageCluster("percona/percona-server:8.4")
		Expect(updated.ValidateUpdate(old)).NotTo(BeEmpty())
	})

	It("allows a patch bump within a series via imageName", func() {
		old := imageCluster("percona/percona-server:8.0.36")
		updated := imageCluster("percona/percona-server:8.0.40")
		Expect(updated.ValidateUpdate(old)).To(BeEmpty())
	})

	It("does not guard when a series cannot be determined", func() {
		old := imageCluster("percona/percona-server@sha256:deadbeef")
		updated := imageCluster("percona/percona-server:8.4")
		Expect(updated.ValidateUpdate(old)).To(BeEmpty())
	})
})

var _ = Describe("BackupBeforeUpgrade defaulting", func() {
	It("defaults to true when upgrade config is absent", func() {
		Expect((&Cluster{}).BackupBeforeUpgradeEnabled()).To(BeTrue())
	})

	It("defaults to true when the flag is unset", func() {
		cluster := &Cluster{Spec: ClusterSpec{Upgrade: &UpgradeConfiguration{}}}
		Expect(cluster.BackupBeforeUpgradeEnabled()).To(BeTrue())
	})

	It("honours an explicit false", func() {
		cluster := &Cluster{Spec: ClusterSpec{Upgrade: &UpgradeConfiguration{
			BackupBeforeUpgrade: ptr.To(false),
		}}}
		Expect(cluster.BackupBeforeUpgradeEnabled()).To(BeFalse())
	})
})
