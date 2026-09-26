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
	"k8s.io/apimachinery/pkg/labels"
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
				EnableSuperuserAccess: new(true),
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
		cluster.Spec.Bootstrap.Recovery.RecoveryTarget = &RecoveryTarget{TargetImmediate: new(true)}
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

	importCluster := func(imp *BootstrapImport) *Cluster {
		cluster := newValidCluster()
		cluster.Spec.Bootstrap = &BootstrapConfiguration{InitDB: &BootstrapInitDB{Import: imp}}
		cluster.Spec.ExternalClusters = []ExternalCluster{
			{Name: "prod", ObjectStore: &S3ObjectStore{Bucket: "backups"}},
			{Name: "no-store"},
		}
		return cluster
	}

	DescribeTable("validates initdb.import",
		func(imp *BootstrapImport, wantField string) {
			errs := importCluster(imp).Validate()
			if wantField == "" {
				Expect(errs).To(BeEmpty())
				return
			}
			Expect(errs).To(ContainElement(HaveField("Field", wantField)))
		},
		Entry("a Backup reference",
			&BootstrapImport{Backup: &LocalObjectReference{Name: "nightly"}}, ""),
		Entry("a source with a backupID and databases",
			&BootstrapImport{Source: "prod", BackupID: "20260612T100000", Databases: []string{"billing"}}, ""),
		Entry("neither backup nor source",
			&BootstrapImport{}, "spec.bootstrap.initdb.import.backup"),
		Entry("an empty backup name",
			&BootstrapImport{Backup: &LocalObjectReference{}}, "spec.bootstrap.initdb.import.backup"),
		Entry("both backup and source",
			&BootstrapImport{Source: "prod", Backup: &LocalObjectReference{Name: "nightly"}},
			"spec.bootstrap.initdb.import.source"),
		Entry("a source missing from externalClusters",
			&BootstrapImport{Source: "staging"}, "spec.bootstrap.initdb.import.source"),
		Entry("a source without an objectStore",
			&BootstrapImport{Source: "no-store"}, "spec.bootstrap.initdb.import.source"),
		Entry("a backupID without a source",
			&BootstrapImport{Backup: &LocalObjectReference{Name: "nightly"}, BackupID: "20260612T100000"},
			"spec.bootstrap.initdb.import.backupID"),
	)

	It("rejects initdb.import together with recovery", func() {
		cluster := importCluster(&BootstrapImport{Backup: &LocalObjectReference{Name: "nightly"}})
		cluster.Spec.Bootstrap.Recovery = &BootstrapRecovery{Backup: &LocalObjectReference{Name: "base"}}
		Expect(cluster.Validate()).To(ContainElement(HaveField("Field", "spec.bootstrap")))
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

var _ = Describe("Credential secret names", func() {
	It("derives the credential Secret names from the cluster name", func() {
		cluster := &Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
		Expect(cluster.RootSecretName()).To(Equal("demo-root"))
		Expect(cluster.AppSecretName()).To(Equal(""), "the app secret is empty without initdb")
		Expect(cluster.ControlSecretName()).To(Equal("demo-control"))
		Expect(cluster.BackupSecretName()).To(Equal("demo-backup"))
		Expect(cluster.DumpSecretName()).To(Equal("demo-dump"))

		cluster.Spec.Bootstrap = &BootstrapConfiguration{InitDB: &BootstrapInitDB{}}
		Expect(cluster.AppSecretName()).To(Equal("demo-app"))

		cluster.Spec.Bootstrap.InitDB.Secret = &LocalObjectReference{Name: "mine"}
		cluster.Spec.RootPasswordSecret = &LocalObjectReference{Name: "my-root"}
		Expect(cluster.AppSecretName()).To(Equal("mine"), "user-provided secret names are honoured")
		Expect(cluster.RootSecretName()).To(Equal("my-root"))
	})
})

var _ = Describe("Cluster helpers", func() {
	It("reports replica mode correctly", func() {
		cluster := &Cluster{}
		Expect(cluster.IsReplica()).To(BeFalse())

		cluster.Spec.Replica = &ReplicaClusterConfiguration{Source: "origin"}
		Expect(cluster.IsReplica()).To(BeTrue())

		cluster.Spec.Replica.Enabled = new(false)
		Expect(cluster.IsReplica()).To(BeFalse())
	})

	It("resolves superuser access default", func() {
		cluster := &Cluster{}
		Expect(cluster.GetEnableSuperuserAccess()).To(BeFalse())
		cluster.Spec.EnableSuperuserAccess = new(true)
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
			BackupBeforeUpgrade: new(false),
		}}}
		Expect(cluster.BackupBeforeUpgradeEnabled()).To(BeFalse())
	})
})

var _ = Describe("Failover policy validation", func() {
	policyCluster := func(policy *FailoverPolicy) *Cluster {
		cluster := &Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
			Spec: ClusterSpec{
				ImageName:      "percona/percona-server:8.0",
				Instances:      3,
				Storage:        StorageConfiguration{Size: "10Gi"},
				FailoverPolicy: policy,
			},
		}
		cluster.SetDefaults()
		return cluster
	}

	It("accepts a preference naming instances of this cluster", func() {
		cluster := policyCluster(&FailoverPolicy{PreferredPrimary: []string{"demo-2", "demo-1"}})
		Expect(cluster.Validate()).To(BeEmpty())
		Expect(cluster.PreferredPrimary()).To(Equal([]string{"demo-2", "demo-1"}))
	})

	It("rejects a preference naming an instance of another cluster", func() {
		cluster := policyCluster(&FailoverPolicy{PreferredPrimary: []string{"other-1"}})
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects a preference naming the same instance twice", func() {
		cluster := policyCluster(&FailoverPolicy{PreferredPrimary: []string{"demo-1", "demo-1"}})
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("rejects negative anti-flapping timers", func() {
		cluster := policyCluster(&FailoverPolicy{
			MinTimeBetweenFailovers: &metav1.Duration{Duration: -time.Minute},
		})
		Expect(cluster.Validate()).NotTo(BeEmpty())
	})

	It("reads the anti-flapping timers back, and zero when unset", func() {
		cluster := policyCluster(&FailoverPolicy{
			MinTimeBetweenFailovers: &metav1.Duration{Duration: 10 * time.Minute},
			PrimaryStabilityWindow:  &metav1.Duration{Duration: 2 * time.Minute},
		})
		Expect(cluster.Validate()).To(BeEmpty())
		Expect(cluster.MinTimeBetweenFailovers()).To(Equal(10 * time.Minute))
		Expect(cluster.PrimaryStabilityWindow()).To(Equal(2 * time.Minute))

		bare := policyCluster(nil)
		Expect(bare.MinTimeBetweenFailovers()).To(BeZero())
		Expect(bare.PrimaryStabilityWindow()).To(BeZero())
		Expect(bare.PreferredPrimary()).To(BeEmpty())
	})
})

var _ = Describe("Cluster admission warnings", func() {
	semiSyncCluster := func(flavor Flavor, minSync, maxSync int) *Cluster {
		cluster := &Cluster{}
		cluster.Spec.Flavor = flavor
		cluster.Spec.Instances = 3
		cluster.Spec.MinSyncReplicas = minSync
		cluster.Spec.MaxSyncReplicas = maxSync
		cluster.Spec.MySQL.SemiSync = &SemiSyncConfiguration{Enabled: true, DataDurability: DataDurabilityRequired}
		return cluster
	}

	It("warns that MariaDB semi-sync ignores an acknowledgement count above one", func() {
		warnings := semiSyncCluster(FlavorMariaDB, 2, 2).Warnings()
		Expect(warnings).To(HaveLen(1))
		Expect(warnings[0]).To(ContainSubstring("exactly one replica acknowledgement"))
		Expect(warnings[0]).To(ContainSubstring("dataDurability"))
	})

	It("does not warn when MariaDB semi-sync asks for a single acknowledgement", func() {
		Expect(semiSyncCluster(FlavorMariaDB, 1, 1).Warnings()).To(BeEmpty())
	})

	It("does not warn when MariaDB semi-sync is disabled", func() {
		cluster := semiSyncCluster(FlavorMariaDB, 2, 2)
		cluster.Spec.MySQL.SemiSync.Enabled = false
		Expect(cluster.Warnings()).To(BeEmpty())
	})

	It("does not warn on MySQL, which honours the acknowledgement count", func() {
		Expect(semiSyncCluster(FlavorMySQL, 2, 2).Warnings()).To(BeEmpty())
		Expect(semiSyncCluster("", 2, 2).Warnings()).To(BeEmpty())
	})
})
