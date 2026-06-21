//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec exercises point-in-time recovery into a Group Replication cluster
// end-to-end against a real Kind cluster backed by in-cluster MinIO. It combines
// the M7.2 PiTR mechanism with the M-GR.7 fresh-group recovery guarantee: a
// 3-member source group takes a base backup and continuously archives its
// binlogs; a fresh 3-member group then bootstraps from that base backup and
// replays the archive up to a chosen GTID.
//
// The load-bearing assertions are GR-specific:
//   - recovery forms a FRESH group (new pinned group name, never rejoining the
//     source's group), bootstrapped on the recovered data;
//   - replay is bounded by the target — the recovered group contains the write at
//     the target but NOT a later write committed past it;
//   - every secondary joins the fresh group via distributed recovery and ends up
//     with the same point-in-time data as the primary.
//
// One Percona version is exercised (the first of the archive matrix) to bound
// runtime; the replay mechanism itself is version-agnostic.
var _ = Describe("Group Replication point-in-time recovery", Ordered, func() {
	const (
		sourceCluster   = "gr-pitr-src"
		restoredCluster = "gr-pitr-restored"
		backupName      = "gr-pitr-base"
		instances       = 3
	)
	version := archiveVersions()[0]

	var (
		password        string
		targetGTID      string
		sourceGroupName string
		ns, prevNS      string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("gr-pitr")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating a 3-member Group Replication source cluster with continuous archiving enabled")
		applyManifest(sourceCluster, grArchivingClusterManifest(sourceCluster, version, instances))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, grArchivingClusterManifest(sourceCluster, version, instances))
		})
		expectClusterReady(sourceCluster, instances, 20*time.Minute)
		password = appPassword(sourceCluster)

		var gnErr error
		sourceGroupName, gnErr = clusterField(sourceCluster, "{.status.groupReplication.groupName}")
		Expect(gnErr).NotTo(HaveOccurred())
		Expect(sourceGroupName).NotTo(BeEmpty(), "source group must have a pinned group name")
	})

	It("recovers a fresh group to a chosen GTID, seeing the target write but not a later one", func() {
		primary := clusterPrimary(sourceCluster)

		By("taking a base backup before any application data exists")
		applyManifest(backupName, backupManifest(backupName, sourceCluster))
		DeferCleanup(func() { deleteManifest(backupName, backupManifest(backupName, sourceCluster)) })
		Eventually(func(g Gomega) {
			phase, err := kubectl("get", "backup", backupName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).NotTo(Equal("failed"), "base backup failed")
			g.Expect(phase).To(Equal("completed"), "base backup not completed yet")
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("writing the target row (id=1) after the base backup")
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE ledger (id INT PRIMARY KEY, note VARCHAR(32)); "+
				"INSERT INTO ledger VALUES (1, 'target');")
		Expect(err).NotTo(HaveOccurred(), "Failed to write the target row")

		By("capturing gtid_executed at the target and waiting for the archive to cover it")
		targetGTID = flushBinaryLogs(sourceCluster, primary, password)
		Expect(targetGTID).NotTo(BeEmpty(), "target GTID parsed empty")
		expectArchiveCovers(sourceCluster, targetGTID, 5*time.Minute)

		By("writing a later row (id=2) that must NOT be recovered")
		_, err = mysqlExec(primary, "app", password, "app",
			"INSERT INTO ledger VALUES (2, 'past-target');")
		Expect(err).NotTo(HaveOccurred(), "Failed to write the post-target row")
		// Rotate so the post-target write is shippable too; the archive holding it
		// must not change the recovery result, which is bounded by targetGTID.
		flushBinaryLogs(sourceCluster, primary, password)

		By("bootstrapping a fresh 3-member group recovering to the target GTID")
		applyManifest(restoredCluster, grPitrRecoveryClusterManifest(restoredCluster, version, instances, backupName, targetGTID))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, grPitrRecoveryClusterManifest(restoredCluster, version, instances, backupName, targetGTID))
		})
		expectClusterReady(restoredCluster, instances, 20*time.Minute)

		By("verifying the restored cluster bootstrapped a FRESH group (new, distinct group name)")
		Eventually(func(g Gomega) {
			bootstrapped, err := clusterField(restoredCluster, "{.status.groupReplication.bootstrapped}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(bootstrapped).To(Equal("true"), "the restored cluster must bootstrap its own group")

			restoredGroupName, err := clusterField(restoredCluster, "{.status.groupReplication.groupName}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(restoredGroupName).NotTo(BeEmpty(), "a fresh group name must be pinned")
			g.Expect(restoredGroupName).NotTo(Equal(sourceGroupName),
				"recovery must form a fresh group, never rejoin the source's group")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying all three restored members are ONLINE with quorum")
		Eventually(func(g Gomega) {
			quorum, err := clusterField(restoredCluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"))
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())

		By("verifying the recovered primary has the target row but not the later one")
		restoredPrimary := clusterPrimary(restoredCluster)
		// Recovery reuses the source's app credentials (no new app Secret is
		// generated for a recovery bootstrap).
		Eventually(func(g Gomega) {
			out, err := mysqlExec(restoredPrimary, "app", password, "app",
				"SELECT note FROM ledger WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("target"), "target row missing after recovery")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		out, err := mysqlExec(restoredPrimary, "app", password, "app",
			"SELECT COUNT(*) FROM ledger WHERE id = 2;")
		Expect(err).NotTo(HaveOccurred())
		Expect(parseSingleValue(out)).To(Equal("0"),
			"recovered primary contains a write past the recovery target")

		By("verifying every secondary reached the same point in time via distributed recovery")
		for _, secondary := range []string{restoredCluster + "-2", restoredCluster + "-3"} {
			Eventually(func(g Gomega) {
				got, err := mysqlExec(secondary, "app", password, "",
					"SELECT note FROM app.ledger WHERE id = 1;")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got).To(ContainSubstring("target"),
					"secondary %s must join the fresh group and recover the target row", secondary)
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

			later, err := mysqlExec(secondary, "app", password, "",
				"SELECT COUNT(*) FROM app.ledger WHERE id = 2;")
			Expect(err).NotTo(HaveOccurred())
			Expect(parseSingleValue(later)).To(Equal("0"),
				"secondary %s contains a write past the recovery target", secondary)
		}
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// grArchivingClusterManifest renders a Group Replication Cluster with continuous
// binlog archiving enabled, pinned to a specific instance image version. A tight
// RPO and small max binlog size keep the archiving loop active during the short
// lifetime of an e2e spec.
func grArchivingClusterManifest(name, version string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  replication:
    mode: groupReplication
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
%[7]s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
`, name, testNamespace, instances, instanceImageFor(version), e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

// grPitrRecoveryClusterManifest renders a Group Replication Cluster that
// bootstraps a fresh group by recovering a base backup and replaying archived
// binlogs up to targetGTID. It points at the same object store (for the binlog
// archive) but does not re-enable archiving.
func grPitrRecoveryClusterManifest(name, version string, instances int, backup, targetGTID string) string {
	return fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  replication:
    mode: groupReplication
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    recovery:
      backup:
        name: %[7]s
      recoveryTarget:
        targetGTID: "%[8]s"
  backup:
%[9]s
`, name, testNamespace, instances, instanceImageFor(version), e2eInstanceResources, e2eMySQLParameters, backup, targetGTID, objectStoreYAML("    "))
}
