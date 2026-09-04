//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec exercises point-in-time recovery (M7.2) end-to-end against a real
// Kind cluster backed by in-cluster MinIO. It builds on the M7.1 archiving
// foundation: a source cluster takes a base backup and continuously archives its
// binlogs; a fresh cluster then bootstraps from that base backup and replays the
// archive up to a chosen GTID. Correctness is asserted by data: the recovered
// cluster must contain the write at the target and must NOT contain a later write
// committed past it. The window also contains account-management statements
// committed manually mid-window (a CREATE USER + GRANT, as an operator would run
// them on a live cluster): the replay loads the restored grant tables before
// streaming (FLUSH PRIVILEGES prologue), so those statements execute instead of
// failing with ERROR 1290 under --skip-grant-tables.
//
// One Percona version is exercised (the first of the archive matrix) to bound
// runtime; the replay mechanism itself is version-agnostic and covered per
// version by the binlog integration test.
var _ = Describe("Point-in-time recovery", Ordered, Label("flavor"), func() {
	const (
		sourceCluster   = "pitr-src"
		restoredCluster = "pitr-restored"
		backupName      = "pitr-base"
	)
	version := archiveVersions()[0]

	var (
		password   string
		targetGTID string
		ns, prevNS string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("pitr")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating the source cluster with continuous archiving enabled")
		applyManifest(sourceCluster, continuousArchivingClusterManifest(sourceCluster, version, 1))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, continuousArchivingClusterManifest(sourceCluster, version, 1))
		})
		expectClusterReady(sourceCluster, 1, 20*time.Minute)
		password = appPassword(sourceCluster)
	})

	It("recovers to a chosen GTID, replaying a mid-window user grant but not later writes", func() {
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
		}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())

		By("writing the target row (id=1) after the base backup")
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE ledger (id INT PRIMARY KEY, note VARCHAR(32)); "+
				"INSERT INTO ledger VALUES (1, 'target');")
		Expect(err).NotTo(HaveOccurred(), "Failed to write the target row")

		By("creating a user manually and granting it on the app db, inside the replay window")
		_, err = mysqlExec(primary, "root", rootPassword(sourceCluster), "",
			"CREATE USER 'pitr_user'@'%' IDENTIFIED BY 'pitr-in-window'; "+
				"GRANT SELECT ON app.* TO 'pitr_user'@'%';")
		Expect(err).NotTo(HaveOccurred(), "Failed to create and grant the in-window user")

		By("capturing gtid_executed at the target and waiting for the archive to cover it")
		targetGTID = flushBinaryLogs(sourceCluster, primary, password)
		Expect(targetGTID).NotTo(BeEmpty(), "target GTID parsed empty")
		expectArchiveCovers(sourceCluster, targetGTID, 5*time.Minute)

		By("writing a later row (id=2) and a later user that must NOT be recovered")
		_, err = mysqlExec(primary, "app", password, "app",
			"INSERT INTO ledger VALUES (2, 'past-target');")
		Expect(err).NotTo(HaveOccurred(), "Failed to write the post-target row")
		_, err = mysqlExec(primary, "root", rootPassword(sourceCluster), "",
			"CREATE USER 'pitr_late'@'%' IDENTIFIED BY 'pitr-after-target'; "+
				"GRANT SELECT ON app.* TO 'pitr_late'@'%';")
		Expect(err).NotTo(HaveOccurred(), "Failed to create the post-target user")
		// Rotate so the post-target writes are shippable too; the archive holding
		// them must not change the recovery result, which is bounded by targetGTID.
		flushBinaryLogs(sourceCluster, primary, password)

		By("bootstrapping a fresh cluster recovering to the target GTID")
		applyManifest(restoredCluster, pitrRecoveryClusterManifest(restoredCluster, version, backupName, targetGTID))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, pitrRecoveryClusterManifest(restoredCluster, version, backupName, targetGTID))
		})
		expectClusterReady(restoredCluster, 1, 20*time.Minute)

		By("verifying the recovered cluster has the target row but not the later one")
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
			"recovered cluster contains a write past the recovery target")

		By("verifying the mid-window user was replayed and can use its grant")
		// Logging in as the replayed user and reading through its grant proves
		// both the account and the GRANT statement executed during replay: under
		// --skip-grant-tables they fail with ERROR 1290 unless the replay loaded
		// the restored grant tables first.
		Eventually(func(g Gomega) {
			out, err := mysqlExec(restoredPrimary, "pitr_user", "pitr-in-window", "app",
				"SELECT COUNT(*) FROM ledger WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred(),
				"replayed user could not authenticate or lacked its grant")
			g.Expect(parseSingleValue(out)).To(Equal("1"), "replayed user saw wrong data")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the post-target user was NOT replayed")
		// Connecting with the account's own password: success would mean the
		// post-target CREATE USER was replayed, so only "Access denied" passes.
		_, err = mysqlExec(restoredPrimary, "pitr_late", "pitr-after-target", "app", "SELECT 1;")
		Expect(err).To(HaveOccurred(), "post-target user exists in the recovered cluster")
		Expect(err.Error()).To(ContainSubstring("Access denied"),
			"expected authentication failure for the post-target user, got: %v", err)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// pitrRecoveryClusterManifest renders a Cluster that bootstraps from a base
// backup and replays archived binlogs up to targetGTID. It points at the same
// object store (for the binlog archive) but does not re-enable archiving.
func pitrRecoveryClusterManifest(name, version, backup, targetGTID string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageName: %[4]s
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    recovery:
      backup:
        name: %[3]s
      recoveryTarget:
        targetGTID: "%[7]s"
  backup:
%[8]s
`, name, testNamespace, backup, instanceImageFor(version), e2eInstanceResources,
		e2eMySQLParameters, targetGTID, objectStoreYAML("    "))
}
