//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The MariaDB counterpart of the "Failover + PITR under heavy writes" suite: a
// 3-instance MariaDB cluster sustaining a heavy write workload, surviving an
// automatic failover, continuing to accept writes on the promoted primary, and
// then undergoing point-in-time recovery to a MariaDB GTID (@@gtid_current_pos)
// captured after the failover. It runs in the dedicated mariadb-heavy lane. SQL
// goes through the mariadb client (mariadbExec) and recovery targets a MariaDB
// GTID, so the MySQL helpers (mysqlExec / gtid_executed / expectArchiveCovers) are
// reimplemented here rather than reused.
var _ = Describe("MariaDB failover + PITR under heavy writes", Ordered, Label("flavor", "mariadb", "heavy"), func() {
	const (
		sourceCluster   = "mdb-stress-pitr-src"
		restoredCluster = "mdb-stress-pitr-restored"
		backupName      = "mdb-stress-pitr-base"
		numPreFailover  = 2000
		numPostFailover = 2000
		numPastTarget   = 500
		writeBatchSize  = 100
	)

	var (
		password   string
		targetGTID string
		ns, prevNS string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-stress-pitr")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating a 3-instance MariaDB cluster with continuous archiving")
		applyManifest(sourceCluster, mariadbStressArchivingClusterManifest(sourceCluster))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, mariadbStressArchivingClusterManifest(sourceCluster))
		})
		expectClusterReady(sourceCluster, 3, e2eTimeout(20*time.Minute))
		password = appPassword(sourceCluster)

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
	})

	It("sustains heavy writes through a failover and enables PITR past the failover point", func() {
		primary := clusterPrimary(sourceCluster)

		By(fmt.Sprintf("writing %d rows pre-failover on %s", numPreFailover, primary))
		_, err := mariadbExec(primary, "app", password, "app",
			"CREATE TABLE stress_test (id INT PRIMARY KEY, phase VARCHAR(16), ts BIGINT);")
		Expect(err).NotTo(HaveOccurred(), "failed to create stress table")
		writeMariadbStressRows(sourceCluster, password, 1, numPreFailover, writeBatchSize, "pre-failover")

		By("triggering automatic failover by force-deleting the primary")
		_, err = kubectl("delete", "pod", primary, "-n", testNamespace,
			"--grace-period=0", "--force")
		Expect(err).NotTo(HaveOccurred(), "failed to force-delete the primary")

		By("waiting for a surviving replica to be promoted")
		var newPrimary string
		Eventually(func(g Gomega) {
			p, err := clusterField(sourceCluster, "{.status.currentPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(p).NotTo(BeEmpty())
			g.Expect(p).NotTo(Equal(primary), "primary must move off the failed instance")
			newPrimary = p
		}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterReady(sourceCluster, 3, e2eTimeout(20*time.Minute))

		By(fmt.Sprintf("verifying the new primary %s is writable", newPrimary))
		Eventually(func(g Gomega) {
			_, err := mariadbExec(newPrimary, "app", password, "app",
				"INSERT INTO stress_test VALUES (-1, 'probe', UNIX_TIMESTAMP()); DELETE FROM stress_test WHERE id = -1;")
			g.Expect(err).NotTo(HaveOccurred(), "new primary is not writable yet")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("writing %d rows post-failover on the new primary", numPostFailover))
		writeMariadbStressRows(sourceCluster, password, numPreFailover+1, numPostFailover, writeBatchSize, "post-failover")

		By("capturing the post-failover GTID as the PITR target")
		targetGTID = flushMariadbBinaryLogs(sourceCluster, newPrimary, password)
		Expect(targetGTID).NotTo(BeEmpty(), "post-failover target GTID parsed empty")

		By(fmt.Sprintf("writing %d rows past the recovery target", numPastTarget))
		writeMariadbStressRows(sourceCluster, password, numPreFailover+numPostFailover+1, numPastTarget, writeBatchSize, "post-target")
		flushMariadbBinaryLogs(sourceCluster, newPrimary, password)

		By("waiting for the archiver to ship the binary logs covering the target")
		// MariaDB GTID sets are not the MySQL UUID form expectArchiveCovers parses,
		// so give the archiver a bounded grace period to ship the flushed logs.
		time.Sleep(e2eTimeout(30 * time.Second))

		By(fmt.Sprintf("bootstrapping a recovery cluster to targetGTID=%s", targetGTID))
		applyManifest(restoredCluster, mariadbPITRClusterManifest(restoredCluster, backupName, sourceCluster, targetGTID))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, mariadbPITRClusterManifest(restoredCluster, backupName, sourceCluster, targetGTID))
		})
		expectClusterReady(restoredCluster, 1, e2eTimeout(20*time.Minute))

		By("verifying data sanity in the recovered cluster")
		restoredPrimary := clusterPrimary(restoredCluster)
		verifyMariadbStressDataSanity(restoredPrimary, password,
			numPreFailover+numPostFailover, numPreFailover+numPostFailover+numPastTarget)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// mariadbStressArchivingClusterManifest is a 3-instance MariaDB cluster with
// continuous binlog archiving enabled, used as the source of the stress + PITR
// run. It mirrors mariadbContinuousArchivingClusterManifest but with instances: 3.
func mariadbStressArchivingClusterManifest(name string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  flavor: mariadb
  instances: 3
  imageName: %s
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
%s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

// writeMariadbStressRows inserts count rows into stress_test starting at startID,
// in batches of batchSize per mariadb client call.
func writeMariadbStressRows(cluster, password string, startID, count, batchSize int, phase string) {
	primary := clusterPrimary(cluster)
	for i := 0; i < count; i += batchSize {
		batchEnd := batchSize
		if i+batchEnd > count {
			batchEnd = count - i
		}
		var values []string
		for j := 0; j < batchEnd; j++ {
			id := startID + i + j
			values = append(values, fmt.Sprintf("(%d, '%s', UNIX_TIMESTAMP())", id, phase))
		}
		sql := fmt.Sprintf("INSERT INTO stress_test VALUES %s", strings.Join(values, ", "))
		Eventually(func(g Gomega) {
			_, err := mariadbExec(primary, "app", password, "app", sql)
			g.Expect(err).NotTo(HaveOccurred(), "batch write at id=%d failed", startID+i)
		}, e2eTimeout(30*time.Second), 2*time.Second).Should(Succeed())
	}
}

// flushMariadbBinaryLogs rotates the primary's active binary log so every
// committed transaction lands in an archivable file, then returns the MariaDB
// GTID position (@@gtid_current_pos) captured immediately after the flush.
func flushMariadbBinaryLogs(cluster, primary, password string) string {
	GinkgoHelper()
	_, err := mariadbExec(primary, "root", rootPassword(cluster), "", "FLUSH BINARY LOGS")
	Expect(err).NotTo(HaveOccurred(), "FLUSH BINARY LOGS failed on %s", primary)
	out, err := mariadbExec(primary, "app", password, "", "SELECT @@gtid_current_pos")
	Expect(err).NotTo(HaveOccurred(), "reading gtid_current_pos from %s", primary)
	return strings.TrimSpace(out)
}

// verifyMariadbStressDataSanity asserts the recovered cluster contains exactly the
// rows up to expectedUpTo and none past it (up to notExpectedFrom).
func verifyMariadbStressDataSanity(pod, password string, expectedUpTo, notExpectedFrom int) {
	By(fmt.Sprintf("verifying row count equals %d (all pre+post-failover rows)", expectedUpTo))
	Eventually(func(g Gomega) {
		out, err := mariadbExec(pod, "app", password, "app",
			fmt.Sprintf("SELECT COUNT(*) FROM stress_test WHERE id BETWEEN 1 AND %d", expectedUpTo))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(out)).To(Equal(strconv.Itoa(expectedUpTo)),
			"recovered cluster has wrong number of rows up to %d", expectedUpTo)
	}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

	By(fmt.Sprintf("verifying no rows exist past the recovery target (ids > %d)", expectedUpTo))
	Eventually(func(g Gomega) {
		out, err := mariadbExec(pod, "app", password, "app",
			fmt.Sprintf("SELECT COUNT(*) FROM stress_test WHERE id > %d", expectedUpTo))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(out)).To(Equal("0"),
			"recovered cluster contains rows past the recovery target")
	}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

	By("verifying the expected boundary rows are present (id=1, id=expectedUpTo/2, id=expectedUpTo)")
	_, err := mariadbExec(pod, "app", password, "app",
		fmt.Sprintf("SELECT id FROM stress_test WHERE id IN (1, %d, %d);",
			expectedUpTo/2, expectedUpTo))
	Expect(err).NotTo(HaveOccurred(), "expected boundary rows are missing from the recovered cluster")
}
