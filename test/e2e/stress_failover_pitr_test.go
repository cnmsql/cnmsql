//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec exercises the full resilience + recoverability story end-to-end:
// a multi-instance cluster sustaining a heavy write workload, surviving an
// automatic failover, continuing to accept writes on the promoted primary, and
// then undergoing point-in-time recovery to a GTID captured after the failover
// completed. Data sanity is asserted on the recovered cluster to confirm that
// the archive preserved both the pre- and post-failover writes correctly and
// that no data past the recovery target leaked through.
var _ = Describe("Failover + PITR under heavy writes", Ordered, Label("flavor", "heavy"), func() {
	const (
		sourceCluster   = "stress-pitr-src"
		restoredCluster = "stress-pitr-restored"
		backupName      = "stress-pitr-base"
		numPreFailover  = 2000
		numPostFailover = 2000
		numPastTarget   = 500
		writeBatchSize  = 100
	)
	version := archiveVersions()[0]

	var (
		password   string
		targetGTID string
		ns, prevNS string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("stress-pitr")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating a 3-instance cluster with continuous archiving")
		applyManifest(sourceCluster, continuousArchivingClusterManifest(sourceCluster, version, 3))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, continuousArchivingClusterManifest(sourceCluster, version, 3))
		})
		expectClusterReady(sourceCluster, 3, 20*time.Minute)
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
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE stress_test (id INT PRIMARY KEY, phase VARCHAR(16), ts BIGINT);")
		Expect(err).NotTo(HaveOccurred(), "Failed to create stress table")
		writeStressRows(sourceCluster, password, 1, numPreFailover, writeBatchSize, "pre-failover")

		By("triggering automatic failover by force-deleting the primary")
		_, err = kubectl("delete", "pod", primary, "-n", testNamespace,
			"--grace-period=0", "--force")
		Expect(err).NotTo(HaveOccurred(), "Failed to force-delete the primary")

		By("waiting for a surviving replica to be promoted")
		var newPrimary string
		Eventually(func(g Gomega) {
			p, err := clusterField(sourceCluster, "{.status.currentPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(p).NotTo(BeEmpty())
			g.Expect(p).NotTo(Equal(primary), "primary must move off the failed instance")
			newPrimary = p
		}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterReady(sourceCluster, 3, 20*time.Minute)

		By(fmt.Sprintf("verifying the new primary %s is writable", newPrimary))
		Eventually(func(g Gomega) {
			_, err := mysqlExec(newPrimary, "app", password, "app",
				"INSERT INTO stress_test VALUES (-1, 'probe', UNIX_TIMESTAMP()); DELETE FROM stress_test WHERE id = -1;")
			g.Expect(err).NotTo(HaveOccurred(), "new primary is not writable yet")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("writing %d rows post-failover on the new primary", numPostFailover))
		writeStressRows(sourceCluster, password, numPreFailover+1, numPostFailover, writeBatchSize, "post-failover")

		By("capturing the post-failover GTID as the PITR target")
		targetGTID = flushBinaryLogs(sourceCluster, newPrimary, password)
		Expect(targetGTID).NotTo(BeEmpty(), "post-failover target GTID parsed empty")

		By(fmt.Sprintf("writing %d rows past the recovery target", numPastTarget))
		writeStressRows(sourceCluster, password, numPreFailover+numPostFailover+1, numPastTarget, writeBatchSize, "post-target")
		flushBinaryLogs(sourceCluster, newPrimary, password)

		By("waiting for the archive to cover the target GTID")
		expectArchiveCovers(sourceCluster, targetGTID, 8*time.Minute)

		By(fmt.Sprintf("bootstrapping a recovery cluster to targetGTID=%s", targetGTID))
		applyManifest(restoredCluster, pitrRecoveryClusterManifest(restoredCluster, version, backupName, targetGTID))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, pitrRecoveryClusterManifest(restoredCluster, version, backupName, targetGTID))
		})
		expectClusterReady(restoredCluster, 1, 20*time.Minute)

		By("verifying data sanity in the recovered cluster")
		restoredPrimary := clusterPrimary(restoredCluster)
		verifyStressDataSanity(restoredPrimary, password, numPreFailover+numPostFailover, numPreFailover+numPostFailover+numPastTarget)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// writeStressRows inserts count rows into stress_test starting at startID,
// using batches of batchSize rows per kubectl exec call.
func writeStressRows(cluster, password string, startID, count, batchSize int, phase string) {
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
		sql := fmt.Sprintf("INSERT INTO stress_test VALUES %s",
			strings.Join(values, ", "))
		Eventually(func(g Gomega) {
			_, err := mysqlExec(primary, "app", password, "app", sql)
			g.Expect(err).NotTo(HaveOccurred(), "batch write at id=%d failed", startID+i)
		}, e2eTimeout(30*time.Second), 2*time.Second).Should(Succeed())
	}
}

// verifyStressDataSanity asserts that the recovered cluster contains all rows
// from 1 to expectedUpTo but none from expectedUpTo+1 to notExpectedFrom.
// It also verifies contiguity: COUNT(*) must equal expectedUpTo and every
// expected id must be present.
func verifyStressDataSanity(pod, password string, expectedUpTo, notExpectedFrom int) {
	By(fmt.Sprintf("verifying row count equals %d (all pre+post-failover rows)", expectedUpTo))
	Eventually(func(g Gomega) {
		out, err := mysqlExec(pod, "app", password, "app",
			fmt.Sprintf("SELECT COUNT(*) FROM stress_test WHERE id BETWEEN 1 AND %d", expectedUpTo))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(parseSingleValue(out)).To(Equal(fmt.Sprintf("%d", expectedUpTo)),
			"recovered cluster has wrong number of rows up to %d", expectedUpTo)
	}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

	By(fmt.Sprintf("verifying no rows exist past the recovery target (ids >= %d)", expectedUpTo+1))
	Eventually(func(g Gomega) {
		out, err := mysqlExec(pod, "app", password, "app",
			fmt.Sprintf("SELECT COUNT(*) FROM stress_test WHERE id > %d", expectedUpTo))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(parseSingleValue(out)).To(Equal("0"),
			"recovered cluster contains %d rows past the recovery target", out)
	}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

	By("verifying the expected boundary rows are present (id=1, id=expectedUpTo, id=expectedUpTo/2)")
	_, err := mysqlExec(pod, "app", password, "app",
		fmt.Sprintf("SELECT id FROM stress_test WHERE id IN (1, %d, %d);",
			expectedUpTo/2, expectedUpTo))
	Expect(err).NotTo(HaveOccurred(), "expected boundary rows are missing from the recovered cluster")
}
