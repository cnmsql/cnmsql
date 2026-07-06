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

// This spec exercises point-in-time recovery across a re-initialisation boundary,
// where no single archived segment spans the whole timeline but their union does.
//
// The archive partitions object keys by per-incarnation identity (MySQL server_uuid,
// MariaDB persisted token). A re-init deletes an instance's Pod+PVC and re-clones a
// fresh copy while preserving its name/ordinal (hence server_id), so before the
// per-incarnation-identity fix the re-cloned primary would either collide with the
// previous incarnation's archived objects (ErrCollision jams archiving) or fold two
// discontiguous binlog histories under one identity. When such a re-cloned instance
// is then promoted and keeps writing, recovering past that point requires stitching:
//
//	seg1  (P1 original) : batch1
//	seg2  (P2 promoted) : batch2 (+ batch1 re-logged)
//	seg3  (P1 re-cloned): batch3
//
// The base backup sits at genesis, so recovery must replay batch1+batch2 from the
// earlier segments and batch3 from the fresh-identity segment — a cross-incarnation
// union. This is validated for both MySQL (native interval-set GTIDs) and MariaDB
// (range-based segment selection + merge-by-sequence positional replay).
var _ = Describe("Reinit union PITR", Ordered, Label("feature", "heavy"), func() {
	const (
		sourceCluster   = "reinit-union-src"
		restoredCluster = "reinit-union-restored"
		backupName      = "reinit-union-base"
	)
	version := archiveVersions()[0]

	f := reinitUnionFlavor{
		writeRows:    writeStressRows,
		flushCapture: flushBinaryLogs,
		verifySanity: verifyStressDataSanity,
		createTable: func(pod, password string) {
			GinkgoHelper()
			_, err := mysqlExec(pod, "app", password, "app",
				"CREATE TABLE stress_test (id INT PRIMARY KEY, phase VARCHAR(16), ts BIGINT);")
			Expect(err).NotTo(HaveOccurred(), "failed to create stress table")
		},
		probeWritable: func(g Gomega, pod, password string) {
			_, err := mysqlExec(pod, "app", password, "app",
				"INSERT INTO stress_test VALUES (-1, 'probe', UNIX_TIMESTAMP()); DELETE FROM stress_test WHERE id = -1;")
			g.Expect(err).NotTo(HaveOccurred(), "new primary is not writable yet")
		},
		sourceManifest: func(name string) string {
			return continuousArchivingClusterManifest(name, version, 2)
		},
		pitrManifest: func(name, backup, targetGTID string) string {
			return pitrRecoveryClusterManifest(name, version, backup, targetGTID)
		},
		awaitArchive: func(cluster, target string) {
			expectArchiveCovers(cluster, target, 8*time.Minute)
		},
	}

	runReinitUnionPITR(f, sourceCluster, restoredCluster, backupName)
})

// reinitUnionFlavor captures the per-flavor operations the reinit-union PITR flow
// needs; MySQL and MariaDB differ only in the SQL client used, the GTID capture
// semantics, the archive-readiness gate, and the manifests.
type reinitUnionFlavor struct {
	// writeRows inserts count rows into stress_test starting at startID, tagged
	// with phase, in batches of batchSize.
	writeRows func(cluster, password string, startID, count, batchSize int, phase string)
	// flushCapture rotates the primary's binary log and returns the GTID position
	// reached, used as the recovery target.
	flushCapture func(cluster, primary, password string) string
	// verifySanity asserts the recovered cluster holds exactly rows 1..expectedUpTo
	// and none past it up to notExpectedFrom.
	verifySanity func(pod, password string, expectedUpTo, notExpectedFrom int)
	// createTable creates the stress_test table on the given primary.
	createTable func(pod, password string)
	// probeWritable asserts (within an Eventually) that the pod accepts a write.
	probeWritable func(g Gomega, pod, password string)
	// sourceManifest renders the 2-instance continuously-archiving source cluster.
	sourceManifest func(name string) string
	// pitrManifest renders the single-instance recovery cluster targeting targetGTID.
	pitrManifest func(name, backup, targetGTID string) string
	// awaitArchive blocks until the archive is known to cover the target (MySQL) or
	// a bounded grace has elapsed (MariaDB, whose GTID form the coverage check can't
	// parse).
	awaitArchive func(cluster, target string)
}

// runReinitUnionPITR registers the shared BeforeAll/It/AfterAll for a reinit-union
// PITR spec. It is flavor-agnostic; f supplies the SQL/manifest specifics.
func runReinitUnionPITR(f reinitUnionFlavor, sourceCluster, restoredCluster, backupName string) {
	const (
		numBatch1     = 300
		numBatch2     = 300
		numBatch3     = 300
		numPastTarget = 100
		writeBatch    = 100
		instances     = 2
	)

	var (
		password   string
		targetGTID string
		ns, prevNS string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("reinit-union")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating a 2-instance cluster with continuous archiving")
		applyManifest(sourceCluster, f.sourceManifest(sourceCluster))
		DeferCleanup(func() { deleteManifest(sourceCluster, f.sourceManifest(sourceCluster)) })
		expectClusterReady(sourceCluster, instances, e2eTimeout(20*time.Minute))
		password = appPassword(sourceCluster)

		By("taking a base backup at genesis (before any application data exists)")
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

	It("recovers past a re-clone boundary by unioning cross-incarnation segments", func() {
		p1 := clusterPrimary(sourceCluster)

		By(fmt.Sprintf("writing batch1 (%d rows) on the original primary %s", numBatch1, p1))
		f.createTable(p1, password)
		f.writeRows(sourceCluster, password, 1, numBatch1, writeBatch, "batch1")
		f.flushCapture(sourceCluster, p1, password)

		By(fmt.Sprintf("failing over: force-deleting the primary %s", p1))
		_, err := kubectl("delete", "pod", p1, "-n", testNamespace, "--grace-period=0", "--force")
		Expect(err).NotTo(HaveOccurred(), "failed to force-delete the primary")

		By("waiting for the surviving replica to be promoted")
		var p2 string
		Eventually(func(g Gomega) {
			p, err := clusterField(sourceCluster, "{.status.currentPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(p).NotTo(BeEmpty())
			g.Expect(p).NotTo(Equal(p1), "primary must move off the failed instance")
			p2 = p
		}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterReady(sourceCluster, instances, e2eTimeout(20*time.Minute))

		By(fmt.Sprintf("writing batch2 (%d rows) on the promoted primary %s", numBatch2, p2))
		f.writeRows(sourceCluster, password, numBatch1+1, numBatch2, writeBatch, "batch2")
		f.flushCapture(sourceCluster, p2, password)

		By(fmt.Sprintf("re-initialising the former primary %s (fresh clone → fresh archive identity)", p1))
		oldPVC := pvcUID(p1)
		clusterAnnotate(sourceCluster, reinitAnnotationKey+"="+p1)
		By("waiting for the instance PVC to be recreated (proves a fresh data directory)")
		Eventually(func(g Gomega) {
			cur := pvcUIDOrEmpty(p1)
			g.Expect(cur).NotTo(BeEmpty(), "PVC not recreated yet")
			g.Expect(cur).NotTo(Equal(oldPVC), "PVC must be a fresh one after reinit")
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterReady(sourceCluster, instances, e2eTimeout(20*time.Minute))

		By(fmt.Sprintf("failing over again: force-deleting %s so the re-cloned %s is promoted", p2, p1))
		_, err = kubectl("delete", "pod", p2, "-n", testNamespace, "--grace-period=0", "--force")
		Expect(err).NotTo(HaveOccurred(), "failed to force-delete the second primary")

		By(fmt.Sprintf("waiting for the re-cloned instance %s to become primary", p1))
		Eventually(func(g Gomega) {
			p, err := clusterField(sourceCluster, "{.status.currentPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(p).To(Equal(p1), "the re-cloned instance must be the sole survivor and get promoted")
		}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("verifying the re-cloned primary %s is writable", p1))
		Eventually(func(g Gomega) {
			f.probeWritable(g, p1, password)
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("writing batch3 (%d rows) on the re-cloned primary %s", numBatch3, p1))
		f.writeRows(sourceCluster, password, numBatch1+numBatch2+1, numBatch3, writeBatch, "batch3")

		By("capturing the post-reclone GTID as the PITR target")
		targetGTID = f.flushCapture(sourceCluster, p1, password)
		Expect(targetGTID).NotTo(BeEmpty(), "post-reclone target GTID parsed empty")

		By(fmt.Sprintf("writing %d rows past the recovery target", numPastTarget))
		f.writeRows(sourceCluster, password, numBatch1+numBatch2+numBatch3+1, numPastTarget, writeBatch, "past-target")
		f.flushCapture(sourceCluster, p1, password)

		By("waiting for the archive to cover the target across all incarnations")
		f.awaitArchive(sourceCluster, targetGTID)

		By(fmt.Sprintf("bootstrapping a recovery cluster to targetGTID=%s", targetGTID))
		applyManifest(restoredCluster, f.pitrManifest(restoredCluster, backupName, targetGTID))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, f.pitrManifest(restoredCluster, backupName, targetGTID))
		})
		expectClusterReady(restoredCluster, 1, e2eTimeout(20*time.Minute))

		By("verifying data sanity in the recovered cluster (all three batches, nothing past the target)")
		restoredPrimary := clusterPrimary(restoredCluster)
		f.verifySanity(restoredPrimary, password,
			numBatch1+numBatch2+numBatch3, numBatch1+numBatch2+numBatch3+numPastTarget)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
}

// reinitAnnotationKey is the Cluster annotation that requests a single instance be
// re-initialised (Pod+PVC deleted and re-cloned from a fresh backup). It mirrors the
// operator's internal reinitAnnotation constant; duplicated here to avoid importing
// the controller package into the e2e suite.
const reinitAnnotationKey = "cnmsql.cnmsql.co/reinit"

// pvcUIDOrEmpty returns a PVC's UID, or "" when the PVC does not exist (e.g. mid
// reinit teardown). Unlike pvcUID it never fails the test, so it is safe to poll
// across the delete/recreate window.
func pvcUIDOrEmpty(name string) string {
	out, err := kubectl("get", "pvc", name, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
