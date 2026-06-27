//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs exercise continuous binary-log archiving (M7.1) end-to-end against
// a real Kind cluster backed by in-cluster MinIO. Binlog archiving is the
// foundation of point-in-time recovery, so correctness here is non-negotiable:
// the suite proves the archive stays gapless through forced rotation, an
// ungraceful primary crash, an automatic failover, and an object-store outage —
// and it proves it on every supported Percona version, since archiving touches
// version-gated surface (binlog expiry, the log_replica_updates rename, the
// super_read_only writability gate, mysqlbinlog output format).
//
// "Gapless" is asserted authoritatively: after rotating the active log, the
// cluster-level archive index (_index.json) in object storage must report a
// covered GTID set that is a superset of the server's gtid_executed. No
// committed transaction may be missing from the archive.
//
// Each Percona version is declared as a separate Describe container so that
// Ginkgo's --procs can run them in parallel across different processes. Each
// version gets its own namespace and its own in-cluster MinIO so there is no
// resource contention.

func init() {
	for _, version := range archiveVersions() {
		v := version
		Describe(fmt.Sprintf("Continuous binlog archiving - %s", v), Ordered, func() {
			var ns, prevNS string

			BeforeAll(func() {
				prevNS = testNamespace
				ns = createTestNamespace("arch-" + sanitize(v))
				setupMinio()
				setupMC()
			})

			AfterAll(func() {
				teardownMC()
				teardownMinio()
				deleteTestNamespace(ns, prevNS)
			})

			archivingVersionSpecs(v)
		})
	}
}

// archivingVersionSpecs declares the full catastrophic-scenario matrix for one
// Percona version. It uses two clusters: a single-instance "solo" cluster for
// the rotation / crash / outage scenarios, and a three-instance "ha" cluster for
// failover continuity. Each is created and torn down within its own context so
// only one cluster for this version is live at a time.
func archivingVersionSpecs(version string) {
	Context("single-instance archiving", Ordered, func() {
		cluster := "arch-" + sanitize(version) + "-solo"
		var password string

		BeforeAll(func() {
			By("creating the single-instance archiving cluster")
			applyManifest(cluster, continuousArchivingClusterManifest(cluster, version, 1))
			DeferCleanup(func() {
				deleteManifest(cluster, continuousArchivingClusterManifest(cluster, version, 1))
			})
			expectClusterReady(cluster, 1, 20*time.Minute)
			password = appPassword(cluster)

			By("seeding a base table")
			primary := clusterPrimary(cluster)
			_, err := mysqlExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS ledger (id INT PRIMARY KEY)")
			Expect(err).NotTo(HaveOccurred(), "Failed to seed the base table")
		})

		It("streams a gapless archive across forced rotations", func() {
			primary := clusterPrimary(cluster)

			By("writing transactions interleaved with forced rotations")
			for i := 1; i <= 6; i++ {
				_, err := mysqlExec(primary, "app", password, "app",
					fmt.Sprintf("INSERT INTO ledger VALUES (%d)", i))
				Expect(err).NotTo(HaveOccurred(), "insert %d failed", i)
				if i%2 == 0 {
					_, err = mysqlExec(primary, "root", rootPassword(cluster), "", "FLUSH BINARY LOGS")
					Expect(err).NotTo(HaveOccurred(), "flush %d failed", i)
				}
			}

			By("rotating the active log and waiting for the archive to cover gtid_executed")
			executed := flushBinaryLogs(cluster, primary, password)
			expectArchiveCovers(cluster, executed, 4*time.Minute)

			By("verifying the cluster reports a healthy archiving frontier")
			Eventually(func(g Gomega) {
				status, err := clusterField(cluster,
					"{.status.conditions[?(@.type=='ContinuousArchiving')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"), "archiving condition is not healthy")

				last, err := clusterField(cluster, "{.status.continuousArchiving.lastArchivedBinlog}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(last).NotTo(BeEmpty(), "no binlog has been archived yet")
			}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
		})

		It("loses no committed transaction when the primary crashes mid-segment", func() {
			primary := clusterPrimary(cluster)

			By("committing transactions that stay in the un-rotated active log")
			for i := 10; i < 15; i++ {
				_, err := mysqlExec(primary, "app", password, "app",
					fmt.Sprintf("INSERT INTO ledger VALUES (%d)", i))
				Expect(err).NotTo(HaveOccurred(), "insert %d failed", i)
			}
			// Capture the committed frontier *before* the crash. With sync_binlog=1
			// these transactions are durable on disk even though no FLUSH rotated
			// them into an immutable file yet.
			executed := gtidExecuted(primary, password)

			By("force-deleting the primary Pod to simulate an ungraceful crash")
			_, err := kubectl("delete", "pod", primary, "-n", testNamespace,
				"--grace-period=0", "--force")
			Expect(err).NotTo(HaveOccurred(), "Failed to force-delete the primary")

			By("waiting for the instance to recover")
			expectClusterReady(cluster, 1, 20*time.Minute)

			// On restart mysqld opens a fresh binary log, so the pre-crash active log
			// becomes immutable and the archiver ships it. Every committed GTID must
			// reappear in the archive — nothing acknowledged is lost across a crash.
			By("verifying every pre-crash committed transaction is in the archive")
			expectArchiveCovers(cluster, executed, 6*time.Minute)
		})

		It("degrades then recovers across an object-store outage", func() {
			primary := clusterPrimary(cluster)

			By("scaling MinIO down to simulate an object-store outage")
			_, err := kubectl("scale", "deployment/minio", "-n", minioNamespace, "--replicas=0")
			Expect(err).NotTo(HaveOccurred(), "Failed to scale MinIO down")
			DeferCleanup(func() {
				_, _ = kubectl("scale", "deployment/minio", "-n", minioNamespace, "--replicas=1")
				_, _ = kubectl("wait", "deployment/minio", "-n", minioNamespace,
					"--for=condition=Available", "--timeout=3m")
			})
			By("waiting for MinIO to have no available replicas")
			Eventually(func(g Gomega) {
				ready, err := kubectl("get", "deployment/minio", "-n", minioNamespace,
					"-o", "jsonpath={.status.availableReplicas}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ready).To(BeEmpty(), "MinIO still has available replicas")
			}, e2eTimeout(2*time.Minute), 3*time.Second).Should(Succeed())

			By("writing and rotating so rotated files pile up un-shippable")
			for i := 20; i < 26; i++ {
				_, err := mysqlExec(primary, "app", password, "app",
					fmt.Sprintf("INSERT INTO ledger VALUES (%d)", i))
				Expect(err).NotTo(HaveOccurred(), "insert %d failed", i)
				_, _ = mysqlExec(primary, "root", rootPassword(cluster), "", "FLUSH BINARY LOGS")
			}

			By("verifying the cluster surfaces a degraded archiving condition")
			Eventually(func(g Gomega) {
				status, err := clusterField(cluster,
					"{.status.conditions[?(@.type=='ContinuousArchiving')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("False"), "archiving condition did not go degraded")

				reason, err := clusterField(cluster, "{.status.continuousArchiving.lastFailureReason}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(reason).NotTo(BeEmpty(), "no archiving failure was recorded")
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

			By("restoring MinIO and waiting for it to come back")
			_, err = kubectl("scale", "deployment/minio", "-n", minioNamespace, "--replicas=1")
			Expect(err).NotTo(HaveOccurred(), "Failed to scale MinIO back up")
			_, err = kubectl("wait", "deployment/minio", "-n", minioNamespace,
				"--for=condition=Available", "--timeout=3m")
			Expect(err).NotTo(HaveOccurred(), "MinIO did not come back")

			By("recreating the MinIO bucket lost when the pod was rescheduled")
			_, err = mcExec("mc", "mb", "--ignore-existing", "local/"+minioBucket)
			Expect(err).NotTo(HaveOccurred(), "Failed to recreate MinIO bucket")

			By("verifying the backlog drains and the archive catches up gaplessly")
			executed := flushBinaryLogs(cluster, primary, password)
			expectArchiveCovers(cluster, executed, 6*time.Minute)
			Eventually(func(g Gomega) {
				status, err := clusterField(cluster,
					"{.status.conditions[?(@.type=='ContinuousArchiving')].status}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(status).To(Equal("True"), "archiving condition did not recover")
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		})
	})

	Context("failover continuity", Ordered, func() {
		cluster := "arch-" + sanitize(version) + "-ha"
		var password string

		BeforeAll(func() {
			By("creating the three-instance archiving cluster")
			applyManifest(cluster, continuousArchivingClusterManifest(cluster, version, 3))
			DeferCleanup(func() {
				deleteManifest(cluster, continuousArchivingClusterManifest(cluster, version, 3))
			})
			expectClusterReady(cluster, 3, 20*time.Minute)
			password = appPassword(cluster)

			By("seeding and archiving under the original primary")
			primary := clusterPrimary(cluster)
			_, err := mysqlExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS ledger (id INT PRIMARY KEY); INSERT INTO ledger VALUES (1)")
			Expect(err).NotTo(HaveOccurred(), "Failed to seed before failover")
			executed := flushBinaryLogs(cluster, primary, password)
			expectArchiveCovers(cluster, executed, 4*time.Minute)
		})

		It("keeps the archive gapless across an automatic failover", func() {
			oldPrimary := clusterPrimary(cluster)

			By("force-deleting the primary to trigger automatic failover")
			_, err := kubectl("delete", "pod", oldPrimary, "-n", testNamespace,
				"--grace-period=0", "--force")
			Expect(err).NotTo(HaveOccurred(), "Failed to force-delete the primary")

			By("waiting for a surviving replica to be promoted")
			var newPrimary string
			Eventually(func(g Gomega) {
				p, err := clusterField(cluster, "{.status.currentPrimary}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(p).NotTo(BeEmpty())
				g.Expect(p).NotTo(Equal(oldPrimary), "primary must move off the failed instance")
				newPrimary = p
			}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())
			expectClusterReady(cluster, 3, 20*time.Minute)

			By("writing on the new primary under its own server UUID")
			Eventually(func(g Gomega) {
				_, err := mysqlExec(newPrimary, "app", password, "app",
					"INSERT INTO ledger VALUES (2)")
				g.Expect(err).NotTo(HaveOccurred(), "new primary is not writable yet")
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

			By("rotating and verifying the post-failover archive covers all writes")
			executed := flushBinaryLogs(cluster, newPrimary, password)
			expectArchiveCovers(cluster, executed, 6*time.Minute)

			By("verifying the archive index spans both pre- and post-failover segments")
			// Continuity is emergent: each primary archives under its own server UUID,
			// so a gapless handoff appears as two distinct segments whose union covers
			// every committed GTID. The promoted instance has its own auto.cnf UUID, so
			// the post-failover writes must land in a second segment.
			idx, err := readArchiveIndex(cluster)
			Expect(err).NotTo(HaveOccurred())
			uuids := map[string]bool{}
			for _, seg := range idx.Segments {
				uuids[seg.ServerUUID] = true
			}
			Expect(len(uuids)).To(BeNumerically(">=", 2),
				"expected archive segments under at least two server UUIDs, got %d (%+v)",
				len(uuids), idx.Segments)
		})
	})
}

// sanitize turns a version like "9.x" into a DNS-label-safe cluster-name
// fragment.
func sanitize(version string) string {
	out := make([]rune, 0, len(version))
	for _, r := range version {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+('a'-'A'))
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}
