//go:build e2e
// +build e2e

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The MariaDB counterpart of the "Scheduled backup retention" suite. The GC
// selection logic is flavor-agnostic (it prunes Backup objects by count/age), but
// this pins that reclaimPolicy: Delete reclamation walks the MariaDB (mariabackup)
// archive layout correctly when a schedule's Backups are garbage-collected. It
// reuses the flavor-agnostic helpers from scheduledbackup_retention_test.go.
var _ = Describe("MariaDB scheduled backup retention", Ordered, Label("flavor", "mariadb"), func() {
	const (
		sourceCluster       = "mdb-sched-ret-src"
		scheduleName        = "mdb-sched-ret-nightly"
		tightSchedule       = "*/20 * * * * *"
		parentLabel         = "mysql.cnmsql.co/scheduled-backup"
		parentLabelSelector = parentLabel + "=" + scheduleName
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-sched-retention")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating the MariaDB source cluster that archives to object storage")
		applyManifest(sourceCluster, mariadbArchivingClusterManifest(sourceCluster))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, mariadbArchivingClusterManifest(sourceCluster))
		})
		expectClusterReady(sourceCluster, 1, e2eTimeout(20*time.Minute))
	})

	It("garbage-collects old Backups and reclaims their mariabackup archives", func() {
		By("creating a Delete-policy schedule that accumulates backups")
		applyManifest(scheduleName, scheduledRetentionManifest(scheduleName, sourceCluster, tightSchedule))
		DeferCleanup(func() {
			deleteManifest(scheduleName, scheduledRetentionManifest(scheduleName, sourceCluster, tightSchedule))
		})

		By("waiting for at least three completed child backups to accumulate")
		Eventually(func(g Gomega) {
			phases := childBackupPhases(g, parentLabelSelector)
			g.Expect(phases).NotTo(ContainElement("failed"), "a scheduled backup failed")
			completed := 0
			for _, p := range phases {
				if p == "completed" {
					completed++
				}
			}
			g.Expect(completed).To(BeNumerically(">=", 3),
				"fewer than three scheduled backups have completed yet")
		}, e2eTimeout(15*time.Minute), 10*time.Second).Should(Succeed())

		By("suspending the schedule and letting any in-flight backup finish")
		_, err := kubectl("patch", "scheduledbackup", scheduleName, "-n", testNamespace,
			"--type=merge", "-p", `{"spec":{"suspend":true}}`)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			phases := childBackupPhases(g, parentLabelSelector)
			for _, p := range phases {
				g.Expect(p).To(Or(Equal("completed"), Equal("failed")),
					"a child backup is still %q; waiting for it to settle", p)
			}
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())

		By("recording the completed backups and their archive locations before GC")
		completedBefore := completedBackupArchives(sourceCluster, parentLabelSelector)
		Expect(len(completedBefore)).To(BeNumerically(">=", 3),
			"need at least three completed backups to prune")
		floor := completedBefore[0]
		pruned := completedBefore[1:]

		By("confirming the to-be-pruned archives exist before GC")
		for _, b := range pruned {
			Expect(mcObjectExists(b.archiveKey)).To(BeTrue(),
				"archive for %s should exist before GC", b.name)
		}

		By("tightening the history limit to 1 so GC prunes everything but the floor")
		_, err = kubectl("patch", "scheduledbackup", scheduleName, "-n", testNamespace,
			"--type=merge", "-p", `{"spec":{"successfulBackupsHistoryLimit":1}}`)
		Expect(err).NotTo(HaveOccurred())

		By("verifying only the newest completed Backup survives")
		Eventually(func(g Gomega) {
			names := backupNamesWithLabel(g, parentLabelSelector)
			g.Expect(names).To(ConsistOf(floor.name),
				"expected only the floor Backup %q to remain, have %v", floor.name, names)
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the pruned Delete-policy archives were reclaimed")
		for _, b := range pruned {
			Eventually(func(g Gomega) {
				g.Expect(mcObjectExists(b.archiveKey)).To(BeFalse(),
					"archive for pruned backup %s should be reclaimed", b.name)
			}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())
		}

		By("verifying the floor Backup's archive is retained")
		Expect(mcObjectExists(floor.archiveKey)).To(BeTrue(),
			"the surviving floor backup's archive must not be reclaimed")
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
