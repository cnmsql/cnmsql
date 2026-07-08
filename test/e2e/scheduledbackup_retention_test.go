//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec exercises ScheduledBackup retention (Backup-object GC) end to end. A
// schedule with reclaimPolicy: Delete produces several real base backups; the
// history limit is then tightened to 1 and the schedule suspended. The operator
// must garbage-collect the older completed Backups down to the newest one, and
// because the generated Backups carry the Delete reclaim policy, the GC'd Backups'
// object-store archives must be reclaimed too.
var _ = Describe("Scheduled backup retention", Ordered, Label("feature"), func() {
	const (
		sourceCluster = "sched-ret-src"
		scheduleName  = "sched-ret-nightly"
		// 6-field cron with seconds: every 20 seconds. The concurrency guard keeps
		// backups from overlapping, so this is a cadence ceiling.
		tightSchedule       = "*/20 * * * * *"
		parentLabel         = "mysql.cnmsql.co/scheduled-backup"
		parentLabelSelector = parentLabel + "=" + scheduleName
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("sched-retention")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating the source cluster that archives to object storage")
		applyManifest(sourceCluster, archivingClusterManifest(sourceCluster))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, archivingClusterManifest(sourceCluster))
		})
		expectClusterReady(sourceCluster, 1, 20*time.Minute)
	})

	It("garbage-collects old Backups and reclaims their archives", func() {
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
		// Newest-first: the first entry is the retention floor that must survive.
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

// backupArchive pairs a completed Backup's name with its object-store archive key.
type backupArchive struct {
	name       string
	archiveKey string
}

// childBackupPhases returns the phase of every child Backup of a schedule.
func childBackupPhases(g Gomega, selector string) []string {
	out, err := kubectl("get", "backups", "-n", testNamespace,
		"-l", selector, "-o", "jsonpath={.items[*].status.phase}")
	g.Expect(err).NotTo(HaveOccurred())
	return strings.Fields(out)
}

// completedBackupArchives returns the completed child Backups newest-first, each
// paired with its mc archive key (local/<bucket>/<cluster>/<name>/<backupId>/backup.xbstream).
func completedBackupArchives(cluster, selector string) []backupArchive {
	out, err := kubectl("get", "backups", "-n", testNamespace, "-l", selector,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}{.status.phase}{\"\\t\"}"+
			"{.status.backupId}{\"\\t\"}{.metadata.creationTimestamp}{\"\\n\"}{end}")
	Expect(err).NotTo(HaveOccurred())

	type row struct {
		name, backupID, created string
	}
	var rows []row
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) != 4 || fields[1] != "completed" {
			continue
		}
		rows = append(rows, row{name: fields[0], backupID: fields[2], created: fields[3]})
	}
	// Newest-first by creationTimestamp (RFC3339 sorts lexically), name as tie-break.
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].created == rows[j].created {
			return rows[i].name > rows[j].name
		}
		return rows[i].created > rows[j].created
	})

	archives := make([]backupArchive, 0, len(rows))
	for _, r := range rows {
		archives = append(archives, backupArchive{
			name: r.name,
			archiveKey: fmt.Sprintf("local/%s/%s/%s/%s/backup.xbstream",
				minioBucket, cluster, r.name, r.backupID),
		})
	}
	return archives
}

func scheduledRetentionManifest(name, cluster, schedule string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: ScheduledBackup
metadata:
  name: %s
  namespace: %s
spec:
  schedule: "%s"
  immediate: true
  reclaimPolicy: Delete
  cluster:
    name: %s
  method: xtrabackup
`, name, testNamespace, schedule, cluster)
}
