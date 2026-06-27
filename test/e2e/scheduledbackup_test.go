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

// This spec exercises the ScheduledBackup controller end to end: an immediate
// backup fires on creation and the cron cadence keeps producing Backups, each
// labeled with its parent ScheduledBackup. It reuses the single-node MinIO store
// stood up for the one-shot backup specs.
var _ = Describe("Scheduled backups", Ordered, Label("feature"), func() {
	const (
		sourceCluster = "sched-src"
		scheduleName  = "sched-nightly"
		// 6-field cron with seconds: every 20 seconds. The concurrency guard
		// keeps backups from overlapping, so this is a cadence ceiling, not a
		// guarantee of a backup every 20s.
		tightSchedule        = "*/20 * * * * *"
		parentLabel          = "mysql.cnmsql.co/scheduled-backup"
		parentLabelSelector  = parentLabel + "=" + scheduleName
		immediateBackupLabel = "mysql.cnmsql.co/immediate-backup"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("scheduled")

		setupMinio()
		DeferCleanup(teardownMinio)

		By("creating the source cluster that archives to object storage")
		applyManifest(sourceCluster, archivingClusterManifest(sourceCluster))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, archivingClusterManifest(sourceCluster))
		})
		expectClusterReady(sourceCluster, 1, 20*time.Minute)
	})

	It("fires an immediate backup and keeps the cron cadence", func() {
		By("creating the ScheduledBackup with immediate and a tight schedule")
		applyManifest(scheduleName, scheduledBackupManifest(scheduleName, sourceCluster, tightSchedule))
		DeferCleanup(func() {
			deleteManifest(scheduleName, scheduledBackupManifest(scheduleName, sourceCluster, tightSchedule))
		})

		By("waiting for the immediate backup to be created with the parent label")
		Eventually(func(g Gomega) {
			names := backupNamesWithLabel(g, parentLabelSelector)
			g.Expect(names).NotTo(BeEmpty(), "no backup created for the scheduled backup yet")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the first backup carries the immediate label")
		immediate, err := kubectl("get", "backups", "-n", testNamespace,
			"-l", parentLabelSelector,
			"-o", "jsonpath={.items[0].metadata.labels."+jsonpathEscape(immediateBackupLabel)+"}")
		Expect(err).NotTo(HaveOccurred())
		Expect(immediate).To(Equal("true"), "first scheduled backup is not the immediate one")

		By("waiting for at least one child backup to complete")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "backups", "-n", testNamespace,
				"-l", parentLabelSelector,
				"-o", "jsonpath={.items[*].status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			phases := strings.Fields(out)
			g.Expect(phases).NotTo(ContainElement("failed"), "a scheduled backup failed")
			g.Expect(phases).To(ContainElement("completed"), "no scheduled backup completed yet")
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())

		By("verifying the schedule advances its status")
		Eventually(func(g Gomega) {
			next, err := kubectl("get", "scheduledbackup", scheduleName, "-n", testNamespace,
				"-o", "jsonpath={.status.nextScheduleTime}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(next).NotTo(BeEmpty(), "nextScheduleTime is not set")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("waiting for a second slot to fire (more than one child backup)")
		Eventually(func(g Gomega) {
			names := backupNamesWithLabel(g, parentLabelSelector)
			g.Expect(len(names)).To(BeNumerically(">=", 2), "the schedule did not produce a second backup")
		}, e2eTimeout(12*time.Minute), 10*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

func backupNamesWithLabel(g Gomega, selector string) []string {
	out, err := kubectl("get", "backups", "-n", testNamespace,
		"-l", selector, "-o", "jsonpath={.items[*].metadata.name}")
	g.Expect(err).NotTo(HaveOccurred())
	return strings.Fields(out)
}

// jsonpathEscape backslash-escapes the dots in a label key so jsonpath treats
// it as a single map key rather than a nested path.
func jsonpathEscape(key string) string {
	return strings.ReplaceAll(key, ".", `\.`)
}

func scheduledBackupManifest(name, cluster, schedule string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: ScheduledBackup
metadata:
  name: %s
  namespace: %s
spec:
  schedule: "%s"
  immediate: true
  cluster:
    name: %s
  method: xtrabackup
`, name, testNamespace, schedule, cluster)
}
