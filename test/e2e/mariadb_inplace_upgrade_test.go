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

// The MariaDB counterpart of the "In-place instance manager upgrade" suite:
// hitting POST /instance/manager/restart-inplace makes the manager re-exec itself
// and adopt the already-running mariadbd, so the server is never restarted. The
// proof is threefold: the manager logs the adopt path, the mysql container's
// restart count stays flat, and the server's uptime keeps climbing.
var _ = Describe("MariaDB in-place instance manager upgrade", Ordered, Label("feature", "mariadb"), func() {
	const (
		cluster   = "mdb-inplace"
		instances = 3
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-inplace")

		By("creating a 3-instance MariaDB cluster")
		manifest := mariadbBasicClusterManifest(cluster, instances)
		manifest = strings.Replace(manifest, "\n  storage:", "\n  failoverDelay: 60\n  storage:", 1)
		applyManifest(cluster, manifest)
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})

	It("re-execs the manager in place on the primary without restarting mariadbd or switching over", func() {
		password := appPassword(cluster)
		primary := clusterPrimary(cluster)

		By(fmt.Sprintf("recording the pre-upgrade restart count and server uptime of primary %s", primary))
		restartsBefore := podRestartCount(primary)
		var uptimeBefore int
		Eventually(func(g Gomega) {
			uptimeBefore = mariadbUptime(g, primary, password)
			g.Expect(uptimeBefore).To(BeNumerically(">", 0))
		}, e2eTimeout(1*time.Minute), 3*time.Second).Should(Succeed())

		By(fmt.Sprintf("triggering an in-place manager re-exec on %s via the control API", primary))
		triggerInPlaceRestart(cluster, primary)

		By("verifying the re-exec'd manager adopted the running server (adopt path in the logs)")
		Eventually(func(g Gomega) {
			logs, err := kubectl("logs", primary, "-n", testNamespace, "-c", "mysql", "--tail=400")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(logs).To(ContainSubstring("Adopting running mysqld after in-place manager upgrade"),
				"the manager must take the adopt path, proving it re-exec'd in place")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the server was never restarted: container restart count stays flat and uptime keeps climbing")
		Consistently(func(g Gomega) {
			g.Expect(podRestartCount(primary)).To(Equal(restartsBefore),
				"the mysql container restarted; the manager swap must not restart the Pod")
			g.Expect(mariadbUptime(g, primary, password)).To(BeNumerically(">=", uptimeBefore),
				"server uptime dropped; the server was restarted instead of adopted")
		}, e2eTimeout(45*time.Second), 10*time.Second).Should(Succeed())

		By("verifying no switchover happened: the primary is unchanged")
		Expect(clusterPrimary(cluster)).To(Equal(primary),
			"an in-place upgrade must not switch the primary over")

		By("verifying the primary still serves writes after the manager swap")
		Eventually(func(g Gomega) {
			_, err := mariadbExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS inplace_probe (id INT PRIMARY KEY); "+
					"REPLACE INTO inplace_probe VALUES (1);")
			g.Expect(err).NotTo(HaveOccurred())
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})
})
