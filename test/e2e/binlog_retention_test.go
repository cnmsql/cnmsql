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

// These specs validate that binary logs are retained on the primary when the
// active purge gate is off (the default), so a lagged or returning replica can
// always catch up via GTID auto-positioning. mysqld's own
// binlog_expire_logs_seconds (set to 7 days by the operator) remains as the
// conservative backstop — the operator never actively purges.
//
// The matrix runs against every archiving version, since the purge gate
// touches version-gated surface (binlog expiry, the processlist command names).

func init() {
	for _, version := range archiveVersions() {
		v := version
		Describe(fmt.Sprintf("Binlog retention - %s", v), Ordered, Label("flavor"), func() {
			var ns, prevNS string

			BeforeAll(func() {
				prevNS = testNamespace
				ns = createTestNamespace("retention-" + sanitize(v))
				setupMinio()
				setupMC()
			})

			AfterAll(func() {
				teardownMC()
				teardownMinio()
				deleteTestNamespace(ns, prevNS)
			})

			retentionVersionSpecs(v)
		})
	}
}

func retentionVersionSpecs(version string) {
	Context("3-instance archiving cluster", Ordered, func() {
		cluster := "ret-" + sanitize(version) + "-ha"
		var password string

		BeforeAll(func() {
			By("creating the three-instance archiving cluster")
			applyManifest(cluster, continuousArchivingClusterManifest(cluster, version, 3))
			DeferCleanup(func() {
				deleteManifest(cluster, continuousArchivingClusterManifest(cluster, version, 3))
			})
			expectClusterReady(cluster, 3, 20*time.Minute)
			password = appPassword(cluster)

			By("seeding a base table")
			primary := clusterPrimary(cluster)
			_, err := mysqlExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS ledger (id INT PRIMARY KEY)")
			Expect(err).NotTo(HaveOccurred(), "Failed to seed the base table")

			By("flushing and archiving the initial binlog")
			executed := flushBinaryLogs(cluster, primary, password)
			expectArchiveCovers(cluster, executed, 4*time.Minute)
		})

		It("retains binlogs on the primary for a returning replica", func() {
			primary := clusterPrimary(cluster)

			By("identifying the replica pods")
			pods := clusterPods(cluster)
			Expect(len(pods)).To(Equal(3), "expected 3 pods, got %v", pods)
			var replica string
			for _, p := range pods {
				if p != primary {
					replica = p
					break
				}
			}
			Expect(replica).NotTo(BeEmpty(), "no replica pod found")

			By("writing and rotating to produce multiple archived binlogs")
			for i := 1; i <= 6; i++ {
				_, err := mysqlExec(primary, "app", password, "app",
					fmt.Sprintf("INSERT INTO ledger VALUES (%d)", i))
				Expect(err).NotTo(HaveOccurred(), "insert %d failed", i)
				_, err = mysqlExec(primary, "root", rootPassword(cluster), "", "FLUSH BINARY LOGS")
				Expect(err).NotTo(HaveOccurred(), "flush %d failed", i)
			}

			By("waiting for the archive to cover all committed GTIDs")
			executed := flushBinaryLogs(cluster, primary, password)
			expectArchiveCovers(cluster, executed, 4*time.Minute)

			By("recording the binlog files present before the replica goes down")
			beforeLogs := listBinaryLogs(cluster, primary)
			Expect(len(beforeLogs)).To(BeNumerically(">", 1),
				"expected multiple binlog files, got %v", beforeLogs)

			By("force-deleting a replica pod to make it unavailable")
			_, err := kubectl("delete", "pod", replica, "-n", testNamespace,
				"--grace-period=0", "--force")
			Expect(err).NotTo(HaveOccurred(), "Failed to force-delete replica %s", replica)

			By("writing more transactions and rotating while the replica is down")
			for i := 10; i < 16; i++ {
				_, err := mysqlExec(primary, "app", password, "app",
					fmt.Sprintf("INSERT INTO ledger VALUES (%d)", i))
				Expect(err).NotTo(HaveOccurred(), "insert %d failed", i)
				_, err = mysqlExec(primary, "root", rootPassword(cluster), "", "FLUSH BINARY LOGS")
				Expect(err).NotTo(HaveOccurred(), "flush %d failed", i)
			}

			By("waiting for the new binlogs to be archived")
			executed = flushBinaryLogs(cluster, primary, password)
			expectArchiveCovers(cluster, executed, 4*time.Minute)

			By("verifying the primary retained all prior binlogs while the replica was down")
			currentLogs := listBinaryLogs(cluster, primary)
			for _, old := range beforeLogs {
				Expect(currentLogs).To(ContainElement(old),
					"binlog %s was purged while a replica was unavailable; current files: %v",
					old, currentLogs)
			}

			By("waiting for the replica to rejoin and catch up")
			expectClusterReady(cluster, 3, 20*time.Minute)

			By("verifying the replica caught up to all transactions written while it was down")
			Eventually(func(g Gomega) {
				out, err := mysqlExec(replica, "app", password, "app",
					"SELECT COUNT(*) FROM ledger WHERE id >= 10 AND id < 16")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("6"),
					"replica %s has not caught up to transactions written while it was down", replica)
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		})
	})
}

// listBinaryLogs returns the names of all binary log files on a pod.
func listBinaryLogs(cluster, pod string) []string {
	out, err := mysqlExec(pod, "root", rootPassword(cluster), "",
		"SHOW BINARY LOGS")
	Expect(err).NotTo(HaveOccurred(), "SHOW BINARY LOGS failed on %s", pod)
	var logs []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "mysql:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) > 0 && strings.HasPrefix(fields[0], "binlog.") {
			logs = append(logs, fields[0])
		}
	}
	return logs
}

// clusterPods returns the names of all pods belonging to the given cluster.
func clusterPods(cluster string) []string {
	out, err := kubectl("get", "pods", "-n", testNamespace,
		"-l", "mysql.cnmsql.co/cluster="+cluster,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}")
	Expect(err).NotTo(HaveOccurred(), "Failed to list pods for cluster %s", cluster)
	var pods []string
	for _, p := range strings.Fields(out) {
		pods = append(pods, p)
	}
	return pods
}
