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

// The MariaDB counterpart of the "Self-healing" suite. MariaDB semi-sync always
// waits for exactly one replica acknowledgement, so there is no count for the
// operator to self-heal. Instead, these tests verify that semi-sync is really
// active (the MySQL variable names it once rendered left it silently off) and
// that the cluster stays writable while a replica is fenced.
var _ = Describe("MariaDB self-healing", Ordered, Label("flavor", "mariadb"), func() {
	const (
		cluster  = "mdb-selfheal"
		minSync  = 1
		maxSync  = 1
		replicas = 3
	)

	var ns, prevNS, rootPass string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-selfheal")

		By("creating a 3-instance MariaDB cluster with semi-sync (minSyncReplicas=1)")
		applyManifest(cluster, mariadbSemiSyncClusterManifest(cluster, replicas, minSync, maxSync, "preferred"))
		DeferCleanup(func() {
			deleteManifest(cluster, mariadbSemiSyncClusterManifest(cluster, replicas, minSync, maxSync, "preferred"))
		})
		expectClusterReady(cluster, replicas, e2eTimeout(20*time.Minute))
		rootPass = secretPassword(cluster + "-root")
	})

	It("has active replication with replicas", func() {
		replica := otherInstance(cluster, replicas, clusterPrimary(cluster))
		By("verifying replicas are connected and replicating")
		Eventually(func(g Gomega) {
			g.Expect(mariadbReplicationHealthy(g, replica, rootPass)).To(Equal(1),
				"replica must have active replication threads")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("runs semi-sync on the primary and every replica", func() {
		primary := clusterPrimary(cluster)
		By("verifying the primary waits for an acknowledgement after sync, from both replicas")
		Eventually(func(g Gomega) {
			g.Expect(mariadbGlobalStatus(g, primary, rootPass, "Rpl_semi_sync_master_status")).To(Equal("ON"))
			g.Expect(mariadbGlobalStatus(g, primary, rootPass, "Rpl_semi_sync_master_clients")).To(Equal("2"))
			out, err := mariadbExec(primary, "root", rootPass, "", "SELECT @@global.rpl_semi_sync_master_wait_point")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("AFTER_SYNC"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying every replica acknowledges as a semi-sync replica")
		for i := 1; i <= replicas; i++ {
			pod := fmt.Sprintf("%s-%d", cluster, i)
			if pod == primary {
				continue
			}
			Eventually(func(g Gomega) {
				g.Expect(mariadbGlobalStatus(g, pod, rootPass, "Rpl_semi_sync_slave_status")).To(Equal("ON"))
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		}
	})

	It("never spuriously restarts a healthy instance (liveness isolation stays green)", func() {
		By("recording the current restart counts of every instance")
		before := map[string]int{}
		for i := 1; i <= replicas; i++ {
			pod := fmt.Sprintf("%s-%d", cluster, i)
			before[pod] = podRestartCount(pod)
		}

		By("holding for 90s and confirming no instance container restarted")
		Consistently(func(g Gomega) {
			for pod, was := range before {
				g.Expect(podRestartCount(pod)).To(Equal(was),
					"instance %s restarted while healthy (false isolation)", pod)
			}
		}, e2eTimeout(90*time.Second), 10*time.Second).Should(Succeed())
	})

	It("stays writable when a replica is fenced and recovers after unfencing", func() {
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, replicas, primary)
		// Find the third instance (neither primary nor the fenced replica).
		otherReplica := ""
		for i := 1; i <= replicas; i++ {
			name := fmt.Sprintf("%s-%d", cluster, i)
			if name != primary && name != replica {
				otherReplica = name
				break
			}
		}

		By(fmt.Sprintf("fencing replica %s", replica))
		_, err := kubectl("annotate", "pod", replica, "-n", testNamespace,
			fencingAnnotation+"=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred(), "failed to fence replica")

		By("verifying the primary stays writable during the degraded window")
		_, err = mariadbExec(primary, "root", rootPass, "",
			"CREATE DATABASE IF NOT EXISTS selfheal_probe; "+
				"CREATE TABLE IF NOT EXISTS selfheal_probe.t (id INT PRIMARY KEY); "+
				"REPLACE INTO selfheal_probe.t VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "primary must accept writes while a replica is fenced")

		By("verifying the remaining replica keeps the primary semi-synchronous")
		Eventually(func(g Gomega) {
			g.Expect(mariadbGlobalStatus(g, primary, rootPass, "Rpl_semi_sync_master_status")).To(Equal("ON"))
		}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed())

		By("verifying the other replica still replicates")
		Eventually(func(g Gomega) {
			g.Expect(mariadbReplicationHealthy(g, otherReplica, rootPass)).To(Equal(1),
				"other replica must still be replicating during fence")
		}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed())

		By("unfencing the replica")
		_, err = kubectl("annotate", "pod", replica, "-n", testNamespace, fencingAnnotation+"-")
		Expect(err).NotTo(HaveOccurred(), "failed to unfence replica")

		By("verifying the fenced replica resumes replication after recovery")
		Eventually(func(g Gomega) {
			g.Expect(mariadbReplicationHealthy(g, replica, rootPass)).To(Equal(1),
				"fenced replica must resume replication after recovery")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

func mariadbSemiSyncClusterManifest(name string, instances, minSync, maxSync int, durability string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  flavor: mariadb
  instances: %[3]d
  imageName: %[4]s
  minSyncReplicas: %[5]d
  maxSyncReplicas: %[6]d
  storage:
    size: 2Gi
%[7]s
  mysql:
    binlogFormat: ROW
%[8]s
    semiSync:
      enabled: true
      timeoutMillis: 10000
      dataDurability: %[9]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instances, mariadbImage, minSync, maxSync,
		e2eInstanceResources, e2eMySQLParameters, durability)
}

// mariadbGlobalStatus returns the value of a MariaDB global status variable.
func mariadbGlobalStatus(g Gomega, pod, rootPass, name string) string {
	out, err := mariadbExec(pod, "root", rootPass, "", fmt.Sprintf(
		"SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = '%s'",
		strings.ToUpper(name)))
	g.Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(out)
}
