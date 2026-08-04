//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec verifies that the operator automatically re-clones a replica whose
// mysqld cannot start because its InnoDB data is damaged. It corrupts a
// replica's data directory, then waits for the instance manager to diagnose the
// damage from mysqld's own output and publish it through the Pod's termination
// message. The controller reads that diagnosis and re-clones without waiting out
// the full crash-loop budget: it sets the reinit annotation, the topology
// reconciler tears down the Pod and PVC, and the bootstrap init-container clones
// a fresh copy from the primary. The replica rejoins and catches up with no data
// loss.
//
// The instance is deliberately never started with innodb_force_recovery: MySQL
// blocks INSERT, UPDATE and DELETE whenever it is above zero, so a
// force-recovered server could not apply relay logs anyway. Re-cloning is the
// remedy.
//
// The test uses the shared single-node cluster (no drain needed), so it runs in
// parallel with other non-disruptive specs.
var _ = Describe("Auto corruption recovery", Label("corruption"), func() {
	const (
		cluster   = "corrupt"
		instances = 2
	)

	It("automatically re-clones a replica with corrupt InnoDB data", func() {
		By("creating a 2-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, instances))
		expectClusterReady(cluster, instances, 20*time.Minute)
		password := appPassword(cluster)

		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, instances, primary)
		By(fmt.Sprintf("primary is %s, replica is %s", primary, replica))

		By("seeding data on the primary before corruption")
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS corruption_test (id INT PRIMARY KEY); "+
				"REPLACE INTO corruption_test VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "failed to seed data before corruption")

		By("corrupting the replica's InnoDB data directory")
		corruptReplica(replica)

		// The diagnosis is what lets the operator act on evidence rather than on a
		// restart count, so assert it reaches the Pod status before checking the
		// re-clone it triggers.
		By("waiting for the instance manager to publish a corruption diagnosis")
		Eventually(func(g Gomega) {
			msg, err := kubectl("get", "pod", replica, "-n", testNamespace,
				"-o", "jsonpath={.status.containerStatuses[?(@.name=='mysql')]"+
					".lastState.terminated.message}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(msg).To(ContainSubstring("CNMSQL_INNODB_CORRUPTION"),
				"the manager must publish the corruption diagnosis in the termination message")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("waiting for the operator to auto-reinit the replica")
		Eventually(func(g Gomega) {
			ann, err := clusterField(cluster,
				fmt.Sprintf("{.metadata.annotations.cnmsql\\.cnmsql\\.co/reinit}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ann).To(ContainSubstring(replica),
				"operator must set the reinit annotation for the crashed replica")
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())

		By("waiting for the cluster to recover after re-clone")
		expectClusterReady(cluster, instances, 20*time.Minute)

		By("verifying the re-cloned replica has the data written before corruption")
		newReplica := otherInstance(cluster, instances, clusterPrimary(cluster))
		Eventually(func(g Gomega) {
			caught, err := mysqlExec(newReplica, "app", password, "app",
				"SELECT id FROM corruption_test WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(caught).To(ContainSubstring("1"),
				"re-cloned replica must have the data written before corruption")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})
})

// corruptReplica corrupts a replica's InnoDB data by deleting the ibdata1
// system tablespace file, making mysqld unable to start. The Pod's mysqld will
// crash repeatedly, escalating through innodb_force_recovery levels 1→2→3
// before the controller auto-reinits.
func corruptReplica(pod string) {
	GinkgoHelper()
	_, err := kubectl("exec", pod, "-n", testNamespace, "-c", "mysql", "--",
		"rm", "-f", "/var/lib/mysql/ibdata1")
	Expect(err).NotTo(HaveOccurred(),
		"failed to corrupt replica %s (delete ibdata1)", pod)
}
