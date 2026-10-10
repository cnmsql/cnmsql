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

// This spec verifies that the operator automatically re-clones a replica whose
// mysqld cannot start because its InnoDB data is damaged. It corrupts a
// replica's data directory; the instance manager diagnoses the damage from
// mysqld's own output and publishes it through the Pod's termination message.
// The controller reads that diagnosis and re-clones without waiting out the
// full crash-loop budget: it sets the reinit annotation, the topology
// reconciler tears down the Pod and PVC, and a join Job clones a fresh copy
// from the primary. The replica rejoins and catches up with no data loss.
//
// The instance is deliberately never started with innodb_force_recovery: MySQL
// blocks INSERT, UPDATE and DELETE whenever it is above zero, so a
// force-recovered server could not apply relay logs anyway. Re-cloning is the
// remedy.
//
// The termination message and the reinit annotation both disappear with the
// teardown (the annotation lasted five seconds in a local run), so the spec
// asserts on what outlives the re-clone: the AutoReinitializing event, which
// names the InnoDB diagnosis, and a new PVC.
var _ = Describe("Auto corruption recovery", Ordered, Label("feature", "corruption"), func() {
	const (
		cluster   = "corrupt"
		instances = 2
	)
	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("corrupt")
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("automatically re-clones a replica with corrupt InnoDB data", func() {
		By("creating a 2-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
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
		waitForRow(replica, password)

		pvcUID, err := kubectl("get", "pvc", replica, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
		Expect(err).NotTo(HaveOccurred())

		By("corrupting the replica's InnoDB data directory")
		corruptReplica(replica)

		By("waiting for the operator to auto-reinit the replica on the InnoDB diagnosis")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "events", "-n", testNamespace,
				"--field-selector", "reason=AutoReinitializing,type=Warning,involvedObject.name="+cluster,
				"-o", "jsonpath={.items[*].message}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("InnoDB reported corrupt data on [%s]", replica),
				"the operator must re-clone %s because of the InnoDB diagnosis", replica)
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("waiting for the replica to come back on a fresh volume")
		Eventually(func(g Gomega) {
			got, err := kubectl("get", "pvc", replica, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(got)).NotTo(BeElementOf("", strings.TrimSpace(pvcUID)),
				"%s has not been given a new volume yet", replica)
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterRecovers(cluster, instances, 20*time.Minute)

		By("verifying the re-cloned replica has the data written before corruption")
		waitForRow(otherInstance(cluster, instances, clusterPrimary(cluster)), password)
	})
})

// waitForRow waits until pod serves the row the spec seeded on the primary.
func waitForRow(pod, password string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		caught, err := mysqlExec(pod, "app", password, "app",
			"SELECT id FROM corruption_test WHERE id = 1;")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(caught).To(ContainSubstring("1"), "%s must hold the row written before corruption", pod)
	}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
}

// corruptReplica corrupts a replica's InnoDB data by deleting the ibdata1
// system tablespace file and killing mysqld, so the next start fails on the
// missing system tablespace. A running server keeps the deleted file open and
// never notices the deletion — the damage only surfaces on a restart, after
// which mysqld crash-loops and the instance manager publishes the corruption
// diagnosis the controller auto-reinits on.
func corruptReplica(pod string) {
	GinkgoHelper()
	_, err := kubectl("exec", pod, "-n", testNamespace, "-c", "mysql", "--",
		"rm", "-f", "/var/lib/mysql/ibdata1")
	Expect(err).NotTo(HaveOccurred(),
		"failed to corrupt replica %s (delete ibdata1)", pod)
	// The image ships pidof but not pkill, so kill through the shell builtin.
	_, err = kubectl("exec", pod, "-n", testNamespace, "-c", "mysql", "--",
		"sh", "-c", "kill -9 $(pidof mysqld)")
	Expect(err).NotTo(HaveOccurred(),
		"failed to corrupt replica %s (kill mysqld)", pod)
}
