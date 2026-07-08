//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The MariaDB counterpart of the "Failover rejoin" suite. It exercises the
// MariaDB replication rejoin path — which diverges from MySQL's (domain-based
// GTIDs, no GTID_NEXT) — by killing the primary Pod while retaining its PVC and
// asserting the recreated former primary reuses that PVC and catches up as a
// replica from the data it already had on disk.
var _ = Describe("MariaDB failover rejoin", Ordered, Label("flavor", "mariadb"), func() {
	const (
		cluster   = "mdb-failjoin"
		instances = 3
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-failjoin")

		By("creating a 3-instance MariaDB cluster")
		applyManifest(cluster, mariadbClusterManifest(cluster))
		DeferCleanup(func() {
			deleteManifest(cluster, mariadbClusterManifest(cluster))
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})

	It("rejoins a deleted former primary from its retained PVC after failover", func() {
		password := appPassword(cluster)
		oldPrimary := clusterPrimary(cluster)
		oldPrimaryPVCUID := pvcUID(oldPrimary)

		By(fmt.Sprintf("seeding data on the original primary %s", oldPrimary))
		_, err := mariadbExec(oldPrimary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS failover_rejoin (id INT PRIMARY KEY); "+
				"REPLACE INTO failover_rejoin VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "failed to seed the original primary")

		By(fmt.Sprintf("deleting primary Pod %s while retaining its PVC", oldPrimary))
		_, err = kubectl("delete", "pod", oldPrimary, "-n", testNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred(), "failed to delete the primary Pod")

		By("waiting for a surviving replica to become primary")
		var newPrimary string
		Eventually(func(g Gomega) {
			primary := clusterPrimary(cluster)
			g.Expect(primary).NotTo(Equal(oldPrimary))
			newPrimary = primary
		}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("writing post-failover data on new primary %s", newPrimary))
		Eventually(func(g Gomega) {
			_, err := mariadbExec(newPrimary, "app", password, "app",
				"REPLACE INTO failover_rejoin VALUES (2);")
			g.Expect(err).NotTo(HaveOccurred())
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the recreated former primary reuses its PVC and catches up as a replica")
		Eventually(func(g Gomega) {
			g.Expect(pvcUID(oldPrimary)).To(Equal(oldPrimaryPVCUID),
				"the instance PVC must be retained across Pod recreation")
			role := podLabel(g, oldPrimary, "mysql.cnmsql.co/role")
			g.Expect(role).To(Equal("replica"), "the old primary must rejoin as a replica")
			out, err := mariadbExec(oldPrimary, "app", password, "",
				"SELECT id FROM app.failover_rejoin WHERE id = 2;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("2"))
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})
})
