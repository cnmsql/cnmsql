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

var _ = Describe("Failover rejoin", Ordered, func() {
	const (
		cluster   = "failjoin"
		instances = 3
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("failjoin")

		By("creating a 3-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteCluster(cluster)
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, 20*time.Minute)
	})

	It("rejoins a deleted former primary from its retained PVC after failover", func() {
		password := appPassword(cluster)
		oldPrimary := clusterPrimary(cluster)
		oldPrimaryPVCUID := pvcUID(oldPrimary)

		By(fmt.Sprintf("seeding data on the original primary %s", oldPrimary))
		_, err := mysqlExec(oldPrimary, "app", password, "app",
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
			_, err := mysqlExec(newPrimary, "app", password, "app",
				"REPLACE INTO failover_rejoin VALUES (2);")
			g.Expect(err).NotTo(HaveOccurred())
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the recreated former primary reuses its PVC and catches up as a replica")
		Eventually(func(g Gomega) {
			g.Expect(pvcUID(oldPrimary)).To(Equal(oldPrimaryPVCUID),
				"the instance PVC must be retained across Pod recreation")
			role := podLabel(g, oldPrimary, "mysql.cloudnative-mysql.io/role")
			g.Expect(role).To(Equal("replica"), "the old primary must rejoin as a replica")
			out, err := mysqlExec(oldPrimary, "app", password, "",
				"SELECT id FROM app.failover_rejoin WHERE id = 2;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("2"))
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		expectClusterReady(cluster, instances, 20*time.Minute)
	})
})

func pvcUID(name string) string {
	out, err := kubectl("get", "pvc", name, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read PVC UID for %s", name)
	return strings.TrimSpace(out)
}

func podLabel(g Gomega, pod, label string) string {
	out, err := kubectl("get", "pod", pod, "-n", testNamespace, "-o",
		fmt.Sprintf("go-template={{ index .metadata.labels %q }}", label))
	g.Expect(err).NotTo(HaveOccurred(), "Failed to read label %s on Pod %s", label, pod)
	return strings.TrimSpace(out)
}
