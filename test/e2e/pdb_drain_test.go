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

// This spec verifies that the replica PodDisruptionBudget does not block node
// drains for a 2-instance cluster. Before the fix, maxUnavailable was
// floor(1/2)=0, which meant zero voluntary disruptions were allowed on the
// replica — a node holding a replica could not be drained without opening a
// maintenance window. The fix clamps maxUnavailable to at least 1 so a replica
// can always be evicted, letting the node drain proceed.
//
// The test needs a multi-node Kind cluster so there is a second node for the
// primary to survive on while the replica's node is drained. It uses the same
// dedicated-cluster pattern as the node-failure spec.
var _ = Describe("PDB draining", Ordered, Serial, Label("disruptive", "node-failure"), func() {
	const (
		cluster   = "pdbdrain"
		instances = 2
	)

	var password, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		By("provisioning a dedicated multi-node Kind cluster for the PDB drain spec")
		dc := provisionDedicated("pdbdrain", "test/e2e/kind-multinode.yaml")
		DeferCleanup(func() {
			testNamespace = prevNS
			dc.teardown()
		})

		createTestNamespace("pdbdrain")

		By("creating a 2-instance cluster pinned one instance per node")
		applyManifest(cluster, spreadClusterManifest(cluster, instances))
		expectClusterReady(cluster, instances, 20*time.Minute)
		password = appPassword(cluster)

		By("confirming the instances landed on distinct nodes")
		Expect(instanceNodes(cluster, instances)).To(HaveLen(instances),
			"instances must spread one per node for the drain to isolate a single instance")
	})

	It("allows draining a replica's node without a maintenance window", func() {
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, instances, primary)
		replicaNode := nodeForPod(replica)

		By(fmt.Sprintf("primary is %s, replica is %s on node %s", primary, replica, replicaNode))

		By("verifying the replica PDB allows at least 1 disruption")
		Eventually(func(g Gomega) {
			mu, err := kubectl("get", "pdb", cluster+"-replicas", "-n", testNamespace,
				"-o", "jsonpath={.spec.maxUnavailable}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(mu)).To(Equal("1"),
				"replica PDB maxUnavailable must be 1 for a 2-instance cluster, got %s", mu)
		}, e2eTimeout(2*time.Minute), 3*time.Second).Should(Succeed())

		By("seeding data on the primary before the drain")
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS pdb_drain (id INT PRIMARY KEY); "+
				"REPLACE INTO pdb_drain VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "failed to seed the primary before the drain")

		By(fmt.Sprintf("draining node %s (evicting the replica) without a maintenance window", replicaNode))
		out, err := drainNode(replicaNode)
		Expect(err).NotTo(HaveOccurred(), "drain of a replica node must not be blocked by the PDB: %s", out)
		DeferCleanup(func() { _, _ = kubectl("uncordon", replicaNode) })

		By("verifying the replica Pod is evicted from the drained node")
		Eventually(func(g Gomega) {
			g.Expect(podExists(replica)).To(BeFalse(),
				"replica %s must be evicted from the drained node", replica)
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("uncordoning the node so the replica can reschedule")
		_, err = kubectl("uncordon", replicaNode)
		Expect(err).NotTo(HaveOccurred(), "failed to uncordon node %s", replicaNode)

		By("waiting for the replica to reschedule and rejoin")
		expectClusterReady(cluster, instances, 20*time.Minute)

		By("verifying the replica caught up to the data written before the drain")
		Eventually(func(g Gomega) {
			caught, err := mysqlExec(replica, "app", password, "app",
				"SELECT id FROM pdb_drain WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(caught).To(ContainSubstring("1"),
				"rejoined replica must have the data written before the drain")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})
})

func podExists(name string) bool {
	_, err := kubectl("get", "pod", name, "-n", testNamespace, "-o", "name")
	return err == nil
}
