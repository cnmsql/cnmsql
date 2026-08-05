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
// drains for a 2-instance cluster. Before the fix the replica budget was stated
// as maxUnavailable=floor(1/2)=0, which allowed zero voluntary disruptions — a
// node holding a replica could not be drained without opening a maintenance
// window. Raising maxUnavailable alone does not fix it: the Cluster CR exposes a
// scale subresource, so Kubernetes resolves the budget's expectedCount to N (the
// whole cluster) while only the N-1 replicas match the selector, leaving
// maxUnavailable-1 allowed disruptions. The fix states the budget as an integer
// minAvailable, which is taken as desiredHealthy verbatim and never resolved
// against the scale, so the single replica can be evicted and the drain proceeds.
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
		// Instance Pods carry a stable, ordinal-derived name, so the operator
		// recreates the evicted replica under the very same name within seconds.
		// The eviction is therefore only observable through the Pod identity: the
		// recreated Pod is a different object. This is the same signal kubectl
		// drain itself waits on.
		replicaUID := podUID(replica)
		Expect(replicaUID).NotTo(BeEmpty(), "failed to read the UID of replica %s before the drain", replica)

		By(fmt.Sprintf("primary is %s, replica is %s on node %s", primary, replica, replicaNode))

		By("verifying the replica PDB is stated as minAvailable=0")
		Eventually(func(g Gomega) {
			ma, err := kubectl("get", "pdb", cluster+"-replicas", "-n", testNamespace,
				"-o", "jsonpath={.spec.minAvailable}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(ma)).To(Equal("0"),
				"replica PDB minAvailable must be 0 for a 2-instance cluster, got %s", ma)

			mu, err := kubectl("get", "pdb", cluster+"-replicas", "-n", testNamespace,
				"-o", "jsonpath={.spec.maxUnavailable}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(mu)).To(BeEmpty(),
				"replica PDB must not also set maxUnavailable, got %s", mu)
		}, e2eTimeout(2*time.Minute), 3*time.Second).Should(Succeed())

		// The budget field is only half the story: assert Kubernetes itself agrees
		// a disruption is permitted before we ask it to drain, so a regression here
		// fails with a clear message rather than a drain timeout.
		By("verifying Kubernetes computes at least 1 allowed disruption")
		Eventually(func(g Gomega) {
			allowed, err := kubectl("get", "pdb", cluster+"-replicas", "-n", testNamespace,
				"-o", "jsonpath={.status.disruptionsAllowed}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(allowed)).To(Equal("1"),
				"replica PDB must permit 1 disruption, got %s", allowed)
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
			uid, node, found := podUIDAndNode(replica)
			if !found {
				// Evicted and not yet recreated: the drain did its job.
				return
			}
			g.Expect(uid).NotTo(Equal(replicaUID),
				"replica %s must be evicted from the drained node", replica)
			g.Expect(node).NotTo(Equal(replicaNode),
				"the recreated replica %s must not land back on the cordoned node", replica)
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

// podUIDAndNode returns a Pod's UID and the node it is scheduled on, reporting
// found=false when the Pod is absent. The node is empty while the Pod is still
// pending scheduling.
func podUIDAndNode(name string) (uid, node string, found bool) {
	out, err := kubectl("get", "pod", name, "-n", testNamespace,
		"-o", "jsonpath={.metadata.uid} {.spec.nodeName}")
	if err != nil {
		return "", "", false
	}
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return "", "", false
	}
	if len(fields) == 1 {
		return fields[0], "", true
	}
	return fields[0], fields[1], true
}
