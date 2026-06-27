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

// These specs exercise Kubernetes node failure end to end: a worker node hosting
// the primary instance is cordoned and drained, and the operator must perform a
// clean switchover, lose no committed data, and rejoin the evicted instance from
// its retained PVC once the node comes back.
//
// Node drain needs more than one node, but the shared suite cluster is
// single-node. So this spec provisions its OWN multi-node Kind cluster (from
// test/e2e/kind-multinode.yaml), deploys the operator there, and tears it down
// afterwards — the shared cluster is never made multi-node and never sees a
// drain. It auto-skips when the kind binary is unavailable.
//
// Serial: it switches the active kube-context to its dedicated cluster for the
// duration of the spec, so it must run alone, never interleaved with specs
// targeting the shared cluster.
var _ = Describe("Node failure", Ordered, Serial, Label("disruptive", "node-failure"), func() {
	const (
		cluster   = "nodefail"
		instances = 3
	)

	var password, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		By("provisioning a dedicated multi-node Kind cluster for the node-failure spec")
		dc := provisionDedicated("nodefail", "test/e2e/kind-multinode.yaml")
		DeferCleanup(func() {
			testNamespace = prevNS
			dc.teardown()
		})

		// From here on, the active kube-context targets the dedicated cluster.
		createTestNamespace("nodefail")

		By("creating a 3-instance cluster pinned one instance per node")
		applyManifest(cluster, spreadClusterManifest(cluster, instances))
		expectClusterReady(cluster, instances, 20*time.Minute)
		password = appPassword(cluster)

		By("confirming the instances landed on distinct nodes")
		Expect(instanceNodes(cluster, instances)).To(HaveLen(instances),
			"instances must spread one per node for the drain to isolate a single instance")
	})

	It("tears down both PDBs while a node maintenance window is open and restores them when closed", func() {
		By("verifying the operator created the primary and replica PodDisruptionBudgets")
		Expect(pdbExists(cluster+"-primary")).To(BeTrue(), "primary PDB must exist in steady state")
		Expect(pdbExists(cluster+"-replicas")).To(BeTrue(), "replica PDB must exist in steady state")

		By("opening a node maintenance window")
		setMaintenanceWindow(cluster, true)
		DeferCleanup(func() { setMaintenanceWindow(cluster, false) })

		By("verifying both PDBs are removed so the kubelet can drain a node")
		// The primary PDB must go too: its narrow role=primary selector (one Pod)
		// against a 3-replica StatefulSet makes Kubernetes allow zero voluntary
		// disruptions, which would otherwise block the primary's node from draining.
		Eventually(func() bool {
			return pdbExists(cluster+"-replicas") || pdbExists(cluster+"-primary")
		}, e2eTimeout(2*time.Minute), 3*time.Second).Should(BeFalse(),
			"both PDBs must be torn down during a maintenance window")

		By("closing the maintenance window")
		setMaintenanceWindow(cluster, false)

		By("verifying both PDBs are recreated once maintenance ends")
		Eventually(func() bool {
			return pdbExists(cluster+"-replicas") && pdbExists(cluster+"-primary")
		}, e2eTimeout(2*time.Minute), 3*time.Second).Should(BeTrue(),
			"both PDBs must be restored after the maintenance window closes")
	})

	It("performs a clean switchover with no data loss when the primary's node is drained, then rejoins the old primary", func() {
		oldPrimary := clusterPrimary(cluster)
		node := nodeForPod(oldPrimary)
		oldPrimaryPVCUID := pvcUID(oldPrimary)

		By(fmt.Sprintf("seeding data on the primary %s (node %s)", oldPrimary, node))
		_, err := mysqlExec(oldPrimary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS node_failure (id INT PRIMARY KEY); "+
				"REPLACE INTO node_failure VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "failed to seed the primary before the drain")

		By("opening a node maintenance window so the drain is not blocked by the PDBs")
		setMaintenanceWindow(cluster, true)
		DeferCleanup(func() { setMaintenanceWindow(cluster, false) })

		By("verifying the primary PDB is removed so the kubelet can evict the primary")
		// The primary PDB allows zero voluntary disruptions (narrow selector vs the
		// StatefulSet scale), so it must be gone before the eviction can proceed.
		Eventually(func() bool {
			return pdbExists(cluster + "-primary")
		}, e2eTimeout(2*time.Minute), 3*time.Second).Should(BeFalse(),
			"primary PDB must be torn down during a maintenance window")

		By(fmt.Sprintf("draining node %s (evicting the primary)", node))
		out, err := drainNode(node)
		Expect(err).NotTo(HaveOccurred(), "failed to drain node %s: %s", node, out)
		DeferCleanup(func() { _, _ = kubectl("uncordon", node) })

		By("waiting for a surviving replica to be promoted to primary (clean switchover)")
		var newPrimary string
		Eventually(func(g Gomega) {
			primary := clusterPrimary(cluster)
			g.Expect(primary).NotTo(Equal(oldPrimary), "a new primary must be elected off the drained node")
			g.Expect(nodeForPod(primary)).NotTo(Equal(node), "the new primary must run on a surviving node")
			newPrimary = primary
		}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("verifying the new primary %s is writable and retained the seeded data (no data loss)", newPrimary))
		Eventually(func(g Gomega) {
			seeded, err := mysqlExec(newPrimary, "app", password, "app",
				"SELECT id FROM node_failure WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(seeded).To(ContainSubstring("1"), "data committed before the drain must survive the switchover")
			_, err = mysqlExec(newPrimary, "app", password, "app",
				"REPLACE INTO node_failure VALUES (2);")
			g.Expect(err).NotTo(HaveOccurred(), "the new primary must accept writes after the switchover")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the promotion went through the planned switchover path, not an emergency failover")
		Expect(clusterEventExists(cluster, "FailingOver")).To(BeFalse(),
			"a graceful drain of the primary must hand off via switchover, never an emergency failover")
		Expect(clusterEventExists(cluster, "Switchover")).To(BeTrue(),
			"the operator must record a planned switchover for the draining primary")

		By(fmt.Sprintf("uncordoning node %s so the evicted instance can reschedule", node))
		_, err = kubectl("uncordon", node)
		Expect(err).NotTo(HaveOccurred(), "failed to uncordon node %s", node)
		setMaintenanceWindow(cluster, false)

		By("verifying the old primary reuses its PVC, rejoins as a replica, and catches up post-switchover data")
		Eventually(func(g Gomega) {
			g.Expect(pvcUID(oldPrimary)).To(Equal(oldPrimaryPVCUID),
				"the instance PVC must be retained across the node drain")
			g.Expect(nodeForPod(oldPrimary)).To(Equal(node),
				"the local PVC pins the rejoined instance back to its original node")
			role := podLabel(g, oldPrimary, "mysql.cnmsql.co/role")
			g.Expect(role).To(Equal("replica"), "the old primary must rejoin as a replica")
			caught, err := mysqlExec(oldPrimary, "app", password, "",
				"SELECT id FROM app.node_failure WHERE id = 2;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(caught).To(ContainSubstring("2"), "the rejoined replica must replicate post-switchover writes")
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		expectClusterReady(cluster, instances, 20*time.Minute)
	})
})

// spreadClusterManifest is a basic 3-instance Cluster that additionally pins one
// instance per node via a hard topology spread constraint, so draining a single
// node isolates exactly one instance.
func spreadClusterManifest(name string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  storage:
    size: 2Gi
%[5]s
  topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
        mysql.cnmsql.co/cluster: %[1]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters)
}

// nodeForPod returns the node a Pod is scheduled on.
func nodeForPod(pod string) string {
	out, err := kubectl("get", "pod", pod, "-n", testNamespace, "-o", "jsonpath={.spec.nodeName}")
	Expect(err).NotTo(HaveOccurred(), "failed to read node for Pod %s", pod)
	return strings.TrimSpace(out)
}

// instanceNodes returns the set of nodes the cluster's instance Pods occupy,
// keyed by node name, so callers can assert the spread.
func instanceNodes(cluster string, instances int) map[string]struct{} {
	nodes := map[string]struct{}{}
	for i := 1; i <= instances; i++ {
		nodes[nodeForPod(fmt.Sprintf("%s-%d", cluster, i))] = struct{}{}
	}
	return nodes
}

// drainNode cordons and drains a node, evicting its Pods. DaemonSet Pods are
// ignored and emptyDir data is allowed to be deleted, as a real drain would.
func drainNode(node string) (string, error) {
	return kubectl("drain", node,
		"--ignore-daemonsets", "--delete-emptydir-data", "--force",
		"--timeout="+e2eTimeout(2*time.Minute).String())
}

// clusterEventExists reports whether the operator recorded an Event with the
// given reason against the named Cluster. Used to distinguish the switchover
// path (a "Switchover" event) from an emergency failover (a "FailingOver" event).
func clusterEventExists(cluster, reason string) bool {
	out, err := kubectl("get", "events", "-n", testNamespace,
		"--field-selector",
		fmt.Sprintf("involvedObject.kind=Cluster,involvedObject.name=%s,reason=%s", cluster, reason),
		"-o", "name")
	if err != nil {
		return false
	}
	return strings.TrimSpace(out) != ""
}

// pdbExists reports whether a PodDisruptionBudget with the given name exists in
// the test namespace.
func pdbExists(name string) bool {
	_, err := kubectl("get", "pdb", name, "-n", testNamespace, "-o", "name")
	return err == nil
}

// setMaintenanceWindow toggles spec.nodeMaintenanceWindow.inProgress on the
// cluster. When opened it keeps reusePVC=true so the operator retains the
// instance's PVC across the drain instead of provisioning a fresh one.
func setMaintenanceWindow(cluster string, inProgress bool) {
	GinkgoHelper()
	patch := fmt.Sprintf(
		`{"spec":{"nodeMaintenanceWindow":{"inProgress":%t,"reusePVC":true}}}`, inProgress)
	_, err := kubectl("patch", "cluster", cluster, "-n", testNamespace, "--type", "merge", "-p", patch)
	Expect(err).NotTo(HaveOccurred(), "failed to set maintenance window inProgress=%t on %s", inProgress, cluster)
}
