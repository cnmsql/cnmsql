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

// grClusterManifest renders a Group Replication Cluster manifest. M-GR.2 covers a
// single-member group (instances: 1); M-GR.3 a 3-member group (instances: 3).
func grClusterManifest(name string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  replication:
    mode: groupReplication
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters)
}

var _ = Describe("Group Replication single-member", Ordered, func() {
	const (
		cluster   = "gr-single"
		instances = 1
		primary   = cluster + "-1"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("gr-single")

		By("creating a single-member Group Replication cluster")
		applyManifest(cluster, grClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, 15*time.Minute)
	})

	It("bootstraps the group and reflects it into status", func() {
		By("verifying the operator pinned a group name and marked the group bootstrapped")
		Eventually(func(g Gomega) {
			bootstrapped, err := clusterField(cluster, "{.status.groupReplication.bootstrapped}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(bootstrapped).To(Equal("true"), "the group must be bootstrapped")

			groupName, err := clusterField(cluster, "{.status.groupReplication.groupName}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(groupName).NotTo(BeEmpty(), "a group name must be pinned")

			primaryMember, err := clusterField(cluster, "{.status.groupReplication.primaryMember}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(primaryMember).To(Equal(primary), "the bootstrap member must be the group PRIMARY")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying currentPrimary mirrors the elected PRIMARY")
		Expect(clusterPrimary(cluster)).To(Equal(primary))

		By("verifying the member reports ONLINE PRIMARY in the group view")
		Eventually(func(g Gomega) {
			state, err := clusterField(cluster, `{.status.groupReplication.members[0].state}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(state).To(Equal("ONLINE"))
			role, err := clusterField(cluster, `{.status.groupReplication.members[0].role}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(role).To(Equal("PRIMARY"))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("serves writes on the bootstrapped primary and routes rw to it", func() {
		password := appPassword(cluster)

		By("writing to the group's PRIMARY through the member Pod")
		Eventually(func(g Gomega) {
			_, err := mysqlExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS gr_e2e (id INT PRIMARY KEY); REPLACE INTO gr_e2e VALUES (1);")
			g.Expect(err).NotTo(HaveOccurred(), "the bootstrapped PRIMARY must be writable")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("reading the write back")
		out, err := mysqlExec(primary, "app", password, "", "SELECT id FROM app.gr_e2e WHERE id = 1;")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("1"))

		By("verifying the rw Service routes to the primary member")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "endpointslice", "-n", testNamespace,
				"-l", "kubernetes.io/service-name="+cluster+"-rw",
				"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal(primary), "rw must route to the group PRIMARY")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("does not run the async failover/lease machinery under GR", func() {
		By("verifying no primary Lease is created for a GR cluster")
		// The async split-brain guard (the primary Lease) is unused under GR, where
		// quorum provides safety; the operator must not create it.
		out, err := kubectl("get", "lease", cluster+"-primary", "-n", testNamespace, "--ignore-not-found")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(BeEmpty(), "GR clusters must not provision the async primary Lease")
	})
})

var _ = Describe("Group Replication multi-member", Ordered, func() {
	const (
		cluster   = "gr-multi"
		instances = 3
		primary   = cluster + "-1"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("gr-multi")

		By("creating a 3-member Group Replication cluster")
		applyManifest(cluster, grClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, 20*time.Minute)
	})

	It("forms a 3-member group with one PRIMARY and two ONLINE SECONDARYs", func() {
		By("verifying the bootstrap member is the group PRIMARY")
		Expect(clusterPrimary(cluster)).To(Equal(primary))

		By("verifying all three members are ONLINE")
		Eventually(func(g Gomega) {
			states, err := clusterField(cluster, `{.status.groupReplication.members[*].state}`)
			g.Expect(err).NotTo(HaveOccurred())
			fields := strings.Fields(states)
			g.Expect(fields).To(HaveLen(instances), "the group must report three members")
			for _, state := range fields {
				g.Expect(state).To(Equal("ONLINE"), "every member must be ONLINE")
			}

			roles, err := clusterField(cluster, `{.status.groupReplication.members[*].role}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Count(roles, "PRIMARY")).To(Equal(1), "exactly one PRIMARY")
			g.Expect(strings.Count(roles, "SECONDARY")).To(Equal(2), "two SECONDARYs")

			quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"), "the group must report quorum")
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())
	})

	It("replicates a write from the primary to the secondaries", func() {
		password := appPassword(cluster)

		By("writing to the group's PRIMARY")
		Eventually(func(g Gomega) {
			_, err := mysqlExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS gr_multi (id INT PRIMARY KEY); REPLACE INTO gr_multi VALUES (42);")
			g.Expect(err).NotTo(HaveOccurred(), "the PRIMARY must be writable")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("reading the write back from every secondary")
		for _, secondary := range []string{cluster + "-2", cluster + "-3"} {
			Eventually(func(g Gomega) {
				out, err := mysqlExec(secondary, "app", password, "", "SELECT id FROM app.gr_multi WHERE id = 42;")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("42"), "the write must replicate to %s", secondary)
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		}
	})

	It("rejects writes on a secondary (super_read_only)", func() {
		password := appPassword(cluster)
		_, err := mysqlExec(cluster+"-2", "app", password, "app", "REPLACE INTO gr_multi VALUES (7);")
		Expect(err).To(HaveOccurred(), "a SECONDARY must be read-only")
	})

	It("routes rw to the primary and ro to the online secondaries", func() {
		By("verifying the rw Service routes only to the PRIMARY")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "endpointslice", "-n", testNamespace,
				"-l", "kubernetes.io/service-name="+cluster+"-rw",
				"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(out)).To(ConsistOf(primary), "rw must route only to the PRIMARY")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the ro Service routes to the two ONLINE SECONDARYs")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "endpointslice", "-n", testNamespace,
				"-l", "kubernetes.io/service-name="+cluster+"-ro",
				"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(out)).To(ConsistOf(cluster+"-2", cluster+"-3"),
				"ro must route to both SECONDARYs and never the PRIMARY")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})
})
