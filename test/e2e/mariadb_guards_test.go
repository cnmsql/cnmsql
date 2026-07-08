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

// The MariaDB counterpart of the "Guards" suite: instance fencing must pull a
// fenced Pod out of the routing Services and hold it read-only, then restore it
// when unfenced. Fencing is flavor-agnostic operator behaviour (it edits the
// routing EndpointSlices, not the server), but this pins that a MariaDB cluster
// is fenced and unfenced the same way a MySQL one is. It reuses the fencing
// annotation and rServiceEndpoints helpers from guards_test.go.
var _ = Describe("MariaDB guards", Ordered, Label("flavor", "mariadb"), func() {
	const (
		cluster  = "mdb-guards"
		replicas = 3
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-guards")

		By("creating a 3-instance MariaDB cluster")
		applyManifest(cluster, mariadbBasicClusterManifest(cluster, replicas))
		DeferCleanup(func() {
			deleteManifest(cluster, mariadbBasicClusterManifest(cluster, replicas))
		})
		expectClusterReady(cluster, replicas, e2eTimeout(20*time.Minute))
	})

	It("removes a fenced instance from the read Service and restores it when unfenced", func() {
		primary := clusterPrimary(cluster)
		victim := otherInstance(cluster, replicas, primary)

		By(fmt.Sprintf("confirming %s is initially a routing endpoint of the r Service", victim))
		Eventually(func(g Gomega) {
			g.Expect(rServiceEndpoints(g, cluster)).To(ContainElement(victim))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("fencing replica %s", victim))
		_, err := kubectl("annotate", "pod", victim, "-n", testNamespace,
			fencingAnnotation+"=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the operator records it as fenced and drops it from routing")
		Eventually(func(g Gomega) {
			fenced, err := clusterField(cluster, "{.status.fencedInstances[*]}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(fenced)).To(ContainElement(victim))
			g.Expect(rServiceEndpoints(g, cluster)).NotTo(ContainElement(victim),
				"a fenced instance must not be a routing endpoint")
		}, e2eTimeout(4*time.Minute), 5*time.Second).Should(Succeed())

		By("unfencing the replica")
		_, err = kubectl("annotate", "pod", victim, "-n", testNamespace,
			fencingAnnotation+"-")
		Expect(err).NotTo(HaveOccurred())

		By("verifying it returns to routing once unfenced")
		Eventually(func(g Gomega) {
			g.Expect(rServiceEndpoints(g, cluster)).To(ContainElement(victim))
		}, e2eTimeout(4*time.Minute), 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
