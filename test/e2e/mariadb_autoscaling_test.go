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

// The MariaDB counterpart of the "Horizontal Pod Autoscaler" suite: a scale
// sub-resource replica write must drive spec.instances and the converged count
// must be reflected back, on a MariaDB cluster.
var _ = Describe("MariaDB Horizontal Pod Autoscaler", Ordered, Label("feature", "mariadb"), func() {
	const (
		cluster = "mdb-hpa"
		initial = 1
		scaled  = 2
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-hpa")

		By("creating a single-instance MariaDB cluster")
		applyManifest(cluster, mariadbBasicClusterManifest(cluster, initial))
		DeferCleanup(func() {
			deleteManifest(cluster, mariadbBasicClusterManifest(cluster, initial))
		})
		expectClusterReady(cluster, initial, e2eTimeout(15*time.Minute))
	})

	It("scales the cluster through writes to the scale sub-resource replicas", func() {
		By(fmt.Sprintf("scaling up to %d instances via the scale sub-resource", scaled))
		Eventually(func() error {
			_, err := kubectl("scale", "cluster", cluster, "-n", testNamespace,
				fmt.Sprintf("--replicas=%d", scaled))
			return err
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("confirming the scale write reached spec.instances")
		Eventually(func(g Gomega) {
			instances, err := clusterField(cluster, "{.spec.instances}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(instances).To(Equal(fmt.Sprintf("%d", scaled)),
				"a scale sub-resource replica write must drive spec.instances")
		}, e2eTimeout(1*time.Minute), 5*time.Second).Should(Succeed())

		expectClusterReady(cluster, scaled, e2eTimeout(15*time.Minute))

		By("confirming the scale sub-resource reports the converged replica count")
		Eventually(func(g Gomega) {
			statusReplicas, err := kubectl("get", "cluster", cluster, "-n", testNamespace,
				"--subresource=scale", "-o", "jsonpath={.status.replicas}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(statusReplicas).To(Equal(fmt.Sprintf("%d", scaled)),
				"scale status.replicas must mirror the ready instance count an HPA reads back")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("scaling back down to %d via the scale sub-resource", initial))
		Eventually(func() error {
			_, err := kubectl("scale", "cluster", cluster, "-n", testNamespace,
				fmt.Sprintf("--replicas=%d", initial))
			return err
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterReady(cluster, initial, e2eTimeout(10*time.Minute))
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// The MariaDB counterpart of the "Vertical Pod Autoscaler" suite: the operator
// publishes a scale selector that resolves to the instance Pods and must not
// fight an in-place (VPA) resize of a running MariaDB Pod's requests.
var _ = Describe("MariaDB Vertical Pod Autoscaler", Ordered, Label("feature", "mariadb"), func() {
	const (
		cluster  = "mdb-vpa"
		replicas = 1
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-vpa")

		By("creating a single-instance MariaDB cluster")
		applyManifest(cluster, mariadbBasicClusterManifest(cluster, replicas))
		DeferCleanup(func() {
			deleteManifest(cluster, mariadbBasicClusterManifest(cluster, replicas))
		})
		expectClusterReady(cluster, replicas, e2eTimeout(15*time.Minute))
	})

	It("publishes a scale sub-resource selector that resolves to the instance Pods", func() {
		var selector string
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "cluster", cluster, "-n", testNamespace,
				"--subresource=scale", "-o", "jsonpath={.status.selector}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty(), "scale sub-resource must publish a selector")
			selector = out
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("confirming the scale replicas mirror spec.instances")
		specReplicas, err := kubectl("get", "cluster", cluster, "-n", testNamespace,
			"--subresource=scale", "-o", "jsonpath={.spec.replicas}")
		Expect(err).NotTo(HaveOccurred())
		Expect(specReplicas).To(Equal(fmt.Sprintf("%d", replicas)),
			"scale spec.replicas must equal the requested instance count")

		By(fmt.Sprintf("confirming the selector %q resolves to exactly the instance Pods", selector))
		pods, err := kubectl("get", "pods", "-n", testNamespace, "-l", selector,
			"-o", "jsonpath={.items[*].metadata.name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Fields(pods)).To(HaveLen(replicas),
			"the published selector must match the cluster's instance Pods and nothing else")
	})

	It("does not roll a Pod whose requests were changed by an in-place (VPA) resize", func() {
		primary := clusterPrimary(cluster)

		uid, err := kubectl("get", "pod", primary, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
		Expect(err).NotTo(HaveOccurred())
		Expect(uid).NotTo(BeEmpty())

		By("simulating a VPA in-place resize of the mysql container's CPU request")
		resize := `{"spec":{"containers":[{"name":"mysql","resources":{"requests":{"cpu":"300m"}}}]}}`
		_, err = kubectl("patch", "pod", primary, "-n", testNamespace,
			"--subresource=resize", "--patch", resize)
		Expect(err).NotTo(HaveOccurred(), "in-place Pod resize should be accepted")

		cpuRequest := "{.spec.containers[?(@.name=='mysql')].resources.requests.cpu}"
		By("confirming the resized request is applied to the running Pod")
		Eventually(func(g Gomega) {
			cpu, err := kubectl("get", "pod", primary, "-n", testNamespace, "-o", "jsonpath="+cpuRequest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cpu).To(Equal("300m"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the operator leaves the resized Pod in place over repeated reconciles")
		Consistently(func(g Gomega) {
			gotUID, err := kubectl("get", "pod", primary, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(gotUID).To(Equal(uid), "operator must not delete/recreate a VPA-resized Pod")

			cpu, err := kubectl("get", "pod", primary, "-n", testNamespace, "-o", "jsonpath="+cpuRequest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cpu).To(Equal("300m"), "operator must not reset the VPA-applied request to spec.resources")
		}, e2eTimeout(90*time.Second), 10*time.Second).Should(Succeed())

		By("confirming the cluster is still Ready after the resize")
		expectClusterReady(cluster, replicas, e2eTimeout(5*time.Minute))
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
