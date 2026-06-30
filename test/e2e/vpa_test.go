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

// These specs exercise the Vertical Pod Autoscaler integration end to end. The
// operator does not run a VPA itself; it makes a Cluster a valid VPA target by
// publishing a scale sub-resource whose selectorpath (.status.labelSelector)
// resolves to the instance Pods, and by never reconciling over the resource
// values a VPA writes onto a running Pod.
//
// The first spec pins the discovery contract (the scale selector matches the
// instance Pods). The second simulates what a VPA in "InPlaceOrRecreate" mode
// does — an in-place resize of a running Pod's requests via the resize
// sub-resource — and asserts the operator leaves that Pod alone instead of
// rolling it back to spec.resources, which would fight the autoscaler into an
// eviction loop.
var _ = Describe("Vertical Pod Autoscaler", Ordered, Label("feature"), func() {
	const (
		cluster  = "vpa"
		replicas = 1
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("vpa")

		By("creating a single-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, replicas))
		DeferCleanup(func() {
			deleteManifest(cluster, basicClusterManifest(cluster, replicas))
		})
		expectClusterReady(cluster, replicas, 15*time.Minute)
	})

	It("publishes a scale sub-resource selector that resolves to the instance Pods", func() {
		// A VPA discovers the Pods to resize through the scale sub-resource's
		// status.selector (mapped from .status.labelSelector). It must be
		// populated and must select exactly this cluster's instance Pods.
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

		// The Pod UID changes if the operator deletes and recreates the Pod, so
		// it is the signal for a roll. Capture it before the resize.
		uid, err := kubectl("get", "pod", primary, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
		Expect(err).NotTo(HaveOccurred())
		Expect(uid).NotTo(BeEmpty())

		By("simulating a VPA in-place resize of the mysql container's CPU request")
		// CPU is resized without a restart by default, so this isolates the
		// "operator must not fight the resize" behavior from container churn. The
		// new request (300m) stays within the manifest's 1-CPU limit. Strategic
		// merge keeps the untouched memory request intact.
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
		expectClusterReady(cluster, replicas, 5*time.Minute)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
