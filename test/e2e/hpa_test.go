//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs exercise the Horizontal Pod Autoscaler integration end to end. A
// HPA scales a target by writing the scale sub-resource's spec.replicas, which
// the CRD maps to spec.instances (specpath=.spec.instances), and reads back
// status.replicas (statuspath=.status.instances) plus the selector to gather
// per-Pod metrics.
//
// `kubectl scale` writes through that exact scale sub-resource path, so it
// reproduces what a HPA does after it computes a target from metrics — without
// needing the HPA controller or metrics-server installed. The spec confirms the
// operator honors the replica write (provisions/removes an instance) and that
// the scale status reflects the converged count.
var _ = Describe("Horizontal Pod Autoscaler", Ordered, Label("feature"), func() {
	const (
		cluster = "hpa"
		initial = 1
		scaled  = 2
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("hpa")

		By("creating a single-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, initial))
		DeferCleanup(func() {
			deleteManifest(cluster, basicClusterManifest(cluster, initial))
		})
		expectClusterReady(cluster, initial, 15*time.Minute)
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

		expectClusterReady(cluster, scaled, 15*time.Minute)

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
		expectClusterReady(cluster, initial, 10*time.Minute)
	})

	// A failover can hand the primary to the highest ordinal just as a scale-down
	// lowers spec.instances. Scale-down never removes a primary, so the operator
	// must switch the role back in range before it can drop the instance. The
	// primary is put on the top ordinal deliberately here, with a switchover, to
	// reproduce that state without relying on a racing failover.
	It("moves a primary above the desired count back in range to finish a scale-down", func() {
		top := fmt.Sprintf("%s-%d", cluster, scaled)
		bottom := fmt.Sprintf("%s-%d", cluster, initial)

		By(fmt.Sprintf("scaling up to %d instances", scaled))
		_, err := kubectl("scale", "cluster", cluster, "-n", testNamespace, fmt.Sprintf("--replicas=%d", scaled))
		Expect(err).NotTo(HaveOccurred())
		expectClusterReady(cluster, scaled, 15*time.Minute)

		By("moving the primary onto the top ordinal " + top)
		requestSwitchoverIn(testNamespace, cluster, top)
		Eventually(func(g Gomega) {
			primary, err := clusterField(cluster, "{.status.currentPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(primary).To(Equal(top))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterReady(cluster, scaled, 5*time.Minute)

		By(fmt.Sprintf("scaling down to %d, below the primary's ordinal", initial))
		_, err = kubectl("scale", "cluster", cluster, "-n", testNamespace, fmt.Sprintf("--replicas=%d", initial))
		Expect(err).NotTo(HaveOccurred())
		expectClusterReady(cluster, initial, 10*time.Minute)

		By("verifying the primary moved back in range and the top instance is gone")
		primary, err := clusterField(cluster, "{.status.currentPrimary}")
		Expect(err).NotTo(HaveOccurred())
		Expect(primary).To(Equal(bottom), "the primary must be handed back to an instance within the desired count")
		Eventually(func() []string {
			return clusterPods(cluster)
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(ConsistOf(bottom),
			"scale-down must remove the former primary once the role has moved")
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
