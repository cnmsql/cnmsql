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

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
