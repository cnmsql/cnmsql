//go:build e2e
// +build e2e

package e2e

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec exercises the storage-pressure observability path end to end: each
// instance manager statfs's its data volume and reports the usage in its control
// status, the operator aggregates it across instances, and it surfaces as the
// Cluster's StoragePressure condition.
//
// The condition appearing at all is the proof the whole chain works — the
// operator only sets it once at least one instance has reported its usage
// (StorageObserved), so a present condition means statfs → instance status →
// operator aggregation all ran. We assert the steady-state value (False, below
// the threshold) rather than forcing a fill: Kind's default local-path backend
// does not enforce a per-PVC quota, so statfs sees the whole node disk and a
// volume cannot be driven to the threshold deterministically. The True->False
// edge and event are covered by unit tests.
var _ = Describe("Storage pressure", Ordered, func() {
	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("storagepressure")
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("publishes a StoragePressure condition reflecting per-instance volume usage", func() {
		const cluster = "pressure"

		By("creating a single-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, 1))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 1, 20*time.Minute)

		By("waiting for the operator to report the StoragePressure condition")
		// The condition is only set once an instance has reported its data-volume
		// usage, so its presence proves the statfs → status → aggregation chain ran.
		Eventually(func() (string, error) {
			out, err := clusterField(cluster,
				"{.status.conditions[?(@.type=='StoragePressure')].status}")
			return strings.TrimSpace(out), err
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Equal("False"),
			"StoragePressure condition was never reported as below the threshold")

		By("verifying the condition carries the BelowThreshold reason")
		reason, err := clusterField(cluster,
			"{.status.conditions[?(@.type=='StoragePressure')].reason}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(reason)).To(Equal("BelowThreshold"),
			"a healthy volume must report reason BelowThreshold")
	})
})
