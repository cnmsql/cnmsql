//go:build e2e
// +build e2e

package e2e

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The MariaDB counterpart of the "Storage pressure" suite. It asserts the same
// two surfaces the instance manager exposes data-volume usage on — the aggregated
// StoragePressure Cluster condition and the mysql_instance_data_volume_* gauges on
// the instance /metrics endpoint — for a MariaDB cluster. The statfs path lives in
// the instance manager, which is flavor-agnostic, so the metric names are shared;
// this pins that a MariaDB instance reports its volume the same way. As in the
// MySQL spec we assert the steady-state (False, below threshold) rather than
// forcing a fill: Kind's local-path backend does not enforce a per-PVC quota.
var _ = Describe("MariaDB storage pressure", Ordered, Label("flavor", "mariadb"), func() {
	const cluster = "mdb-pressure"
	instance := cluster + "-1"

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-storagepressure")
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})

		By("creating a single-instance MariaDB cluster")
		applyManifest(cluster, mariadbBasicClusterManifest(cluster, 1))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 1, e2eTimeout(20*time.Minute))
	})

	It("publishes a StoragePressure condition reflecting the instance-reported usage", func() {
		By("waiting for the operator to report the StoragePressure condition")
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

	It("exposes the data-volume usage gauges on the instance metrics endpoint", func() {
		By("scraping the instance /metrics endpoint")
		body := scrapeInstanceMetrics(instance)

		By("asserting the volume gauges are present and the scrape succeeded")
		Expect(body).To(ContainSubstring("mysql_instance_data_volume_used_bytes"),
			"the used-bytes gauge must be exposed")
		Expect(body).To(ContainSubstring("mysql_instance_data_volume_available_bytes"),
			"the available-bytes gauge must be exposed")
		Expect(body).To(ContainSubstring("mysql_instance_data_volume_scrape_error 0"),
			"the instance manager statfs of the data volume must have succeeded")

		By("verifying the reported capacity and usage are sane")
		capacity, ok := metricValue(body, "mysql_instance_data_volume_capacity_bytes")
		Expect(ok).To(BeTrue(), "capacity gauge missing from:\n%s", body)
		Expect(capacity).To(BeNumerically(">", 0), "data volume capacity must be > 0")
		used, ok := metricValue(body, "mysql_instance_data_volume_used_bytes")
		Expect(ok).To(BeTrue(), "used gauge missing from:\n%s", body)
		Expect(used).To(BeNumerically(">=", 0))
		Expect(used).To(BeNumerically("<=", capacity), "used must not exceed capacity")
	})
})
