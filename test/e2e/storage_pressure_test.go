//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

// This spec exercises the storage observability path end to end through both
// surfaces the instance manager exposes its data-volume usage on:
//
//   - the control-API status, which the operator aggregates into the Cluster's
//     StoragePressure condition (the condition is only set once an instance has
//     reported its usage, so its presence proves statfs → instance status →
//     operator aggregation all ran);
//   - the instance's own /metrics endpoint, where the same statfs is published as
//     the mysql_instance_data_volume_* gauges.
//
// We assert the steady-state condition value (False, below the threshold) rather
// than forcing a fill: Kind's default local-path backend does not enforce a
// per-PVC quota, so statfs sees the whole node disk and a volume cannot be driven
// to the threshold deterministically. The True->False edge and event are covered
// by unit tests.
var _ = Describe("Storage pressure", Ordered, Label("feature"), func() {
	const cluster = "pressure"
	instance := cluster + "-1"

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("storagepressure")
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})

		By("creating a single-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, 1))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 1, 20*time.Minute)
	})

	It("publishes a StoragePressure condition reflecting the instance-reported usage", func() {
		By("waiting for the operator to report the StoragePressure condition")
		// The condition is only set once an instance has reported its data-volume
		// usage in its control status, so its presence proves the instance manager
		// status carried the storage block and the operator aggregated it.
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
		// A non-zero capacity with used <= capacity proves the instance manager ran a
		// real statfs of the data directory rather than emitting placeholder zeros.
		capacity, ok := metricValue(body, "mysql_instance_data_volume_capacity_bytes")
		Expect(ok).To(BeTrue(), "capacity gauge missing from:\n%s", body)
		Expect(capacity).To(BeNumerically(">", 0), "data volume capacity must be > 0")
		used, ok := metricValue(body, "mysql_instance_data_volume_used_bytes")
		Expect(ok).To(BeTrue(), "used gauge missing from:\n%s", body)
		Expect(used).To(BeNumerically(">=", 0))
		Expect(used).To(BeNumerically("<=", capacity), "used must not exceed capacity")
	})
})

// scrapeInstanceMetrics fetches an instance's Prometheus metrics from a one-shot
// curl Pod targeting the instance Pod IP. A basic cluster serves metrics over
// plain HTTP on the metrics port (monitoring TLS is opt-in), so no client
// certificate is needed.
func scrapeInstanceMetrics(instance string) string {
	GinkgoHelper()
	podIP, err := kubectl("get", "pod", instance, "-n", testNamespace, "-o", "jsonpath={.status.podIP}")
	Expect(err).NotTo(HaveOccurred(), "failed to read instance Pod IP")
	podIP = strings.TrimSpace(podIP)
	Expect(podIP).NotTo(BeEmpty(), "instance Pod %s has no IP", instance)

	curlPod := instance + "-metrics-scrape"
	DeferCleanup(func() {
		_, _ = kubectl("delete", "pod", curlPod, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})
	cmd := exec.Command("kubectl", "run", curlPod, "--restart=Never",
		"--namespace", testNamespace,
		"--image=curlimages/curl:latest",
		"--overrides",
		fmt.Sprintf(`{
			"spec": {
				"containers": [{
					"name": "curl",
					"image": "curlimages/curl:latest",
					"command": ["/bin/sh", "-c"],
					"args": [
						"for i in $(seq 1 30); do curl -sS http://%s:9187/metrics && exit 0 || sleep 2; done; exit 1"
					],
					"securityContext": {
						"readOnlyRootFilesystem": true,
						"allowPrivilegeEscalation": false,
						"capabilities": {"drop": ["ALL"]},
						"runAsNonRoot": true,
						"runAsUser": 1000,
						"seccompProfile": {"type": "RuntimeDefault"}
					}
				}]
			}
		}`, podIP))
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "failed to create the metrics scrape pod")

	Eventually(func(g Gomega) {
		phase, err := kubectl("get", "pod", curlPod, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(phase).To(Equal("Succeeded"), "scrape pod has not completed yet")
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

	body, err := kubectl("logs", curlPod, "-n", testNamespace)
	Expect(err).NotTo(HaveOccurred())
	return body
}

// metricValue returns the value of an unlabelled Prometheus gauge from a metrics
// exposition body, and whether it was found.
func metricValue(body, name string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			v, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return 0, false
			}
			return v, true
		}
	}
	return 0, false
}
