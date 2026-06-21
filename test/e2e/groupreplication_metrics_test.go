//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/CloudNative-MySQL/cloudnative-mysql/test/utils"
)

// This spec exercises the M-GR.7 monitoring path: the operator publishes each
// Group Replication cluster's authoritative status on its own /metrics endpoint
// via the controller-runtime registry. With a GR cluster in scope, scraping the
// operator metrics must expose the cnmysql_cluster_gr_* family labelled with the
// cluster, proving the collector rides the existing secure endpoint.
var _ = Describe("Group Replication operator metrics", Ordered, func() {
	const (
		cluster   = "gr-metrics"
		instances = 1
		// A dedicated binding so this spec is independent of the operator-deploy
		// suite that creates the shared metrics binding.
		grMetricsBinding = "cloudnative-mysql-gr-metrics-binding"
		grCurlPod        = "gr-curl-metrics"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("gr-metrics")

		By("creating a single-member Group Replication cluster the operator will report on")
		applyManifest(cluster, grClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteCluster(cluster)
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, 15*time.Minute)

		By("granting the metrics reader role so the scrape is authorized")
		_, _ = kubectl("create", "clusterrolebinding", grMetricsBinding,
			"--clusterrole=cloudnative-mysql-metrics-reader",
			fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName))
		DeferCleanup(func() {
			_, _ = kubectl("delete", "clusterrolebinding", grMetricsBinding, "--ignore-not-found")
			_, _ = kubectl("delete", "pod", grCurlPod, "-n", namespace, "--ignore-not-found", "--wait=false")
		})
	})

	It("exposes the GR quorum and member-state series labelled with the cluster", func() {
		By("waiting for the operator to report a quorum-holding group")
		Eventually(func(g Gomega) {
			quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("obtaining a service account token for the metrics endpoint")
		token, err := serviceAccountToken()
		Expect(err).NotTo(HaveOccurred())
		Expect(token).NotTo(BeEmpty())

		By("scraping the operator metrics endpoint from an in-cluster curl pod")
		cmd := exec.Command("kubectl", "run", grCurlPod, "--restart=Never",
			"--namespace", namespace,
			"--image=curlimages/curl:latest",
			"--overrides",
			fmt.Sprintf(`{
				"spec": {
					"containers": [{
						"name": "curl",
						"image": "curlimages/curl:latest",
						"command": ["/bin/sh", "-c"],
						"args": [
							"for i in $(seq 1 30); do curl -sS -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
						],
						"securityContext": {
							"readOnlyRootFilesystem": true,
							"allowPrivilegeEscalation": false,
							"capabilities": {"drop": ["ALL"]},
							"runAsNonRoot": true,
							"runAsUser": 1000,
							"seccompProfile": {"type": "RuntimeDefault"}
						}
					}],
					"serviceAccountName": "%s"
				}
			}`, token, metricsServiceName, namespace, serviceAccountName))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "failed to create the GR curl-metrics pod")

		By("waiting for the scrape pod to complete")
		Eventually(func(g Gomega) {
			phase, err := kubectl("get", "pod", grCurlPod, "-n", namespace, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Succeeded"), "scrape pod has not completed yet")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("asserting the GR metric families are present and labelled with the cluster")
		out, err := kubectl("logs", grCurlPod, "-n", namespace)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("cnmysql_cluster_gr_has_quorum"),
			"the quorum gauge must be exposed on the operator endpoint")
		Expect(out).To(ContainSubstring("cnmysql_cluster_gr_members"),
			"the per-state member gauge must be exposed")
		Expect(out).To(ContainSubstring(`cluster="`+cluster+`"`),
			"the GR series must be labelled with the cluster name")
		Expect(out).To(ContainSubstring(`cnmysql_cluster_gr_has_quorum{cluster="`+cluster+`",namespace="`+ns+`"} 1`),
			"the operator must report quorum=1 for the ONLINE single-member group")
	})
})
