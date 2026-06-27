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

// These specs exercise the M13.3 guards end to end: instance fencing (a fenced
// Pod is pulled out of the routing Services and held read-only) and the deletion
// guard (an accidental delete is blocked until the bypass annotation is set).
var _ = Describe("Guards", Ordered, Label("feature"), func() {
	const (
		cluster  = "guards"
		replicas = 3
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("guards")

		By("creating a 3-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, replicas))
		DeferCleanup(func() {
			deleteManifest(cluster, basicClusterManifest(cluster, replicas))
		})
		expectClusterReady(cluster, replicas, 20*time.Minute)
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

const fencingAnnotation = "cnmsql.cnmsql.co/fencing"

// clusterPrimaryName returns the cluster's bootstrap primary Pod name without
// requiring an elected primary (used while the cluster is terminating).
func clusterPrimaryName(cluster string) string {
	primary, err := clusterField(cluster, "{.status.currentPrimary}")
	if err != nil || strings.TrimSpace(primary) == "" {
		return cluster + "-1"
	}
	return strings.TrimSpace(primary)
}

// rServiceEndpoints returns the instance Pod names currently backing the r
// (any-instance) routing Service.
func rServiceEndpoints(g Gomega, cluster string) []string {
	out, err := kubectl("get", "endpointslice", "-n", testNamespace,
		"-l", "kubernetes.io/service-name="+cluster+"-r",
		"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
	g.Expect(err).NotTo(HaveOccurred())
	return strings.Fields(out)
}

func basicClusterManifest(name string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
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
