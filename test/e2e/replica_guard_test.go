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

// These specs exercise the replica-creation guard end to end: a replica is never
// provisioned while the primary it would clone from is not OK. This holds both
// at initial bootstrap (replicas wait for the bootstrap primary) and when scaling
// up an existing cluster (a new replica waits for an unavailable primary to
// recover). Cloning from a primary that is unreachable or not acting as primary
// would fail, or seed the replica from a primary about to be failed over.
//
// Serial: these specs deliberately make a primary unavailable (deleting its Pod)
// and drive scheduling churn on the node. Run in isolation so the CPU/IO spikes
// of a recovering primary never starve a timing-sensitive spec running in
// parallel (e.g. the in-place manager re-exec, where an over-long control-API
// outage would otherwise trip automatic failover).
var _ = Describe("Replica creation guard", Serial, Ordered, func() {
	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("replicaguard")
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("does not create replica Pods until the bootstrap primary is ready", func() {
		const (
			cluster   = "bootstrap-guard"
			instances = 2
		)
		primary := cluster + "-1"
		replica := cluster + "-2"

		By("creating a fresh 2-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})

		By("verifying the replica is not created while the primary is still bootstrapping")
		sawPrimaryBootstrapping := false
		deadline := time.Now().Add(e2eTimeout(6 * time.Minute))
		for time.Now().Before(deadline) {
			// The primary is OK once its Pod is Ready and it has been elected primary.
			if instancePodReadyE2E(primary) && clusterPrimaryName(cluster) == primary {
				break
			}
			sawPrimaryBootstrapping = true
			Expect(podExists(replica)).To(BeFalse(),
				"replica %s must not be created before the bootstrap primary %s is ready", replica, primary)
			time.Sleep(3 * time.Second)
		}
		Expect(sawPrimaryBootstrapping).To(BeTrue(),
			"primary became ready before the guard could be observed; the spec did not exercise the bootstrap window")

		By("verifying the cluster converges once the primary is ready")
		expectClusterReady(cluster, instances, 20*time.Minute)
	})

	It("does not create a new replica while the primary is unavailable", func() {
		const cluster = "scaleup-guard"
		replica := cluster + "-2"

		By("creating a single-instance cluster and waiting for it to be ready")
		applyManifest(cluster, basicClusterManifest(cluster, 1))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 1, 20*time.Minute)

		primary := clusterPrimary(cluster)

		By(fmt.Sprintf("making the primary unavailable by deleting its Pod %s", primary))
		_, err := kubectl("delete", "pod", primary, "-n", testNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred(), "failed to delete the primary Pod")

		By("requesting scale-up to 2 instances while the primary is down")
		scaleInstances(cluster, 2)

		By("waiting for the primary to actually become unavailable")
		// kubectl delete --wait=false returns before the kubelet has updated the
		// Pod's Ready condition, so the just-deleted primary still reports Ready=True
		// for a moment. Observe it actually go down before checking the guard,
		// otherwise the guard loop below would see the stale Ready=True and exit
		// immediately without ever exercising the unavailable window.
		Eventually(func() bool {
			return instancePodReadyE2E(primary)
		}, e2eTimeout(2*time.Minute), 2*time.Second).Should(BeFalse(),
			"primary %s never became unavailable after its Pod was deleted", primary)

		By("verifying the new replica is not created while the primary is not ready")
		sawPrimaryDown := false
		deadline := time.Now().Add(e2eTimeout(6 * time.Minute))
		for time.Now().Before(deadline) {
			if instancePodReadyE2E(primary) {
				break
			}
			sawPrimaryDown = true
			Expect(podExists(replica)).To(BeFalse(),
				"replica %s must not be created while the primary %s is unavailable", replica, primary)
			time.Sleep(3 * time.Second)
		}
		Expect(sawPrimaryDown).To(BeTrue(),
			"primary recovered before the guard could be observed; the spec did not exercise the unavailable window")

		By("verifying the replica is created and the cluster converges once the primary recovers")
		expectClusterReady(cluster, 2, 20*time.Minute)
	})
})

// scaleInstances patches a Cluster's spec.instances to request a different
// number of instances.
func scaleInstances(name string, instances int) {
	GinkgoHelper()
	Eventually(func() error {
		out, err := kubectl("patch", "cluster", name, "-n", testNamespace,
			"--type=merge", "-p", fmt.Sprintf(`{"spec":{"instances":%d}}`, instances))
		if err != nil && isTransientWebhookError(out, err) {
			return err
		}
		if err != nil {
			StopTrying("scale rejected").Wrap(err).Now()
		}
		return nil
	}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed(),
		"failed to scale cluster %s to %d instances", name, instances)
}

// podExists reports whether a Pod with the given name is present. A read error
// (including NotFound) is treated as absent.
func podExists(name string) bool {
	_, err := kubectl("get", "pod", name, "-n", testNamespace, "-o", "name")
	return err == nil
}

// instancePodReadyE2E reports whether the named Pod currently reports Ready.
func instancePodReadyE2E(name string) bool {
	out, err := kubectl("get", "pod", name, "-n", testNamespace,
		"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
	if err != nil {
		return false
	}
	return strings.TrimSpace(out) == "True"
}
