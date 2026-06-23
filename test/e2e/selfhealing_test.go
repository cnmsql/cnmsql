//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs exercise the M13.2 self-healing surface end to end against a real
// cluster: semi-sync data durability (the operator lowers the primary's
// acknowledgement count to stay writable when a synchronous replica is lost,
// then restores it) and the liveness isolation guard (a healthy instance keeps
// its API-server contact fresh and is never spuriously restarted).
var _ = Describe("Self-healing", Ordered, func() {
	const (
		cluster  = "selfheal"
		minSync  = 2
		maxSync  = 2
		replicas = 3
	)

	var ns, prevNS, rootPass string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("selfheal")

		By("creating a 3-instance cluster with semi-sync (minSyncReplicas=2, preferred)")
		applyManifest(cluster, semiSyncClusterManifest(cluster, replicas, minSync, maxSync, "preferred"))
		DeferCleanup(func() {
			deleteManifest(cluster, semiSyncClusterManifest(cluster, replicas, minSync, maxSync, "preferred"))
		})
		expectClusterReady(cluster, replicas, 20*time.Minute)
		rootPass = secretPassword(cluster + "-root")
	})

	It("enforces the configured semi-sync acknowledgement count when all replicas are healthy", func() {
		primary := clusterPrimary(cluster)
		By("verifying the primary waits for minSyncReplicas acknowledgements")
		Eventually(func(g Gomega) {
			g.Expect(semiSyncWaitCount(g, primary, rootPass)).To(Equal(minSync),
				"steady-state wait count must equal minSyncReplicas")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("never spuriously restarts a healthy instance (liveness isolation stays green)", func() {
		By("recording the current restart counts of every instance")
		before := map[string]int{}
		for i := 1; i <= replicas; i++ {
			pod := fmt.Sprintf("%s-%d", cluster, i)
			before[pod] = podRestartCount(pod)
		}

		// The isolation detector fails liveness after 30s without API-server
		// contact. Hold steady well past that window plus several probe intervals;
		// a healthy instance must keep contact fresh and never be restarted.
		By("holding for 90s and confirming no instance container restarted")
		Consistently(func(g Gomega) {
			for pod, was := range before {
				g.Expect(podRestartCount(pod)).To(Equal(was),
					"instance %s restarted while healthy (false isolation)", pod)
			}
		}, e2eTimeout(90*time.Second), 10*time.Second).Should(Succeed())
	})

	It("lowers the acknowledgement count to stay writable when a sync replica is fenced, then restores it", func() {
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, replicas, primary)

		By(fmt.Sprintf("fencing replica %s to drop below minSyncReplicas healthy replicas", replica))
		_, err := kubectl("annotate", "pod", replica, "-n", testNamespace,
			fencingAnnotation+"=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred(), "failed to fence replica")

		By("verifying the operator self-heals the wait count below minSyncReplicas (preferred)")
		Eventually(func(g Gomega) {
			g.Expect(semiSyncWaitCount(g, primary, rootPass)).To(BeNumerically("<", minSync),
				"preferred durability must lower the acknowledgement count while a replica is fenced")
		}, e2eTimeout(5*time.Minute), 2*time.Second).Should(Succeed())

		By("verifying the primary stays writable during the degraded window")
		_, err = mysqlExec(primary, "root", rootPass, "",
			"CREATE DATABASE IF NOT EXISTS selfheal_probe; "+
				"CREATE TABLE IF NOT EXISTS selfheal_probe.t (id INT PRIMARY KEY); "+
				"REPLACE INTO selfheal_probe.t VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "primary must accept writes while a sync replica is fenced")

		By("unfencing the replica")
		_, err = kubectl("annotate", "pod", replica, "-n", testNamespace, fencingAnnotation+"-")
		Expect(err).NotTo(HaveOccurred(), "failed to unfence replica")

		By("verifying the wait count is restored to minSyncReplicas once replicas recover")
		Eventually(func(g Gomega) {
			g.Expect(semiSyncWaitCount(g, clusterPrimary(cluster), rootPass)).To(Equal(minSync),
				"wait count must return to minSyncReplicas after recovery")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// semiSyncWaitCount reads the primary's semi-sync acknowledgement count. The
// variable name differs by version (source/replica vs master/slave naming), so
// it matches both with a LIKE pattern and parses the value column.
func semiSyncWaitCount(g Gomega, pod, rootPass string) int {
	out, err := mysqlExec(pod, "root", rootPass, "",
		"SHOW VARIABLES LIKE 'rpl_semi_sync%wait_for%count';")
	g.Expect(err).NotTo(HaveOccurred())
	fields := strings.Fields(out)
	g.Expect(len(fields)).To(BeNumerically(">=", 2),
		"semi-sync wait count variable not found in %q", out)
	count, err := strconv.Atoi(fields[len(fields)-1])
	g.Expect(err).NotTo(HaveOccurred(), "could not parse wait count from %q", out)
	return count
}

// podRestartCount returns the restart count of an instance Pod's mysql
// container, or 0 if it cannot be read yet.
func podRestartCount(pod string) int {
	out, err := kubectl("get", "pod", pod, "-n", testNamespace,
		"-o", "jsonpath={.status.containerStatuses[?(@.name=='mysql')].restartCount}")
	if err != nil {
		return 0
	}
	count, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0
	}
	return count
}

// waitForPodReady blocks until the named Pod exists and its Ready condition is
// True, or the timeout elapses. Use it after a force-delete to wait for the
// StatefulSet to recreate and ready the Pod before exec'ing into it.
func waitForPodReady(pod string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		out, err := kubectl("get", "pod", pod, "-n", testNamespace,
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
		g.Expect(err).NotTo(HaveOccurred(), "Pod %s not found yet", pod)
		g.Expect(strings.TrimSpace(out)).To(Equal("True"), "Pod %s not Ready yet", pod)
	}, e2eTimeout(timeout), 5*time.Second).Should(Succeed())
}

// otherInstance returns an instance Pod name that is not the primary.
func otherInstance(cluster string, instances int, primary string) string {
	for i := 1; i <= instances; i++ {
		name := fmt.Sprintf("%s-%d", cluster, i)
		if name != primary {
			return name
		}
	}
	Fail("no non-primary instance found")
	return ""
}

func semiSyncClusterManifest(name string, instances, minSync, maxSync int, durability string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  minSyncReplicas: %[5]d
  maxSyncReplicas: %[6]d
  storage:
    size: 2Gi
%[7]s
  mysql:
    binlogFormat: ROW
%[8]s
    semiSync:
      enabled: true
      timeoutMillis: 10000
      dataDurability: %[9]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instances, instanceImage, minSync, maxSync,
		e2eInstanceResources, e2eMySQLParameters, durability)
}
