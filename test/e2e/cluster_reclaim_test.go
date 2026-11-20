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

// This spec exercises spec.backup.reclaimPolicy: Delete on a Cluster: deleting
// the Cluster removes its whole archive prefix, and nothing writes under it
// afterwards. The operator used to wipe the prefix while the instances still
// ran (the garbage collector removes them only once the Cluster is gone), and
// an instance archives its last binary log as it shuts down, so objects came
// back under the removed prefix. Two instances matter: with a replica streaming
// from it, the primary's preStop hook waits up to 25s for a demotion that a
// deleted Cluster never sends, and it keeps archiving all that time.
var _ = Describe("Cluster archive reclaim", Ordered, Label("flavor"), func() {
	const reclaimCluster = "reclaim-src"

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("cluster-reclaim")

		setupObjectStore()
		DeferCleanup(teardownObjectStore)
		setupS3Client()
		DeferCleanup(teardownS3Client)
	})

	It("leaves nothing under the cluster's prefix once the Cluster is deleted", func() {
		prefix := objectKey("%s/", reclaimCluster)

		By("creating a cluster that archives with reclaimPolicy: Delete")
		applyManifest(reclaimCluster, reclaimClusterManifest(reclaimCluster))
		expectClusterReady(reclaimCluster, 2, 20*time.Minute)

		By("waiting for archived binary logs to reach the store")
		Eventually(func(g Gomega) {
			g.Expect(listPrefix(g, prefix+"binlogs/")).NotTo(BeEmpty(), "no archived binary log yet")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("writing continuously until the instance stops")
		// The instance archives what it has not uploaded yet as it shuts down;
		// an idle cluster has nothing left to upload and would hide the race.
		pod, err := kubectl("get", "cluster", reclaimCluster, "-n", testNamespace,
			"-o", "jsonpath={.status.currentPrimary}")
		Expect(err).NotTo(HaveOccurred())
		Expect(pod).NotTo(BeEmpty(), "cluster reports no primary")
		pass := rootPassword(reclaimCluster)
		_, err = mysqlExec(pod, "root", pass, "app", "CREATE TABLE ticks (id INT AUTO_INCREMENT PRIMARY KEY)")
		Expect(err).NotTo(HaveOccurred())
		_, err = kubectl("exec", pod, "-n", testNamespace, "-c", "mysql", "--", "env", "MYSQL_PWD="+pass,
			"bash", "-c", `setsid bash -c 'while mysql -uroot app -e "INSERT INTO ticks VALUES ()"; do sleep 0.2; done' >/dev/null 2>&1 < /dev/null &`)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the Cluster")
		_, err = kubectl("delete", "cluster", reclaimCluster, "-n", testNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			_, err := kubectl("get", "cluster", reclaimCluster, "-n", testNamespace)
			g.Expect(err).To(HaveOccurred(), "Cluster should be gone once its archive is reclaimed")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the prefix is empty and stays empty")
		// An instance that outlived the wipe re-uploads within seconds of
		// stopping; watch well past that.
		Consistently(func(g Gomega) {
			g.Expect(listPrefix(g, prefix)).To(BeEmpty(), "objects written under the reclaimed prefix")
		}, 90*time.Second, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// listPrefix lists every object under prefix (a toolbox-addressed key). A failed
// listing fails g rather than reading as empty, so an unreachable store never
// passes an emptiness check.
func listPrefix(g Gomega, prefix string) []string {
	out, err := rcloneExec("lsf", "-R", "--files-only", prefix)
	if err != nil && strings.Contains(out, "directory not found") {
		return nil
	}
	g.Expect(err).NotTo(HaveOccurred(), "listing %s: %s", prefix, out)
	var keys []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line != "" {
			keys = append(keys, line)
		}
	}
	return keys
}

func reclaimClusterManifest(name string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 2
  imageName: %s
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
    reclaimPolicy: Delete
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
%s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}
