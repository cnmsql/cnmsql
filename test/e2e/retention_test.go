//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec exercises the M8.2 base-backup retention GC. Time-based retention's
// smallest configurable window is one day, which can't be aged through in an
// e2e, so it seeds an artificially-old base backup manifest directly into the
// store alongside a real, recent backup, then asserts the operator expires the
// old one while keeping the recent (newest-floor) backup.
var _ = Describe("Backup retention GC", Ordered, func() {
	const (
		retCluster = "ret-src"
		realBackup = "real-backup"
		oldPrefix  = "ret-src/stale-backup/stale-id"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("retention")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating an archiving cluster with a 1-day retention policy")
		applyManifest(retCluster, retentionClusterManifest(retCluster, "1d"))
		DeferCleanup(func() {
			deleteManifest(retCluster, retentionClusterManifest(retCluster, "1d"))
		})
		expectClusterReady(retCluster, 1, 20*time.Minute)
	})

	It("takes a real (recent) base backup", func() {
		// No DeferCleanup here: a DeferCleanup registered inside an It runs at the
		// end of that It, which would delete the backup before the next spec needs
		// it. The AfterAll namespace teardown removes it instead.
		applyManifest(realBackup, backupManifest(realBackup, retCluster))
		Eventually(func(g Gomega) {
			phase, err := kubectl("get", "backup", realBackup, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).NotTo(Equal("failed"), "real backup failed")
			g.Expect(phase).To(Equal("completed"), "real backup not completed yet")
		}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("expires the stale base backup while keeping the recent one", func() {
		By("seeding an artificially-old base backup manifest into the store")
		oldTime := time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)
		meta := fmt.Sprintf(`{"backupID":"stale-id","clusterName":"%s","backupName":"stale-backup",`+
			`"method":"xtrabackup","archiveKey":"%s/backup.xbstream","sizeBytes":1,`+
			`"startedAt":"%s","completedAt":"%s"}`, retCluster, oldPrefix, oldTime, oldTime)
		mcPipe(meta, fmt.Sprintf("local/%s/%s/metadata.json", minioBucket, oldPrefix))
		mcPipe("stale-archive-bytes", fmt.Sprintf("local/%s/%s/backup.xbstream", minioBucket, oldPrefix))

		By("confirming the stale backup is present before GC")
		Expect(mcObjectExists(fmt.Sprintf("local/%s/%s/metadata.json", minioBucket, oldPrefix))).
			To(BeTrue(), "seeded stale backup should exist before GC")

		By("clearing the retention throttle so the next reconcile runs the pass")
		// The cluster already ran (and stamped) a retention pass when it first went
		// ready, before the stale backup existed. The pass is throttled to 1h, so
		// clear lastRetentionRunTime to let the next resync run it against the now
		// non-empty archive.
		_, err := kubectl("patch", "cluster", retCluster, "-n", testNamespace,
			"--subresource=status", "--type=merge",
			"-p", `{"status":{"lastRetentionRunTime":null}}`)
		Expect(err).NotTo(HaveOccurred())

		By("nudging a reconcile so the retention pass runs promptly")
		clusterAnnotate(retCluster, "cnmsql.co/retention-nudge="+fmt.Sprint(time.Now().Unix()))

		By("verifying the stale backup directory is GC'd")
		Eventually(func(g Gomega) {
			g.Expect(mcObjectExists(fmt.Sprintf("local/%s/%s/metadata.json", minioBucket, oldPrefix))).
				To(BeFalse(), "stale backup metadata should be deleted")
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())

		By("verifying the recent backup survives as the retention floor")
		id, err := kubectl("get", "backup", realBackup, "-n", testNamespace,
			"-o", "jsonpath={.status.backupId}")
		Expect(err).NotTo(HaveOccurred())
		Expect(id).NotTo(BeEmpty())
		out, err := mcExec("mc", "--quiet", "ls", "-r",
			fmt.Sprintf("local/%s/%s/", minioBucket, retCluster))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring(realBackup), "recent backup should still be present")

		By("verifying the cluster recorded a retention run")
		Eventually(func(g Gomega) {
			ts, err := clusterField(retCluster, "{.status.lastRetentionRunTime}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ts).NotTo(BeEmpty(), "lastRetentionRunTime should be stamped")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// mcPipe writes content to the given object key through the mc toolbox pod.
func mcPipe(content, key string) {
	_, err := mcExec("sh", "-c", fmt.Sprintf("printf '%%s' %q | mc --quiet pipe %s", content, key))
	Expect(err).NotTo(HaveOccurred(), "Failed to write object %s", key)
}

// mcObjectExists reports whether an object exists in the store via mc stat.
func mcObjectExists(key string) bool {
	_, err := mcExec("mc", "--quiet", "stat", key)
	return err == nil
}

func retentionClusterManifest(name, policy string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 1
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
    retentionPolicy: %s
%s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, policy, objectStoreYAML("    "))
}
