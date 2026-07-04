//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The MariaDB counterpart of the "Backup retention GC" suite. Time-based
// retention's smallest window is one day, which can't be aged through in an e2e,
// so it seeds an artificially-old base backup manifest directly into the store
// alongside a real, recent mariabackup, then asserts the operator expires the old
// one while keeping the recent (newest-floor) backup. Retention GC is
// object-store logic, but this pins that the MariaDB archive layout is GC'd the
// same way.
var _ = Describe("MariaDB backup retention GC", Ordered, Label("feature", "mariadb"), func() {
	const (
		retCluster = "mdb-ret-src"
		realBackup = "mdb-real-backup"
		oldPrefix  = "mdb-ret-src/stale-backup/stale-id"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-retention")

		setupMinio()
		DeferCleanup(teardownMinio)
		setupMC()
		DeferCleanup(teardownMC)

		By("creating a MariaDB archiving cluster with a 1-day retention policy")
		applyManifest(retCluster, mariadbRetentionClusterManifest(retCluster, "1d"))
		DeferCleanup(func() {
			deleteManifest(retCluster, mariadbRetentionClusterManifest(retCluster, "1d"))
		})
		expectClusterReady(retCluster, 1, e2eTimeout(20*time.Minute))
	})

	It("takes a real (recent) base backup", func() {
		// No DeferCleanup here: it would delete the backup before the next spec
		// needs it. The AfterAll namespace teardown removes it instead.
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
			`"method":"xtrabackup","archiveKey":"%s/backup.mbstream","sizeBytes":1,`+
			`"startedAt":"%s","completedAt":"%s"}`, retCluster, oldPrefix, oldTime, oldTime)
		mcPipe(meta, fmt.Sprintf("local/%s/%s/metadata.json", minioBucket, oldPrefix))
		mcPipe("stale-archive-bytes", fmt.Sprintf("local/%s/%s/backup.mbstream", minioBucket, oldPrefix))

		By("confirming the stale backup is present before GC")
		Expect(mcObjectExists(fmt.Sprintf("local/%s/%s/metadata.json", minioBucket, oldPrefix))).
			To(BeTrue(), "seeded stale backup should exist before GC")

		By("clearing the retention throttle so the next reconcile runs the pass")
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

func mariadbRetentionClusterManifest(name, policy string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  flavor: mariadb
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
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters, policy, objectStoreYAML("    "))
}
