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

// The binlog archive goes to its own bucket (continuousArchiving.objectStore)
// while base backups stay in the suite's bucket. PITR from the Backup CR and
// raw-S3 recovery (externalClusters with binlogObjectStore) must both find the
// archive there, and nothing may land under binlogs/ in the base bucket.
var _ = Describe("Separate binlog archive store", Ordered, Label("feature"), func() {
	const (
		sourceCluster = "sep-src"
		fromBackup    = "sep-pitr"
		fromRawS3     = "sep-raw"
		backupName    = "sep-base"
		binlogBucket  = "cnmsql-binlogs"
	)
	version := archiveVersions()[0]

	var (
		password   string
		targetGTID string
		ns, prevNS string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("sepstore")
		setupObjectStore()
		DeferCleanup(teardownObjectStore)
		setupS3Client()
		DeferCleanup(teardownS3Client)

		By("creating the binlog bucket")
		out, err := rcloneExec("mkdir", fmt.Sprintf("%s:%s", s3Remote, binlogBucket))
		Expect(err).NotTo(HaveOccurred(), "Failed to create the binlog bucket: %s", out)

		By("creating the source cluster archiving to the binlog bucket")
		applyManifest(sourceCluster, separateStoreClusterManifest(sourceCluster, version, binlogBucket))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, separateStoreClusterManifest(sourceCluster, version, binlogBucket))
		})
		expectClusterReady(sourceCluster, 1, 20*time.Minute)
		password = appPassword(sourceCluster)
	})

	It("archives to the binlog bucket and recovers from it", func() {
		primary := clusterPrimary(sourceCluster)

		By("taking a base backup")
		applyManifest(backupName, backupManifest(backupName, sourceCluster))
		DeferCleanup(func() { deleteManifest(backupName, backupManifest(backupName, sourceCluster)) })
		expectBackupCompleted(backupName, 8*time.Minute)
		recorded, err := kubectl("get", "backup", backupName, "-n", testNamespace,
			"-o", "jsonpath={.status.binlogObjectStore.bucket}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(recorded)).To(Equal(binlogBucket), "Backup did not record the archive store")

		By("writing the target row and waiting for the archive to cover it")
		_, err = mysqlExec(primary, "app", password, "app",
			"CREATE TABLE ledger (id INT PRIMARY KEY, note VARCHAR(32)); INSERT INTO ledger VALUES (1, 'target');")
		Expect(err).NotTo(HaveOccurred())
		targetGTID = flushBinaryLogs(sourceCluster, primary, password)
		expectArchiveCoversIn(binlogBucket, sourceCluster, targetGTID, 5*time.Minute)

		By("writing a row past the target")
		_, err = mysqlExec(primary, "app", password, "app", "INSERT INTO ledger VALUES (2, 'past-target');")
		Expect(err).NotTo(HaveOccurred())
		flushBinaryLogs(sourceCluster, primary, password)

		By("checking nothing was archived to the base bucket")
		listing, err := rcloneExec("lsf", "-R", objectKey("%s/", sourceCluster))
		Expect(err).NotTo(HaveOccurred(), "Failed to list the base bucket: %s", listing)
		Expect(listing).To(ContainSubstring("metadata.json"), "base backup missing from the base bucket")
		for _, line := range strings.Split(listing, "\n") {
			Expect(line).NotTo(HavePrefix("binlogs/"), "binlogs landed in the base bucket")
		}

		for _, tc := range []struct{ name, manifest string }{
			{fromBackup, pitrRecoveryClusterManifest(fromBackup, version, backupName, targetGTID)},
			{fromRawS3, separateStoreRawRecoveryManifest(fromRawS3, version, sourceCluster, binlogBucket, targetGTID)},
		} {
			By("recovering " + tc.name + " to the target GTID")
			applyManifest(tc.name, tc.manifest)
			DeferCleanup(func() { deleteManifest(tc.name, tc.manifest) })
			expectClusterReady(tc.name, 1, 20*time.Minute)
			restored := clusterPrimary(tc.name)
			Eventually(func(g Gomega) {
				out, err := mysqlExec(restored, "app", password, "app", "SELECT note FROM ledger WHERE id = 1;")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("target"))
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
			out, err := mysqlExec(restored, "app", password, "app", "SELECT COUNT(*) FROM ledger WHERE id = 2;")
			Expect(err).NotTo(HaveOccurred())
			Expect(parseSingleValue(out)).To(Equal("0"), "%s contains a write past the target", tc.name)
		}
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// separateStoreClusterManifest renders an archiving Cluster whose base backups
// go to the suite's bucket and whose binlog archive goes to binlogBucket.
func separateStoreClusterManifest(name, version, binlogBucket string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageName: %[3]s
  storage:
    size: 2Gi
%[4]s
  mysql:
    binlogFormat: ROW
%[5]s
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
%[6]s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
%[7]s
`, name, testNamespace, instanceImageFor(version), e2eInstanceResources, e2eMySQLParameters,
		objectStoreYAML("    "), objectStoreYAMLFor("      ", binlogBucket))
}

// separateStoreRawRecoveryManifest renders a Cluster recovering straight from
// the object store, base backups from the suite's bucket and binlogs from
// binlogBucket.
func separateStoreRawRecoveryManifest(name, version, source, binlogBucket, targetGTID string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageName: %[3]s
  storage:
    size: 2Gi
%[4]s
  mysql:
    binlogFormat: ROW
%[5]s
  bootstrap:
    recovery:
      source: %[6]s
      recoveryTarget:
        targetGTID: "%[7]s"
  externalClusters:
    - name: %[6]s
%[8]s
%[9]s
`, name, testNamespace, instanceImageFor(version), e2eInstanceResources, e2eMySQLParameters,
		source, targetGTID, objectStoreYAML("      "),
		strings.Replace(objectStoreYAMLFor("      ", binlogBucket), "objectStore:", "binlogObjectStore:", 1))
}
