//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs exercise physical backups to object storage, bootstrapping a new
// cluster by recovering one of those backups, and the safety guard that keeps a
// fresh cluster from overwriting a non-empty backup destination. They stand up a
// single-node MinIO inside the test cluster to act as the S3-compatible store.
var _ = Describe("Physical backup and recovery", Ordered, func() {
	const (
		sourceCluster   = "bkp-src"
		restoredCluster = "bkp-restored"
		backupName      = "full-backup"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("backup")

		setupMinio()
		DeferCleanup(teardownMinio)

		By("creating the source cluster that archives to object storage")
		applyManifest(sourceCluster, archivingClusterManifest(sourceCluster))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, archivingClusterManifest(sourceCluster))
		})
		expectClusterReady(sourceCluster, 1, 12*time.Minute)

		By("seeding data on the source cluster")
		primary := clusterPrimary(sourceCluster)
		password := appPassword(sourceCluster)
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS notes (id INT PRIMARY KEY, body VARCHAR(64)); "+
				"REPLACE INTO notes VALUES (1, 'hello-from-backup');")
		Expect(err).NotTo(HaveOccurred(), "Failed to seed data on the source cluster")
	})

	It("takes a physical backup to object storage", func() {
		By("creating the Backup object")
		applyManifest(backupName, backupManifest(backupName, sourceCluster))

		By("waiting for the backup to complete")
		Eventually(func(g Gomega) {
			phase, err := kubectl("get", "backup", backupName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).NotTo(Equal("failed"), "backup failed")
			g.Expect(phase).To(Equal("completed"), "backup is not completed yet")
		}, 8*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying the backup recorded an id and a destination path")
		id, err := kubectl("get", "backup", backupName, "-n", testNamespace,
			"-o", "jsonpath={.status.backupId}")
		Expect(err).NotTo(HaveOccurred())
		Expect(id).NotTo(BeEmpty(), "backup has no backupId")

		dest, err := kubectl("get", "backup", backupName, "-n", testNamespace,
			"-o", "jsonpath={.status.destinationPath}")
		Expect(err).NotTo(HaveOccurred())
		Expect(dest).To(ContainSubstring("s3://"+minioBucket), "unexpected destination path")
	})

	It("bootstraps a new cluster by recovering the backup", func() {
		By("creating a cluster that recovers from the backup")
		applyManifest(restoredCluster, recoveryClusterManifest(restoredCluster, backupName))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, recoveryClusterManifest(restoredCluster, backupName))
		})
		expectClusterReady(restoredCluster, 1, 12*time.Minute)

		By("verifying the seeded row is present after recovery")
		primary := clusterPrimary(restoredCluster)
		// Recovery restores the source's data verbatim, including its app user and
		// password, so the restored cluster authenticates with the source's app
		// credentials (no new app Secret is generated for a recovery bootstrap).
		password := appPassword(sourceCluster)
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, "app", password, "app",
				"SELECT body FROM notes WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("hello-from-backup"), "recovered data is missing")
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("blocks a fresh cluster from overwriting a non-empty destination", func() {
		const guardCluster = "bkp-guard"

		By("seeding an existing object under the guard cluster's destination prefix")
		// ClusterPrefix is "<cluster>/"; a fresh cluster pointed there must refuse
		// to adopt it. A standalone marker keeps this independent of the source
		// cluster's lifecycle (and free of delete/recreate races).
		seedObjectStoreMarker(guardCluster + "/existing-backup/marker")

		By("creating a fresh cluster pointed at the non-empty destination")
		applyManifest(guardCluster, archivingClusterManifest(guardCluster))
		DeferCleanup(func() {
			deleteManifest(guardCluster, archivingClusterManifest(guardCluster))
		})

		By("verifying the fresh cluster is Blocked instead of overwriting the archive")
		Eventually(func(g Gomega) {
			phase, err := clusterField(guardCluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"), "fresh cluster was not blocked on the non-empty destination")
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("verifying the block reason references the non-empty destination")
		reason, err := clusterField(guardCluster, "{.status.phaseReason}")
		Expect(err).NotTo(HaveOccurred())
		Expect(reason).To(ContainSubstring("not empty"))
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

func archivingClusterManifest(name string) string {
	return fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
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
%s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

func recoveryClusterManifest(name, backup string) string {
	return fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
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
    recovery:
      backup:
        name: %s
  backup:
%s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, backup, objectStoreYAML("    "))
}

func backupManifest(name, cluster string) string {
	return fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Backup
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  method: xtrabackup
`, name, testNamespace, cluster)
}
