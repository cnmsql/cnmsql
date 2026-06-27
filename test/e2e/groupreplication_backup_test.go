//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec exercises the M-GR.7 backup/restore-into-a-fresh-group path: a
// physical backup is taken from a 3-member group (offloaded to an ONLINE
// secondary) and then recovered into a brand-new Group Replication cluster. The
// load-bearing GR guarantee is that recovery forms a FRESH group — a new pinned
// group name, never rejoining the source's group — while the data is restored
// verbatim and secondaries join the new group via distributed recovery.
var _ = Describe("Group Replication backup and restore into a fresh group", Ordered, Label("flavor", "heavy"), func() {
	const (
		sourceCluster   = "gr-bkp-src"
		restoredCluster = "gr-bkp-restored"
		backupName      = "gr-full-backup"
		instances       = 3
	)

	var ns, prevNS, sourceGroupName string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("gr-backup")

		setupMinio()
		DeferCleanup(teardownMinio)

		By("creating a 3-member Group Replication source cluster that archives to object storage")
		applyManifest(sourceCluster, grBackupClusterManifest(sourceCluster, instances))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, grBackupClusterManifest(sourceCluster, instances))
		})
		expectClusterReady(sourceCluster, instances, 20*time.Minute)

		By("seeding data on the source group's primary")
		primary := clusterPrimary(sourceCluster)
		password := appPassword(sourceCluster)
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS gr_notes (id INT PRIMARY KEY, body VARCHAR(64)); "+
				"REPLACE INTO gr_notes VALUES (1, 'hello-from-gr-backup');")
		Expect(err).NotTo(HaveOccurred(), "failed to seed data on the source group")

		var gnErr error
		sourceGroupName, gnErr = clusterField(sourceCluster, "{.status.groupReplication.groupName}")
		Expect(gnErr).NotTo(HaveOccurred())
		Expect(sourceGroupName).NotTo(BeEmpty(), "source group must have a pinned group name")
	})

	It("takes a physical backup of the group to object storage", func() {
		By("creating the Backup object")
		applyManifest(backupName, backupManifest(backupName, sourceCluster))

		By("waiting for the backup to complete")
		Eventually(func(g Gomega) {
			phase, err := kubectl("get", "backup", backupName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).NotTo(Equal("failed"), "backup failed")
			g.Expect(phase).To(Equal("completed"), "backup is not completed yet")
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("recovers the backup into a brand-new group with a fresh group name and no data loss", func() {
		By("creating a Group Replication cluster that recovers from the backup")
		applyManifest(restoredCluster, grRecoveryClusterManifest(restoredCluster, instances, backupName))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, grRecoveryClusterManifest(restoredCluster, instances, backupName))
		})
		expectClusterReady(restoredCluster, instances, 20*time.Minute)

		By("verifying the restored cluster bootstrapped a FRESH group (new, distinct group name)")
		Eventually(func(g Gomega) {
			bootstrapped, err := clusterField(restoredCluster, "{.status.groupReplication.bootstrapped}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(bootstrapped).To(Equal("true"), "the restored cluster must bootstrap its own group")

			restoredGroupName, err := clusterField(restoredCluster, "{.status.groupReplication.groupName}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(restoredGroupName).NotTo(BeEmpty(), "a fresh group name must be pinned")
			g.Expect(restoredGroupName).NotTo(Equal(sourceGroupName),
				"recovery must form a fresh group, never rejoin the source's group")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying all three restored members are ONLINE with quorum")
		Eventually(func(g Gomega) {
			quorum, err := clusterField(restoredCluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"))
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())

		By("verifying the seeded row is present on the recovered primary")
		// Recovery restores the source's data verbatim, including its app user and
		// password, so the restored cluster authenticates with the source's app
		// credentials.
		password := appPassword(sourceCluster)
		primary := clusterPrimary(restoredCluster)
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, "app", password, "app",
				"SELECT body FROM gr_notes WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("hello-from-gr-backup"), "recovered data is missing")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the seeded row reached the secondaries via distributed recovery into the fresh group")
		for _, secondary := range []string{restoredCluster + "-2", restoredCluster + "-3"} {
			Eventually(func(g Gomega) {
				out, err := mysqlExec(secondary, "app", password, "", "SELECT body FROM app.gr_notes WHERE id = 1;")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("hello-from-gr-backup"),
					"the secondary %s must join the fresh group and catch up", secondary)
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		}
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// grBackupClusterManifest renders a Group Replication Cluster that archives to
// the e2e object store.
func grBackupClusterManifest(name string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  replication:
    mode: groupReplication
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
  backup:
%[7]s
`, name, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

// grRecoveryClusterManifest renders a Group Replication Cluster that bootstraps
// by recovering a physical backup into a fresh group.
func grRecoveryClusterManifest(name string, instances int, backup string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  replication:
    mode: groupReplication
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    recovery:
      backup:
        name: %[7]s
  backup:
%[8]s
`, name, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters, backup, objectStoreYAML("    "))
}
