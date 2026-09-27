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

// This spec reproduces issue 138: a physical backup taken FROM A REPLICA (the
// prefer-standby default) carries the replica's replication metadata — the
// source connection, the applier metadata and the relay-log position — but no
// relay logs. A restored instance that kept the metadata would run START REPLICA
// on it, fail with Error 1872 ("Replica failed to initialize applier metadata
// structure from the repository") and crash-loop the Pod. The restore clears the
// inherited metadata (RESET REPLICA ALL), so the recovered instance boots as a
// clean new primary without a single restart and never tries to replicate from
// the backup source's host.
var _ = Describe("Recovery from a replica backup", Ordered, Label("flavor"), func() {
	const (
		sourceCluster   = "repl-src"
		restoredCluster = "repl-restored"
		backupName      = "standby-backup"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("replica-backup")

		setupObjectStore()
		DeferCleanup(teardownObjectStore)

		By("creating a two-instance source cluster that archives to object storage")
		applyManifest(sourceCluster, replicaBackupClusterManifest(sourceCluster))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, replicaBackupClusterManifest(sourceCluster))
		})
		expectClusterReady(sourceCluster, 2, 20*time.Minute)

		By("seeding data on the source cluster")
		primary := clusterPrimary(sourceCluster)
		password := appPassword(sourceCluster)
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS notes (id INT PRIMARY KEY, body VARCHAR(64)); "+
				"REPLACE INTO notes VALUES (1, 'hello-from-standby-backup');")
		Expect(err).NotTo(HaveOccurred(), "Failed to seed data on the source cluster")
	})

	It("takes a physical backup from the standby", func() {
		By("creating the Backup with the prefer-standby target")
		applyManifest(backupName, backupManifest(backupName, sourceCluster))

		By("waiting for the backup to complete")
		expectBackupCompleted(backupName, 8*time.Minute)

		// The load-bearing precondition for the recovery spec: the backup must
		// come from a replica, so the archive carries the replica's replication
		// metadata that the restore must clear.
		status := backupStatus(backupName)
		Expect(status.InstanceName).NotTo(Equal(clusterPrimary(sourceCluster)),
			"prefer-standby should back up a replica, not the primary")
	})

	It("restores the replica backup with zero pod restarts", func() {
		By("creating a cluster that recovers the standby backup")
		applyManifest(restoredCluster, recoveryClusterManifest(restoredCluster, backupName))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, recoveryClusterManifest(restoredCluster, backupName))
		})
		expectClusterReady(restoredCluster, 1, 20*time.Minute)

		By("verifying the seeded row is present after recovery")
		primary := clusterPrimary(restoredCluster)
		// Recovery restores the source's data verbatim, including its app user
		// and password, so the restored cluster authenticates with the source's
		// app credentials.
		password := appPassword(sourceCluster)
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, "app", password, "app",
				"SELECT body FROM notes WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("hello-from-standby-backup"), "recovered data is missing")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the restored instance never replicated from the backup source")
		// The restore must have cleared the inherited replication metadata: a
		// recovered cluster is a new primary, not a replica of the source.
		out, err := mysqlExec(primary, "root", rootPassword(restoredCluster), "",
			"SHOW REPLICA STATUS")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(BeEmpty(),
			"a restored instance must have no replication configuration: %q", out)

		By("verifying the restored instance never restarted")
		// The mysql container is the instance Pod's only regular container, so
		// any crash-loop of the restored primary shows up in the restart counts.
		expectNoInstanceRestarts(restoredCluster)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// replicaBackupClusterManifest renders a two-instance source cluster that
// archives to object storage, so a prefer-standby backup has a replica to
// stream from.
func replicaBackupClusterManifest(name string) string {
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
%s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

// expectNoInstanceRestarts asserts that no instance Pod of the cluster has any
// container restart. instanceRestarts reports "name/uid=restartCount; ..." per
// Pod.
func expectNoInstanceRestarts(cluster string) {
	GinkgoHelper()
	out := instanceRestarts(cluster)
	for _, segment := range strings.Split(out, "; ") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		idx := strings.LastIndex(segment, "=")
		Expect(idx).To(BeNumerically(">", 0), "unparseable restart report %q", out)
		Expect(segment[idx+1:]).To(Equal("0"),
			"instance %s restarted while recovering\n%s", segment[:idx], out)
	}
}
