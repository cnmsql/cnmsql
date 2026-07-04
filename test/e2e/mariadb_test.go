//go:build e2e
// +build e2e

package e2e

import (
	"encoding/base64"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

var _ = Describe("MariaDB", Ordered, Label("flavor", "mariadb"), func() {
	const (
		clusterName = "mdb-e2e"
		minioNS     = "e2e-minio"
	)

	var (
		ns, prevNS string
		primary    string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb")

		By("applying the MariaDB cluster")
		applyManifest(clusterName, mariadbClusterManifest(clusterName))
		DeferCleanup(func() {
			deleteManifest(clusterName, mariadbClusterManifest(clusterName))
			deleteTestNamespace(ns, prevNS)
		})
	})

	SetDefaultEventuallyTimeout(e2eTimeout(4 * time.Minute))
	SetDefaultEventuallyPollingInterval(time.Second)

	It("bootstraps and reaches ready state", func() {
		By("waiting for all 3 instances to become ready")
		expectClusterReady(clusterName, 3, e2eTimeout(15*time.Minute))

		By("verifying the primary is set")
		primary = clusterPrimary(clusterName)
		Expect(primary).NotTo(BeEmpty(), "no primary elected")

		By("verifying the flavor is mariadb")
		flavor, err := kubectl("get", "cluster", clusterName, "-n", testNamespace,
			"-o", "jsonpath={.spec.flavor}")
		Expect(err).NotTo(HaveOccurred())
		Expect(flavor).To(Equal("mariadb"), "cluster flavor is not mariadb")

		By("verifying the resolved status flavor")
		statusFlavor, err := kubectl("get", "cluster", clusterName, "-n", testNamespace,
			"-o", "jsonpath={.status.flavor}")
		Expect(err).NotTo(HaveOccurred())
		Expect(statusFlavor).To(Equal("mariadb"), "status flavor is not mariadb")
	})

	It("writes data and reads it back on replicas", func() {
		password := appPassword(clusterName)
		Expect(password).NotTo(BeEmpty(), "app password secret not ready")

		By("writing data on the primary")
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS notes (id INT PRIMARY KEY, body VARCHAR(64)); "+
				"INSERT INTO notes VALUES (1, 'hello-mariadb') ON DUPLICATE KEY UPDATE body='hello-mariadb';")
		Expect(err).NotTo(HaveOccurred(), "failed to write data")

		By("verifying data on replica cluster-sample-2")
		Eventually(func(g Gomega) {
			out, err := mysqlExec("cluster-sample-2", "app", password, "app",
				"SELECT body FROM notes WHERE id = 1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("hello-mariadb"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying data on replica cluster-sample-3")
		Eventually(func(g Gomega) {
			out, err := mysqlExec("cluster-sample-3", "app", password, "app",
				"SELECT body FROM notes WHERE id = 1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("hello-mariadb"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("performs a planned switchover and preserves data", func() {
		By("identifying a replica to promote")
		var replica string
		if primary == "cluster-sample-1" {
			replica = "cluster-sample-2"
		} else {
			replica = "cluster-sample-1"
		}

		By("patching targetPrimary to trigger switchover")
		patch := fmt.Sprintf(`[{"op":"replace","path":"/status/targetPrimary","value":"%s"}]`, replica)
		_, err := kubectl("patch", "cluster", clusterName, "-n", testNamespace,
			"--type=json", "-p", patch, "--subresource=status")
		Expect(err).NotTo(HaveOccurred(), "failed to patch targetPrimary")

		By("waiting for the switchover to complete")
		Eventually(func(g Gomega) {
			cur, _ := clusterField(clusterName, ".status.currentPrimary")
			g.Expect(cur).To(Equal(replica))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		oldPrimary := primary
		primary = replica
		GinkgoWriter.Printf("Switchover completed: %s → %s\n", oldPrimary, primary)

		By("verifying data survived the switchover")
		password := appPassword(clusterName)
		out, err := mysqlExec(primary, "app", password, "app",
			"SELECT body FROM notes WHERE id = 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("hello-mariadb"))

		By("verifying the old primary became a replica and is replicating")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(oldPrimary, "app", password, "app",
				"SELECT body FROM notes WHERE id = 1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("hello-mariadb"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("writing new data through the new primary")
		_, err = mysqlExec(primary, "app", password, "app",
			"INSERT INTO notes VALUES (2, 'after-switchover') ON DUPLICATE KEY UPDATE body='after-switchover';")
		Expect(err).NotTo(HaveOccurred())
	})

	It("recovers from a primary failure (failover)", func() {
		By("deleting the primary pod")
		deleteArgs := []string{"kubectl", "delete", "pod", primary, "-n", testNamespace, "--grace-period=0", "--force"}
		cmd := exec.Command(deleteArgs[0], deleteArgs[1:]...)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "failed to delete primary pod")

		By("waiting for a new primary to be elected")
		var newPrimary string
		Eventually(func(g Gomega) {
			cur, _ := clusterField(clusterName, ".status.currentPrimary")
			g.Expect(cur).NotTo(BeEmpty())
			g.Expect(cur).NotTo(Equal(primary))
			newPrimary = cur
		}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

		GinkgoWriter.Printf("Failover completed: new primary is %s\n", newPrimary)
		primary = newPrimary

		By("verifying all instances are ready after failover")
		expectClusterReady(clusterName, 3, e2eTimeout(10*time.Minute))

		By("verifying data survived the failover")
		password := appPassword(clusterName)
		out, err := mysqlExec(primary, "app", password, "app",
			"SELECT body FROM notes WHERE id = 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("hello-mariadb"))

		out, err = mysqlExec(primary, "app", password, "app",
			"SELECT body FROM notes WHERE id = 2")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("after-switchover"))
	})

	It("takes a physical backup and restores to a new cluster", func() {
		const (
			bkpSource   = "mdb-bkp-src"
			bkpRestored = "mdb-bkp-restored"
			bkpName     = "mdb-backup"
		)

		setupMinio()
		DeferCleanup(teardownMinio)

		By("creating a source cluster with archiving enabled")
		applyManifest(bkpSource, mariadbArchivingClusterManifest(bkpSource))
		DeferCleanup(func() {
			deleteManifest(bkpSource, mariadbArchivingClusterManifest(bkpSource))
		})
		expectClusterReady(bkpSource, 1, e2eTimeout(15*time.Minute))

		By("seeding data on the source cluster")
		bkpPrimary := clusterPrimary(bkpSource)
		password := appPassword(bkpSource)
		_, err := mysqlExec(bkpPrimary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS notes (id INT PRIMARY KEY, body VARCHAR(64)); "+
				"INSERT INTO notes VALUES (1, 'backup-test') ON DUPLICATE KEY UPDATE body='backup-test';")
		Expect(err).NotTo(HaveOccurred())

		By("creating a Backup object")
		applyManifest(bkpName, backupManifest(bkpName, bkpSource))
		DeferCleanup(func() {
			deleteManifest(bkpName, backupManifest(bkpName, bkpSource))
		})

		By("waiting for the backup to complete")
		Eventually(func(g Gomega) {
			phase, err := kubectl("get", "backup", bkpName, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("completed"), "backup phase: %s", phase)
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("restoring the backup to a new cluster")
		applyManifest(bkpRestored, mariadbRecoveryClusterManifest(bkpRestored, bkpName))
		DeferCleanup(func() {
			deleteManifest(bkpRestored, mariadbRecoveryClusterManifest(bkpRestored, bkpName))
		})
		expectClusterReady(bkpRestored, 1, e2eTimeout(15*time.Minute))

		By("verifying data was restored")
		restoredPrimary := clusterPrimary(bkpRestored)
		password = appPassword(bkpRestored)
		out, err := mysqlExec(restoredPrimary, "app", password, "app",
			"SELECT body FROM notes WHERE id = 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("backup-test"))
	})

	It("performs point-in-time recovery from an archived backup", func() {
		const (
			pitrSource   = "mdb-pitr-src"
			pitrRestored = "mdb-pitr-restored"
			pitrBackup   = "mdb-pitr-backup"
		)

		setupMinio()
		DeferCleanup(teardownMinio)

		By("creating the source cluster that archives to object store")
		applyManifest(pitrSource, mariadbContinuousArchivingClusterManifest(pitrSource))
		DeferCleanup(func() {
			deleteManifest(pitrSource, mariadbContinuousArchivingClusterManifest(pitrSource))
		})
		expectClusterReady(pitrSource, 1, e2eTimeout(15*time.Minute))

		pitrPrimary := clusterPrimary(pitrSource)
		password := appPassword(pitrSource)

		By("seeding initial data")
		_, err := mysqlExec(pitrPrimary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS events (id INT PRIMARY KEY AUTO_INCREMENT, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, msg VARCHAR(128)); "+
				"INSERT INTO events (msg) VALUES ('pre-backup');")
		Expect(err).NotTo(HaveOccurred())

		By("taking a base backup")
		applyManifest(pitrBackup, backupManifest(pitrBackup, pitrSource))
		DeferCleanup(func() {
			deleteManifest(pitrBackup, backupManifest(pitrBackup, pitrSource))
		})

		Eventually(func(g Gomega) {
			phase, _ := kubectl("get", "backup", pitrBackup, "-n", testNamespace,
				"-o", "jsonpath={.status.phase}")
			g.Expect(phase).To(Equal("completed"))
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("writing data after the backup for PITR validation")
		_, err = mysqlExec(pitrPrimary, "app", password, "app",
			"INSERT INTO events (msg) VALUES ('post-backup');")
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(5 * time.Second)

		By("recording the binlog position after post-backup write")
		gtidOut, err := mysqlExec(pitrPrimary, "app", password, "app",
			"SELECT @@gtid_current_pos")
		Expect(err).NotTo(HaveOccurred())
		targetGTID := strings.TrimSpace(gtidOut)
		GinkgoWriter.Printf("PITR target GTID: %s\n", targetGTID)

		By("restoring to the target GTID via PITR")
		applyManifest(pitrRestored, mariadbPITRClusterManifest(pitrRestored, pitrBackup, pitrSource, targetGTID))
		DeferCleanup(func() {
			deleteManifest(pitrRestored, mariadbPITRClusterManifest(pitrRestored, pitrBackup, pitrSource, targetGTID))
		})
		expectClusterReady(pitrRestored, 1, e2eTimeout(20*time.Minute))

		By("verifying both pre- and post-backup data exist")
		restoredPrimary := clusterPrimary(pitrRestored)
		password = appPassword(pitrRestored)
		out, err := mysqlExec(restoredPrimary, "app", password, "app",
			"SELECT COUNT(*) FROM events")
		Expect(err).NotTo(HaveOccurred())
		count, err := strconv.Atoi(strings.TrimSpace(out))
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(BeNumerically(">=", 2),
			"PITR should recover at least pre-backup and post-backup rows; got %d", count)

		out, err = mysqlExec(restoredPrimary, "app", password, "app",
			"SELECT msg FROM events WHERE msg='post-backup'")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("post-backup"),
			"post-backup row should exist after PITR to the target GTID")
	})

	It("manages database users via the plugin", func() {
		password := appPassword(clusterName)

		By("creating a database user via the instance manager API")
		createUser := fmt.Sprintf(
			`{"name":"e2euser","host":"%%","password":"%s","superuser":false,"privileges":[{"privileges":["SELECT","INSERT"],"on":"app.*"}]}`,
			password)
		podName := clusterPrimary(clusterName)
		cmd := exec.Command("kubectl", "exec", podName, "-n", testNamespace, "-c", "mysql",
			"--", "curl", "-s", "-k",
			"--cert", "/etc/cnmsql/tls-server/tls.crt",
			"--key", "/etc/cnmsql/tls-server/tls.key",
			"--cacert", "/etc/cnmsql/client-ca/ca.crt",
			"-X", "POST",
			"-H", "Content-Type: application/json",
			"-d", createUser,
			"https://localhost:8080/user/create")
		out, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "failed to create user: %s", string(out))

		By("verifying the user can connect and query")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, "e2euser", password, "app",
				"SELECT body FROM notes WHERE id = 1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("hello-mariadb"))
		}, e2eTimeout(1*time.Minute), 5*time.Second).Should(Succeed())
	})
})

// MariaDB cluster manifests. Each uses mariadbImageFor(sampleVersion()) for the
// instance image, sets flavor: mariadb, and uses the same resource/tuning
// constraints as the existing MySQL e2e manifests.

var mariadbImage = mariadbInstanceImageFor(mariadbSampleVersion())

func mariadbClusterManifest(name string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  flavor: mariadb
  instances: 3
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
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters)
}

func mariadbArchivingClusterManifest(name string) string {
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
%s
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

func mariadbRecoveryClusterManifest(name, backup string) string {
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
    recovery:
      backup:
        name: %s
      recoveryTarget: {}
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters, backup)
}

func mariadbContinuousArchivingClusterManifest(name string) string {
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
%s
  archiving:
    enabled: true
    purgeGate: false
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

func mariadbPITRClusterManifest(name, backup, sourceCluster, targetGTID string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(targetGTID))
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
    recovery:
      backup:
        name: %s
      recoveryTarget:
        targetGTID: %s
      sourceCluster: %s
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters,
		backup, encoded, sourceCluster)
}
