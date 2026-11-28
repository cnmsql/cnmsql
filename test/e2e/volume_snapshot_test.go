//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

// VolumeSnapshot backups end to end (design 045): csi-driver-host-path stands
// in for a real CSI driver. An online snapshot of the replica is restored into
// a new cluster with point-in-time recovery between two write sets, and a cold
// snapshot fences the replica for the cut and unfences it after.
var _ = Describe("VolumeSnapshot backup and recovery", Ordered, Label("heavy"), func() {
	const (
		source   = "snap-src"
		online   = "snap-online"
		cold     = "snap-cold"
		pitr     = "snap-pitr"
		coldCopy = "snap-cold-copy"
	)
	var ns, prevNS, password string

	BeforeAll(func() {
		By("installing the snapshot controller and csi-driver-host-path")
		_, err := utils.Run(exec.Command("./hack/e2e-csi-hostpath.sh"))
		Expect(err).NotTo(HaveOccurred(), "installing CSI snapshot support")

		prevNS = testNamespace
		ns = createTestNamespace("snapshot")
		setupObjectStore()
		DeferCleanup(teardownObjectStore)

		By("creating an archiving source cluster on CSI volumes")
		applyManifest(source, snapshotClusterManifest(source, "", 2))
		DeferCleanup(func() { deleteManifest(source, snapshotClusterManifest(source, "", 2)) })
		expectClusterReady(source, 2, 20*time.Minute)

		password = appPassword(source)
		_, err = mysqlExec(clusterPrimary(source), "app", password, "app",
			"CREATE TABLE notes (id INT PRIMARY KEY, body VARCHAR(64)); INSERT INTO notes VALUES (1, 'before-snapshot');")
		Expect(err).NotTo(HaveOccurred(), "seeding the source cluster")
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})

	It("takes an online snapshot of the replica and recovers it to a point in time", func() {
		primary := clusterPrimary(source)
		By("taking an online volumeSnapshot Backup")
		applyManifest(online, snapshotBackupManifest(online, source, true))
		expectBackupCompleted(online, 15*time.Minute)

		for path, want := range map[string]string{
			"{.status.method}":                       "volumeSnapshot",
			"{.status.online}":                       "true",
			"{.status.volumeSnapshots[0].readyToUse}": "true",
			"{.status.volumeSnapshots[0].name}":       online,
		} {
			got, err := backupField(online, path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want), "backup %s", path)
		}
		instance, err := backupField(online, "{.status.instanceName}")
		Expect(err).NotTo(HaveOccurred())
		Expect(instance).NotTo(Equal(primary), "prefer-standby took the snapshot from the primary")
		anchor, err := backupField(online, "{.status.endGTID}")
		Expect(err).NotTo(HaveOccurred())
		Expect(anchor).NotTo(BeEmpty(), "an online snapshot records its anchor")
		snapAnchor, err := kubectl("get", "volumesnapshot", online, "-n", testNamespace,
			"-o", `jsonpath={.metadata.annotations.mysql\.cnmsql\.co/anchor-gtid}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(snapAnchor).To(Equal(anchor), "the VolumeSnapshot carries the Backup's anchor")

		By("writing after the snapshot, then past the recovery target")
		_, err = mysqlExec(primary, "app", password, "app", "INSERT INTO notes VALUES (2, 'after-snapshot');")
		Expect(err).NotTo(HaveOccurred())
		target := flushBinaryLogs(source, primary, password)
		_, err = mysqlExec(primary, "app", password, "app", "INSERT INTO notes VALUES (3, 'past-target');")
		Expect(err).NotTo(HaveOccurred())
		flushBinaryLogs(source, primary, password)
		expectArchiveCovers(source, target, 5*time.Minute)

		By("recovering a new cluster from the snapshot up to the target")
		manifest := snapshotClusterManifest(pitr, fmt.Sprintf(`  bootstrap:
    recovery:
      backup:
        name: %s
      recoveryTarget:
        targetGTID: "%s"
`, online, target), 1)
		applyManifest(pitr, manifest)
		DeferCleanup(func() { deleteManifest(pitr, manifest) })
		expectClusterReady(pitr, 1, 20*time.Minute)

		pvcSource, err := kubectl("get", "pvc", pitr+"-1", "-n", testNamespace, "-o", "jsonpath={.spec.dataSource.name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(pvcSource).To(Equal(online), "the primary volume was not created from the snapshot")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(clusterPrimary(pitr), "app", password, "app", "SELECT id FROM notes ORDER BY id;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(parseSingleValueLines(out))).To(Equal([]string{"1", "2"}),
				"recovery should hold the snapshot and the replayed write, not the one past the target")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("takes a cold snapshot of the replica and unfences it", func() {
		By("taking a cold volumeSnapshot Backup")
		applyManifest(cold, snapshotBackupManifest(cold, source, false))
		expectBackupCompleted(cold, 15*time.Minute)
		got, err := backupField(cold, "{.status.online}")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("false"))

		instance, err := backupField(cold, "{.status.instanceName}")
		Expect(err).NotTo(HaveOccurred())
		fenced, err := kubectl("get", "pod", instance, "-n", testNamespace,
			"-o", `jsonpath={.metadata.annotations.cnmsql\.cnmsql\.co/fencing}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(fenced).To(BeEmpty(), "the source replica is still fenced after the cut")
		expectClusterReady(source, 2, 10*time.Minute)

		By("recovering a new cluster from the cold snapshot")
		manifest := snapshotClusterManifest(coldCopy, fmt.Sprintf(`  bootstrap:
    recovery:
      backup:
        name: %s
`, cold), 1)
		applyManifest(coldCopy, manifest)
		DeferCleanup(func() { deleteManifest(coldCopy, manifest) })
		expectClusterReady(coldCopy, 1, 20*time.Minute)
		Eventually(func(g Gomega) {
			out, err := mysqlExec(clusterPrimary(coldCopy), "app", password, "app", "SELECT body FROM notes WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("before-snapshot"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})
})

// snapshotClusterManifest is an archiving cluster on csi-hostpath-sc volumes
// with a volumeSnapshot section. bootstrap, when empty, is an initdb.
func snapshotClusterManifest(name, bootstrap string, instances int) string {
	if bootstrap == "" {
		bootstrap = `  bootstrap:
    initdb:
      database: app
      owner: app
`
	}
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  storage:
    size: 2Gi
    storageClass: csi-hostpath-sc
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
%[7]s  backup:
%[8]s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
    volumeSnapshot:
      className: csi-hostpath-snapclass
`, name, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters, bootstrap, objectStoreYAML("    "))
}

func snapshotBackupManifest(name, cluster string, online bool) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Backup
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  method: volumeSnapshot
  online: %t
`, name, testNamespace, cluster, online)
}

// parseSingleValueLines keeps the value lines of mysql -N output, one per
// row, dropping the client's warnings.
func parseSingleValueLines(out string) string {
	var rows []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "mysql:") || strings.Contains(line, "can be insecure") ||
			strings.HasPrefix(line, "Defaulted container") {
			continue
		}
		rows = append(rows, line)
	}
	return strings.Join(rows, " ")
}
