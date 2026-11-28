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

// VolumeSnapshot backups end to end (design 045), on MySQL and on MariaDB,
// whose hold speaks another dialect and whose replay is positional.
// csi-driver-host-path stands in for a real CSI driver.
var _ = Describe("VolumeSnapshot backup and recovery", Ordered, Label("heavy"), func() {
	volumeSnapshotSpecs(mysqlForkFlavor, "snap")
})

var _ = Describe("MariaDB VolumeSnapshot backup and recovery", Ordered, Label("heavy", "mariadb"), func() {
	volumeSnapshotSpecs(mariadbForkFlavor, "mdb-snap")
})

// volumeSnapshotSpecs checks what the feature promises beyond producing a
// snapshot: a recovery holds exactly what was served up to the target, the
// source is always released (a failed quiesce leaves the replica replicating,
// a cold snapshot unfences it), an online snapshot of the primary stalls
// writes only briefly and never fails over, and reclaimPolicy: Delete removes
// the VolumeSnapshot.
func volumeSnapshotSpecs(f forkFlavor, prefix string) {
	var (
		source   = prefix + "-src"
		ns       string
		prevNS   string
		password string
	)
	sql := func(pod, statement string) (string, error) { return f.exec(pod, "app", password, "app", statement) }
	replicaOf := func() string {
		primary := clusterPrimary(source)
		for _, name := range []string{source + "-1", source + "-2"} {
			if name != primary {
				return name
			}
		}
		return ""
	}
	// expectRows waits until pod's notes table holds exactly ids.
	expectRows := func(pod string, ids ...string) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			out, err := sql(pod, "SELECT id FROM notes ORDER BY id;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(parseSingleValueLines(out))).To(Equal(ids))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	}
	recoverInto := func(name, backup string, target pitrTarget, instances int) {
		GinkgoHelper()
		bootstrap := fmt.Sprintf("  bootstrap:\n    recovery:\n      backup:\n        name: %s\n", backup)
		if target != (pitrTarget{}) {
			bootstrap += target.yaml()
		}
		manifest := snapshotClusterManifest(f, name, bootstrap, instances)
		applyManifest(name, manifest)
		DeferCleanup(func() { deleteManifest(name, manifest) })
		expectClusterReady(name, instances, 20*time.Minute)
		dataSource, err := kubectl("get", "pvc", name+"-1", "-n", testNamespace, "-o", "jsonpath={.spec.dataSource.name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(dataSource).To(Equal(backup), "the primary volume was not created from the snapshot")
	}

	BeforeAll(func() {
		By("installing the snapshot controller and csi-driver-host-path")
		_, err := utils.Run(exec.Command("./hack/e2e-csi-hostpath.sh"))
		Expect(err).NotTo(HaveOccurred(), "installing CSI snapshot support")

		prevNS = testNamespace
		ns = createTestNamespace(prefix)
		setupObjectStore()
		DeferCleanup(teardownObjectStore)

		By("creating an archiving source cluster on CSI volumes")
		manifest := snapshotClusterManifest(f, source, "", 2)
		applyManifest(source, manifest)
		DeferCleanup(func() { deleteManifest(source, manifest) })
		expectClusterReady(source, 2, 20*time.Minute)

		password = appPassword(source)
		_, err = sql(clusterPrimary(source),
			"CREATE TABLE notes (id INT PRIMARY KEY, body VARCHAR(64)); INSERT INTO notes VALUES (1, 'before-snapshot');")
		Expect(err).NotTo(HaveOccurred(), "seeding the source cluster")
		expectRows(replicaOf(), "1")
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})

	It("takes an online snapshot of the replica and recovers it to a point in time", func() {
		const backup = "online"
		primary := clusterPrimary(source)
		By("taking an online volumeSnapshot Backup")
		applyManifest(backup, snapshotBackupManifest(backup, source, "online: true\n"))
		expectBackupCompleted(backup, 15*time.Minute)

		for path, want := range map[string]string{
			"{.status.method}":                        "volumeSnapshot",
			"{.status.online}":                        "true",
			"{.status.instanceName}":                  replicaOf(),
			"{.status.volumeSnapshots[0].readyToUse}": "true",
			"{.status.volumeSnapshots[0].name}":       backup,
		} {
			got, err := backupField(backup, path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want), "backup %s", path)
		}
		anchor, err := backupField(backup, "{.status.endGTID}")
		Expect(err).NotTo(HaveOccurred())
		Expect(anchor).NotTo(BeEmpty(), "an online snapshot records its anchor")
		snapAnchor, err := kubectl("get", "volumesnapshot", backup, "-n", testNamespace,
			"-o", `jsonpath={.metadata.annotations.mysql\.cnmsql\.co/anchor-gtid}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(snapAnchor).To(Equal(anchor), "the VolumeSnapshot carries the Backup's anchor")

		By("replicating again once released")
		_, err = sql(primary, "INSERT INTO notes VALUES (2, 'after-snapshot');")
		Expect(err).NotTo(HaveOccurred())
		expectRows(replicaOf(), "1", "2")

		By("writing past the recovery target")
		target := f.flush(source, primary, password)
		_, err = sql(primary, "INSERT INTO notes VALUES (3, 'past-target');")
		Expect(err).NotTo(HaveOccurred())
		f.flush(source, primary, password)
		f.covers(source, target, 5*time.Minute)

		By("recovering a new cluster from the snapshot up to the target")
		recoverInto(prefix+"-pitr", backup, pitrTarget{GTID: target}, 2)
		// The snapshot plus the replayed write, not the one past the target,
		// on the recovered primary and on the replica that cloned it.
		expectRows(clusterPrimary(prefix+"-pitr"), "1", "2")
		for _, name := range []string{prefix + "-pitr-1", prefix + "-pitr-2"} {
			expectRows(name, "1", "2")
		}
		_, err = sql(clusterPrimary(prefix+"-pitr"), "INSERT INTO notes VALUES (4, 'recovered-writes');")
		Expect(err).NotTo(HaveOccurred(), "the recovered primary does not take writes")
	})

	It("releases the replica when the quiesce fails", func() {
		const backup = "quiesce-fails"
		replica := replicaOf()
		By("holding the notes table open on the replica with a long query")
		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(done)
			_, _ = f.exec(replica, "root", rootPassword(source), "app", "SELECT SLEEP(60) FROM notes LIMIT 1;")
		}()
		Eventually(func(g Gomega) {
			out, err := f.exec(replica, "root", rootPassword(source), "",
				"SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE INFO LIKE 'SELECT SLEEP(60)%';")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(parseSingleValue(out)).NotTo(Equal("0"))
		}, e2eTimeout(time.Minute), 2*time.Second).Should(Succeed())

		By("taking a snapshot whose lock cannot be had within its lock timeout")
		applyManifest(backup, snapshotBackupManifest(backup, source, "volumeSnapshot:\n    lockTimeout: 2s\n"))
		Eventually(func(g Gomega) {
			phase, err := backupField(backup, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("failed"))
		}, e2eTimeout(3*time.Minute), 3*time.Second).Should(Succeed())
		reason, err := backupField(backup, `{.status.conditions[?(@.type=="Progressing")].reason}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(reason).To(BeElementOf("SnapshotPrepareFailed", "SnapshotLockExpired"))
		finalizers, err := backupField(backup, "{.metadata.finalizers}")
		Expect(err).NotTo(HaveOccurred())
		Expect(finalizers).NotTo(ContainSubstring("snapshot-release"))
		<-done

		By("replicating again: the applier was restarted and the lock dropped")
		_, err = sql(clusterPrimary(source), "INSERT INTO notes VALUES (5, 'after-failed-quiesce');")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			out, err := sql(replica, "SELECT COUNT(*) FROM notes WHERE id = 5;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(parseSingleValue(out)).To(Equal("1"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterReady(source, 2, 5*time.Minute)
	})

	It("snapshots the primary without failing over and reclaims the snapshot", func() {
		const backup = "on-primary"
		primary := clusterPrimary(source)
		By("taking an online snapshot of the primary while writing")
		applyManifest(backup, snapshotBackupManifest(backup, source, "target: primary\n  reclaimPolicy: Delete\n"))
		expectBackupCompleted(backup, 15*time.Minute)
		instance, err := backupField(backup, "{.status.instanceName}")
		Expect(err).NotTo(HaveOccurred())
		Expect(instance).To(Equal(primary))
		Expect(clusterPrimary(source)).To(Equal(primary), "an online snapshot of the primary failed over")
		_, err = sql(primary, "INSERT INTO notes VALUES (6, 'after-primary-snapshot');")
		Expect(err).NotTo(HaveOccurred(), "the primary does not take writes after its snapshot")
		expectClusterReady(source, 2, 5*time.Minute)

		By("deleting the Backup removes its VolumeSnapshot")
		_, err = kubectl("delete", "backup", backup, "-n", testNamespace, "--wait=true", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			_, err := kubectl("get", "volumesnapshot", backup, "-n", testNamespace)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(ContainSubstring("NotFound"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("takes a cold snapshot of the replica, unfences it, and recovers it", func() {
		const backup = "cold"
		By("taking a cold volumeSnapshot Backup")
		applyManifest(backup, snapshotBackupManifest(backup, source, "online: false\n"))
		expectBackupCompleted(backup, 15*time.Minute)
		got, err := backupField(backup, "{.status.online}")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal("false"))
		anchor, err := backupField(backup, "{.status.endGTID}")
		Expect(err).NotTo(HaveOccurred())
		Expect(anchor).NotTo(BeEmpty(), "a cold snapshot records the anchor read on the replica's next start")

		instance, err := backupField(backup, "{.status.instanceName}")
		Expect(err).NotTo(HaveOccurred())
		fenced, err := kubectl("get", "pod", instance, "-n", testNamespace,
			"-o", `jsonpath={.metadata.annotations.cnmsql\.cnmsql\.co/fencing}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(fenced).To(BeEmpty(), "the source replica is still fenced after the cut")
		expectClusterReady(source, 2, 10*time.Minute)

		By("recovering a new cluster from the cold snapshot")
		recoverInto(prefix+"-cold", backup, pitrTarget{}, 1)
		expectRows(clusterPrimary(prefix+"-cold"), "1", "2", "3", "5", "6")
	})
}

// snapshotClusterManifest is an archiving cluster on csi-hostpath-sc volumes
// with a volumeSnapshot section. bootstrap, when empty, is an initdb.
func snapshotClusterManifest(f forkFlavor, name, bootstrap string, instances int) string {
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
%[3]s  instances: %[4]d
  imageName: %[5]s
  storage:
    size: 2Gi
    storageClass: csi-hostpath-sc
%[6]s
  mysql:
    binlogFormat: ROW
%[7]s
%[8]s  backup:
%[9]s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
    volumeSnapshot:
      className: csi-hostpath-snapclass
`, name, testNamespace, f.flavorLine(), instances, f.image(), e2eInstanceResources, e2eMySQLParameters,
		bootstrap, objectStoreYAML("    "))
}

// snapshotBackupManifest is a volumeSnapshot Backup; extra is appended to its
// spec, indented by two spaces on its first line.
func snapshotBackupManifest(name, cluster, extra string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Backup
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  method: volumeSnapshot
  %s`, name, testNamespace, cluster, extra)
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
