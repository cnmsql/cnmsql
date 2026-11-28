//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

// VolumeSnapshot backups of a Group Replication cluster (design 045). An online
// snapshot of a secondary cannot stop its applier without leaving the group, so
// the read lock blocks the group's applier on that member for the hold and
// flow control has to absorb it: the group must keep its quorum and every
// member, the snapshotted one included, must apply what the primary wrote
// meanwhile. Cold snapshots are refused, and the snapshot recovers into a fresh
// group.
var _ = Describe("Group Replication VolumeSnapshot backup and recovery", Ordered, Label("flavor", "heavy"), func() {
	const (
		source    = "gr-snap-src"
		restored  = "gr-snap-restored"
		backup    = "gr-snap-online"
		instances = 3
	)
	var ns, prevNS, password, sourceGroupName string

	sql := func(pod, statement string) (string, error) { return mysqlExec(pod, "app", password, "app", statement) }
	expectRowOn := func(pod, id string) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			out, err := sql(pod, "SELECT COUNT(*) FROM gr_notes WHERE id = "+id+";")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(parseSingleValue(out)).To(Equal("1"), "row %s has not reached %s", id, pod)
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	}
	expectQuorum := func(cluster string) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			quorum, err := clusterField(cluster, "{.status.groupReplication.hasQuorum}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"))
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())
	}

	BeforeAll(func() {
		By("installing the snapshot controller and csi-driver-host-path")
		_, err := utils.Run(exec.Command("./hack/e2e-csi-hostpath.sh"))
		Expect(err).NotTo(HaveOccurred(), "installing CSI snapshot support")

		prevNS = testNamespace
		ns = createTestNamespace("gr-snap")
		setupObjectStore()
		DeferCleanup(teardownObjectStore)

		By("creating a 3-member Group Replication cluster on CSI volumes")
		manifest := grSnapshotClusterManifest(source, instances, "")
		applyManifest(source, manifest)
		DeferCleanup(func() { deleteManifest(source, manifest) })
		expectClusterReady(source, instances, 20*time.Minute)

		password = appPassword(source)
		_, err = sql(clusterPrimary(source),
			"CREATE TABLE gr_notes (id INT PRIMARY KEY, body VARCHAR(64)); INSERT INTO gr_notes VALUES (1, 'before-snapshot');")
		Expect(err).NotTo(HaveOccurred(), "seeding the group")
		sourceGroupName, err = clusterField(source, "{.status.groupReplication.groupName}")
		Expect(err).NotTo(HaveOccurred())
		Expect(sourceGroupName).NotTo(BeEmpty())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})

	It("takes an online snapshot of a secondary while the group keeps applying", func() {
		primary := clusterPrimary(source)
		By("writing on the primary for the length of the snapshot")
		stop := make(chan struct{})
		writerDone := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(writerDone)
			for id := 100; ; id++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = sql(primary, fmt.Sprintf("INSERT INTO gr_notes VALUES (%d, 'during-snapshot');", id))
				time.Sleep(200 * time.Millisecond)
			}
		}()

		By("taking an online volumeSnapshot Backup of a secondary")
		applyManifest(backup, snapshotBackupManifest(backup, source, "online: true\n"))
		expectBackupCompleted(backup, 15*time.Minute)
		close(stop)
		<-writerDone

		instance, err := backupField(backup, "{.status.instanceName}")
		Expect(err).NotTo(HaveOccurred())
		Expect(instance).NotTo(Equal(primary), "the default target snapshotted the primary")
		anchor, err := backupField(backup, "{.status.endGTID}")
		Expect(err).NotTo(HaveOccurred())
		Expect(anchor).NotTo(BeEmpty(), "an online snapshot records its anchor")
		finalizers, err := backupField(backup, "{.metadata.finalizers}")
		Expect(err).NotTo(HaveOccurred())
		Expect(finalizers).NotTo(ContainSubstring("snapshot-release"), "the secondary is still held")

		By("keeping the group whole: same primary, quorum, every member applying")
		Expect(clusterPrimary(source)).To(Equal(primary), "the snapshot caused a primary change")
		expectQuorum(source)
		expectClusterReady(source, instances, 10*time.Minute)
		_, err = sql(primary, "INSERT INTO gr_notes VALUES (2, 'after-snapshot');")
		Expect(err).NotTo(HaveOccurred(), "the primary does not take writes after the snapshot")
		for i := 1; i <= instances; i++ {
			expectRowOn(fmt.Sprintf("%s-%d", source, i), "2")
		}
	})

	It("refuses a cold snapshot", func() {
		const cold = "gr-snap-cold"
		applyManifest(cold, snapshotBackupManifest(cold, source, "online: false\n"))
		Eventually(func(g Gomega) {
			phase, err := backupField(cold, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("failed"))
		}, e2eTimeout(3*time.Minute), 3*time.Second).Should(Succeed())
		reason, err := backupField(cold, `{.status.conditions[?(@.type=="Progressing")].reason}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(reason).To(Equal("UnsupportedOnGroupReplication"))
		for i := 1; i <= instances; i++ {
			fenced, err := kubectl("get", "pod", fmt.Sprintf("%s-%d", source, i), "-n", testNamespace,
				"-o", `jsonpath={.metadata.annotations.cnmsql\.cnmsql\.co/fencing}`)
			Expect(err).NotTo(HaveOccurred())
			Expect(fenced).To(BeEmpty(), "a refused cold snapshot fenced a member")
		}
	})

	It("recovers the snapshot into a fresh group", func() {
		bootstrap := fmt.Sprintf("  bootstrap:\n    recovery:\n      backup:\n        name: %s\n", backup)
		manifest := grSnapshotClusterManifest(restored, instances, bootstrap)
		applyManifest(restored, manifest)
		DeferCleanup(func() { deleteManifest(restored, manifest) })
		expectClusterReady(restored, instances, 20*time.Minute)
		expectQuorum(restored)

		dataSource, err := kubectl("get", "pvc", restored+"-1", "-n", testNamespace, "-o", "jsonpath={.spec.dataSource.name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(dataSource).To(Equal(backup), "the primary volume was not created from the snapshot")
		groupName, err := clusterField(restored, "{.status.groupReplication.groupName}")
		Expect(err).NotTo(HaveOccurred())
		Expect(groupName).NotTo(BeEmpty())
		Expect(groupName).NotTo(Equal(sourceGroupName), "recovery rejoined the source's group")

		By("holding the snapshot's data on every member of the new group")
		for i := 1; i <= instances; i++ {
			expectRowOn(fmt.Sprintf("%s-%d", restored, i), "1")
		}
		_, err = sql(clusterPrimary(restored), "INSERT INTO gr_notes VALUES (3, 'recovered-writes');")
		Expect(err).NotTo(HaveOccurred(), "the recovered group does not take writes")
	})
})

// grSnapshotClusterManifest is an archiving Group Replication cluster on
// csi-hostpath-sc volumes with a volumeSnapshot section. bootstrap, when
// empty, is an initdb.
func grSnapshotClusterManifest(name string, instances int, bootstrap string) string {
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
  replication:
    mode: groupReplication
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
`, name, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters, bootstrap,
		objectStoreYAML("    "))
}
