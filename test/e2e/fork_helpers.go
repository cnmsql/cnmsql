//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// The fork specs (design 038) build a dead branch on purpose: freeze a replica
// by fencing it, commit on the primary what the replica will never receive, take
// the primary down and hold it down by fencing it too, then unfence the replica
// so the operator promotes it. Whatever the old primary archived past that point
// is disowned by the surviving timeline. Every spec then checks, by data, that
// point-in-time recovery reproduces what the live cluster actually served.

// forkFlavor is what the fork specs need to know about an engine flavor.
type forkFlavor struct {
	name    string
	mariadb bool
	exec    func(pod, user, password, database, sql string) (string, error)
}

var (
	mysqlForkFlavor   = forkFlavor{name: "mysql", exec: mysqlExec}
	mariadbForkFlavor = forkFlavor{name: "mariadb", mariadb: true, exec: mariadbExec}
)

func (f forkFlavor) image() string {
	if f.mariadb {
		return mariadbImage
	}
	return instanceImageFor(archiveVersions()[0])
}

func (f forkFlavor) flavorLine() string {
	if f.mariadb {
		return "  flavor: mariadb\n"
	}
	return ""
}

// position is the instance's executed position: gtid_executed (MySQL) or
// gtid_current_pos (MariaDB).
func (f forkFlavor) position(pod, password string) string {
	GinkgoHelper()
	if !f.mariadb {
		return gtidExecuted(pod, password)
	}
	out, err := mariadbExec(pod, "app", password, "", "SELECT @@gtid_current_pos")
	Expect(err).NotTo(HaveOccurred(), "reading gtid_current_pos from %s", pod)
	return strings.TrimSpace(out)
}

// flush reads the primary's position, then rotates its binlog so everything up
// to that position lands in an archivable file.
func (f forkFlavor) flush(cluster, primary, password string) string {
	GinkgoHelper()
	if f.mariadb {
		return flushMariadbBinaryLogs(cluster, primary, password)
	}
	return flushBinaryLogs(cluster, primary, password)
}

// covers waits for the archive to cover want.
func (f forkFlavor) covers(cluster, want string, timeout time.Duration) {
	GinkgoHelper()
	if f.mariadb {
		expectMariadbArchiveCovers(cluster, want, timeout)
		return
	}
	expectArchiveCovers(cluster, want, timeout)
}

// forkClusterManifest is an archiving cluster for the fork specs. rpoSeconds
// sets how often the archiver forces a rotation; the drain specs use a long one
// so a tail they leave un-rotated stays un-rotated.
func forkClusterManifest(f forkFlavor, name string, instances, rpoSeconds int) string {
	maxBinlogMB := 1
	if rpoSeconds > 60 {
		maxBinlogMB = 64
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
%[6]s
  mysql:
    binlogFormat: ROW
%[7]s
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
%[8]s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: %[9]d
      maxBinlogSizeMB: %[10]d
`, name, testNamespace, f.flavorLine(), instances, f.image(), e2eInstanceResources, e2eMySQLParameters,
		objectStoreYAML("    "), rpoSeconds, maxBinlogMB)
}

// pitrTarget is a point-in-time recovery target. The zero value is latest
// (`recoveryTarget: {}`).
type pitrTarget struct {
	GTID      string
	Time      string
	Immediate bool
}

func (t pitrTarget) String() string {
	switch {
	case t.GTID != "":
		return "targetGTID " + t.GTID
	case t.Time != "":
		return "targetTime " + t.Time
	case t.Immediate:
		return "targetImmediate"
	default:
		return "latest"
	}
}

func (t pitrTarget) yaml() string {
	switch {
	case t.GTID != "":
		return fmt.Sprintf("      recoveryTarget:\n        targetGTID: %q\n", t.GTID)
	case t.Time != "":
		return fmt.Sprintf("      recoveryTarget:\n        targetTime: %q\n", t.Time)
	case t.Immediate:
		return "      recoveryTarget:\n        targetImmediate: true\n"
	default:
		return "      recoveryTarget: {}\n"
	}
}

// forkRecoveryManifest bootstraps a one-instance cluster from backup, replaying
// the source's archive to target.
func forkRecoveryManifest(f forkFlavor, name, backup string, target pitrTarget) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
%[3]s  instances: 1
  imageName: %[4]s
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
%[8]s  backup:
%[9]s
`, name, testNamespace, f.flavorLine(), f.image(), e2eInstanceResources, e2eMySQLParameters,
		backup, target.yaml(), objectStoreYAML("    "))
}

// primaryBackupManifest is a base backup taken on the primary, whatever the
// replicas' state.
func primaryBackupManifest(name, cluster string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Backup
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  method: xtrabackup
  target: primary
`, name, testNamespace, cluster)
}

// forkTable holds one row per transaction the specs write, tagged with the
// branch it was written on.
const forkTable = "fork_rows"

func createForkTable(f forkFlavor, pod, password string) {
	GinkgoHelper()
	_, err := f.exec(pod, "app", password, "app",
		"CREATE TABLE IF NOT EXISTS "+forkTable+" (id INT PRIMARY KEY, branch VARCHAR(16));")
	Expect(err).NotTo(HaveOccurred(), "creating %s on %s", forkTable, pod)
}

// writeForkRows commits one transaction per id on pod.
func writeForkRows(f forkFlavor, pod, password, branch string, ids []int) {
	GinkgoHelper()
	var sql strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&sql, "INSERT INTO %s VALUES (%d, '%s');", forkTable, id, branch)
	}
	Eventually(func(g Gomega) {
		_, err := f.exec(pod, "app", password, "app", sql.String())
		g.Expect(err).NotTo(HaveOccurred(), "writing %s rows on %s", branch, pod)
	}, e2eTimeout(2*time.Minute), 3*time.Second).Should(Succeed())
}

// forkRowIDs returns the ids in forkTable on pod, ascending, comma-separated.
func forkRowIDs(f forkFlavor, pod, password string) (string, error) {
	out, err := f.exec(pod, "app", password, "app",
		"SELECT COALESCE(GROUP_CONCAT(id ORDER BY id SEPARATOR ','), '') FROM "+forkTable+";")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(parseSingleValue(out)), nil
}

// expectForkRows waits for pod to hold exactly the given ids.
func expectForkRows(f forkFlavor, pod, password string, want []int, why string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		got, err := forkRowIDs(f, pod, password)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got).To(Equal(idList(want)), why)
	}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
}

func idRange(from, to int) []int {
	var out []int
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

func ids(groups ...[]int) []int {
	var out []int
	for _, g := range groups {
		out = append(out, g...)
	}
	slices.Sort(out)
	return out
}

func idList(want []int) string {
	parts := make([]string, len(want))
	for i, id := range want {
		parts[i] = strconv.Itoa(id)
	}
	return strings.Join(parts, ",")
}

// waitPrimaryIs waits until the operator names want as primary and it accepts
// writes.
func waitPrimaryIs(f forkFlavor, cluster, want, password string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		p, err := clusterField(cluster, "{.status.currentPrimary}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(p)).To(Equal(want), "the operator must promote %s", want)
	}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())
	Eventually(func(g Gomega) {
		_, err := f.exec(want, "app", password, "app", "DELETE FROM "+forkTable+" WHERE id = -1;")
		g.Expect(err).NotTo(HaveOccurred(), "%s is not writable yet", want)
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
}

// writablePrimary waits until the instance the operator names primary accepts
// writes, and returns it.
func writablePrimary(f forkFlavor, cluster, password string) string {
	GinkgoHelper()
	var primary string
	Eventually(func(g Gomega) {
		p, err := clusterField(cluster, "{.status.currentPrimary}")
		g.Expect(err).NotTo(HaveOccurred())
		p = strings.TrimSpace(p)
		g.Expect(p).NotTo(BeEmpty(), "%s has no primary", cluster)
		_, err = f.exec(p, "app", password, "app", "DELETE FROM "+forkTable+" WHERE id = -1;")
		g.Expect(err).NotTo(HaveOccurred(), "%s is not writable yet", p)
		primary = p
	}, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())
	return primary
}

// promoteFrozenReplica takes the primary down and holds it down (fencing stops
// its mysqld), then unfences the frozen replica, which the operator promotes
// although it misses everything the primary committed after the freeze.
func promoteFrozenReplica(f forkFlavor, cluster, primary, replica, password string) {
	GinkgoHelper()
	failOver(f, cluster, primary, replica, password, true)
}

// failOver takes the primary down and holds it down, and waits for the
// operator to promote candidate. A frozen candidate is unfenced first.
func failOver(f forkFlavor, cluster, primary, candidate, password string, frozen bool) {
	GinkgoHelper()
	By(fmt.Sprintf("fencing the primary %s to take it down and hold it down", primary))
	fence(primary)
	expectFenced(cluster, primary)
	if frozen {
		By(fmt.Sprintf("unfencing the lagging replica %s so the operator promotes it", candidate))
		unfence(candidate)
	}
	waitPrimaryIs(f, cluster, candidate, password)
}

// waitReplicated waits until pod holds every id in want.
func waitReplicated(f forkFlavor, pod, password string, want []int) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		got, err := forkRowIDs(f, pod, password)
		g.Expect(err).NotTo(HaveOccurred())
		have := map[string]bool{}
		for _, id := range strings.Split(got, ",") {
			have[id] = true
		}
		for _, id := range want {
			g.Expect(have[strconv.Itoa(id)]).To(BeTrue(), "%s has not replicated row %d", pod, id)
		}
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
}

// segmentsOf returns the archive segments an instance wrote.
func segmentsOf(idx objectstore.ArchiveIndex, instance string) []objectstore.ArchiveSegment {
	var out []objectstore.ArchiveSegment
	for _, seg := range idx.Segments {
		if seg.InstanceName == instance {
			out = append(out, seg)
		}
	}
	return out
}

// expectForkOn waits for a fork record on a segment instance wrote, and returns
// that segment.
func expectForkOn(cluster, instance string) objectstore.ArchiveSegment {
	GinkgoHelper()
	var forked objectstore.ArchiveSegment
	Eventually(func(g Gomega) {
		idx, err := readArchiveIndex(cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(idx.ForkCheck).NotTo(BeNil(), "no primary has fork-checked the index yet")
		found := false
		for _, seg := range segmentsOf(idx, instance) {
			if seg.Fork != nil {
				forked, found = seg, true
			}
		}
		g.Expect(found).To(BeTrue(), "no fork recorded on %s's segment yet: %+v", instance, idx.Segments)
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	return forked
}

// expectNoForks waits for a fork check and asserts it recorded nothing.
func expectNoForks(cluster string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		idx, err := readArchiveIndex(cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(idx.ForkCheck).NotTo(BeNil(), "no primary has fork-checked the index yet")
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	idx, err := readArchiveIndex(cluster)
	Expect(err).NotTo(HaveOccurred())
	for _, seg := range idx.Segments {
		Expect(seg.Fork).To(BeNil(), "segment %s (%s) recorded a fork after a clean failover", seg.ServerUUID, seg.InstanceName)
	}
}

// expectCondition waits for a cluster condition to reach status.
func expectCondition(cluster, condition, status string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		got, err := clusterField(cluster, fmt.Sprintf("{.status.conditions[?(@.type=='%s')].status}", condition))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got).To(Equal(status), "condition %s", condition)
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
}

// expectWarningEvent waits for a Warning event with reason on cluster.
func expectWarningEvent(cluster, reason string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		out, err := kubectl("get", "events", "-n", testNamespace,
			"--field-selector", "reason="+reason+",type=Warning,involvedObject.name="+cluster,
			"-o", "jsonpath={.items[*].reason}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(ContainSubstring(reason), "no %s event on %s", reason, cluster)
	}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
}

// expectDiverged waits for instance to be listed in status.divergedInstances.
func expectDiverged(cluster, instance string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		out, err := clusterField(cluster, "{.status.divergedInstances[*]}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.Fields(out)).To(ContainElement(instance), "%s must be marked diverged", instance)
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
}

// reinitAndRecover re-clones a diverged instance and waits for the cluster to
// be whole again.
func reinitAndRecover(cluster, instance string, instances int) {
	GinkgoHelper()
	By(fmt.Sprintf("re-initialising the diverged %s", instance))
	clusterAnnotate(cluster, "cnmsql.cnmsql.co/reinit="+instance)
	expectClusterRecovers(cluster, instances, e2eTimeout(20*time.Minute))
	Eventually(func(g Gomega) {
		out, err := clusterField(cluster, "{.status.divergedInstances}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(out)).To(BeEmpty())
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
}

// restoreAndExpectRows bootstraps a recovery cluster from backup to target and
// asserts, by data, that it holds exactly want. The cluster is deleted once
// checked, so specs can run several recoveries in a row.
func restoreAndExpectRows(f forkFlavor, name, backup, password string, target pitrTarget, want []int) {
	GinkgoHelper()
	By(fmt.Sprintf("recovering %s from %s to %s", name, backup, target))
	manifest := forkRecoveryManifest(f, name, backup, target)
	applyManifest(name, manifest)
	defer deleteCluster(name)
	expectClusterReady(name, 1, e2eTimeout(20*time.Minute))
	expectForkRows(f, clusterPrimary(name), password, want,
		fmt.Sprintf("recovery to %s must hold exactly the rows the cluster served at that point", target))
}

// expectRestoreFails bootstraps a recovery cluster that must not come up, and
// waits until its restore log carries needle.
func expectRestoreFails(f forkFlavor, name, backup string, target pitrTarget, needle string) {
	GinkgoHelper()
	By(fmt.Sprintf("recovering %s from %s to %s, which must fail closed", name, backup, target))
	manifest := forkRecoveryManifest(f, name, backup, target)
	applyManifest(name, manifest)
	defer deleteCluster(name)
	Eventually(func(g Gomega) {
		out, _ := kubectl("logs", "-n", testNamespace, "-l", "mysql.cnmsql.co/bootstrap-instance="+name+"-1",
			"--all-containers", "--tail=-1", "--prefix")
		g.Expect(out).To(ContainSubstring(needle), "the restore must fail with %q", needle)
	}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())
	Consistently(func(g Gomega) {
		p, _ := clusterField(name, "{.status.currentPrimary}")
		g.Expect(strings.TrimSpace(p)).To(BeEmpty(), "a recovery from a dead-branch backup must never come up")
	}, e2eTimeout(30*time.Second), 5*time.Second).Should(Succeed())
}

// recoveryStamp is a recovery timestamp at second precision that falls strictly
// after every write committed before the call. Binlog events carry whole
// seconds and a time target stops at the first event at or after it, so a stamp
// truncated into the same second as those writes would drop them: it rounds up
// to the next second and waits for it instead.
func recoveryStamp() string {
	stamp := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(stamp))
	return stamp.Format(time.RFC3339)
}

// waitPast sleeps until the wall clock is past a recorded RFC3339 instant, so
// writes made afterwards fall after it even at second precision.
func waitPast(stamp string) {
	t, err := time.Parse(time.RFC3339, stamp)
	Expect(err).NotTo(HaveOccurred())
	for time.Now().Before(t.Add(2 * time.Second)) {
		time.Sleep(250 * time.Millisecond)
	}
}

// mariadbReplicating reports whether a MariaDB instance's default replication
// connection has both threads running (status Slave_running).
func mariadbReplicating(pod, rootPassword string) (bool, error) {
	out, err := mariadbExec(pod, "root", rootPassword, "", "SHOW GLOBAL STATUS LIKE 'Slave_running'")
	if err != nil {
		return false, err
	}
	return strings.Contains(strings.ToUpper(out), "ON"), nil
}
