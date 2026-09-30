//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs validate the purge gate's replica floor (design 034): with
// purgeAfterArchive on, the primary purges a binary log only once it is
// archived AND every expected instance has applied the transactions in it.
//
// One cluster walks through every case in order, on the async topology for
// MySQL and MariaDB and on Group Replication:
//
//   - with every replica caught up, archived binlogs are purged;
//   - a fenced (down) replica holds the files it has not applied, is reported
//     in status.continuousArchiving.purgeHeldBy, catches up from the primary's
//     binlogs once unfenced (no re-clone, no broken replication), and the purge
//     then resumes;
//   - a scaled-down instance stops holding the purge at once, and a fresh
//     instance added afterwards joins and lets the purge move on;
//   - after a switchover the new primary purges on the same rules;
//   - point-in-time recovery still reaches targets whose binlogs the primary has
//     purged, both inside one server's segment and across the switchover,
//     replaying exactly up to the target.
//
// Every recovery is checked by data: the row count and id sum at the target
// must match exactly, so a missing or an extra transaction both fail.

// purgeFlavor adapts the scenario to a flavor and topology.
type purgeFlavor struct {
	slug string
	// exec runs SQL on a pod as the given user.
	exec func(pod, user, password, database, sql string) (string, error)
	// flush reads the primary's executed GTID position, then rotates its binlog
	// (see flushBinaryLogs for why in that order).
	flush func(cluster, primary, password string) string
	// covers waits for the archive to cover a GTID position.
	covers func(cluster, want string, timeout time.Duration)
	// source renders the source cluster with the purge gate on.
	source func(name string, instances int) string
	// recovery renders a single-instance cluster recovered to targetGTID.
	recovery func(name, backup, source, targetGTID string) string
	// groupReplication skips the cases that are async-only in this suite
	// (scale-down and switchover are exercised on the async topologies).
	groupReplication bool
}

func init() {
	version := archiveVersions()[0]

	Describe("Binlog purge gate - MySQL", Ordered, Label("flavor"), func() {
		purgeGateSpecs(purgeFlavor{
			slug:   "mysql",
			exec:   mysqlExec,
			flush:  flushBinaryLogs,
			covers: expectArchiveCovers,
			source: func(name string, instances int) string {
				return purgeGateManifest(continuousArchivingClusterManifest(name, version, instances))
			},
			recovery: func(name, backup, _, target string) string {
				return pitrRecoveryClusterManifest(name, version, backup, target)
			},
		})
	})

	Describe("Binlog purge gate - MariaDB", Ordered, Label("flavor", "mariadb"), func() {
		purgeGateSpecs(purgeFlavor{
			slug:   "mariadb",
			exec:   mariadbExec,
			flush:  flushMariadbBinaryLogs,
			covers: expectMariadbArchiveCovers,
			source: func(name string, instances int) string {
				manifest := strings.Replace(mariadbContinuousArchivingClusterManifest(name),
					"  instances: 1\n", fmt.Sprintf("  instances: %d\n", instances), 1)
				return purgeGateManifest(manifest)
			},
			recovery: mariadbPITRClusterManifest,
		})
	})

	Describe("Binlog purge gate - Group Replication", Ordered, Label("flavor", "heavy"), func() {
		purgeGateSpecs(purgeFlavor{
			slug:   "gr",
			exec:   mysqlExec,
			flush:  flushBinaryLogs,
			covers: expectArchiveCovers,
			source: func(name string, instances int) string {
				return purgeGateManifest(grArchivingClusterManifest(name, version, instances))
			},
			recovery: func(name, backup, _, target string) string {
				return grPitrRecoveryClusterManifest(name, version, 1, backup, target)
			},
			groupReplication: true,
		})
	})
}

// purgeGateManifest turns the purge gate on in an archiving cluster manifest.
func purgeGateManifest(manifest string) string {
	const anchor = "    continuousArchiving:\n      enabled: true\n"
	Expect(manifest).To(ContainSubstring(anchor), "manifest has no continuousArchiving block")
	return strings.Replace(manifest, anchor, anchor+"      purgeAfterArchive: true\n", 1)
}

// recoveryPoint is a PITR target and the ledger content expected at it.
type recoveryPoint struct {
	gtid  string
	count int
	sum   int
}

func purgeGateSpecs(f purgeFlavor) {
	cluster := "purge-" + f.slug
	backup := "purge-" + f.slug + "-base"
	const instances = 3

	var (
		ns, prevNS string
		password   string
		// written is every ledger id committed so far, in order.
		written []int
		// midPoint lies inside binlogs the first primary has purged; lastPoint is
		// after the switchover (async) or at the end (GR).
		midPoint, lastPoint recoveryPoint
	)

	sql := func(pod, query string) (string, error) {
		return f.exec(pod, "app", password, "app", query)
	}
	root := func(pod, query string) (string, error) {
		return f.exec(pod, "root", rootPassword(cluster), "", query)
	}

	// writeRound commits one ledger row and rotates the primary's binlog, so
	// every round lands in its own archivable file.
	writeRound := func(id int) {
		GinkgoHelper()
		primary := clusterPrimary(cluster)
		_, err := sql(primary, fmt.Sprintf("INSERT INTO ledger VALUES (%d)", id))
		Expect(err).NotTo(HaveOccurred(), "insert %d on %s failed", id, primary)
		_, err = root(primary, "FLUSH BINARY LOGS")
		Expect(err).NotTo(HaveOccurred(), "flush after insert %d failed", id)
		written = append(written, id)
	}
	writeRounds := func(from, n int) {
		GinkgoHelper()
		for id := from; id < from+n; id++ {
			writeRound(id)
		}
	}
	// capture pins a recovery point at the primary's current position and waits
	// for the archive to cover it.
	capture := func() recoveryPoint {
		GinkgoHelper()
		primary := clusterPrimary(cluster)
		point := recoveryPoint{gtid: f.flush(cluster, primary, password), count: len(written)}
		for _, id := range written {
			point.sum += id
		}
		Expect(point.gtid).NotTo(BeEmpty(), "recovery point GTID parsed empty")
		f.covers(cluster, point.gtid, 5*time.Minute)
		return point
	}

	binlogs := func(pod string) []string {
		GinkgoHelper()
		out, err := root(pod, "SHOW BINARY LOGS")
		Expect(err).NotTo(HaveOccurred(), "SHOW BINARY LOGS failed on %s", pod)
		var logs []string
		for line := range strings.SplitSeq(out, "\n") {
			if fields := strings.Fields(line); len(fields) > 0 && strings.HasPrefix(fields[0], "binlog.") {
				logs = append(logs, fields[0])
			}
		}
		Expect(logs).NotTo(BeEmpty(), "no binary logs listed on %s: %s", pod, out)
		return logs
	}
	activeBinlog := func(pod string) string {
		GinkgoHelper()
		logs := binlogs(pod)
		return logs[len(logs)-1]
	}
	heldBy := func() []string {
		out, err := clusterField(cluster, "{.status.continuousArchiving.purgeHeldBy[*]}")
		Expect(err).NotTo(HaveOccurred())
		return strings.Fields(out)
	}
	// expectPurgedPast writes rounds until the primary has purged file (and so
	// everything before it). Writing keeps the archiver shipping, which is also
	// what refreshes the replicas' positions in the Cluster status.
	nextID := 10000
	expectPurgedPast := func(file string, why string) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			writeRound(nextID)
			nextID++
			primary := clusterPrimary(cluster)
			g.Expect(binlogs(primary)).NotTo(ContainElement(file),
				"%s: %s still on %s (purgeHeldBy=%v, %s)", why, file, primary, heldBy(), archivingDiagnostics(cluster))
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())
	}
	expectRows := func(pod string, ids ...int) {
		GinkgoHelper()
		list := make([]string, len(ids))
		for i, id := range ids {
			list[i] = strconv.Itoa(id)
		}
		Eventually(func(g Gomega) {
			out, err := sql(pod, "SELECT COUNT(*) FROM ledger WHERE id IN ("+strings.Join(list, ",")+")")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(parseSingleValue(out)).To(Equal(strconv.Itoa(len(ids))),
				"%s has not applied ledger ids %v", pod, ids)
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	}
	expectReplicationHealthy := func() {
		GinkgoHelper()
		broken, err := clusterField(cluster, "{.status.replicationBrokenInstances[*]}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(broken)).To(BeEmpty(), "replication broken on %s", broken)
	}
	aReplica := func() string {
		GinkgoHelper()
		primary := clusterPrimary(cluster)
		for i := 1; i <= instances; i++ {
			if name := fmt.Sprintf("%s-%d", cluster, i); name != primary {
				return name
			}
		}
		Fail("no replica found")
		return ""
	}
	ids := func(from, n int) []int {
		out := make([]int, n)
		for i := range out {
			out[i] = from + i
		}
		return out
	}

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("purge-" + f.slug)
		setupObjectStore()
		DeferCleanup(teardownObjectStore)
		setupS3Client()
		DeferCleanup(teardownS3Client)

		By("creating a three-instance archiving cluster with the purge gate on")
		applyManifest(cluster, f.source(cluster, instances))
		DeferCleanup(func() { deleteManifest(cluster, f.source(cluster, instances)) })
		expectClusterReady(cluster, instances, 20*time.Minute)
		password = appPassword(cluster)

		_, err := sql(clusterPrimary(cluster), "CREATE TABLE ledger (id INT PRIMARY KEY)")
		Expect(err).NotTo(HaveOccurred(), "failed to create the ledger table")

		By("taking the base backup every recovery below starts from")
		applyManifest(backup, backupManifest(backup, cluster))
		DeferCleanup(func() { deleteManifest(backup, backupManifest(backup, cluster)) })
		expectBackupCompleted(backup, 8*time.Minute)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})

	It("purges archived binlogs once every instance has applied them", func() {
		primary := clusterPrimary(cluster)

		By("writing and rotating a first batch, pinning a recovery point in the middle")
		writeRounds(1, 4)
		midFile := activeBinlog(primary)
		midPoint = capture()
		writeRounds(5, 4)
		capture()

		By("waiting for the primary to purge the file holding the recovery point")
		expectPurgedPast(midFile, "every replica applied it")

		By("checking every replica still has every row")
		for i := 1; i <= instances; i++ {
			expectRows(fmt.Sprintf("%s-%d", cluster, i), ids(1, 8)...)
		}
		expectReplicationHealthy()
	})

	It("keeps the binlogs a fenced instance has not applied, then lets it catch up", func() {
		replica := aReplica()
		primary := clusterPrimary(cluster)

		By(fmt.Sprintf("fencing %s, which stops its server", replica))
		fence(replica)
		expectFenced(cluster, replica)
		heldFrom := activeBinlog(primary)

		By("writing and rotating while it is down")
		writeRounds(101, 6)
		capture()

		By("waiting for the primary to report the fenced instance as holding the purge")
		Eventually(func(g Gomega) {
			writeRound(nextID)
			nextID++
			g.Expect(heldBy()).To(ContainElement(replica), archivingDiagnostics(cluster))
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())

		By("checking the primary keeps every file written since the fence while it keeps archiving")
		Consistently(func(g Gomega) {
			writeRound(nextID)
			nextID++
			g.Expect(binlogs(primary)).To(ContainElement(heldFrom),
				"%s was purged while %s still needed it", heldFrom, replica)
		}, 60*time.Second, 10*time.Second).Should(Succeed())

		By("unfencing it and checking it catches up from the primary's binlogs")
		unfence(replica)
		expectClusterReady(cluster, instances, 10*time.Minute)
		expectRows(replica, ids(101, 6)...)
		expectReplicationHealthy()

		By("waiting for the purge to move past the files it held")
		// purgeHeldBy may still name it for a moment: the operator's snapshot of
		// its position trails the newest files, which is the normal steady state.
		expectPurgedPast(heldFrom, "the unfenced instance caught up")
	})

	It("stops holding the purge for a scaled-down instance", func() {
		if f.groupReplication {
			Skip("scale-down is exercised on the async topologies")
		}
		removed := fmt.Sprintf("%s-%d", cluster, instances)
		if clusterPrimary(cluster) == removed {
			By("moving the primary off the instance the scale-down removes")
			requestSwitchoverIn(testNamespace, cluster, cluster+"-1")
			Eventually(func() string { return clusterPrimary(cluster) },
				e2eTimeout(5*time.Minute), 5*time.Second).Should(Equal(cluster + "-1"))
			expectClusterReady(cluster, instances, 10*time.Minute)
		}
		primary := clusterPrimary(cluster)

		By(fmt.Sprintf("fencing %s so it holds the purge", removed))
		fence(removed)
		expectFenced(cluster, removed)
		heldFrom := activeBinlog(primary)
		writeRounds(201, 4)
		Eventually(func(g Gomega) {
			writeRound(nextID)
			nextID++
			g.Expect(heldBy()).To(ContainElement(removed), archivingDiagnostics(cluster))
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())

		By("scaling down to two instances")
		scaleInstances(cluster, instances-1)
		expectClusterReady(cluster, instances-1, 10*time.Minute)

		By("waiting for the purge to move past what the removed instance held")
		expectPurgedPast(heldFrom, "the instance holding it was removed")
		Expect(heldBy()).NotTo(ContainElement(removed))

		By("scaling back up with a fresh volume and checking the new instance joins")
		// Scale-down keeps a bootstrapped volume; reusing it would bring the old
		// data back, which the primary no longer has the binlogs for.
		_, err := kubectl("delete", "pvc", removed, "-n", testNamespace, "--timeout=3m")
		Expect(err).NotTo(HaveOccurred(), "failed to delete the retained volume of %s", removed)
		scaleInstances(cluster, instances)
		expectClusterReady(cluster, instances, 20*time.Minute)
		expectRows(removed, written...)
		expectReplicationHealthy()

		By("checking the purge moves on once the new instance has caught up")
		expectPurgedPast(activeBinlog(clusterPrimary(cluster)), "the new instance caught up")
	})

	It("keeps purging on the new primary after a switchover", func() {
		if f.groupReplication {
			Skip("switchover is exercised on the async topologies")
		}
		oldPrimary := clusterPrimary(cluster)
		target := aReplica()

		By(fmt.Sprintf("switching over from %s to %s", oldPrimary, target))
		requestSwitchoverIn(testNamespace, cluster, target)
		Eventually(func() string { return clusterPrimary(cluster) },
			e2eTimeout(5*time.Minute), 5*time.Second).Should(Equal(target))
		expectClusterReady(cluster, instances, 10*time.Minute)

		By("writing on the new primary and waiting for it to purge")
		first := binlogs(target)[0]
		writeRounds(301, 4)
		expectPurgedPast(first, "the new primary archived it and every instance applied it")
		expectRows(oldPrimary, ids(301, 4)...)
		expectReplicationHealthy()
	})

	It("recovers to points inside binlogs the primary purged", func() {
		lastPoint = capture()

		By("writing rows past the last recovery point, which no recovery may contain")
		writeRounds(900, 3)
		capture()

		for i, point := range []recoveryPoint{midPoint, lastPoint} {
			name := fmt.Sprintf("purge-%s-pitr-%d", f.slug, i+1)
			manifest := f.recovery(name, backup, cluster, point.gtid)
			By(fmt.Sprintf("recovering %s to %s", name, point.gtid))
			applyManifest(name, manifest)
			expectClusterReady(name, 1, 20*time.Minute)

			restored := clusterPrimary(name)
			Eventually(func(g Gomega) {
				out, err := f.exec(restored, "app", password, "app", "SELECT COUNT(*), COALESCE(SUM(id), 0) FROM ledger")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.Fields(parseSingleValue(out))).To(Equal(
					[]string{strconv.Itoa(point.count), strconv.Itoa(point.sum)}),
					"recovered ledger (count, sum) does not match the target")
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

			deleteManifest(name, manifest)
		}
	})
}
