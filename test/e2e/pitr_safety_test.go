//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// PITR archive safety end to end (design 039). Each spec reproduces a way the
// archive or the backup choice used to make point-in-time recovery either fail
// or, worse, silently recover a state the cluster never served, and checks by
// data that it now recovers what was served or fails closed with a reason.

// archiveIndexKey is the rclone key of a cluster's archive index.
func archiveIndexKey(cluster string) string {
	return fmt.Sprintf("%s:%s/%s/binlogs/_index.json", s3Remote, objectStoreBucket, cluster)
}

// rewriteArchiveIndex applies mutate to the archive index and writes it back,
// the way a writer that lost a race or a stale writer would, and waits until
// the write reads back.
func rewriteArchiveIndex(cluster string, mutate func(*objectstore.ArchiveIndex), check func(objectstore.ArchiveIndex) bool) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		idx, err := readArchiveIndex(cluster)
		g.Expect(err).NotTo(HaveOccurred())
		if !check(idx) {
			mutate(&idx)
			raw, err := json.Marshal(idx)
			g.Expect(err).NotTo(HaveOccurred())
			s3Pipe(string(raw), archiveIndexKey(cluster))
		}
		idx, err = readArchiveIndex(cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(check(idx)).To(BeTrue(), "the index rewrite did not stick")
	}, e2eTimeout(2*time.Minute), 3*time.Second).Should(Succeed())
}

// backupField reads a jsonpath of a Backup.
func backupField(name, jsonpath string) (string, error) {
	return kubectl("get", "backup", name, "-n", testNamespace, "-o", "jsonpath="+jsonpath)
}

// rawRecoveryManifest bootstraps a one-instance cluster straight from the
// source's object-store prefix, letting the operator choose the base backup.
func rawRecoveryManifest(f forkFlavor, name, source string, target pitrTarget) string {
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
      source: %[7]s
%[8]s  externalClusters:
    - name: %[7]s
%[9]s
`, name, testNamespace, f.flavorLine(), f.image(), e2eInstanceResources, e2eMySQLParameters,
		source, target.yaml(), objectStoreYAML("      "))
}

// expectPlanBlocked applies a recovery cluster whose plan the operator must
// refuse, and waits for the reason.
func expectPlanBlocked(name, manifest, needle string) {
	GinkgoHelper()
	applyManifest(name, manifest)
	defer deleteCluster(name)
	Eventually(func(g Gomega) {
		phase, err := clusterField(name, "{.status.phase}")
		g.Expect(err).NotTo(HaveOccurred())
		reason, err := clusterField(name, "{.status.phaseReason}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(phase).To(Equal("Blocked"))
		g.Expect(reason).To(ContainSubstring(needle))
	}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
}

// restartPod deletes an instance Pod so its instance manager starts afresh.
func restartPod(cluster, pod string, instances int) {
	GinkgoHelper()
	By(fmt.Sprintf("restarting %s", pod))
	_, err := kubectl("delete", "pod", pod, "-n", testNamespace, "--wait=false")
	Expect(err).NotTo(HaveOccurred())
	expectClusterRecovers(cluster, instances, e2eTimeout(10*time.Minute))
}

// lostIndexWriteSpec: a file whose index write failed after its status write
// was never indexed again, and recovery replayed past it. The repair folds it
// back in from the manifest the next time the archiver starts.
func lostIndexWriteSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	first := idRange(1, 10)
	second := idRange(11, 20)
	var primary, dropped string

	It("loses the index entry of an archived file", func() {
		primary = clusterPrimary(cluster)
		writeForkRows(f, primary, s.password, "first", first)
		f.covers(cluster, f.flush(cluster, primary, s.password), 5*time.Minute)

		idx, err := readArchiveIndex(cluster)
		Expect(err).NotTo(HaveOccurred())
		segs := segmentsOf(idx, primary)
		Expect(segs).NotTo(BeEmpty())
		files := segs[len(segs)-1].Binlogs
		Expect(len(files)).To(BeNumerically(">=", 2), "need a file to drop that is not the segment's only one")
		dropped = files[0]

		By(fmt.Sprintf("dropping %s from the index, as a lost write would", dropped))
		rewriteArchiveIndex(cluster, func(idx *objectstore.ArchiveIndex) {
			for i := range idx.Segments {
				if idx.Segments[i].InstanceName == primary {
					idx.Segments[i].Binlogs = slices.DeleteFunc(idx.Segments[i].Binlogs,
						func(n string) bool { return n == dropped })
				}
			}
		}, func(idx objectstore.ArchiveIndex) bool {
			for _, seg := range segmentsOf(idx, primary) {
				if slices.Contains(seg.Binlogs, dropped) {
					return false
				}
			}
			return true
		})
	})

	It("folds the file back into the index once the archiver restarts", func() {
		restartPod(cluster, primary, s.instances)
		primary = clusterPrimary(cluster)
		writeForkRows(f, primary, s.password, "second", second)
		f.covers(cluster, f.flush(cluster, primary, s.password), 5*time.Minute)
		Eventually(func(g Gomega) {
			idx, err := readArchiveIndex(cluster)
			g.Expect(err).NotTo(HaveOccurred())
			var names []string
			for _, seg := range idx.Segments {
				names = append(names, seg.Binlogs...)
			}
			g.Expect(names).To(ContainElement(dropped), "the archived file must be indexed again")
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())
	})

	It("recovers every row to latest", func() {
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{}, ids(first, second))
	})
}

// cloneGapSpec: a replica is re-cloned while the primary holds transactions it
// has not archived yet; the primary then dies with a dead tail, so it is
// diverged and its drain can never ship the stretch the clone point covers.
// The successor archives from after its clone point, leaving a gap. Recovery
// used to replay straight over it (MySQL) and recover a state that never
// existed; it now fails closed, the cluster reports ArchiveGap and takes a
// backup past it, and recovery from that backup is whole.
func cloneGapSpec(f forkFlavor, cluster string) {
	// No forced rotation: the gap's rows stay in the primary's active binlog.
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 3600}
	s.setUp()

	common := idRange(1, 5)
	hole := idRange(51, 55)
	dead := idRange(101, 103)
	live := idRange(201, 205)
	var primary, replica, beforeGap string

	It("leaves a stretch only the lost primary and the clone point ever held", func() {
		primary = clusterPrimary(cluster)
		replica = otherInstance(cluster, s.instances, primary)
		writeForkRows(f, primary, s.password, "common", common)
		waitReplicated(f, replica, s.password, common)
		f.covers(cluster, f.flush(cluster, primary, s.password), 5*time.Minute)
		beforeGap = recoveryStamp()
		waitPast(beforeGap)

		By("writing rows the archive does not get yet, then re-cloning the replica past them")
		writeForkRows(f, primary, s.password, "hole", hole)
		reinitAndRecover(cluster, replica, s.instances)
		waitReplicated(f, replica, s.password, hole)

		By("freezing the re-cloned replica and committing a dead tail on the primary")
		fence(replica)
		expectFenced(cluster, replica)
		writeForkRows(f, primary, s.password, "dead", dead)

		promoteFrozenReplica(f, cluster, primary, replica, s.password)
		writeForkRows(f, replica, s.password, "live", live)
		f.covers(cluster, f.flush(cluster, replica, s.password), 5*time.Minute)

		By(fmt.Sprintf("letting %s return: it is diverged, so its drain never ships the gap", primary))
		unfence(primary)
		expectDiverged(cluster, primary)
	})

	It("reports the gap and takes a base backup past it", func() {
		Eventually(func(g Gomega) {
			gaps, err := clusterField(cluster, "{.status.continuousArchiving.gaps[*]}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(gaps)).NotTo(BeEmpty(), "the archive gap must be reported")
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())
		By("waiting out the grace a former primary's drain gets")
		Eventually(func(g Gomega) {
			got, err := clusterField(cluster, "{.status.conditions[?(@.type=='ArchiveGap')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal("True"))
		}, e2eTimeout(15*time.Minute), 15*time.Second).Should(Succeed())
		expectWarningEvent(cluster, "ArchiveGap")

		var gapBackup string
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "backups", "-n", testNamespace,
				"-l", "mysql.cnmsql.co/archive-gap-backup=true", "-o", "jsonpath={.items[*].metadata.name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(out)).To(HaveLen(1))
			gapBackup = strings.Fields(out)[0]
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		expectBackupCompleted(gapBackup, 8*time.Minute)
		Eventually(func(g Gomega) {
			reason, err := clusterField(cluster, "{.status.conditions[?(@.type=='ArchiveGap')].reason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("GapBehindNewestBackup"))
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())

		By("recovering to latest from the backup taken past the gap")
		restoreAndExpectRows(f, cluster+"-pastgap", gapBackup, s.password, pitrTarget{}, ids(common, hole, live))
	})

	It("refuses to recover across the gap", func() {
		needle := "missing transactions"
		if f.mariadb {
			needle = "forked or has a gap"
		}
		expectRestoreFails(f, cluster+"-across", s.backup, pitrTarget{}, needle)
	})

	It("recovers a time before the gap", func() {
		restoreAndExpectRows(f, cluster+"-before", s.backup, s.password, pitrTarget{Time: beforeGap}, common)
		reinitAndRecover(cluster, primary, s.instances)
	})
}

// deadTailBackupSpec: the primary takes a base backup while its replica is
// frozen, then dies before rotating, so its dead tail never reaches the
// archive and no fork is ever recorded. Recovery from that backup to latest
// used to replay the survivor's history on top of the dead tail. The operator
// now judges the backup's anchor, marks it, and recovery fails closed; a
// recovery that lets the operator choose skips it.
func deadTailBackupSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 3600}
	s.setUp()

	common := idRange(1, 5)
	dead := idRange(101, 103)
	live := idRange(201, 205)
	deadBackup := cluster + "-deadtail"
	var primary, replica string

	It("takes a backup holding a tail the archive never receives", func() {
		primary = clusterPrimary(cluster)
		replica = otherInstance(cluster, s.instances, primary)
		writeForkRows(f, primary, s.password, "common", common)
		waitReplicated(f, replica, s.password, common)
		f.covers(cluster, f.flush(cluster, primary, s.password), 5*time.Minute)

		fence(replica)
		expectFenced(cluster, replica)
		writeForkRows(f, primary, s.password, "dead", dead)
		applyManifest(deadBackup, primaryBackupManifest(deadBackup, cluster))
		expectBackupCompleted(deadBackup, 8*time.Minute)

		promoteFrozenReplica(f, cluster, primary, replica, s.password)
		writeForkRows(f, replica, s.password, "live", live)
		f.covers(cluster, f.flush(cluster, replica, s.password), 5*time.Minute)
	})

	It("records the backup's anchor and marks it on a dead branch", func() {
		anchor, err := backupField(deadBackup, "{.status.endGTID}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(anchor)).NotTo(BeEmpty(), "a physical backup must record its anchor")
		Eventually(func(g Gomega) {
			got, err := backupField(deadBackup, "{.status.conditions[?(@.type=='DeadBranch')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal("True"))
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())
		expectWarningEvent(cluster, "DeadBranch")
		Eventually(func(g Gomega) {
			idx, err := readArchiveIndex(cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(idx.Disowned).NotTo(BeNil(), "the archive must record the dead tail the backup holds")
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())
		genesis, err := backupField(s.backup, "{.status.conditions[?(@.type=='DeadBranch')].status}")
		Expect(err).NotTo(HaveOccurred())
		Expect(genesis).NotTo(Equal("True"), "the genesis backup is on the surviving timeline")
	})

	It("refuses a latest recovery from it", func() {
		expectRestoreFails(f, cluster+"-refused", deadBackup, pitrTarget{}, "dead branch")
	})

	It("skips it when the operator chooses the backup", func() {
		By("recovering from the raw object store, newest usable backup first")
		name := cluster + "-raw"
		applyManifest(name, rawRecoveryManifest(f, name, cluster, pitrTarget{}))
		defer deleteCluster(name)
		expectClusterReady(name, 1, e2eTimeout(20*time.Minute))
		expectForkRows(f, clusterPrimary(name), s.password, ids(common, live),
			"raw recovery must start from the backup on the surviving timeline")
		unfence(primary)
		expectDiverged(cluster, primary)
		reinitAndRecover(cluster, primary, s.instances)
	})
}

// returnAfterSuccessorSpec: the old primary held an archived dead branch and
// comes back exactly when its successor goes down, so no live primary is
// there to compare it with. It used to be a failover candidate that would
// resurrect its dead branch and make every later backup unrecoverable; the
// archive's disowned set now marks it diverged and failover refuses it.
func returnAfterSuccessorSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	common := idRange(1, 5)
	dead := idRange(101, 103)
	live := idRange(201, 205)
	var primary, replica string

	It("leaves an archived dead branch on the old primary", func() {
		primary = clusterPrimary(cluster)
		replica = otherInstance(cluster, s.instances, primary)
		writeForkRows(f, primary, s.password, "common", common)
		waitReplicated(f, replica, s.password, common)
		fence(replica)
		expectFenced(cluster, replica)
		writeForkRows(f, primary, s.password, "dead", dead)
		f.covers(cluster, f.flush(cluster, primary, s.password), 5*time.Minute)

		promoteFrozenReplica(f, cluster, primary, replica, s.password)
		writeForkRows(f, replica, s.password, "live", live)
		f.covers(cluster, f.flush(cluster, replica, s.password), 5*time.Minute)
		expectForkOn(cluster, primary)
		if !f.mariadb {
			Eventually(func(g Gomega) {
				out, err := clusterField(cluster, "{.status.continuousArchiving.disownedGTIDs}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).NotTo(BeEmpty())
			}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())
		}
	})

	It("never promotes the old primary when it returns as its successor dies", func() {
		By(fmt.Sprintf("taking %s down and bringing %s back at once", replica, primary))
		fence(replica)
		unfence(primary)
		expectDiverged(cluster, primary)
		Consistently(func(g Gomega) {
			p, err := clusterField(cluster, "{.status.currentPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(p)).NotTo(Equal(primary), "the instance holding the dead branch was promoted")
			target, err := clusterField(cluster, "{.status.targetPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(target)).NotTo(Equal(primary), "the instance holding the dead branch was elected")
		}, e2eTimeout(2*time.Minute), 10*time.Second).Should(Succeed())
	})

	It("recovers once the successor returns", func() {
		unfence(replica)
		waitPrimaryIs(f, cluster, replica, s.password)
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{}, ids(common, live))
		reinitAndRecover(cluster, primary, s.instances)
	})
}

// staleWriterSpec reproduces the demoted primary that finishes an archive pass
// after its successor started archiving: it judged the successor's segment
// against its own executed set and recorded the successor's transactions as
// a dead branch, refusing every backup taken after the failover. Each primary
// now stamps its generation into the index, and a writer behind it does not
// judge. The spec plays the stale writer by raising the index's generation
// past the current primary's and adding a segment holding transactions the
// primary has never executed.
func staleWriterSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	const foreign = "0b5e7f00-0000-11e1-9e33-c80aa9429562"
	rows := idRange(1, 5)
	more := idRange(6, 10)
	var primary, replica string
	var generation int64

	It("stamps each primary's generation into the archive", func() {
		primary = clusterPrimary(cluster)
		replica = otherInstance(cluster, s.instances, primary)
		before, err := strconv.ParseInt(mustClusterFieldOr(cluster, "{.status.currentPrimaryGeneration}", "0"), 10, 64)
		Expect(err).NotTo(HaveOccurred())

		failOver(f, cluster, primary, replica, s.password, false)
		unfence(primary)
		expectClusterRecovers(cluster, s.instances, e2eTimeout(20*time.Minute))
		generation, err = strconv.ParseInt(mustClusterField(cluster, "{.status.currentPrimaryGeneration}"), 10, 64)
		Expect(err).NotTo(HaveOccurred())
		Expect(generation).To(Equal(before+1), "a promotion raises the generation by one")

		writeForkRows(f, replica, s.password, "rows", rows)
		f.covers(cluster, f.flush(cluster, replica, s.password), 5*time.Minute)
		Eventually(func(g Gomega) {
			idx, err := readArchiveIndex(cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(idx.Generation).To(Equal(generation))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("does not judge an index a newer generation wrote", func() {
		rewriteArchiveIndex(cluster, func(idx *objectstore.ArchiveIndex) {
			idx.Generation = generation + 100
			idx.Segments = append(idx.Segments, objectstore.ArchiveSegment{
				ServerUUID: foreign, InstanceName: "e2e-newer-primary",
				Binlogs: []string{"binlog.000001"}, GTIDSet: foreign + ":1-50",
			})
		}, func(idx objectstore.ArchiveIndex) bool {
			return idx.Generation > generation && len(segmentsOf(idx, "e2e-newer-primary")) == 1
		})
		writeForkRows(f, replica, s.password, "more", more)
		f.flush(cluster, replica, s.password)
		Consistently(func(g Gomega) {
			idx, err := readArchiveIndex(cluster)
			g.Expect(err).NotTo(HaveOccurred())
			for _, seg := range segmentsOf(idx, "e2e-newer-primary") {
				g.Expect(seg.Fork).To(BeNil(), "a primary behind the index judged a newer primary's segment")
			}
		}, e2eTimeout(90*time.Second), 10*time.Second).Should(Succeed())
	})

	It("judges it again once it is the newest writer", func() {
		rewriteArchiveIndex(cluster, func(idx *objectstore.ArchiveIndex) {
			idx.Generation = generation
		}, func(idx objectstore.ArchiveIndex) bool { return idx.Generation == generation })
		f.flush(cluster, replica, s.password)
		seg := expectForkOn(cluster, "e2e-newer-primary")
		contains, err := replication.GTIDContains(seg.Fork.GTIDSet, foreign+":1-50")
		Expect(err).NotTo(HaveOccurred())
		Expect(contains).To(BeTrue(), "the current primary must judge what it never executed")

		By("removing the injected segment")
		rewriteArchiveIndex(cluster, func(idx *objectstore.ArchiveIndex) {
			idx.Segments = slices.DeleteFunc(idx.Segments, func(s objectstore.ArchiveSegment) bool {
				return s.InstanceName == "e2e-newer-primary"
			})
			idx.Disowned = nil
		}, func(idx objectstore.ArchiveIndex) bool { return len(segmentsOf(idx, "e2e-newer-primary")) == 0 })
	})
}

// mustClusterFieldOr is mustClusterField with a default for an empty field.
func mustClusterFieldOr(cluster, jsonpath, fallback string) string {
	GinkgoHelper()
	if out := strings.TrimSpace(mustClusterField(cluster, jsonpath)); out != "" {
		return out
	}
	return fallback
}

// recoveryTargetSpec: recovery used to take the newest backup whatever the
// target said, so a targetTime before it silently recovered a later state, and
// a targetTime past the archive silently recovered less.
func recoveryTargetSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	before := idRange(1, 5)
	after := idRange(6, 10)
	later := cluster + "-later"
	var primary, stamp string

	It("takes a second backup after a recorded point in time", func() {
		primary = clusterPrimary(cluster)
		writeForkRows(f, primary, s.password, "before", before)
		f.covers(cluster, f.flush(cluster, primary, s.password), 5*time.Minute)
		stamp = recoveryStamp()
		waitPast(stamp)
		writeForkRows(f, primary, s.password, "after", after)
		f.covers(cluster, f.flush(cluster, primary, s.password), 5*time.Minute)
		applyManifest(later, backupManifest(later, cluster))
		expectBackupCompleted(later, 8*time.Minute)
	})

	It("chooses a backup that precedes the target", func() {
		name := cluster + "-raw"
		applyManifest(name, rawRecoveryManifest(f, name, cluster, pitrTarget{Time: stamp}))
		defer deleteCluster(name)
		expectClusterReady(name, 1, e2eTimeout(20*time.Minute))
		expectForkRows(f, clusterPrimary(name), s.password, before,
			"a recovery to a time before the newest backup must not start from it")
	})

	It("refuses a named backup that completed after the target", func() {
		name := cluster + "-named"
		expectPlanBlocked(name, forkRecoveryManifest(f, name, later, pitrTarget{Time: stamp}), "is before backup")
	})

	It("refuses a targetTime past what the archive holds", func() {
		future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		expectRestoreFails(f, cluster+"-future", s.backup, pitrTarget{Time: future}, "beyond archive coverage")
	})
}

var _ = Describe("PITR safety: lost index write", Ordered, Label("feature", "pitr"), func() {
	lostIndexWriteSpec(mysqlForkFlavor, "pitr-lostidx")
})

var _ = Describe("PITR safety: clone-point gap", Ordered, Label("feature", "pitr"), func() {
	cloneGapSpec(mysqlForkFlavor, "pitr-gap")
})

var _ = Describe("PITR safety: dead tail in a backup", Ordered, Label("feature", "pitr"), func() {
	deadTailBackupSpec(mysqlForkFlavor, "pitr-deadtail")
})

var _ = Describe("PITR safety: old primary returns as its successor dies", Ordered, Label("feature", "pitr"), func() {
	returnAfterSuccessorSpec(mysqlForkFlavor, "pitr-return")
})

var _ = Describe("PITR safety: stale archive writer", Ordered, Label("feature", "pitr"), func() {
	staleWriterSpec(mysqlForkFlavor, "pitr-stale")
})

var _ = Describe("PITR safety: recovery targets and backup choice", Ordered, Label("feature", "pitr"), func() {
	recoveryTargetSpec(mysqlForkFlavor, "pitr-target")
})

var _ = Describe("MariaDB PITR safety: lost index write", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	lostIndexWriteSpec(mariadbForkFlavor, "mdb-pitr-lostidx")
})

var _ = Describe("MariaDB PITR safety: clone-point gap", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	cloneGapSpec(mariadbForkFlavor, "mdb-pitr-gap")
})

var _ = Describe("MariaDB PITR safety: dead tail in a backup", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	deadTailBackupSpec(mariadbForkFlavor, "mdb-pitr-deadtail")
})

var _ = Describe("MariaDB PITR safety: old primary returns as its successor dies", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	returnAfterSuccessorSpec(mariadbForkFlavor, "mdb-pitr-return")
})

var _ = Describe("MariaDB PITR safety: recovery targets and backup choice", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	recoveryTargetSpec(mariadbForkFlavor, "mdb-pitr-target")
})
