//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// Archive fork detection end to end (design 038). Each spec is written once and
// registered for both flavors at the bottom of this file.

// forkSuite is the per-spec setup every fork spec shares: its own namespace,
// object store and archiving source cluster, and a base backup at genesis.
type forkSuite struct {
	f          forkFlavor
	cluster    string
	instances  int
	rpoSeconds int
	backup     string
	password   string
}

func (s *forkSuite) setUp() {
	var ns, prevNS string
	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace(s.cluster)

		setupObjectStore()
		DeferCleanup(teardownObjectStore)
		setupS3Client()
		DeferCleanup(teardownS3Client)

		By(fmt.Sprintf("creating a %d-instance %s cluster with continuous archiving", s.instances, s.f.name))
		applyManifest(s.cluster, forkClusterManifest(s.f, s.cluster, s.instances, s.rpoSeconds))
		expectClusterReady(s.cluster, s.instances, e2eTimeout(20*time.Minute))
		s.password = appPassword(s.cluster)

		By("taking a base backup at genesis")
		s.backup = s.cluster + "-base"
		applyManifest(s.backup, backupManifest(s.backup, s.cluster))
		expectBackupCompleted(s.backup, 8*time.Minute)

		createForkTable(s.f, clusterPrimary(s.cluster), s.password)
	})
	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
}

// mariadbSeq returns the sequence a MariaDB position reached in domain 0.
func mariadbSeq(pos string) uint64 {
	GinkgoHelper()
	gtids, err := engine.ParseMariaDBPosition(pos)
	Expect(err).NotTo(HaveOccurred(), "parsing MariaDB position %q", pos)
	for _, g := range gtids {
		if g.Domain == 0 {
			return g.Seq
		}
	}
	return 0
}

// expectDeadBranchRecorded checks the fork record against what was written on
// the dead branch: MySQL records exactly disowned GTIDs (a superset of the dead
// rows', since heartbeats were disowned too, and none of the survivor's);
// MariaDB records the cut below the dead rows.
func expectDeadBranchRecorded(f forkFlavor, seg objectstore.ArchiveSegment, beforeDead, deadPos, livePos string) {
	GinkgoHelper()
	if !f.mariadb {
		deadSet, err := replication.DifferenceGTIDStrings(deadPos, beforeDead)
		Expect(err).NotTo(HaveOccurred())
		Expect(deadSet).NotTo(BeEmpty())
		covers, err := replication.GTIDContains(seg.Fork.GTIDSet, deadSet)
		Expect(err).NotTo(HaveOccurred())
		Expect(covers).To(BeTrue(), "fork %q must hold the dead branch %q", seg.Fork.GTIDSet, deadSet)
		mixed, err := replication.IntersectsGTIDStrings(seg.Fork.GTIDSet, livePos)
		Expect(err).NotTo(HaveOccurred())
		Expect(mixed).To(BeFalse(), "fork %q must not name the survivor's transactions %q", seg.Fork.GTIDSet, livePos)
		return
	}
	cut, ok := seg.Fork.AfterSeq[0]
	Expect(ok).To(BeTrue(), "MariaDB fork must cut domain 0: %+v", seg.Fork)
	Expect(cut).To(BeNumerically("<=", mariadbSeq(beforeDead)), "the cut is at most what the lagged replica held")
	Expect(cut).To(BeNumerically("<", mariadbSeq(deadPos)), "the dead rows are past the cut")
}

// laggedPromotionSpec is case 4 of the design, the motivating one: the old
// primary rotated and uploaded transactions its lagged successor never
// received, then lost authority.
func laggedPromotionSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	common := idRange(1, 10)
	dead := idRange(101, 105)
	live := idRange(201, 210)
	post := idRange(301, 305)
	var (
		primary, replica             string
		beforeDead, deadPos, livePos string
		liveTime                     string
		frozenSegments               []objectstore.ArchiveSegment
	)

	It("archives a dead branch the lagged replica never receives", func() {
		primary = clusterPrimary(cluster)
		replica = otherInstance(cluster, s.instances, primary)

		By("writing the common history and letting the replica apply it")
		writeForkRows(f, primary, s.password, "common", common)
		waitReplicated(f, replica, s.password, common)

		By(fmt.Sprintf("freezing %s: fencing stops its mysqld", replica))
		fence(replica)
		expectFenced(cluster, replica)

		By("committing the dead branch on the primary, then rotating and archiving it")
		beforeDead = f.position(primary, s.password)
		writeForkRows(f, primary, s.password, "dead", dead)
		deadPos = f.flush(cluster, primary, s.password)
		f.covers(cluster, deadPos, 5*time.Minute)
	})

	It("promotes the lagging replica and keeps writing on it", func() {
		promoteFrozenReplica(f, cluster, primary, replica, s.password)

		writeForkRows(f, replica, s.password, "live", live)
		livePos = f.flush(cluster, replica, s.password)
		f.covers(cluster, livePos, 5*time.Minute)

		liveTime = rfc3339Now()
		waitPast(liveTime)
		writeForkRows(f, replica, s.password, "post", post)
		f.covers(cluster, f.flush(cluster, replica, s.password), 5*time.Minute)
		expectForkRows(f, replica, s.password, ids(common, live, post), "the live cluster never served the dead branch")
	})

	It("records the dead branch on the segment that holds it", func() {
		seg := expectForkOn(cluster, primary)
		expectDeadBranchRecorded(f, seg, beforeDead, deadPos, livePos)

		idx, err := readArchiveIndex(cluster)
		Expect(err).NotTo(HaveOccurred())
		for _, other := range segmentsOf(idx, replica) {
			Expect(other.Fork).To(BeNil(), "the surviving primary's segment must never carry a fork")
		}

		expectCondition(cluster, "ArchiveForked", "True")
		expectWarningEvent(cluster, "ArchiveForked")
		gtids, err := clusterField(cluster, "{.status.continuousArchiving.forkGTIDs[*]}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(gtids)).NotTo(BeEmpty(), "status must name the disowned transactions")
		detected, err := clusterField(cluster, "{.status.continuousArchiving.forkDetectedAt}")
		Expect(err).NotTo(HaveOccurred())
		Expect(detected).NotTo(BeEmpty())
		archiving, err := clusterField(cluster, "{.status.conditions[?(@.type=='ContinuousArchiving')].status}")
		Expect(err).NotTo(HaveOccurred())
		Expect(archiving).To(Equal("True"), "a fork is not an archiving failure")

		if f.mariadb {
			last, err := clusterField(cluster, "{.status.mariadbTimeline[-1:].instance}")
			Expect(err).NotTo(HaveOccurred())
			Expect(last).To(Equal(replica), "the promotion must be on the timeline")
		}
		frozenSegments = segmentsOf(idx, primary)
	})

	It("marks the returning old primary diverged and never archives more of its dead branch", func() {
		By(fmt.Sprintf("unfencing the old primary %s", primary))
		unfence(primary)
		expectDiverged(cluster, primary)

		if f.mariadb {
			// Its role reconciler may point it at the new primary before the mark
			// lands; the GTID handshake then refuses it (1236). Either way it never
			// replicates, and it stays out of the failover candidates.
			By("verifying the forked MariaDB instance never replicates from the new primary")
			Consistently(func(g Gomega) {
				running, err := mariadbReplicating(primary, rootPassword(cluster))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(running).To(BeFalse(), "a forked instance must never stream from the surviving primary")
			}, e2eTimeout(60*time.Second), 5*time.Second).Should(Succeed())
		}

		By("verifying the old primary's segments do not grow")
		Consistently(func(g Gomega) {
			idx, err := readArchiveIndex(cluster)
			g.Expect(err).NotTo(HaveOccurred())
			now := segmentsOf(idx, primary)
			g.Expect(now).To(HaveLen(len(frozenSegments)))
			for i := range now {
				g.Expect(now[i].GTIDSet).To(Equal(frozenSegments[i].GTIDSet), "the drain shipped part of the dead branch")
				g.Expect(now[i].Binlogs).To(Equal(frozenSegments[i].Binlogs))
			}
		}, e2eTimeout(2*time.Minute), 10*time.Second).Should(Succeed())
	})

	It("recovers the history the cluster actually served to latest, a time and an immediate target", func() {
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{}, ids(common, live, post))
		restoreAndExpectRows(f, cluster+"-time", s.backup, s.password, pitrTarget{Time: liveTime}, ids(common, live))
		restoreAndExpectRows(f, cluster+"-immediate", s.backup, s.password, pitrTarget{Immediate: true}, ids(common, live, post))
	})

	It("recovers the survivor's branch when targetGTID names it", func() {
		restoreAndExpectRows(f, cluster+"-survivor", s.backup, s.password, pitrTarget{GTID: livePos}, ids(common, live))
	})

	It("recovers the dead branch when targetGTID names it", func() {
		restoreAndExpectRows(f, cluster+"-dead", s.backup, s.password, pitrTarget{GTID: deadPos}, ids(common, dead))
	})

	It("keeps reporting the fork after the dead instance is re-cloned and after a switchover", func() {
		reinitAndRecover(cluster, primary, s.instances)
		expectCondition(cluster, "ArchiveForked", "True")

		By(fmt.Sprintf("switching over to the re-cloned %s", primary))
		requestSwitchoverIn(testNamespace, cluster, primary)
		waitPrimaryIs(f, cluster, primary, s.password)
		Consistently(func(g Gomega) {
			got, err := clusterField(cluster, "{.status.conditions[?(@.type=='ArchiveForked')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(got).To(Equal("True"), "the fork is still in the archive")
		}, e2eTimeout(60*time.Second), 5*time.Second).Should(Succeed())

		if f.mariadb {
			last, err := clusterField(cluster, "{.status.mariadbTimeline[-1:].instance}")
			Expect(err).NotTo(HaveOccurred())
			Expect(last).To(Equal(primary), "the switchover must be on the timeline")
		}
		expectForkRows(f, primary, s.password, ids(common, live, post), "the re-cloned primary holds the served history")
	})

	It("clears ArchiveForked once retention drops the forked segment", func() {
		By("taking a base backup after the fork")
		recent := cluster + "-recent"
		applyManifest(recent, backupManifest(recent, cluster))
		expectBackupCompleted(recent, 8*time.Minute)

		By("aging the genesis backup past a one-day retention policy")
		ageBackup(cluster, s.backup, 30*24*time.Hour)
		_, err := kubectl("patch", "cluster", cluster, "-n", testNamespace, "--type=merge",
			"-p", `{"spec":{"backup":{"retentionPolicy":"1d"}}}`)
		Expect(err).NotTo(HaveOccurred())
		_, err = kubectl("patch", "cluster", cluster, "-n", testNamespace,
			"--subresource=status", "--type=merge", "-p", `{"status":{"lastRetentionRunTime":null}}`)
		Expect(err).NotTo(HaveOccurred())
		clusterAnnotate(cluster, "cnmsql.co/retention-nudge="+fmt.Sprint(time.Now().Unix()))

		By("waiting for retention to drop the forked segment")
		Eventually(func(g Gomega) {
			idx, err := readArchiveIndex(cluster)
			g.Expect(err).NotTo(HaveOccurred())
			for _, seg := range idx.Segments {
				g.Expect(seg.Fork).To(BeNil(), "segment %s (%s) still holds the dead branch", seg.ServerUUID, seg.InstanceName)
			}
			g.Expect(idx.ForkCheck).NotTo(BeNil(), "retention must keep the index's forkCheck")
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())
		expectCondition(cluster, "ArchiveForked", "False")
	})
}

func mustClusterField(cluster, jsonpath string) string {
	GinkgoHelper()
	out, err := clusterField(cluster, jsonpath)
	Expect(err).NotTo(HaveOccurred())
	return out
}

// ageBackup rewrites a base backup's manifest so retention sees it as old.
func ageBackup(cluster, backup string, age time.Duration) {
	GinkgoHelper()
	listing, err := rcloneExec("lsf", "-R", "--files-only", objectKey("%s/", cluster))
	Expect(err).NotTo(HaveOccurred(), listing)
	var key string
	for _, line := range strings.Split(listing, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasSuffix(line, "/"+objectstore.BackupMetadataName) && strings.Contains(line, backup+"/") {
			key = objectKey("%s/%s", cluster, line)
		}
	}
	Expect(key).NotTo(BeEmpty(), "no manifest for backup %s in %s", backup, listing)
	raw, err := rcloneExec("cat", key)
	Expect(err).NotTo(HaveOccurred(), raw)
	var meta map[string]any
	Expect(json.Unmarshal([]byte(raw), &meta)).To(Succeed())
	old := time.Now().Add(-age).UTC().Format(time.RFC3339)
	meta["startedAt"], meta["completedAt"] = old, old
	aged, err := json.Marshal(meta)
	Expect(err).NotTo(HaveOccurred())
	s3Pipe(string(aged), key)
}

// cleanFailoverSpec is case 1: a failover onto a caught-up replica leaves the
// old segment nested in the successor's history. Nothing is recorded, and every
// kind of recovery target lands exactly where the cluster was.
func cleanFailoverSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	pre := idRange(1, 10)
	post := idRange(11, 20)
	late := idRange(21, 25)
	var (
		primary, replica  string
		prePos, postPos   string
		preTime, postTime string
	)

	It("fails over onto a caught-up replica and keeps archiving", func() {
		primary = clusterPrimary(cluster)
		replica = otherInstance(cluster, s.instances, primary)

		writeForkRows(f, primary, s.password, "pre", pre)
		prePos = f.flush(cluster, primary, s.password)
		f.covers(cluster, prePos, 5*time.Minute)
		waitReplicated(f, replica, s.password, pre)
		preTime = rfc3339Now()
		waitPast(preTime)

		By("failing over: the old primary is taken down with its replica caught up")
		failOver(f, cluster, primary, replica, s.password, false)
		By("letting the old primary rejoin as a replica")
		unfence(primary)
		expectClusterRecovers(cluster, s.instances, e2eTimeout(20*time.Minute))

		writeForkRows(f, replica, s.password, "post", post)
		postPos = f.flush(cluster, replica, s.password)
		f.covers(cluster, postPos, 5*time.Minute)
		postTime = rfc3339Now()
		waitPast(postTime)

		writeForkRows(f, replica, s.password, "late", late)
		f.covers(cluster, f.flush(cluster, replica, s.password), 5*time.Minute)
	})

	It("records no fork and reports the archive unforked", func() {
		expectNoForks(cluster)
		expectCondition(cluster, "ArchiveForked", "False")
		out, err := clusterField(cluster, "{.status.divergedInstances}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(BeEmpty(), "a caught-up former primary is not diverged")
		if f.mariadb {
			// Older entries may be pruned once nothing references them; the
			// failover itself must be the newest.
			instances, err := clusterField(cluster, "{.status.mariadbTimeline[*].instance}")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.Fields(instances)).NotTo(BeEmpty())
			Expect(strings.Fields(instances)[len(strings.Fields(instances))-1]).To(Equal(replica),
				"the timeline must record the failover")
		}
	})

	It("recovers to latest and to targetImmediate", func() {
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{}, ids(pre, post, late))
		restoreAndExpectRows(f, cluster+"-immediate", s.backup, s.password, pitrTarget{Immediate: true}, ids(pre, post, late))
	})

	It("recovers to a time before and a time after the failover", func() {
		restoreAndExpectRows(f, cluster+"-time-pre", s.backup, s.password, pitrTarget{Time: preTime}, pre)
		restoreAndExpectRows(f, cluster+"-time-post", s.backup, s.password, pitrTarget{Time: postTime}, ids(pre, post))
	})

	It("recovers to a GTID before and a GTID after the failover", func() {
		restoreAndExpectRows(f, cluster+"-gtid-pre", s.backup, s.password, pitrTarget{GTID: prePos}, pre)
		restoreAndExpectRows(f, cluster+"-gtid-post", s.backup, s.password, pitrTarget{GTID: postPos}, ids(pre, post))
	})
}

// failbackSpec is case 6: A -> S -> A, where S uploaded transactions before it
// lost authority to a lagging A. A's segment already exists, so only A's first
// writable pass after the failback can find the fork on S's segment.
func failbackSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	common := idRange(1, 5)
	interim := idRange(11, 15)
	dead := idRange(101, 104)
	live := idRange(201, 204)
	var a, sInst string

	It("fails over to S and back to a lagging A", func() {
		a = clusterPrimary(cluster)
		sInst = otherInstance(cluster, s.instances, a)
		writeForkRows(f, a, s.password, "common", common)
		waitReplicated(f, sInst, s.password, common)
		f.covers(cluster, f.flush(cluster, a, s.password), 5*time.Minute)

		By("failing over cleanly from A to S, and letting A rejoin")
		failOver(f, cluster, a, sInst, s.password, false)
		unfence(a)
		expectClusterRecovers(cluster, s.instances, e2eTimeout(20*time.Minute))
		writeForkRows(f, sInst, s.password, "interim", interim)
		waitReplicated(f, a, s.password, interim)

		By("freezing A, then archiving a branch on S that A never receives")
		fence(a)
		expectFenced(cluster, a)
		writeForkRows(f, sInst, s.password, "dead", dead)
		f.covers(cluster, f.flush(cluster, sInst, s.password), 5*time.Minute)

		By("failing back to the lagging A")
		promoteFrozenReplica(f, cluster, sInst, a, s.password)
		writeForkRows(f, a, s.password, "live", live)
		f.covers(cluster, f.flush(cluster, a, s.password), 5*time.Minute)
	})

	It("records the fork on the interim primary's segment", func() {
		expectForkOn(cluster, sInst)
		idx, err := readArchiveIndex(cluster)
		Expect(err).NotTo(HaveOccurred())
		for _, seg := range segmentsOf(idx, a) {
			Expect(seg.Fork).To(BeNil(), "the returning primary's own segment must never carry a fork")
		}
		expectCondition(cluster, "ArchiveForked", "True")
		if f.mariadb {
			// The genesis entry may be pruned; the interim epoch and the failback
			// are what the fork verdict needs.
			instances := strings.Fields(mustClusterField(cluster, "{.status.mariadbTimeline[*].instance}"))
			Expect(len(instances)).To(BeNumerically(">=", 2))
			Expect(instances[len(instances)-2:]).To(Equal([]string{sInst, a}), "the failback must be on the timeline")
		}
	})

	It("marks S diverged and recovers what A served", func() {
		unfence(sInst)
		expectDiverged(cluster, sInst)
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{}, ids(common, interim, live))
		reinitAndRecover(cluster, sInst, s.instances)
	})
}

// drainGateSpec is cases 2 and 3: the dead branch was never rotated, so it can
// only reach the archive through the returning old primary's drain. The drain
// gate must defer it forever, whether or not replication streams before the
// operator marks the instance diverged.
func drainGateSpec(f forkFlavor, cluster string) {
	// No forced rotation: the archiver ships only what the spec flushes, so the
	// dead tail stays in the old primary's active binlog until it restarts.
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 3600}
	s.setUp()

	common := idRange(1, 5)
	dead := idRange(101, 105)
	live := idRange(201, 205)
	var primary, replica, beforeDead, deadPos string

	It("strands an un-rotated dead tail on the old primary", func() {
		primary = clusterPrimary(cluster)
		replica = otherInstance(cluster, s.instances, primary)
		writeForkRows(f, primary, s.password, "common", common)
		waitReplicated(f, replica, s.password, common)
		f.covers(cluster, f.flush(cluster, primary, s.password), 5*time.Minute)

		fence(replica)
		expectFenced(cluster, replica)
		beforeDead = f.position(primary, s.password)
		writeForkRows(f, primary, s.password, "dead", dead)
		deadPos = f.position(primary, s.password)

		promoteFrozenReplica(f, cluster, primary, replica, s.password)
		writeForkRows(f, replica, s.password, "live", live)
		f.covers(cluster, f.flush(cluster, replica, s.password), 5*time.Minute)
	})

	It("never archives the stranded dead tail", func() {
		By(fmt.Sprintf("unfencing %s: its restart closes the binlog holding the dead tail", primary))
		unfence(primary)
		expectDiverged(cluster, primary)
		Consistently(func(g Gomega) {
			idx, err := readArchiveIndex(cluster)
			g.Expect(err).NotTo(HaveOccurred())
			if !f.mariadb {
				deadSet, err := replication.DifferenceGTIDStrings(deadPos, beforeDead)
				g.Expect(err).NotTo(HaveOccurred())
				mixed, err := replication.IntersectsGTIDStrings(idx.CoveredGTIDSet, deadSet)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(mixed).To(BeFalse(), "the archive took part of the dead tail %q", deadSet)
				return
			}
			for _, seg := range segmentsOf(idx, primary) {
				g.Expect(mariadbSeq(seg.GTIDSet)).To(BeNumerically("<=", mariadbSeq(beforeDead)),
					"the old primary's segment grew into the dead tail")
			}
		}, e2eTimeout(3*time.Minute), 10*time.Second).Should(Succeed())
		expectCondition(cluster, "ArchiveForked", "False")
	})

	It("recovers what the cluster served", func() {
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{}, ids(common, live))
		reinitAndRecover(cluster, primary, s.instances)
	})
}

// deadBranchBackupSpec: a base backup taken on the old primary after the
// replica froze holds disowned transactions. Replay cannot remove them, so a
// time or latest recovery from it fails closed; a targetGTID that contains the
// backup's position still recovers the dead branch.
func deadBranchBackupSpec(f forkFlavor, cluster string) {
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	common := idRange(1, 5)
	dead := idRange(101, 103)
	more := idRange(111, 112)
	live := idRange(201, 203)
	deadBackup := cluster + "-dead"
	var primary, replica, deadPos string

	It("takes a base backup on the dead branch", func() {
		primary = clusterPrimary(cluster)
		replica = otherInstance(cluster, s.instances, primary)
		writeForkRows(f, primary, s.password, "common", common)
		waitReplicated(f, replica, s.password, common)
		fence(replica)
		expectFenced(cluster, replica)
		writeForkRows(f, primary, s.password, "dead", dead)

		applyManifest(deadBackup, primaryBackupManifest(deadBackup, cluster))
		expectBackupCompleted(deadBackup, 8*time.Minute)
		writeForkRows(f, primary, s.password, "dead", more)
		deadPos = f.flush(cluster, primary, s.password)
		f.covers(cluster, deadPos, 5*time.Minute)

		promoteFrozenReplica(f, cluster, primary, replica, s.password)
		writeForkRows(f, replica, s.password, "live", live)
		f.covers(cluster, f.flush(cluster, replica, s.password), 5*time.Minute)
		expectForkOn(cluster, primary)
	})

	It("refuses a latest recovery from the dead-branch backup", func() {
		expectRestoreFails(f, cluster+"-refused", deadBackup, pitrTarget{}, "dead branch")
	})

	It("recovers the dead branch from it when targetGTID names it", func() {
		restoreAndExpectRows(f, cluster+"-deadgtid", deadBackup, s.password, pitrTarget{GTID: deadPos}, ids(common, dead, more))
	})

	It("still recovers the served history from the genesis backup", func() {
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{}, ids(common, live))
	})
}

var _ = Describe("Archive fork: lagged promotion", Ordered, Label("feature", "pitr"), func() {
	laggedPromotionSpec(mysqlForkFlavor, "fork-lagged")
})

var _ = Describe("Archive fork: clean failover and recovery targets", Ordered, Label("feature", "pitr"), func() {
	cleanFailoverSpec(mysqlForkFlavor, "fork-clean")
})

var _ = Describe("Archive fork: failback", Ordered, Label("feature", "pitr"), func() {
	failbackSpec(mysqlForkFlavor, "fork-failback")
})

var _ = Describe("Archive fork: drain gate", Ordered, Label("feature", "pitr"), func() {
	drainGateSpec(mysqlForkFlavor, "fork-drain")
})

var _ = Describe("Archive fork: dead-branch backup", Ordered, Label("feature", "pitr"), func() {
	deadBranchBackupSpec(mysqlForkFlavor, "fork-deadbak")
})

var _ = Describe("MariaDB archive fork: lagged promotion", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	laggedPromotionSpec(mariadbForkFlavor, "mdb-fork-lagged")
})

var _ = Describe("MariaDB archive fork: clean failover and recovery targets", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	cleanFailoverSpec(mariadbForkFlavor, "mdb-fork-clean")
})

var _ = Describe("MariaDB archive fork: failback", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	failbackSpec(mariadbForkFlavor, "mdb-fork-failback")
})

var _ = Describe("MariaDB archive fork: drain gate", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	drainGateSpec(mariadbForkFlavor, "mdb-fork-drain")
})

var _ = Describe("MariaDB archive fork: dead-branch backup", Ordered, Label("flavor", "mariadb", "pitr"), func() {
	deadBranchBackupSpec(mariadbForkFlavor, "mdb-fork-deadbak")
})
