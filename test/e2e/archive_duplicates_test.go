//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// A demoted primary keeps writing binary logs: as a replica it re-logs every
// transaction its successor commits, and the successor archives those itself.
// These specs check the archive does not store that copy, and that the copy is
// still used when it is the only source of a binlog the archive lost.

// archivedBinlog is one binary log the archive index lists, with its manifest.
type archivedBinlog struct {
	segment objectstore.ArchiveSegment
	meta    objectstore.BinlogMetadata
}

func (b archivedBinlog) String() string {
	return fmt.Sprintf("%s/%s (%s)", b.segment.InstanceName, b.meta.BinlogName, b.meta.GTIDSet)
}

// foreignOnly reports whether the file holds transactions, none of which its
// own server authored: a pure re-log of another server's history.
func (b archivedBinlog) foreignOnly() (bool, error) {
	set, err := replication.ParseGTIDSet(b.meta.GTIDSet)
	if err != nil {
		return false, fmt.Errorf("parsing the gtid set of %s: %w", b, err)
	}
	return !set.IsEmpty() && len(set[strings.ToLower(b.segment.ServerUUID)]) == 0, nil
}

func binlogManifestKey(cluster, serverUUID, name string) string {
	return objectKey("%s/binlogs/%s/%s%s", cluster, serverUUID, name, objectstore.BinlogManifestSuffix)
}

// archivedBinlogsOf returns the binary logs the index lists in the segments
// instance wrote.
func archivedBinlogsOf(cluster, instance string) ([]archivedBinlog, error) {
	idx, err := readArchiveIndex(cluster)
	if err != nil {
		return nil, err
	}
	var out []archivedBinlog
	for _, seg := range segmentsOf(idx, instance) {
		for _, name := range seg.Binlogs {
			key := binlogManifestKey(cluster, seg.ServerUUID, name)
			raw, err := rcloneExec("cat", key)
			if err != nil {
				return nil, fmt.Errorf("reading manifest %s: %w (%s)", key, err, raw)
			}
			var meta objectstore.BinlogMetadata
			if err := json.Unmarshal([]byte(raw), &meta); err != nil {
				return nil, fmt.Errorf("decoding manifest %s: %w (%s)", key, err, raw)
			}
			out = append(out, archivedBinlog{segment: seg, meta: meta})
		}
	}
	return out, nil
}

// relogsOf returns the archived binary logs of instance that only re-log
// another server's transactions.
func relogsOf(cluster, instance string) ([]archivedBinlog, error) {
	files, err := archivedBinlogsOf(cluster, instance)
	if err != nil {
		return nil, err
	}
	var out []archivedBinlog
	for _, file := range files {
		foreign, err := file.foreignOnly()
		if err != nil {
			return nil, err
		}
		if foreign {
			out = append(out, file)
		}
	}
	return out, nil
}

// closeBinlog rotates a replica's active binary log, so what it re-logged so
// far sits in a closed file its archiver may ship. LOCAL keeps the statement
// out of the binary log.
func closeBinlog(cluster, replica string) {
	GinkgoHelper()
	_, err := mysqlExec(replica, "root", rootPassword(cluster), "", "FLUSH LOCAL BINARY LOGS")
	Expect(err).NotTo(HaveOccurred(), "FLUSH LOCAL BINARY LOGS failed on %s", replica)
}

// loseArchivedBinlogs removes from the archive every binary log of instance
// that holds part of gtids: the bytes, the manifest and the index entry. It
// returns the names removed and everything they held.
func loseArchivedBinlogs(cluster, instance, gtids string) (names []string, lost string) {
	GinkgoHelper()
	files, err := archivedBinlogsOf(cluster, instance)
	Expect(err).NotTo(HaveOccurred())
	for _, file := range files {
		hit, err := replication.IntersectsGTIDStrings(file.meta.GTIDSet, gtids)
		Expect(err).NotTo(HaveOccurred())
		if !hit {
			continue
		}
		names = append(names, file.meta.BinlogName)
		lost, err = replication.UnionGTIDStrings(lost, file.meta.GTIDSet)
		Expect(err).NotTo(HaveOccurred())
		manifest := binlogManifestKey(cluster, file.segment.ServerUUID, file.meta.BinlogName)
		for _, key := range []string{manifest, strings.TrimSuffix(manifest, objectstore.BinlogManifestSuffix)} {
			out, err := rcloneExec("deletefile", key)
			Expect(err).NotTo(HaveOccurred(), "deleting %s: %s", key, out)
		}
	}
	Expect(names).NotTo(BeEmpty(), "%s archived no binary log holding %s", instance, gtids)

	// The primary rewrites the index on every pass, and a write of its own can
	// land over this one: drop the entries until they stay dropped.
	indexKey := objectKey("%s/binlogs/%s", cluster, objectstore.ArchiveIndexName)
	Eventually(func(g Gomega) {
		idx, err := readArchiveIndex(cluster)
		g.Expect(err).NotTo(HaveOccurred())
		listed := false
		var covered []string
		for i := range idx.Segments {
			seg := &idx.Segments[i]
			if seg.InstanceName == instance {
				kept := slices.DeleteFunc(slices.Clone(seg.Binlogs), func(name string) bool {
					return slices.Contains(names, name)
				})
				if len(kept) != len(seg.Binlogs) {
					listed = true
					seg.Binlogs = kept
					seg.GTIDSet, err = replication.DifferenceGTIDStrings(seg.GTIDSet, lost)
					g.Expect(err).NotTo(HaveOccurred())
				}
			}
			covered = append(covered, seg.GTIDSet)
		}
		if !listed {
			return
		}
		idx.CoveredGTIDSet, err = replication.UnionGTIDStrings(covered...)
		g.Expect(err).NotTo(HaveOccurred())
		raw, err := json.Marshal(idx)
		g.Expect(err).NotTo(HaveOccurred())
		s3Pipe(string(raw), indexKey)
		g.Expect(listed).To(BeFalse(), "the index still listed %v", names)
	}, e2eTimeout(3*time.Minute), 15*time.Second).Should(Succeed())
	return names, lost
}

// demotedPrimarySpec fails over from A to B and lets A rejoin as a replica.
func demotedPrimarySpec(cluster string) {
	f := mysqlForkFlavor
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	pre := idRange(1, 10)
	relogged := idRange(101, 130)
	late := idRange(201, 205)
	hole := idRange(301, 305)
	after := idRange(401, 405)
	var a, b, holePos, lost string

	It("demotes a primary that then re-logs its successor's transactions", func() {
		a = clusterPrimary(cluster)
		b = otherInstance(cluster, s.instances, a)
		writeForkRows(f, a, s.password, "pre", pre)
		f.covers(cluster, f.flush(cluster, a, s.password), 5*time.Minute)
		waitReplicated(f, b, s.password, pre)

		By(fmt.Sprintf("failing over from %s to %s, and letting %s rejoin as a replica", a, b, a))
		failOver(f, cluster, a, b, s.password, false)
		unfence(a)
		expectClusterRecovers(cluster, s.instances, e2eTimeout(20*time.Minute))

		By("committing on the new primary across several archived binlogs")
		for round := range 3 {
			writeForkRows(f, b, s.password, "relogged", relogged[round*10:(round+1)*10])
			f.covers(cluster, f.flush(cluster, b, s.password), 5*time.Minute)
		}

		By(fmt.Sprintf("closing the binlog %s re-logged them into", a))
		waitReplicated(f, a, s.password, relogged)
		closeBinlog(cluster, a)

		// The archive moves past the copy, which leaves it nothing to wait for.
		writeForkRows(f, b, s.password, "late", late)
		f.covers(cluster, f.flush(cluster, b, s.password), 5*time.Minute)
	})

	It("never archives the demoted primary's copy", func() {
		Consistently(func(g Gomega) {
			relogs, err := relogsOf(cluster, a)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(relogs).To(BeEmpty(), "%s archived transactions %s archives itself", a, b)
		}, e2eTimeout(3*time.Minute), 15*time.Second).Should(Succeed())
	})

	It("archives the demoted primary's copy of a binlog the archive lost", func() {
		before := f.position(b, s.password)
		writeForkRows(f, b, s.password, "hole", hole)
		holePos = f.flush(cluster, b, s.password)
		f.covers(cluster, holePos, 5*time.Minute)
		holeSet, err := replication.DifferenceGTIDStrings(holePos, before)
		Expect(err).NotTo(HaveOccurred())

		writeForkRows(f, b, s.password, "after", after)
		afterPos := f.flush(cluster, b, s.password)
		f.covers(cluster, afterPos, 5*time.Minute)
		waitReplicated(f, a, s.password, ids(hole, after))
		closeBinlog(cluster, a)

		By(fmt.Sprintf("losing the binlogs %s archived the hole rows in", b))
		var names []string
		names, lost = loseArchivedBinlogs(cluster, b, holeSet)

		By(fmt.Sprintf("waiting for %s to ship its copy of %v", a, names))
		// The demoted primary re-reads the archive's coverage every five minutes.
		Eventually(func(g Gomega) {
			idx, err := readArchiveIndex(cluster)
			g.Expect(err).NotTo(HaveOccurred())
			covers, err := replication.GTIDContains(idx.CoveredGTIDSet, lost)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(covers).To(BeTrue(), "the archive covers %q, still without %q", idx.CoveredGTIDSet, lost)
		}, e2eTimeout(12*time.Minute), 15*time.Second).Should(Succeed())
		f.covers(cluster, afterPos, 5*time.Minute)

		By("checking it shipped nothing but what the archive lacked")
		relogs, err := relogsOf(cluster, a)
		Expect(err).NotTo(HaveOccurred())
		Expect(relogs).NotTo(BeEmpty(), "%s must be the one that filled the hole", a)
		for _, file := range relogs {
			fills, err := replication.IntersectsGTIDStrings(file.meta.GTIDSet, lost)
			Expect(err).NotTo(HaveOccurred())
			Expect(fills).To(BeTrue(), "%s holds nothing the archive lacked", file)
		}
	})

	It("recovers across the binlogs the demoted primary filled in", func() {
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{},
			ids(pre, relogged, late, hole, after))
		restoreAndExpectRows(f, cluster+"-hole", s.backup, s.password, pitrTarget{GTID: holePos},
			ids(pre, relogged, late, hole))
	})
}

var _ = Describe("Archive duplicates: demoted primary", Ordered, Label("feature", "pitr"), func() {
	demotedPrimarySpec("dup-demoted")
})
