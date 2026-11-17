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

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
)

// A replica re-logs every transaction it replicates. After a failover both
// instances hold such copies: the promoted replica of what the old primary
// archived, the demoted primary of what its successor archives. These specs
// check the archive stores neither (design 041).

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

// archivedBy returns the GTID set of everything the index says instance
// archived.
func archivedBy(cluster, instance string) (string, error) {
	idx, err := readArchiveIndex(cluster)
	if err != nil {
		return "", err
	}
	var sets []string
	for _, seg := range segmentsOf(idx, instance) {
		sets = append(sets, seg.GTIDSet)
	}
	return replication.UnionGTIDStrings(sets...)
}

// failoverSpec fails over from A to B and lets A rejoin as a replica.
func failoverSpec(cluster string) {
	f := mysqlForkFlavor
	s := &forkSuite{f: f, cluster: cluster, instances: 2, rpoSeconds: 10}
	s.setUp()

	pre := idRange(1, 10)
	relogged := idRange(101, 130)
	late := idRange(201, 205)
	var a, b string

	It("fails over between two instances that re-log each other", func() {
		a = clusterPrimary(cluster)
		b = otherInstance(cluster, s.instances, a)
		writeForkRows(f, a, s.password, "pre", pre)
		f.covers(cluster, f.flush(cluster, a, s.password), 5*time.Minute)

		By(fmt.Sprintf("closing the binlog %s re-logged them into", b))
		waitReplicated(f, b, s.password, pre)
		closeBinlog(cluster, b)

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

		writeForkRows(f, b, s.password, "late", late)
		f.covers(cluster, f.flush(cluster, b, s.password), 5*time.Minute)
	})

	It("archives neither copy", func() {
		Consistently(func(g Gomega) {
			relogs, err := relogsOf(cluster, a)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(relogs).To(BeEmpty(), "%s archived as a replica", a)

			// A file of the promoted replica may hold the old primary's
			// unarchived tail, but never only what the old primary archived.
			archived, err := archivedBy(cluster, a)
			g.Expect(err).NotTo(HaveOccurred())
			relogs, err = relogsOf(cluster, b)
			g.Expect(err).NotTo(HaveOccurred())
			for _, file := range relogs {
				repeats, err := replication.GTIDContains(archived, file.meta.GTIDSet)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(repeats).To(BeFalse(), "%s holds only what %s archived (%s)", file, a, archived)
			}
		}, e2eTimeout(3*time.Minute), 15*time.Second).Should(Succeed())
	})

	It("recovers across the failover", func() {
		restoreAndExpectRows(f, cluster+"-latest", s.backup, s.password, pitrTarget{}, ids(pre, relogged, late))
	})
}

var _ = Describe("Archive duplicates: failover", Ordered, Label("feature", "pitr"), func() {
	failoverSpec("dup-failover")
})
