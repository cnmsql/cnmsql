//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The maxTransactionsBehind failover guard. A replica that has stopped receiving
// still looks healthy, because its relay log drains and it then reports itself
// caught up, so the election will happily promote it and discard every
// transaction it never received. spec.failoverPolicy.maxTransactionsBehind bounds
// that: when no replica is within the bound the operator refuses the failover and
// moves the cluster to Blocked, keeping the writes a promotion would have thrown
// away.
//
// These specs build the scenario by freezing a replica (fencing stops its mysqld),
// burning transactions on the primary that the replica therefore never sees, and
// only then taking the primary down. They take the primary down by fencing it
// rather than by deleting its Pod, because a deleted Pod is recreated within a
// minute or two and would race the assertions, while a fenced instance stays down
// until it is unfenced. That holds the cluster in the exact state issue #76
// describes, a live and healthy but far-behind replica with no primary, for as
// long as the assertions need it.

// lagGuardSpec runs the guard scenario against one flavor. exec is the flavor's
// SQL client helper (mysqlExec / mariadbExec), which share a signature.
func lagGuardSpec(
	cluster, flavor, image string,
	exec func(pod, user, password, database, sql string) (string, error),
) {
	const (
		instances = 2
		// The bound, and the number of transactions the frozen replica will miss.
		// The gap is two orders of magnitude past the bound so that no plausible
		// staleness in the operator's GTID snapshot can bring it back inside.
		maxBehind = 5
		burn      = 100
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace(cluster)

		By(fmt.Sprintf("creating a 2-instance %s cluster with maxTransactionsBehind=%d", flavor, maxBehind))
		applyManifest(cluster, lagGuardClusterManifest(cluster, flavor, image, instances, maxBehind))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})

	It("refuses to promote a replica that is further behind than maxTransactionsBehind", func() {
		password := appPassword(cluster)
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, instances, primary)

		By(fmt.Sprintf("seeding a table on the primary %s and letting %s replicate it", primary, replica))
		_, err := exec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS lag_guard (id INT PRIMARY KEY);"+
				"REPLACE INTO lag_guard VALUES (0);")
		Expect(err).NotTo(HaveOccurred(), "failed to seed the primary")
		Eventually(func(g Gomega) {
			out, rerr := exec(replica, "app", password, "app", "SELECT id FROM lag_guard WHERE id = 0;")
			g.Expect(rerr).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("0"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed(),
			"the replica must be in sync before it is frozen, so the only gap is the one this test creates")

		By(fmt.Sprintf("fencing %s to freeze it: mysqld stops, so it receives nothing from here on", replica))
		fence(replica)
		expectFenced(cluster, replica)

		frozenAt := recordedGTID(cluster, primary)

		By(fmt.Sprintf("committing %d transactions on the primary that the frozen replica will never receive", burn))
		_, err = exec(primary, "app", password, "app", burnTransactions(burn))
		Expect(err).NotTo(HaveOccurred(), "failed to burn transactions on the primary")

		By("waiting for the operator to record the primary's advanced GTID position")
		// This snapshot is the reference the gap is measured against once the primary
		// is gone, so the assertions below are only meaningful after it has caught up
		// with the writes. It is a lower bound on the primary's true position: if it
		// were stale the measured gap would be understated, never overstated.
		Eventually(func(g Gomega) {
			g.Expect(recordedGTID(cluster, primary)).NotTo(Or(BeEmpty(), Equal(frozenAt)),
				"the operator must observe the primary past the point the replica froze at")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("fencing the primary %s to take it down and hold it down", primary))
		fence(primary)
		expectFenced(cluster, primary)

		By(fmt.Sprintf("unfencing %s so it comes back healthy, but %d transactions behind", replica, burn))
		unfence(replica)

		By("verifying the operator blocks the failover and names the gap it refused to lose")
		Eventually(func(g Gomega) {
			phase, perr := clusterField(cluster, "{.status.phase}")
			g.Expect(perr).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal(string(topologyPhaseBlocked)),
				"a cluster with no promotable replica must park in Blocked")

			reason, rerr := clusterField(cluster, "{.status.phaseReason}")
			g.Expect(rerr).NotTo(HaveOccurred())
			g.Expect(reason).To(ContainSubstring("maxTransactionsBehind"),
				"the block must be attributed to the lag bound, not to some other failure")
			g.Expect(reason).To(ContainSubstring("transactions behind"),
				"the reason must quantify the data loss that was refused")
		}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the lagging replica is never promoted while the primary is down")
		Consistently(func(g Gomega) {
			g.Expect(clusterPrimary(cluster)).NotTo(Equal(replica),
				"promoting the lagging replica would discard the transactions it never received")
			target, terr := clusterField(cluster, "{.status.targetPrimary}")
			g.Expect(terr).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(target)).NotTo(Equal(replica),
				"the lagging replica must not even be elected as a target")
		}, e2eTimeout(90*time.Second), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("unfencing the primary %s to recover the cluster", primary))
		unfence(primary)
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))

		By("verifying the transactions the guard refused to lose are still there, on both instances")
		// This is what the block bought. Had the replica been promoted, these rows,
		// committed on the primary but never received by the replica, would have been
		// gone for good.
		for _, pod := range []string{primary, replica} {
			Eventually(func(g Gomega) {
				out, cerr := exec(pod, "app", password, "app", "SELECT COUNT(*) FROM lag_guard;")
				g.Expect(cerr).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal(fmt.Sprintf("%d", burn+1)),
					"%s must hold the seed row plus all %d burned transactions", pod, burn)
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		}
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
}

var _ = Describe("Failover lag guard", Ordered, Label("feature"), func() {
	lagGuardSpec("lagguard", "mysql", instanceImage, mysqlExec)
})

// The MariaDB counterpart. The guard itself is flavor-agnostic operator logic,
// but the transaction gap it measures is not: MariaDB counts GTIDs per
// replication domain rather than per server UUID, so the arithmetic behind the
// bound is a different implementation and needs its own end-to-end proof.
var _ = Describe("MariaDB failover lag guard", Ordered, Label("flavor", "mariadb"), func() {
	lagGuardSpec("mdb-lagguard", "mariadb", mariadbImage, mariadbExec)
})

// burnTransactions builds a batch of single-row inserts. Each is its own
// autocommitted transaction, hence its own GTID, which is what the bound counts.
func burnTransactions(n int) string {
	var sql strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&sql, "REPLACE INTO lag_guard VALUES (%d);", i)
	}
	return sql.String()
}

// recordedGTID reads the position the operator last observed for an instance.
func recordedGTID(cluster, instance string) string {
	out, err := clusterField(cluster, fmt.Sprintf("{.status.gtidExecutedByInstance['%s']}", instance))
	Expect(err).NotTo(HaveOccurred(), "failed to read the recorded GTID of %s", instance)
	return strings.TrimSpace(out)
}

func fence(pod string) {
	GinkgoHelper()
	_, err := kubectl("annotate", "pod", pod, "-n", testNamespace, fencingAnnotation+"=true", "--overwrite")
	Expect(err).NotTo(HaveOccurred(), "failed to fence %s", pod)
}

func unfence(pod string) {
	GinkgoHelper()
	_, err := kubectl("annotate", "pod", pod, "-n", testNamespace, fencingAnnotation+"-")
	Expect(err).NotTo(HaveOccurred(), "failed to unfence %s", pod)
}

// expectFenced waits for the operator to acknowledge the fence, which is also
// when it has stopped the instance's mysqld.
func expectFenced(cluster, pod string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		fenced, err := clusterField(cluster, "{.status.fencedInstances[*]}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.Fields(fenced)).To(ContainElement(pod))
	}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
}

func lagGuardClusterManifest(name, flavor, image string, instances, maxBehind int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  flavor: %[3]s
  instances: %[4]d
  imageName: %[5]s
  failoverDelay: 10
  failoverPolicy:
    maxTransactionsBehind: %[6]d
  storage:
    size: 2Gi
%[7]s
  mysql:
    binlogFormat: ROW
%[8]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, flavor, instances, image, maxBehind, e2eInstanceResources, e2eMySQLParameters)
}
