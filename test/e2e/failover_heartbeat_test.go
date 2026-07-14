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

// The maxReplicationLag failover guard, and the heartbeat it reads.
//
// maxTransactionsBehind bounds data loss in transactions. This bounds it in
// seconds of writes, which is the unit a recovery objective is actually written
// in: a hundred transactions may be a millisecond of writes or an hour of them,
// and only the clock version can be held against an RPO.
//
// The number comes from the heartbeat the primary's instance manager stamps into
// a replicated table once a second. A replica subtracts the newest stamp it has
// applied from its own clock, and the difference is how old the most recent write
// it has caught up to is. The server cannot answer that question itself:
// Seconds_Behind_Source times the applier against the events it already received,
// so a replica that stopped receiving reads zero once its relay log drains, and
// it reads NULL whenever the IO thread is disconnected, which is the state every
// replica is in the moment its primary dies.
//
// The specs below freeze a replica by fencing it, let the primary run on for
// well past the bound, and only then take the primary down (by fencing it too,
// so it stays down for as long as the assertions need rather than being
// recreated underneath them). The frozen replica comes back healthy but tens of
// seconds of writes behind, and the operator must refuse to promote it.

// heartbeatSpec runs the scenario against one flavor. exec is the flavor's SQL
// client helper (mysqlExec / mariadbExec), which share a signature.
func heartbeatSpec(
	cluster, flavor, image string,
	exec func(pod, user, password, database, sql string) (string, error),
) {
	const (
		instances = 2
		// The bound, in seconds of writes.
		maxLagSeconds = 5
		// How long the replica stays frozen while the primary keeps committing. It
		// is comfortably past the bound, so no plausible timing slack in the
		// operator's downtime subtraction can bring the replica back inside it.
		freeze = 40 * time.Second
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace(cluster)

		By(fmt.Sprintf("creating a 2-instance %s cluster with maxReplicationLag=%ds", flavor, maxLagSeconds))
		applyManifest(cluster, heartbeatClusterManifest(cluster, flavor, image, instances, maxLagSeconds))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})

	It("measures replication lag from the heartbeat the primary stamps", func() {
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, instances, primary)

		By("verifying the heartbeat schema is not exposed to the application user")
		// The heartbeat is the operator's own bookkeeping, so the app owner has no
		// grant on it. Nothing enforces that deliberately, it falls out of the schema
		// being created outside the app database, which makes it worth pinning: a
		// future change that hands the app user broader grants would otherwise quietly
		// expose it.
		_, err := exec(replica, "app", appPassword(cluster), "heartbeat", "SELECT COUNT(*) FROM heartbeat;")
		Expect(err).To(HaveOccurred(), "the application user must not be able to read the heartbeat schema")

		By("verifying the primary stamps the heartbeat table and the replica receives it")
		// The replica reads the table it received through replication, not one of its
		// own: only the writable primary may stamp it. A stamp written on a replica
		// would be a transaction its primary never issued, an errant transaction that
		// would strand it as permanently diverged.
		root := rootPassword(cluster)
		Eventually(func(g Gomega) {
			out, rerr := exec(replica, "root", root, "heartbeat", "SELECT COUNT(*) FROM heartbeat;")
			g.Expect(rerr).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("1"),
				"the replica must hold exactly the one row the primary stamps")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying a healthy replica reports a lag of a few seconds at most")
		Eventually(func(g Gomega) {
			lag := heartbeatLag(cluster, replica)
			g.Expect(lag).NotTo(BeNil(), "a healthy replica must report a heartbeat reading")
			g.Expect(*lag).To(BeNumerically("<", (maxLagSeconds*time.Second)),
				"a replica in sync must read well inside the bound")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("refuses to promote a replica that is further behind than maxReplicationLag", func() {
		password := appPassword(cluster)
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, instances, primary)

		By(fmt.Sprintf("seeding a table on the primary %s and letting %s replicate it", primary, replica))
		_, err := exec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS rpo_guard (id INT PRIMARY KEY);"+
				"REPLACE INTO rpo_guard VALUES (0);")
		Expect(err).NotTo(HaveOccurred(), "failed to seed the primary")
		Eventually(func(g Gomega) {
			out, rerr := exec(replica, "app", password, "app", "SELECT id FROM rpo_guard WHERE id = 0;")
			g.Expect(rerr).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("0"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed(),
			"the replica must be in sync before it is frozen, so the only gap is the one this test creates")

		By(fmt.Sprintf("fencing %s to freeze it: mysqld stops, so no further heartbeat reaches it", replica))
		fence(replica)
		expectFenced(cluster, replica)

		By(fmt.Sprintf("committing on the primary for %s, which the frozen replica will never receive", freeze))
		// Both bounds need this: it is the transactions the replica misses, and, since
		// the writes are spread over freeze, it is also the seconds of writes it falls
		// behind by. Without any write the primary's heartbeat would still advance, but
		// there would be no lost data to measure a lag against.
		deadline := time.Now().Add(freeze)
		for i := 1; time.Now().Before(deadline); i++ {
			_, err = exec(primary, "app", password, "app", fmt.Sprintf("REPLACE INTO rpo_guard VALUES (%d);", i))
			Expect(err).NotTo(HaveOccurred(), "failed to write to the primary")
			time.Sleep(time.Second)
		}

		By(fmt.Sprintf("fencing the primary %s to take it down and hold it down", primary))
		fence(primary)
		expectFenced(cluster, primary)

		By(fmt.Sprintf("unfencing %s so it comes back healthy, but ~%s of writes behind", replica, freeze))
		unfence(replica)

		By("verifying the operator blocks the failover and names the writes it refused to lose")
		// The replica's raw heartbeat reading keeps climbing while the primary is down,
		// so it will read far more than freeze by now. The operator subtracts how long
		// the primary has been failing, which leaves the lag as it stood when the
		// primary died. That is the number the bound is checked against and the number
		// the reason must quote.
		Eventually(func(g Gomega) {
			phase, perr := clusterField(cluster, "{.status.phase}")
			g.Expect(perr).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal(string(topologyPhaseBlocked)),
				"a cluster with no replica inside the RPO must park in Blocked")

			reason, rerr := clusterField(cluster, "{.status.phaseReason}")
			g.Expect(rerr).NotTo(HaveOccurred())
			g.Expect(reason).To(ContainSubstring("maxReplicationLag"),
				"the block must be attributed to the time bound, not to some other failure")
			g.Expect(reason).To(ContainSubstring("of writes behind"),
				"the reason must quantify, in seconds of writes, the data loss that was refused")
		}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the lagging replica is never promoted while the primary is down")
		Consistently(func(g Gomega) {
			g.Expect(clusterPrimary(cluster)).NotTo(Equal(replica),
				"promoting the replica would discard the writes it never received")
			target, terr := clusterField(cluster, "{.status.targetPrimary}")
			g.Expect(terr).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(target)).NotTo(Equal(replica),
				"the lagging replica must not even be elected as a target")
		}, e2eTimeout(90*time.Second), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("unfencing the primary %s to recover the cluster", primary))
		unfence(primary)
		expectClusterRecovers(cluster, instances, e2eTimeout(20*time.Minute))

		By("verifying the writes the guard refused to lose are still there, on both instances")
		// This is what the block bought. Had the replica been promoted, every row
		// committed during the freeze would have been gone for good.
		for _, pod := range []string{primary, replica} {
			Eventually(func(g Gomega) {
				out, cerr := exec(pod, "app", password, "app", "SELECT COUNT(*) FROM rpo_guard;")
				g.Expect(cerr).NotTo(HaveOccurred())
				count := strings.TrimSpace(out)
				g.Expect(count).NotTo(Equal("1"),
					"%s holds only the seed row: the writes made during the freeze were lost", pod)
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		}
	})
}

var _ = Describe("Failover replication-lag guard", Ordered, Label("feature"), func() {
	heartbeatSpec("rpoguard", "mysql", instanceImage, mysqlExec)
})

// The MariaDB counterpart. The heartbeat SQL is deliberately flavor-neutral, but
// "deliberately" is not "proven": UTC_TIMESTAMP(6), TIMESTAMPDIFF and INSERT ...
// ON DUPLICATE KEY UPDATE all have to behave the same way on both engines for the
// reading to mean anything, and the guard reads it on both.
var _ = Describe("MariaDB failover replication-lag guard", Ordered, Label("flavor", "mariadb"), func() {
	heartbeatSpec("mdb-rpoguard", "mariadb", mariadbImage, mariadbExec)
})

// heartbeatLag reads an instance's heartbeat reading out of the Cluster status,
// as the operator sees it. It returns nil when the instance reported none.
func heartbeatLag(cluster, instance string) *time.Duration {
	out, err := clusterField(cluster,
		fmt.Sprintf("{.status.replicationLagByInstance['%s']}", instance))
	Expect(err).NotTo(HaveOccurred(), "failed to read the heartbeat reading of %s", instance)
	raw := strings.TrimSpace(out)
	if raw == "" {
		return nil
	}
	var millis int64
	_, err = fmt.Sscanf(raw, "%d", &millis)
	Expect(err).NotTo(HaveOccurred(), "unparseable heartbeat reading %q for %s", raw, instance)
	lag := time.Duration(millis) * time.Millisecond
	return &lag
}

func heartbeatClusterManifest(name, flavor, image string, instances, maxLagSeconds int) string {
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
    maxReplicationLag: %[6]ds
  replication:
    heartbeat:
      enabled: true
      interval: 1s
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
`, name, testNamespace, flavor, instances, image, maxLagSeconds, e2eInstanceResources, e2eMySQLParameters)
}
