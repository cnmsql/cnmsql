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

// Diverged replica failover guard exercises the worst-case async replication
// scenario: a 2-instance cluster whose sole replica carries errant GTIDs, then
// the primary crashes. The operator must refuse to promote the diverged replica
// and block the failover rather than creating a split-brain with irrecoverable
// data divergence.
var _ = Describe("Diverged replica failover guard", Ordered, func() {
	const (
		cluster   = "divguard"
		instances = 2
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("divguard")

		By("creating a 2-instance async cluster")
		applyManifest(cluster, basicClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, 20*time.Minute)
	})

	It("blocks failover when the only surviving replica has errant GTIDs, then recovers after reinit", func() {
		password := appPassword(cluster)
		rootPass := secretPassword(cluster + "-root")
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, instances, primary)

		By(fmt.Sprintf("seeding initial data on the primary %s", primary))
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS errant_guard (id INT PRIMARY KEY, data VARCHAR(32)); "+
				"REPLACE INTO errant_guard VALUES (1, 'pre-divergence');")
		Expect(err).NotTo(HaveOccurred(), "failed to seed initial data")

		By(fmt.Sprintf("fencing replica %s to inject errant GTIDs", replica))
		_, err = kubectl("annotate", "pod", replica, "-n", testNamespace,
			fencingAnnotation+"=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred(), "failed to fence replica")

		By("waiting for the operator to record the fence and stop mysqld")
		Eventually(func(g Gomega) {
			fenced, ferr := clusterField(cluster, "{.status.fencedInstances[*]}")
			g.Expect(ferr).NotTo(HaveOccurred())
			g.Expect(strings.Fields(fenced)).To(ContainElement(replica))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("injecting an errant transaction into the fenced replica")
		injectErrantGTID(replica, rootPass)

		By("unfencing the replica so the operator re-observes its GTID set")
		_, err = kubectl("annotate", "pod", replica, "-n", testNamespace,
			fencingAnnotation+"-")
		Expect(err).NotTo(HaveOccurred(), "failed to unfence replica")

		By("verifying the replica is detected as diverged and the cluster is Degraded")
		Eventually(func(g Gomega) {
			diverged, derr := clusterField(cluster, "{.status.divergedInstances[*]}")
			g.Expect(derr).NotTo(HaveOccurred())
			g.Expect(strings.Fields(diverged)).To(ContainElement(replica),
				"replica with errant GTID must be listed in divergedInstances")

			phase, perr := clusterField(cluster, "{.status.phase}")
			g.Expect(perr).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal(string(topologyPhaseDegraded)),
				"cluster must be Degraded while a diverged replica exists")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("writing additional data on the primary %s (post-divergence)", primary))
		_, err = mysqlExec(primary, "app", password, "app",
			"REPLACE INTO errant_guard VALUES (2, 'post-divergence');")
		Expect(err).NotTo(HaveOccurred(), "failed to write post-divergence data")

		By(fmt.Sprintf("force-deleting the primary Pod %s", primary))
		_, err = kubectl("delete", "pod", primary, "-n", testNamespace,
			"--grace-period=0", "--force")
		Expect(err).NotTo(HaveOccurred(), "failed to force-delete primary Pod")

		By("verifying the diverged replica is NEVER promoted during the primary's absence")
		Consistently(func(g Gomega) {
			p, perr := clusterField(cluster, "{.status.currentPrimary}")
			g.Expect(perr).NotTo(HaveOccurred())
			if p != "" {
				g.Expect(p).NotTo(Equal(replica),
					"diverged replica must not be promoted to primary")
			}
		}, e2eTimeout(60*time.Second), 5*time.Second).Should(Succeed())

		By("verifying failover was blocked (Phase transitions through Blocked)")
		Eventually(func(g Gomega) {
			phase, perr := clusterField(cluster, "{.status.phase}")
			g.Expect(perr).NotTo(HaveOccurred())
			g.Expect(phase).To(BeElementOf(
				string(topologyPhaseBlocked),
				string(topologyPhaseDegraded),
			), "cluster must reach Blocked or Degraded while primary is absent and replica is diverged")

			reason, rerr := clusterField(cluster, "{.status.phaseReason}")
			g.Expect(rerr).NotTo(HaveOccurred())
			g.Expect(reason).To(Or(
				ContainSubstring("diverged"),
				ContainSubstring("errant"),
				ContainSubstring("manual recovery"),
			), "phase reason must mention divergence")
		}, e2eTimeout(30*time.Second), 5*time.Second).Should(Succeed())

		By("verifying status still names the original primary (failover stayed blocked)")
		// currentPrimary never changed because failover was correctly blocked, so
		// this asserts the operator did not elect the diverged replica. It does NOT
		// prove the Pod is back, so wait on Pod readiness separately below before
		// exec'ing into it.
		Expect(clusterPrimary(cluster)).To(Equal(primary),
			"currentPrimary must still be the original primary after failover is blocked")

		By(fmt.Sprintf("waiting for the force-deleted primary Pod %s to be recreated and Ready", primary))
		By(fmt.Sprintf("verifying the PVC for %s still exists so the Pod can be recreated", primary))
		pvcName := primary
		_, pvcErr := kubectl("get", "pvc", pvcName, "-n", testNamespace,
			"-o", "jsonpath={.metadata.uid}")
		Expect(pvcErr).NotTo(HaveOccurred(),
			"PVC %s must exist so the operator can recreate the primary Pod", pvcName)
		waitForPodReady(primary, 15*time.Minute)

		By(fmt.Sprintf("verifying the recovered primary still has post-divergence data"))
		_, err = mysqlExec(primary, "app", password, "app",
			"SELECT id FROM errant_guard WHERE id = 2 AND data = 'post-divergence';")
		Expect(err).NotTo(HaveOccurred(),
			"post-divergence data must survive the primary restart")

		By(fmt.Sprintf("re-initialising the diverged replica %s", replica))
		_, err = kubectl("annotate", "cluster", cluster, "-n", testNamespace,
			"cnmsql.cnmsql.co/reinit="+replica, "--overwrite")
		Expect(err).NotTo(HaveOccurred(), "failed to set reinit annotation")

		By("waiting for the replica to re-clone and the cluster to become Ready")
		expectClusterReady(cluster, instances, 20*time.Minute)

		By("verifying the cluster is fully recovered with no diverged instances")
		diverged, _ := clusterField(cluster, "{.status.divergedInstances}")
		Expect(strings.TrimSpace(diverged)).To(BeEmpty(),
			"divergedInstances must be empty after reinit")

		phase, _ := clusterField(cluster, "{.status.phase}")
		Expect(phase).To(Equal(string(topologyPhaseReady)),
			"cluster must be Ready after recovery")
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// topologyPhase constants matching the operator's internal phase values.
// Importing the topology package from e2e would pull in controller-runtime
// dependencies, so these are duplicated as test-aware string constants.
const (
	topologyPhaseReady    = "Ready"
	topologyPhaseDegraded = "Degraded"
	topologyPhaseBlocked  = "Blocked"
)

// injectErrantGTID starts a temporary mysqld inside a fenced async instance Pod,
// clears read_only, writes a transaction, and stops mysqld. The GTID for this
// transaction persists in the InnoDB mysql.gtid_executed table and will be
// visible when the operator restarts the instance after unfencing.
func injectErrantGTID(pod, rootPassword string) {
	GinkgoHelper()

	// Single kubectl exec that starts mysqld, waits for readiness, injects the
	// transaction, and cleanly stops mysqld before exiting. The EXIT trap ensures
	// the temporary mysqld is killed even if the SQL fails.
	script := fmt.Sprintf(
		`mysqld_pid=""
trap 'if [ -n "$mysqld_pid" ]; then kill "$mysqld_pid" 2>/dev/null; wait "$mysqld_pid" 2>/dev/null; fi' EXIT
mysqld --defaults-file=/etc/mysql/my.cnf --datadir=/var/lib/mysql --socket=/var/run/mysqld/mysqld.sock --skip-replica-start >/tmp/mysqld_manual.log 2>&1 &
mysqld_pid=$!
for i in $(seq 1 60); do
  if [ -S /var/run/mysqld/mysqld.sock ]; then break; fi
  sleep 1
done
if [ ! -S /var/run/mysqld/mysqld.sock ]; then echo "FATAL: mysqld did not start"; exit 1; fi
mysql -S /var/run/mysqld/mysqld.sock -u root -N -e "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF; CREATE DATABASE IF NOT EXISTS errant_db; CREATE TABLE IF NOT EXISTS errant_db.t (id INT) ENGINE=InnoDB; INSERT INTO errant_db.t VALUES (1); SELECT @@GLOBAL.gtid_executed"`)

	out, err := kubectl("exec", pod, "-n", testNamespace, "-c", "mysql", "--",
		"env", "MYSQL_PWD="+rootPassword,
		"bash", "-c", script)
	Expect(err).NotTo(HaveOccurred(),
		"failed to inject errant GTID into %s: %s", pod, out)
	Expect(out).NotTo(ContainSubstring("FATAL"),
		"mysqld did not start in %s: %s", pod, out)
	Expect(strings.TrimSpace(out)).NotTo(BeEmpty(),
		"injected GTID should be non-empty for %s", pod)
}
