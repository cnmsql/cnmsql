//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The steering and damping halves of spec.failoverPolicy: preferredPrimary,
// which says where the primary role belongs, and minTimeBetweenFailovers, which
// says how often the operator may move it on its own.
//
// Both are flavor-agnostic operator logic rather than engine arithmetic, so
// unlike the lag guard (whose GTID counting differs between MySQL and MariaDB)
// they are proven once, against MySQL.
//
// As in the lag guard, an instance is taken down by fencing it rather than by
// deleting its Pod: a deleted Pod is recreated within a minute or two and would
// race the assertions, while a fenced instance stays down until it is unfenced.

var _ = Describe("Failover policy: preferred primary", Ordered, Label("feature"), func() {
	const (
		cluster   = "preferred"
		instances = 3
	)
	// The bootstrap primary is always ordinal 1, so naming ordinals 2 and 3 as the
	// preference means the operator has to move the role to satisfy it.
	var (
		first     = fmt.Sprintf("%s-1", cluster)
		preferred = fmt.Sprintf("%s-2", cluster)
		runnerUp  = fmt.Sprintf("%s-3", cluster)
		ns        string
		prevNS    string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace(cluster)

		By(fmt.Sprintf("creating a %d-instance cluster that prefers %s, then %s", instances, preferred, runnerUp))
		applyManifest(cluster, preferredPrimaryClusterManifest(cluster, instances, preferred, runnerUp))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})

	It("moves the primary onto the preferred instance without any failure", func() {
		// Nothing has gone wrong here. The cluster bootstrapped its primary on
		// ordinal 1, the preference asks for ordinal 2, and the operator performs a
		// planned switchover to get there.
		By(fmt.Sprintf("waiting for the operator to switch the primary from %s to %s", first, preferred))
		expectPrimary(cluster, preferred, 10*time.Minute)
		expectClusterReady(cluster, instances, e2eTimeout(10*time.Minute))

		By("verifying no failover was recorded: a preference is served by switchover, not by failing over")
		stamp, err := clusterField(cluster, "{.status.lastFailoverTimestamp}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(stamp)).To(BeEmpty(),
			"a planned switchover onto the preferred primary must not count as a failover")
	})

	It("elects the next preferred instance when the preferred primary fails", func() {
		By(fmt.Sprintf("fencing the preferred primary %s to take it down", preferred))
		fence(preferred)
		expectFenced(cluster, preferred)

		// Both survivors are healthy and, on an idle cluster, hold identical GTID
		// sets. Ordinal order alone would elect the lower one; the preference reaches
		// past it to the instance the cluster actually asked for.
		By(fmt.Sprintf("verifying the failover elects %s, the next preferred, over the lower-ordinal %s", runnerUp, first))
		expectPrimary(cluster, runnerUp, 10*time.Minute)
	})

	It("brings the primary back to the preferred instance once it recovers", func() {
		By(fmt.Sprintf("unfencing %s so it rejoins as a healthy replica", preferred))
		unfence(preferred)

		By(fmt.Sprintf("waiting for the operator to fail back to %s", preferred))
		expectPrimary(cluster, preferred, 10*time.Minute)
		expectClusterRecovers(cluster, instances, e2eTimeout(15*time.Minute))
	})
})

var _ = Describe("Failover policy: anti-flapping timers", Ordered, Label("feature"), func() {
	const (
		cluster   = "antiflap"
		instances = 3
		// Long enough that the second failover cannot possibly clear it while the
		// assertions run, so a promotion during the window is a real regression and
		// never a timing artefact.
		cooldown = "30m"
	)
	var (
		ns          string
		prevNS      string
		firstFailed string
		promoted    string
	)

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace(cluster)

		By(fmt.Sprintf("creating a %d-instance cluster with minTimeBetweenFailovers=%s", instances, cooldown))
		applyManifest(cluster, antiFlapClusterManifest(cluster, instances, cooldown))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})

	It("performs the first failover and records when it happened", func() {
		firstFailed = clusterPrimary(cluster)

		By(fmt.Sprintf("fencing the primary %s to take it down", firstFailed))
		fence(firstFailed)
		expectFenced(cluster, firstFailed)

		By("waiting for the operator to promote a replica")
		Eventually(func(g Gomega) {
			current, err := clusterField(cluster, "{.status.currentPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(current)).NotTo(Or(BeEmpty(), Equal(firstFailed)))
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())
		promoted = clusterPrimary(cluster)

		By("verifying the promotion is stamped, since it is what the cooldown is measured from")
		stamp, err := clusterField(cluster, "{.status.lastFailoverTimestamp}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(stamp)).NotTo(BeEmpty(),
			"an automatic failover must record lastFailoverTimestamp")
	})

	It("refuses a second failover inside the cooldown", func() {
		// A third instance is still up and would be a perfectly good candidate, so a
		// block here can only be attributed to the cooldown.
		survivor := remainingInstance(cluster, instances, firstFailed, promoted)

		By(fmt.Sprintf("fencing the freshly promoted primary %s, moments after it was promoted", promoted))
		fence(promoted)
		expectFenced(cluster, promoted)

		By("verifying the operator blocks the failover and names the time it is waiting out")
		Eventually(func(g Gomega) {
			phase, err := clusterField(cluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal(string(topologyPhaseBlocked)),
				"a failover refused by the cooldown must park the cluster in Blocked")

			reason, err := clusterField(cluster, "{.status.phaseReason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(ContainSubstring("minTimeBetweenFailovers"),
				"the block must be attributed to the cooldown, not to some other failure")
		}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("verifying the role is never walked onto %s while the cooldown runs", survivor))
		Consistently(func(g Gomega) {
			current, err := clusterField(cluster, "{.status.currentPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(current)).To(Equal(promoted),
				"the primary must stay where the first failover put it")

			target, err := clusterField(cluster, "{.status.targetPrimary}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(target)).NotTo(Equal(survivor),
				"the surviving replica must not even be elected as a target")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("recovers the cluster once the failed instances come back", func() {
		// The refusal never stopped the cluster from healing itself: unfencing brings
		// the instances back, and the primary that was blocked from moving is the one
		// that recovers in place, which is the outcome the cooldown exists to allow.
		By("unfencing both fenced instances")
		unfence(promoted)
		unfence(firstFailed)
		expectClusterRecovers(cluster, instances, e2eTimeout(20*time.Minute))
	})
})

// expectPrimary waits for a Cluster to settle on the named primary.
func expectPrimary(cluster, want string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		current, err := clusterField(cluster, "{.status.currentPrimary}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(current)).To(Equal(want))
	}, e2eTimeout(timeout), 5*time.Second).Should(Succeed(),
		"cluster %s did not settle on %s as its primary", cluster, want)
}

// remainingInstance returns the instance that is none of the excluded ones.
func remainingInstance(cluster string, instances int, excluded ...string) string {
	GinkgoHelper()
	for i := 1; i <= instances; i++ {
		name := fmt.Sprintf("%s-%d", cluster, i)
		if !slices.Contains(excluded, name) {
			return name
		}
	}
	Fail("no instance left outside the excluded set")
	return ""
}

func preferredPrimaryClusterManifest(name string, instances int, preferred ...string) string {
	var list strings.Builder
	for _, instance := range preferred {
		fmt.Fprintf(&list, "\n      - %s", instance)
	}
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  failoverDelay: 10
  failoverPolicy:
    preferredPrimary:%[5]s
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
`, name, testNamespace, instances, instanceImage, list.String(), e2eInstanceResources, e2eMySQLParameters)
}

func antiFlapClusterManifest(name string, instances int, cooldown string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  failoverDelay: 10
  failoverPolicy:
    minTimeBetweenFailovers: %[5]s
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
`, name, testNamespace, instances, instanceImage, cooldown, e2eInstanceResources, e2eMySQLParameters)
}
