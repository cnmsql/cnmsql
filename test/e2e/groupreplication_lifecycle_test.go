//go:build e2e
// +build e2e

package e2e

import (
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs exercise the M-GR.7 lifecycle paths end to end on a single group:
// scale up and back down with quorum preserved, an in-place instance-manager
// upgrade on the primary (no switchover, mysqld never restarts), and
// total-outage re-bootstrap (every member down, then guarded re-form from the
// most-advanced survivor with no data loss). They run Ordered and sequentially
// against one cluster so each step builds on the previous group state.
var _ = Describe("Group Replication lifecycle", Ordered, func() {
	const (
		cluster   = "gr-lifecycle"
		instances = 3
		scaledUp  = 5
	)

	var ns, prevNS, password string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("gr-lifecycle")

		By("creating a 3-member Group Replication cluster")
		applyManifest(cluster, grClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteCluster(cluster)
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, 20*time.Minute)
		password = appPassword(cluster)

		By("seeding a row so later steps can prove the data survives the lifecycle")
		primary := clusterPrimary(cluster)
		Eventually(func(g Gomega) {
			_, err := mysqlExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS gr_life (id INT PRIMARY KEY); REPLACE INTO gr_life VALUES (1);")
			g.Expect(err).NotTo(HaveOccurred())
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("scales up to 5 members with quorum preserved and the new members ONLINE", func() {
		By("requesting scale-up to 5 instances")
		scaleInstances(cluster, scaledUp)

		By("waiting for the cluster to reach Ready at 5 instances")
		expectClusterReady(cluster, scaledUp, 20*time.Minute)

		By("verifying all five members report ONLINE and the group keeps quorum")
		Eventually(func(g Gomega) {
			states, err := clusterField(cluster, `{.status.groupReplication.members[*].state}`)
			g.Expect(err).NotTo(HaveOccurred())
			fields := strings.Fields(states)
			g.Expect(fields).To(HaveLen(scaledUp), "the group must report five members")
			for _, state := range fields {
				g.Expect(state).To(Equal("ONLINE"), "every member must be ONLINE after scale-up")
			}
			quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"), "the group must retain quorum across scale-up")
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())

		By("verifying the quorum denominator (observedViewMax) grew to the new group size")
		Eventually(func(g Gomega) {
			viewMax, err := clusterField(cluster, `{.status.groupReplication.observedViewMax}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(viewMax).To(Equal(strconv.Itoa(scaledUp)),
				"observedViewMax must track the grown group size")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the seeded row replicated to the freshly joined members")
		for _, member := range []string{cluster + "-4", cluster + "-5"} {
			Eventually(func(g Gomega) {
				out, err := mysqlExec(member, "app", password, "", "SELECT id FROM app.gr_life WHERE id = 1;")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(ContainSubstring("1"), "the new member %s must catch up via distributed recovery", member)
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		}
	})

	It("scales back down to 3 members with quorum preserved", func() {
		By("requesting scale-down to 3 instances")
		scaleInstances(cluster, instances)

		By("waiting for the cluster to reach Ready at 3 instances")
		expectClusterReady(cluster, instances, 20*time.Minute)

		By("verifying the group settles to exactly three ONLINE members and keeps quorum")
		Eventually(func(g Gomega) {
			states, err := clusterField(cluster, `{.status.groupReplication.members[*].state}`)
			g.Expect(err).NotTo(HaveOccurred())
			fields := strings.Fields(states)
			g.Expect(fields).To(HaveLen(instances), "the group must report three members after scale-down")
			for _, state := range fields {
				g.Expect(state).To(Equal("ONLINE"), "every remaining member must be ONLINE")
			}
			quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"), "the group must retain quorum across scale-down")
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())

		By("verifying the removed members are gone from routing")
		Eventually(func(g Gomega) {
			for _, removed := range []string{cluster + "-4", cluster + "-5"} {
				g.Expect(rServiceEndpoints(g, cluster)).NotTo(ContainElement(removed),
					"scaled-down member %s must leave routing", removed)
			}
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("upgrades the primary's manager in place without a switchover or mysqld restart", func() {
		primary := clusterPrimary(cluster)

		By("recording the pre-upgrade restart count and mysqld uptime of the primary")
		restartsBefore := podRestartCount(primary)
		var uptimeBefore int
		Eventually(func(g Gomega) {
			uptimeBefore = mysqldUptime(g, primary, password)
			g.Expect(uptimeBefore).To(BeNumerically(">", 0))
		}, e2eTimeout(1*time.Minute), 3*time.Second).Should(Succeed())

		By("triggering an in-place manager re-exec on the primary")
		triggerInPlaceRestart(cluster, primary)

		By("verifying mysqld was never restarted and the primary did not change")
		Consistently(func(g Gomega) {
			g.Expect(podRestartCount(primary)).To(Equal(restartsBefore),
				"the mysql container restarted; the manager swap must not restart the Pod")
			g.Expect(mysqldUptime(g, primary, password)).To(BeNumerically(">=", uptimeBefore),
				"mysqld uptime dropped; the server was restarted instead of adopted")
		}, e2eTimeout(45*time.Second), 10*time.Second).Should(Succeed())
		Expect(clusterPrimary(cluster)).To(Equal(primary),
			"an in-place upgrade must not switch the GR primary over")

		By("verifying the group stays ONLINE with quorum after the in-place upgrade")
		Eventually(func(g Gomega) {
			quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		expectClusterReady(cluster, instances, 15*time.Minute)
	})

	It("rolling-restarts every member primary-last while keeping quorum", func() {
		members := []string{cluster + "-1", cluster + "-2", cluster + "-3"}

		By("recording each member's pod UID before the rollout")
		uidBefore := map[string]string{}
		for _, m := range members {
			uid, err := kubectl("get", "pod", m, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(uid)).NotTo(BeEmpty())
			uidBefore[m] = strings.TrimSpace(uid)
		}

		By("requesting a rolling restart via the restart annotation")
		_, err := kubectl("annotate", "cluster", cluster, "-n", testNamespace,
			"cnmsql.cnmsql.co/restart="+time.Now().UTC().Format(time.RFC3339), "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("verifying every member's Pod is recreated (new UID) and the group regains all three ONLINE")
		Eventually(func(g Gomega) {
			for _, m := range members {
				uid, err := kubectl("get", "pod", m, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(uid)).NotTo(Equal(uidBefore[m]),
					"member %s must be recreated by the rolling restart", m)
			}
			states, err := clusterField(cluster, `{.status.groupReplication.members[*].state}`)
			g.Expect(err).NotTo(HaveOccurred())
			fields := strings.Fields(states)
			g.Expect(fields).To(HaveLen(instances))
			for _, state := range fields {
				g.Expect(state).To(Equal("ONLINE"), "every member must be ONLINE after the rollout")
			}
		}, e2eTimeout(20*time.Minute), 10*time.Second).Should(Succeed())

		By("verifying the group never lost quorum and the primary still serves writes")
		quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(quorum).To(Equal("true"), "a rolling restart must preserve quorum throughout")
		// The pre-rollout primary is restarted last, so after the rollout a primary
		// is elected and writable again (either the same member or a clean handover).
		Eventually(func(g Gomega) {
			p := clusterPrimary(cluster)
			g.Expect(p).NotTo(BeEmpty())
			_, err := mysqlExec(p, "app", password, "app", "REPLACE INTO gr_life VALUES (2);")
			g.Expect(err).NotTo(HaveOccurred(), "primary must serve writes after the rolling restart")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		expectClusterReady(cluster, instances, 15*time.Minute)
	})

	It("re-bootstraps the same group after a total outage with no data loss", func() {
		By("killing every member so no ONLINE survivor remains (total outage)")
		for _, member := range []string{cluster + "-1", cluster + "-2", cluster + "-3"} {
			_, err := kubectl("delete", "pod", member, "-n", testNamespace, "--wait=false")
			Expect(err).NotTo(HaveOccurred(), "failed to delete %s", member)
		}

		// Detection and recovery are watched in a single loop so the brief FullOutage
		// window cannot slip through a gap between two separate Eventually blocks.
		// A --wait=false delete terminates gracefully, so the group keeps quorum for
		// the termination grace period; only once every member is gone does the
		// operator surface FullOutage, and it then auto re-bootstraps from the
		// last-seen primary (no annotation required). phase==FullOutage already
		// implies hasQuorum=false, so the phase is the unambiguous detection signal —
		// polling it directly (and fast) avoids racing two separate status reads.
		By("verifying the operator surfaces FullOutage then auto re-forms the group from the last-seen primary")
		sawFullOutage := false
		Eventually(func(g Gomega) {
			phase, err := clusterField(cluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			if phase == "FullOutage" {
				sawFullOutage = true
			}
			g.Expect(sawFullOutage).To(BeTrue(), "a total outage must surface the FullOutage phase")

			quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"), "the group must auto re-form and regain quorum after re-bootstrap")
		}, e2eTimeout(15*time.Minute), 2*time.Second).Should(Succeed())

		By("verifying the seeded row survived the re-bootstrap (no data loss)")
		Eventually(func(g Gomega) {
			primary := clusterPrimary(cluster)
			out, err := mysqlExec(primary, "app", password, "", "SELECT id FROM app.gr_life WHERE id = 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("1"), "the pre-outage row must survive the re-bootstrap")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("waiting for the cluster to return to full readiness")
		expectClusterReady(cluster, instances, 20*time.Minute)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
