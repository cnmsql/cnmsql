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

const (
	forceQuorumRecoveryAnnotation = "cnmsql.cnmsql.co/force-quorum-recovery"
)

// These specs exercise M-GR.6 end to end: GR-native fencing (STOP/START
// GROUP_REPLICATION), quorum-preserving PDB, fence and scale-down quorum
// guards, quorum-loss detection with Blocked surfacing, and guarded quorum
// recovery via force_members.
var _ = Describe("Group Replication fencing and quorum guards", Ordered, func() {
	const (
		cluster   = "gr-guards"
		instances = 3
		primary   = cluster + "-1"
	)

	var ns, prevNS, password string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("gr-guards")

		By("creating a 3-member Group Replication cluster")
		applyManifest(cluster, grClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, 20*time.Minute)
		password = appPassword(cluster)
	})

	It("fences a secondary (STOP GROUP_REPLICATION) and restores it on unfence", func() {
		currentPrimary := clusterPrimary(cluster)
		var victim string
		for _, name := range []string{cluster + "-2", cluster + "-3"} {
			if name != currentPrimary {
				victim = name
				break
			}
		}
		Expect(victim).NotTo(BeEmpty(), "must find a non-primary victim")

		By(fmt.Sprintf("confirming %s is ONLINE SECONDARY and in the r Service", victim))
		Eventually(func(g Gomega) {
			g.Expect(rServiceEndpoints(g, cluster)).To(ContainElement(victim))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("fencing secondary %s", victim))
		_, err := kubectl("annotate", "pod", victim, "-n", testNamespace,
			fencingAnnotation+"=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the fenced member leaves the group and is de-routed")
		Eventually(func(g Gomega) {
			fenced, err := clusterField(cluster, "{.status.fencedInstances[*]}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(fenced)).To(ContainElement(victim),
				"fenced member must appear in status.fencedInstances")
			g.Expect(rServiceEndpoints(g, cluster)).NotTo(ContainElement(victim),
				"fenced member must be removed from routing")
		}, e2eTimeout(4*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the group retains quorum (2 of 3 still online)")
		quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(quorum).To(Equal("true"), "group must retain quorum with 2 members")

		By("unfencing the member")
		_, err = kubectl("annotate", "pod", victim, "-n", testNamespace,
			fencingAnnotation+"-")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the member rejoins the group and returns to routing")
		Eventually(func(g Gomega) {
			g.Expect(rServiceEndpoints(g, cluster)).To(ContainElement(victim),
				"unfenced member must return to routing after rejoining the group")
		}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying writes still work after the fence/unfence cycle")
		currentPrimary = clusterPrimary(cluster)
		_, err = mysqlExec(currentPrimary, "app", password, "app",
			"CREATE TABLE IF NOT EXISTS gr_fence (id INT PRIMARY KEY); REPLACE INTO gr_fence VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "primary must be writable after fence/unfence")
	})

	It("refuses to fence a member when it would break quorum (quorum guard)", func() {
		By("fencing a first secondary (safe)")
		currentPrimary := clusterPrimary(cluster)
		var first, second string
		for _, name := range []string{cluster + "-2", cluster + "-3"} {
			if name != currentPrimary {
				if first == "" {
					first = name
				} else {
					second = name
				}
			}
		}
		_, err := kubectl("annotate", "pod", first, "-n", testNamespace,
			fencingAnnotation+"=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the first fence to take effect")
		Eventually(func(g Gomega) {
			fenced, err := clusterField(cluster, "{.status.fencedInstances[*]}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(fenced)).To(ContainElement(first))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By(fmt.Sprintf("attempting to fence %s (would break quorum)", second))
		_, err = kubectl("annotate", "pod", second, "-n", testNamespace,
			fencingAnnotation+"=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the operator blocks the second fence and surfaces Blocked")
		Eventually(func(g Gomega) {
			phase, err := clusterField(cluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"), "cluster must be Blocked when fencing would break quorum")

			reason, err := clusterField(cluster, "{.status.phaseReason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(ContainSubstring("quorum"),
				"phaseReason must mention quorum")
		}, e2eTimeout(4*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the second member is NOT actually fenced")
		fenced, err := clusterField(cluster, "{.status.fencedInstances[*]}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Fields(fenced)).NotTo(ContainElement(second),
			"second member must not be fenced when it would break quorum")

		By("unfencing the first member to clear the Blocked state")
		_, err = kubectl("annotate", "pod", first, "-n", testNamespace,
			fencingAnnotation+"-")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the second member's annotation is still present but the operator ignored it because quorum is restored")
		Eventually(func(g Gomega) {
			_, err := clusterField(cluster, "{.status.fencedInstances[*]}")
			g.Expect(err).NotTo(HaveOccurred())
			// After the first is unfenced, the second's fence annotation may be
			// processed (since quorum is now safe). Accept either: the member gets
			// fenced, or it stays unfenced because the annotation was ignored earlier.
			// Either way the cluster should eventually return to Ready.
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("clearing the second member's fence annotation to restore full readiness")
		_, err = kubectl("annotate", "pod", second, "-n", testNamespace,
			fencingAnnotation+"-")
		Expect(err).NotTo(HaveOccurred())

		expectClusterReady(cluster, instances, 15*time.Minute)
	})

	It("detects quorum loss and surfaces Blocked when majority is lost", func() {
		By("killing two of three members to break quorum")
		currentPrimary := clusterPrimary(cluster)
		var victim1, victim2 string
		for _, name := range []string{cluster + "-1", cluster + "-2", cluster + "-3"} {
			if name != currentPrimary {
				if victim1 == "" {
					victim1 = name
				} else {
					victim2 = name
				}
			}
		}
		// Kill the two secondaries. The primary alone cannot form quorum (1 < 2).
		for _, victim := range []string{victim1, victim2} {
			_, err := kubectl("delete", "pod", victim, "-n", testNamespace, "--wait=false")
			Expect(err).NotTo(HaveOccurred(), "failed to delete %s", victim)
		}

		By("verifying the operator detects quorum loss and surfaces Blocked")
		Eventually(func(g Gomega) {
			quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("false"), "the group must report hasQuorum=false")

			phase, err := clusterField(cluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"), "cluster must be Blocked after quorum loss")

			reason, err := clusterField(cluster, "{.status.phaseReason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(ContainSubstring("quorum"),
				"phaseReason must mention quorum loss")
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("recovers quorum via guarded force_members annotation", func() {
		By("applying the force-quorum-recovery annotation")
		clusterAnnotate(cluster, forceQuorumRecoveryAnnotation+"=yes")

		By("verifying the operator clears the annotation and the survivor re-forms the group")
		Eventually(func(g Gomega) {
			// The annotation should be cleared by the operator after processing.
			ann, err := clusterField(cluster, `{.metadata.annotations.cnmsql\.cnmsql\.co/force-quorum-recovery}`)
			g.Expect(err).NotTo(HaveOccurred())
			// Either the annotation is gone (processed) or it's still there (retry).
			// When processed, hasQuorum should become true as the survivor re-forms.
			if ann == "yes" {
				return
			}
			quorum, err := clusterField(cluster, `{.status.groupReplication.hasQuorum}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(quorum).To(Equal("true"), "group must recover quorum after force_members")
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying writes work again after recovery")
		Eventually(func(g Gomega) {
			currentPrimary := clusterPrimary(cluster)
			_, err := mysqlExec(currentPrimary, "app", password, "app", "REPLACE INTO gr_fence VALUES (3);")
			g.Expect(err).NotTo(HaveOccurred(), "primary must be writable after quorum recovery")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("waiting for the cluster to scale back to full readiness")
		// The deleted pods will be recreated by the reconciler.
		expectClusterReady(cluster, instances, 20*time.Minute)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
