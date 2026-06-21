//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/CloudNative-MySQL/cloudnative-mysql/test/utils"
)

// This spec proves the Phase 2 slice-2 path end to end on a real cluster: with
// spec.inPlaceInstanceManagerUpdates=true, an operator version bump rolls every
// instance manager forward by *streaming the new binary* to each Pod and
// re-execing in place — no Pod restart and no switchover — instead of deleting
// and recreating Pods (the rolling path the "Operator Upgrade" spec covers). The
// proof is that every instance's reported executable hash converges to the new
// operator hash while the mysql container restart count stays flat, mysqld uptime
// keeps climbing, and the primary never changes.
//
// Serial: `make deploy` rolls the shared cluster-wide operator, which watches
// every namespace. While it rolls it stops reconciling Clusters in other specs'
// namespaces, so this must not run alongside any other spec.
var _ = Describe("In-place operator upgrade (streamed)", Ordered, Serial, func() {
	const (
		v3Image     = "example.com/cloudnative-mysql:v0.0.3-inplace"
		clusterName = "inplace-upgrade"
	)

	BeforeAll(func() {
		By("building a manager image with a distinct binary hash")
		insertInPlaceMarker()
		cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", v3Image))
		_, err := utils.Run(cmd)
		restoreE2EMarker()
		Expect(err).NotTo(HaveOccurred(), "Failed to build the in-place upgrade manager image")

		By("loading the in-place upgrade manager image on Kind")
		Expect(utils.LoadImageToKindClusterWithName(v3Image)).To(Succeed(),
			"Failed to load the in-place upgrade manager image into Kind")
	})

	AfterAll(func() {
		restoreE2EMarker()
	})

	It("upgrades every instance in place without restarting mysqld or switching over", func() {
		ns := createTestNamespace("inplace-op-upgrade")
		defer deleteTestNamespace(ns, defaultTestNamespace)

		By("applying a 3-instance Cluster on the currently-deployed operator")
		manifest := fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 3
  imageName: %s
  inPlaceInstanceManagerUpdates: true
  storage:
    size: 1Gi
  mysql:
%s
  bootstrap:
    initdb:
      database: app
      owner: app
%s
`, clusterName, ns, instanceImage, e2eMySQLParameters, e2eInstanceResources)
		applyManifest(clusterName, manifest)
		DeferCleanup(func() {
			deleteCluster(clusterName)
		})

		By("waiting for the cluster to become ready on the initial operator")
		expectClusterReady(clusterName, 3, 20*time.Minute)

		instances := []string{clusterName + "-1", clusterName + "-2", clusterName + "-3"}
		primary := clusterPrimary(clusterName)
		password := appPassword(clusterName)

		By("recording the pre-upgrade operator hash, restart counts and mysqld uptimes")
		initialHash, err := kubectl("get", "cluster", clusterName, "-n", testNamespace,
			"-o", "jsonpath={.status.operatorExecutableHash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(initialHash).NotTo(BeEmpty())

		restartsBefore := map[string]int{}
		uptimeBefore := map[string]int{}
		for _, inst := range instances {
			restartsBefore[inst] = podRestartCount(inst)
			Eventually(func(g Gomega) {
				uptimeBefore[inst] = mysqldUptime(g, inst, password)
				g.Expect(uptimeBefore[inst]).To(BeNumerically(">", 0))
			}, e2eTimeout(1*time.Minute), 3*time.Second).Should(Succeed())
		}

		By("deploying the new operator (the cluster opts into in-place updates via its spec)")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", v3Image)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the in-place upgrade operator")
		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment", "cloudnative-mysql-controller-manager",
			"-n", namespace, "--timeout=180s"))
		Expect(err).NotTo(HaveOccurred(), "Operator did not roll out after the new deploy")

		By("waiting for the operator hash to change after the upgrade")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "cluster", clusterName, "-n", testNamespace,
				"-o", "jsonpath={.status.operatorExecutableHash}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
			g.Expect(out).NotTo(Equal(initialHash), "operator hash should change after the new deploy")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("waiting for every instance manager to converge to the new hash in place")
		Eventually(func(g Gomega) {
			target, err := kubectl("get", "cluster", clusterName, "-n", testNamespace,
				"-o", "jsonpath={.status.operatorExecutableHash}")
			g.Expect(err).NotTo(HaveOccurred())
			for _, inst := range instances {
				instHash, err := kubectl("get", "cluster", clusterName, "-n", testNamespace,
					"-o", fmt.Sprintf(`go-template={{index .status.executableHashByInstance "%s"}}`, inst))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(instHash).To(Equal(target),
					"instance %s manager hash should converge to the new operator hash", inst)
			}
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying each instance took the adopt path (proving an in-place re-exec, not a Pod restart)")
		for _, inst := range instances {
			Eventually(func(g Gomega) {
				logs, err := kubectl("logs", inst, "-n", testNamespace, "-c", "mysql", "--tail=600")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(logs).To(ContainSubstring("Adopting running mysqld after in-place manager upgrade"),
					"instance %s must adopt the running mysqld, proving the manager re-exec'd in place", inst)
			}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
		}

		By("verifying mysqld was never restarted: container restart counts stay flat and uptime keeps climbing")
		for _, inst := range instances {
			Expect(podRestartCount(inst)).To(Equal(restartsBefore[inst]),
				"instance %s mysql container restarted; the streamed manager swap must not restart the Pod", inst)
			Eventually(func(g Gomega) {
				g.Expect(mysqldUptime(g, inst, password)).To(BeNumerically(">=", uptimeBefore[inst]),
					"instance %s mysqld uptime dropped; the server was restarted instead of adopted", inst)
			}, e2eTimeout(1*time.Minute), 3*time.Second).Should(Succeed())
		}

		By("verifying no switchover happened: the primary is unchanged")
		Expect(clusterPrimary(clusterName)).To(Equal(primary),
			"an in-place operator upgrade must not switch the primary over")

		By("waiting for the cluster to return to Ready after the in-place upgrade")
		expectClusterReady(clusterName, 3, 20*time.Minute)

		By("verifying the primary still serves writes after the manager swap")
		Eventually(func(g Gomega) {
			_, err := mysqlExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS inplace_op_probe (id INT PRIMARY KEY); "+
					"REPLACE INTO inplace_op_probe VALUES (1);")
			g.Expect(err).NotTo(HaveOccurred())
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})
})

// insertInPlaceMarker appends a line to cmd/main.go so the next build produces a
// binary whose hash differs from both the baseline operator and the rolling
// "Operator Upgrade" spec's image. restoreE2EMarker reverts it.
func insertInPlaceMarker() {
	appendMainGoMarker(`var _ = "e2e-inplace-upgrade"`)
}
