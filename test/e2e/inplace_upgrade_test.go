//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This spec exercises the Phase 2 in-place instance-manager upgrade mechanism end
// to end: hitting POST /instance/manager/restart-inplace makes the manager re-exec
// itself and adopt the already-running mysqld, so the server is never restarted
// (no Pod restart, no switchover). The proof is threefold: the manager logs the
// adopt path, the mysql container's restart count stays flat, and mysqld's uptime
// keeps climbing (a restart would reset it to ~0).
var _ = Describe("In-place instance manager upgrade", Ordered, func() {
	const (
		cluster   = "inplace"
		instances = 3
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("inplace")

		By("creating a 3-instance cluster")
		manifest := basicClusterManifest(cluster, instances)
		manifest = strings.Replace(manifest, "\n  storage:", "\n  failoverDelay: 60\n  storage:", 1)
		applyManifest(cluster, manifest)
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, 20*time.Minute)
	})

	It("re-execs the manager in place on the primary without restarting mysqld or switching over", func() {
		password := appPassword(cluster)
		primary := clusterPrimary(cluster)

		By(fmt.Sprintf("recording the pre-upgrade restart count and mysqld uptime of primary %s", primary))
		restartsBefore := podRestartCount(primary)
		var uptimeBefore int
		Eventually(func(g Gomega) {
			uptimeBefore = mysqldUptime(g, primary, password)
			g.Expect(uptimeBefore).To(BeNumerically(">", 0))
		}, e2eTimeout(1*time.Minute), 3*time.Second).Should(Succeed())

		By(fmt.Sprintf("triggering an in-place manager re-exec on %s via the control API", primary))
		triggerInPlaceRestart(cluster, primary)

		By("verifying the re-exec'd manager adopted the running mysqld (adopt path in the logs)")
		Eventually(func(g Gomega) {
			logs, err := kubectl("logs", primary, "-n", testNamespace, "-c", "mysql", "--tail=400")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(logs).To(ContainSubstring("Adopting running mysqld after in-place manager upgrade"),
				"the manager must take the adopt path, proving it re-exec'd in place")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying mysqld was never restarted: container restart count stays flat and uptime keeps climbing")
		Consistently(func(g Gomega) {
			g.Expect(podRestartCount(primary)).To(Equal(restartsBefore),
				"the mysql container restarted; the manager swap must not restart the Pod")
			g.Expect(mysqldUptime(g, primary, password)).To(BeNumerically(">=", uptimeBefore),
				"mysqld uptime dropped; the server was restarted instead of adopted")
		}, e2eTimeout(45*time.Second), 10*time.Second).Should(Succeed())

		By("verifying no switchover happened: the primary is unchanged")
		Expect(clusterPrimary(cluster)).To(Equal(primary),
			"an in-place upgrade must not switch the primary over")

		By("verifying the primary still serves writes after the manager swap")
		Eventually(func(g Gomega) {
			_, err := mysqlExec(primary, "app", password, "app",
				"CREATE TABLE IF NOT EXISTS inplace_probe (id INT PRIMARY KEY); "+
					"REPLACE INTO inplace_probe VALUES (1);")
			g.Expect(err).NotTo(HaveOccurred())
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		expectClusterReady(cluster, instances, 20*time.Minute)
	})
})

// mysqldUptime returns the running server's Uptime (seconds) via SHOW GLOBAL
// STATUS. It never resets while mysqld stays up, so it is the load-bearing signal
// that an in-place manager swap left the server running.
func mysqldUptime(g Gomega, pod, password string) int {
	out, err := mysqlExec(pod, "app", password, "", "SHOW GLOBAL STATUS LIKE 'Uptime';")
	g.Expect(err).NotTo(HaveOccurred())
	fields := strings.Fields(out)
	g.Expect(len(fields)).To(BeNumerically(">=", 2), "Uptime not found in %q", out)
	uptime, err := strconv.Atoi(fields[len(fields)-1])
	g.Expect(err).NotTo(HaveOccurred(), "could not parse Uptime from %q", out)
	return uptime
}

// triggerInPlaceRestart POSTs to the instance manager's mTLS control API to
// request an in-place re-exec, using a one-shot curl Pod that mounts the cluster
// client certificate and CA (the same material the operator authenticates with).
// It blocks until the Pod reports the call returned HTTP 200.
func triggerInPlaceRestart(cluster, instance string) {
	GinkgoHelper()
	name := "inplace-trigger-" + instance
	url := fmt.Sprintf("https://%s.%s.svc:8080/instance/manager/restart-inplace", instance, testNamespace)
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  restartPolicy: Never
  containers:
  - name: curl
    image: curlimages/curl:latest
    command: ["sh", "-c"]
    args:
    - >
      code=$(curl -sS -X POST --retry 10 --retry-connrefused --retry-delay 2
      --cert /client/tls.crt --key /client/tls.key --cacert /ca/ca.crt
      -o /dev/null -w '%%{http_code}' %[3]s);
      echo "control API returned $code"; test "$code" = "200"
    volumeMounts:
    - {name: client, mountPath: /client, readOnly: true}
    - {name: ca, mountPath: /ca, readOnly: true}
  volumes:
  - name: client
    secret:
      secretName: %[4]s-client-tls
  - name: ca
    secret:
      secretName: %[4]s-ca
`, name, testNamespace, url, cluster)

	applyManifest(name, manifest)
	DeferCleanup(func() {
		_, _ = kubectl("delete", "pod", name, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	Eventually(func(g Gomega) {
		phase, err := kubectl("get", "pod", name, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
		g.Expect(err).NotTo(HaveOccurred())
		phase = strings.TrimSpace(phase)
		if phase == "Failed" {
			logs, _ := kubectl("logs", name, "-n", testNamespace)
			Fail(fmt.Sprintf("in-place restart trigger Pod failed: %s", logs))
		}
		g.Expect(phase).To(Equal("Succeeded"), "trigger Pod has not reported a 200 yet")
	}, e2eTimeout(2*time.Minute), 3*time.Second).Should(Succeed())
}
