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

var _ = Describe("TLS Certificate Renewal", Ordered, func() {
	const cluster = "tlsrenew"

	var ns, prevNS, rootPass string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("tlsrenew")

		By("creating a single-instance cluster with auto-generated TLS certificates")
		applyManifest(cluster, tlsRenewalClusterManifest(cluster))
		DeferCleanup(func() {
			deleteManifest(cluster, tlsRenewalClusterManifest(cluster))
		})
		expectClusterReady(cluster, 1, 8*time.Minute)
		rootPass = secretPassword(cluster + "-root")
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})

	It("populates certificate expirations in the Cluster status", func() {
		Eventually(func(g Gomega) {
			out, err := clusterField(cluster, "{.status.certificates.expirations}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty(), "Cluster %s should report certificate expirations", cluster)
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())
	})

	It("can write and TLS is active before any renewal", func() {
		primary := clusterPrimary(cluster)

		By("verifying writes succeed on the primary")
		_, err := mysqlExec(primary, "root", rootPass, "app",
			"CREATE TABLE IF NOT EXISTS e2e_tls (id INT PRIMARY KEY); "+
				"REPLACE INTO e2e_tls VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "Failed to write on primary")

		By("verifying mysqld reports TLS is active")
		out, err := mysqlExec(primary, "root", rootPass, "",
			"SHOW STATUS LIKE 'Ssl_server_not_after'")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).NotTo(BeEmpty(), "Ssl_server_not_after should be populated when TLS is configured")
	})

	It("reloads TLS certificates on SIGHUP and stays healthy", func() {
		primary := clusterPrimary(cluster)

		By("sending SIGHUP to the instance manager PID1")
		_, err := kubectl("exec", primary, "-n", testNamespace, "-c", "mysql", "--",
			"/controller/manager", "instance", "signal", "--pid=1", "--signal=HUP")
		Expect(err).NotTo(HaveOccurred(), "Failed to send SIGHUP to instance manager")

		By("verifying the instance manager logged the reload")
		Eventually(func(g Gomega) {
			logs, err := kubectl("logs", primary, "-n", testNamespace, "-c", "mysql")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(logs).To(ContainSubstring("TLS certificates reloaded"))
		}, e2eTimeout(30*time.Second), 2*time.Second).Should(Succeed())

		By("verifying the cluster is still Ready after SIGHUP")
		Eventually(func(g Gomega) {
			ready, err := clusterField(cluster, "{.status.conditions[?(@.type=='Ready')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(Equal("True"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying writes still succeed after SIGHUP reload")
		_, err = mysqlExec(primary, "root", rootPass, "app",
			"REPLACE INTO e2e_tls VALUES (2);")
		Expect(err).NotTo(HaveOccurred(), "Failed to write after SIGHUP reload")
	})

	It("reloads certificates and stays healthy when cert-manager re-issues the server cert", func() {
		primary := clusterPrimary(cluster)
		secretName := primary + "-server-tls"

		By("recording the current server TLS certificate")
		beforeChecksum, err := kubectl("get", "secret", secretName, "-n", testNamespace,
			"-o", "jsonpath={.data.tls\\.crt}")
		Expect(err).NotTo(HaveOccurred())
		Expect(beforeChecksum).NotTo(BeEmpty())

		By("recording the current mysqld TLS cert notAfter value")
		beforeNotAfter := rawMysqlStatus(primary, rootPass, "Ssl_server_not_after")
		Expect(beforeNotAfter).NotTo(BeEmpty())

		By("deleting the server TLS secret to force cert-manager to re-issue it")
		_, err = kubectl("delete", "secret", secretName, "-n", testNamespace, "--ignore-not-found")
		Expect(err).NotTo(HaveOccurred())

		By("waiting for cert-manager to create a new certificate with different content")
		Eventually(func(g Gomega) {
			afterChecksum, err := kubectl("get", "secret", secretName, "-n", testNamespace,
				"-o", "jsonpath={.data.tls\\.crt}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(afterChecksum).NotTo(BeEmpty())
			g.Expect(afterChecksum).NotTo(Equal(beforeChecksum),
				"cert-manager should have issued a new certificate")
		}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())

		By("waiting for the instance manager to detect and reload the renewed certificate")
		Eventually(func(g Gomega) {
			logs, err := kubectl("logs", primary, "-n", testNamespace, "-c", "mysql")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(logs).To(ContainSubstring("Certificate files changed, reloading"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the cluster remains Ready after certificate renewal")
		expectClusterReady(cluster, 1, 5*time.Minute)

		By("verifying mysqld loaded the new certificate via ALTER INSTANCE RELOAD TLS")
		Eventually(func(g Gomega) {
			afterNotAfter := rawMysqlStatus(primary, rootPass, "Ssl_server_not_after")
			g.Expect(afterNotAfter).NotTo(Equal(beforeNotAfter),
				"mysqld should have picked up the renewed certificate")
		}, e2eTimeout(2*time.Minute), 10*time.Second).Should(Succeed())

		By("verifying writes still succeed after certificate re-issuance and reload")
		_, err = mysqlExec(primary, "root", rootPass, "app",
			"REPLACE INTO e2e_tls VALUES (3);")
		Expect(err).NotTo(HaveOccurred(), "Failed to write after certificate renewal")

		By("verifying certificate expirations are still reported in status")
		Eventually(func(g Gomega) {
			out, err := clusterField(cluster, "{.status.certificates.expirations}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty())
		}, e2eTimeout(3*time.Minute), 10*time.Second).Should(Succeed())
	})
})

func tlsRenewalClusterManifest(name string) string {
	return fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 1
  imageName: %s
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters)
}

func rawMysqlStatus(pod, rootPass, variable string) string {
	out, err := mysqlExec(pod, "root", rootPass, "",
		fmt.Sprintf("SHOW STATUS LIKE '%s'", variable))
	if err != nil {
		return ""
	}
	fields := strings.Fields(out)
	if len(fields) < 2 {
		return ""
	}
	return strings.Join(fields[1:], " ")
}
