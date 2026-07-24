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

// The MariaDB counterpart of the superuser specs in databaseuser_test.go. The
// superuser bit is reconciled by reading SHOW GRANTS back, and MariaDB renders
// that line differently from MySQL (the authentication clause sits between the
// account and WITH GRANT OPTION), so the parity run is what proves the operator
// reads its own work correctly on this engine. It reuses the flavor-agnostic
// DatabaseUser/Secret manifests but drives SQL through mariadbExec, since the
// MariaDB instance image ships no `mysql` symlink.
var _ = Describe("MariaDB DatabaseUser superuser", Ordered, Label("flavor", "mariadb"), func() {
	const (
		cluster = "mdb-dbusr"
		suCR    = "rootish"
		suPass  = "rootish-secret"
		suSec   = "rootish-pw"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-dbusr")

		By("creating a single-instance MariaDB cluster")
		applyManifest(cluster, mariadbBasicClusterManifest(cluster, 1))
		DeferCleanup(func() {
			deleteManifest(cluster, mariadbBasicClusterManifest(cluster, 1))
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, 1, e2eTimeout(20*time.Minute))
	})

	It("creates a superuser account with the grant option", func() {
		By("creating the superuser DatabaseUser")
		applyManifest(suSec, passwordSecretManifest(suSec, suPass))
		applyManifest(suCR, databaseUserSuperuserManifest(suCR, cluster, suSec))

		By("waiting for the DatabaseUser to report applied=true")
		Eventually(func(g Gomega) {
			applied, err := kubectl("get", "databaseuser", suCR, "-n", testNamespace,
				"-o", "jsonpath={.status.applied}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(applied).To(Equal("true"), "DatabaseUser is not applied yet")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		primary := clusterPrimary(cluster)
		rootPass := rootPassword(cluster)

		By("verifying the account holds ALL PRIVILEGES with the grant option")
		grants, err := mariadbExec(primary, "root", rootPass, "",
			fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%';", suCR))
		Expect(err).NotTo(HaveOccurred())
		Expect(grants).To(ContainSubstring("ALL PRIVILEGES ON *.*"))
		Expect(grants).To(ContainSubstring("WITH GRANT OPTION"))

		By("verifying the superuser can authenticate and act globally")
		Eventually(func(g Gomega) {
			out, err := mariadbExec(primary, suCR, suPass, "", "SHOW DATABASES;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("mysql"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("restores the superuser grant option after an out-of-band revoke", func() {
		primary := clusterPrimary(cluster)
		rootPass := rootPassword(cluster)

		// Revoking only the grant option leaves ALL PRIVILEGES in place, so a diff
		// that reads the superuser bit off the grants alone sees nothing to do.
		By("revoking the grant option out of band, keeping ALL PRIVILEGES")
		_, err := mariadbExec(primary, "root", rootPass, "",
			fmt.Sprintf("REVOKE GRANT OPTION ON *.* FROM '%s'@'%%';", suCR))
		Expect(err).NotTo(HaveOccurred())

		By("verifying drift detection puts the grant option back")
		Eventually(func(g Gomega) {
			grants, err := mariadbExec(primary, "root", rootPass, "",
				fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%';", suCR))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(grants).To(ContainSubstring("WITH GRANT OPTION"),
				"superuser drift was not corrected")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("reclaims the account on deletion under reclaimPolicy delete", func() {
		primary := clusterPrimary(cluster)
		rootPass := rootPassword(cluster)

		By("deleting the DatabaseUser CR")
		_, err := kubectl("delete", "databaseuser", suCR, "-n", testNamespace, "--wait=true", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the account is dropped from the primary")
		Eventually(func(g Gomega) {
			out, err := mariadbExec(primary, "root", rootPass, "",
				fmt.Sprintf("SELECT User FROM mysql.user WHERE User = '%s';", suCR))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(BeEmpty(), "account was not reclaimed on delete")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})
})
