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

// These specs exercise the standalone DatabaseUser CR end to end: an
// installation-wide account (not scoped to any Database) is created on the
// primary with grants spanning multiple schemas, its password rotates when the
// Secret changes, a pre-existing account is refused until adopted, and the
// account is reclaimed on deletion under a `delete` policy.
var _ = Describe("DatabaseUser", Ordered, func() {
	const (
		cluster   = "dbusr"
		userCR    = "tenant"
		userPass  = "tenant-secret"
		userSec   = "tenant-pw"
		adoptCR   = "legacy"
		adoptUser = "legacy"
		adoptPass = "legacy-secret"
		adoptSec  = "legacy-pw"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("dbusr")

		By("creating a single-instance cluster")
		applyManifest(cluster, basicClusterManifest(cluster, 1))
		DeferCleanup(func() {
			deleteManifest(cluster, basicClusterManifest(cluster, 1))
		})
		expectClusterReady(cluster, 1, 20*time.Minute)
	})

	It("creates an installation-wide user with multi-schema grants", func() {
		By("creating the user's password Secret")
		applyManifest(userSec, passwordSecretManifest(userSec, userPass))

		By("creating the DatabaseUser CR")
		applyManifest(userCR, databaseUserManifest(userCR, cluster, userSec, "delete"))

		By("waiting for the DatabaseUser to report applied=true")
		Eventually(func(g Gomega) {
			applied, err := kubectl("get", "databaseuser", userCR, "-n", testNamespace,
				"-o", "jsonpath={.status.applied}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(applied).To(Equal("true"), "DatabaseUser is not applied yet")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")

		By("verifying the grants span both targets")
		grants, err := mysqlExec(primary, "root", rootPass, "",
			fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%';", userCR))
		Expect(err).NotTo(HaveOccurred())
		Expect(grants).To(ContainSubstring("SELECT"))
		Expect(grants).To(ContainSubstring("`app`"))

		By("verifying the user can authenticate")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, userCR, userPass, "", "SELECT 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("1"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("rotates the password when the Secret changes", func() {
		const rotated = "tenant-rotated"
		By("updating the password Secret")
		applyManifest(userSec, passwordSecretManifest(userSec, rotated))

		primary := clusterPrimary(cluster)
		By("verifying the user authenticates with the new password")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, userCR, rotated, "", "SELECT 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("1"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("refuses a pre-existing account until adopted", func() {
		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")

		By("hand-creating an account out of band")
		_, err := mysqlExec(primary, "root", rootPass, "",
			fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s';", adoptUser, adoptPass))
		Expect(err).NotTo(HaveOccurred())

		By("creating a DatabaseUser that targets the same account")
		applyManifest(adoptSec, passwordSecretManifest(adoptSec, adoptPass))
		applyManifest(adoptCR, databaseUserManifest(adoptCR, cluster, adoptSec, "retain"))

		By("expecting it to stay unapplied with a UserConflict reason")
		Eventually(func(g Gomega) {
			reason, err := kubectl("get", "databaseuser", adoptCR, "-n", testNamespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("UserConflict"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("adopting the account via kubectl cnmsql databaseuser adopt")
		_, err = kubectl("annotate", "databaseuser", adoptCR, "-n", testNamespace,
			"mysql.cnmsql.co/adopt=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("expecting it to become applied")
		Eventually(func(g Gomega) {
			applied, err := kubectl("get", "databaseuser", adoptCR, "-n", testNamespace,
				"-o", "jsonpath={.status.applied}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(applied).To(Equal("true"), "adopted user is not applied yet")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("reclaims the account on deletion under reclaimPolicy delete", func() {
		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")

		By("deleting the DatabaseUser CR")
		_, err := kubectl("delete", "databaseuser", userCR, "-n", testNamespace, "--wait=true", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the account is dropped from the primary")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, "root", rootPass, "",
				fmt.Sprintf("SELECT User FROM mysql.user WHERE User = '%s';", userCR))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(BeEmpty(), "account was not reclaimed on delete")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	// --- Defensive specs: the safety nets must hold even on hostile input. ---

	It("rejects a grant that would break the cluster control plane", func() {
		const denyCR, denySec = "evil", "evil-pw"
		applyManifest(denySec, passwordSecretManifest(denySec, "evil-secret"))

		By("declaring a user that asks for REPLICATION_SLAVE_ADMIN")
		applyManifest(denyCR, databaseUserCustomManifest(denyCR, cluster, "", denySec, "retain",
			`    - privileges: ["REPLICATION_SLAVE_ADMIN"]
      "on": "*.*"`))
		DeferCleanup(func() {
			_, _ = kubectl("delete", "databaseuser", denyCR, "-n", testNamespace, "--ignore-not-found")
		})

		By("expecting the user to be rejected as Invalid, never applied")
		Eventually(func(g Gomega) {
			reason, err := kubectl("get", "databaseuser", denyCR, "-n", testNamespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("Invalid"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		applied, err := kubectl("get", "databaseuser", denyCR, "-n", testNamespace, "-o", "jsonpath={.status.applied}")
		Expect(err).NotTo(HaveOccurred())
		Expect(applied).NotTo(Equal("true"), "a denied-privilege user must never be applied")

		By("verifying the account was never created on the primary")
		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")
		out, err := mysqlExec(primary, "root", rootPass, "",
			fmt.Sprintf("SELECT User FROM mysql.user WHERE User = '%s';", denyCR))
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(BeEmpty(), "rejected user must not exist in MySQL")
	})

	It("rejects a reserved operator account name", func() {
		const resCR, resSec = "sneaky", "sneaky-pw"
		applyManifest(resSec, passwordSecretManifest(resSec, "sneaky-secret"))

		By("declaring a user whose MySQL name shadows cnmsql_repl")
		applyManifest(resCR, databaseUserCustomManifest(resCR, cluster, "cnmsql_repl", resSec, "retain",
			`    - privileges: ["SELECT"]
      "on": "*.*"`))
		DeferCleanup(func() {
			_, _ = kubectl("delete", "databaseuser", resCR, "-n", testNamespace, "--ignore-not-found")
		})

		By("expecting the user to be rejected as Invalid")
		Eventually(func(g Gomega) {
			reason, err := kubectl("get", "databaseuser", resCR, "-n", testNamespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("Invalid"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("rejects ALL on *.* as a grant (it would grant dynamic admin privileges)", func() {
		const allCR, allSec = "allglobal", "allglobal-pw"
		applyManifest(allSec, passwordSecretManifest(allSec, "allglobal-secret"))

		By("declaring a user that asks for ALL on *.*")
		applyManifest(allCR, databaseUserCustomManifest(allCR, cluster, "", allSec, "retain",
			`    - privileges: ["ALL"]
      "on": "*.*"`))
		DeferCleanup(func() {
			_, _ = kubectl("delete", "databaseuser", allCR, "-n", testNamespace, "--ignore-not-found")
		})

		By("expecting it to be rejected as Invalid, never applied")
		Eventually(func(g Gomega) {
			reason, err := kubectl("get", "databaseuser", allCR, "-n", testNamespace,
				"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].reason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("Invalid"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("confines a DBaaS admin: broad *.* grant carved out of the system schemas", func() {
		const dbaasCR, dbaasPass, dbaasSec = "dbaas-admin", "dbaas-secret", "dbaas-admin-pw"
		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")

		By("enabling partial_revokes on the server (users set this via spec.mysql.parameters)")
		_, err := mysqlExec(primary, "root", rootPass, "", "SET PERSIST partial_revokes = ON;")
		Expect(err).NotTo(HaveOccurred())

		applyManifest(dbaasSec, passwordSecretManifest(dbaasSec, dbaasPass))

		By("creating a DBaaS admin: broad static privileges on *.*, system schemas revoked")
		staticPrivs := `["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", ` +
			`"INDEX", "CREATE VIEW", "SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "EVENT", "TRIGGER"]`
		writes := `["INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"]`
		spec := "    - privileges: " + staticPrivs + "\n      \"on\": \"*.*\"\n" +
			"  revokes:\n" +
			"    - privileges: " + writes + "\n      \"on\": \"mysql.*\"\n" +
			"    - privileges: " + writes + "\n      \"on\": \"sys.*\""
		applyManifest(dbaasCR, databaseUserCustomManifest(dbaasCR, cluster, "", dbaasSec, "delete", spec))
		DeferCleanup(func() {
			_, _ = kubectl("delete", "databaseuser", dbaasCR, "-n", testNamespace, "--ignore-not-found")
		})

		By("waiting for it to be applied")
		Eventually(func(g Gomega) {
			applied, err := kubectl("get", "databaseuser", dbaasCR, "-n", testNamespace, "-o", "jsonpath={.status.applied}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(applied).To(Equal("true"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying it can work an application schema")
		_, err = mysqlExec(primary, dbaasCR, dbaasPass, "app",
			"CREATE TABLE IF NOT EXISTS confined (id INT PRIMARY KEY); REPLACE INTO confined VALUES (1);")
		Expect(err).NotTo(HaveOccurred(), "DBaaS admin must have full data access to tenant schemas")

		By("verifying the system-schema carve-out blocks writes to the grant tables")
		_, err = mysqlExec(primary, dbaasCR, dbaasPass, "",
			"UPDATE mysql.user SET account_locked = 'Y' WHERE User = 'cnmsql_repl';")
		Expect(err).To(HaveOccurred(), "DBaaS admin must not be able to write mysql.user")

		By("verifying it can no longer self-escalate a cluster-control privilege")
		_, _ = mysqlExec(primary, dbaasCR, dbaasPass, "",
			fmt.Sprintf("GRANT REPLICATION_SLAVE_ADMIN ON *.* TO '%s'@'%%';", dbaasCR))
		grants, err := mysqlExec(primary, "root", rootPass, "",
			fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%';", dbaasCR))
		Expect(err).NotTo(HaveOccurred())
		for _, priv := range []string{
			"REPLICATION_SLAVE_ADMIN", "SYSTEM_VARIABLES_ADMIN",
			"GROUP_REPLICATION_ADMIN", "CONNECTION_ADMIN", "SHUTDOWN", "SUPER",
		} {
			Expect(grants).NotTo(ContainSubstring(priv),
				"DBaaS admin must not hold the %s privilege", priv)
		}

		By("verifying it cannot control replication or drop operator accounts")
		_, err = mysqlExec(primary, dbaasCR, dbaasPass, "", "STOP REPLICA;")
		Expect(err).To(HaveOccurred(), "DBaaS admin must not be able to control replication")
		_, err = mysqlExec(primary, dbaasCR, dbaasPass, "", "DROP USER 'cnmsql_repl'@'%';")
		Expect(err).To(HaveOccurred(), "DBaaS admin must not be able to drop operator accounts")

		By("verifying the operator account is still intact")
		out, err := mysqlExec(primary, "root", rootPass, "",
			"SELECT User FROM mysql.user WHERE User = 'cnmsql_repl';")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("cnmsql_repl"), "operator account must survive a hostile tenant")
	})

	It("retains the account on deletion under reclaimPolicy retain", func() {
		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")

		By("deleting the adopted (retain) DatabaseUser CR")
		_, err := kubectl("delete", "databaseuser", adoptCR, "-n", testNamespace, "--wait=true", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the MySQL account survives the deletion")
		Consistently(func(g Gomega) {
			out, err := mysqlExec(primary, "root", rootPass, "",
				fmt.Sprintf("SELECT User FROM mysql.user WHERE User = '%s';", adoptUser))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(adoptUser), "retain policy must keep the account")
		}, 20*time.Second, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

func databaseUserManifest(name, cluster, userSecret, reclaim string) string {
	return databaseUserCustomManifest(name, cluster, "", userSecret, reclaim,
		`    - privileges: ["SELECT"]
      "on": "app.*"
    - privileges: ["SELECT"]
      "on": "*.*"`)
}

// databaseUserCustomManifest builds a DatabaseUser with an optional MySQL user
// name override and a caller-supplied (already-indented) grants block.
func databaseUserCustomManifest(name, cluster, userName, userSecret, reclaim, grantsYAML string) string {
	nameLine := ""
	if userName != "" {
		nameLine = fmt.Sprintf("  name: %s\n", userName)
	}
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: DatabaseUser
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
%s  reclaimPolicy: %s
  passwordSecret:
    name: %s
    key: password
  grants:
%s
`, name, testNamespace, cluster, nameLine, reclaim, userSecret, grantsYAML)
}
