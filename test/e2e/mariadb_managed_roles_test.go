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

// The MariaDB counterpart of the "Managed roles and databases" suite. It proves
// the declarative user/database surface works against the MariaDB engine, whose
// grant and user SQL diverges from MySQL: a managed role declared in the Cluster
// spec is created on the primary with the right privileges, and a namespaced
// Database CR creates a schema plus a schema-scoped user, then drops the schema
// on deletion under a `delete` reclaim policy. It reuses the flavor-agnostic
// Database/Secret manifests from managed_roles_databases_test.go.
var _ = Describe("MariaDB managed roles and databases", Ordered, Label("flavor", "mariadb"), func() {
	const (
		cluster    = "mdb-mrdb"
		roleName   = "reporter"
		dbCR       = "mdb-appdb"
		dbSchema   = "reportdb"
		dbUser     = "dbuser"
		dbUserPass = "db-user-secret"
		dbUserSec  = "mdb-appdb-user"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-mrdb")

		By("creating a MariaDB cluster with a managed role")
		applyManifest(cluster, mariadbManagedRoleClusterManifest(cluster, roleName))
		DeferCleanup(func() {
			deleteManifest(cluster, mariadbManagedRoleClusterManifest(cluster, roleName))
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, 1, e2eTimeout(20*time.Minute))
	})

	It("creates the managed role on the primary with its privileges", func() {
		primary := clusterPrimary(cluster)
		rootPass := rootPassword(cluster)

		By("waiting for the role to be reconciled onto the primary")
		Eventually(func(g Gomega) {
			out, err := mariadbExec(primary, "root", rootPass, "",
				fmt.Sprintf("SELECT User FROM mysql.user WHERE User = '%s';", roleName))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(roleName), "managed role not created yet")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the role's grants are scoped to app.* SELECT")
		grants, err := mariadbExec(primary, "root", rootPass, "",
			fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%';", roleName))
		Expect(err).NotTo(HaveOccurred())
		Expect(grants).To(ContainSubstring("SELECT"))
		Expect(grants).To(ContainSubstring("`app`"))

		By("verifying the role can authenticate with its generated password")
		rolePass := secretPassword(cluster + "-" + roleName)
		Eventually(func(g Gomega) {
			out, err := mariadbExec(primary, roleName, rolePass, "", "SELECT 1;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("1"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("creates a schema and schema-scoped user from a Database CR", func() {
		By("creating the Database user's password Secret")
		applyManifest(dbUserSec, passwordSecretManifest(dbUserSec, dbUserPass))

		By("creating the Database CR with a delete reclaim policy")
		applyManifest(dbCR, databaseManifest(dbCR, cluster, dbSchema, dbUser, dbUserSec))

		By("waiting for the Database to report applied=true")
		Eventually(func(g Gomega) {
			applied, err := kubectl("get", "database", dbCR, "-n", testNamespace,
				"-o", "jsonpath={.status.applied}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(applied).To(Equal("true"), "Database is not applied yet")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		primary := clusterPrimary(cluster)
		rootPass := rootPassword(cluster)

		By("verifying the schema exists on the primary")
		out, err := mariadbExec(primary, "root", rootPass, "",
			fmt.Sprintf("SHOW DATABASES LIKE '%s';", dbSchema))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring(dbSchema), "schema was not created")

		By("verifying the schema-scoped user can write to its schema")
		Eventually(func(g Gomega) {
			_, err := mariadbExec(primary, dbUser, dbUserPass, dbSchema,
				"CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY); REPLACE INTO t VALUES (7);")
			g.Expect(err).NotTo(HaveOccurred(), "schema user cannot write to its schema")
			read, err := mariadbExec(primary, dbUser, dbUserPass, dbSchema, "SELECT id FROM t WHERE id = 7;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(read).To(ContainSubstring("7"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("drops the schema when the Database is deleted under reclaimPolicy delete", func() {
		primary := clusterPrimary(cluster)
		rootPass := rootPassword(cluster)

		By("deleting the Database CR")
		_, err := kubectl("delete", "database", dbCR, "-n", testNamespace, "--wait=true", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "failed to delete Database CR")

		By("verifying the schema is dropped from the primary")
		Eventually(func(g Gomega) {
			out, err := mariadbExec(primary, "root", rootPass, "",
				fmt.Sprintf("SHOW DATABASES LIKE '%s';", dbSchema))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(BeEmpty(), "schema was not reclaimed on delete")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})
})

func mariadbManagedRoleClusterManifest(name, role string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  flavor: mariadb
  instances: 1
  imageName: %s
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
  managed:
    roles:
      - name: %s
        privileges:
          - privileges: ["SELECT"]
            "on": "app.*"
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters, role)
}
