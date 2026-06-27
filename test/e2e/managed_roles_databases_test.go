//go:build e2e
// +build e2e

package e2e

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs exercise the declarative user/database surface end to end against a
// real cluster: a managed role declared in the Cluster spec is created on the
// primary with the right privileges, and a namespaced Database CR creates a
// schema and a schema-scoped user, then drops the schema on deletion under a
// `delete` reclaim policy.
var _ = Describe("Managed roles and databases", Ordered, Label("feature"), func() {
	const (
		cluster = "mrdb"
		// Managed role declared in the Cluster spec (generated password).
		roleName = "reporter"
		// Database CR resources.
		dbCR       = "appdb"
		dbSchema   = "reportdb"
		dbUser     = "dbuser"
		dbUserPass = "db-user-secret"
		dbUserSec  = "appdb-user"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mrdb")

		By("creating a cluster with a managed role")
		applyManifest(cluster, managedRoleClusterManifest(cluster, roleName))
		DeferCleanup(func() {
			deleteManifest(cluster, managedRoleClusterManifest(cluster, roleName))
		})
		expectClusterReady(cluster, 1, 20*time.Minute)
	})

	It("creates the managed role on the primary with its privileges", func() {
		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")

		By("waiting for the role to be reconciled onto the primary")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, "root", rootPass, "",
				fmt.Sprintf("SELECT User FROM mysql.user WHERE User = '%s';", roleName))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(roleName), "managed role not created yet")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the role's grants are scoped to app.* SELECT")
		grants, err := mysqlExec(primary, "root", rootPass, "",
			fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%';", roleName))
		Expect(err).NotTo(HaveOccurred())
		Expect(grants).To(ContainSubstring("SELECT"))
		Expect(grants).To(ContainSubstring("`app`"))

		By("verifying the role can authenticate with its generated password")
		rolePass := secretPassword(cluster + "-" + roleName)
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, roleName, rolePass, "", "SELECT 1;")
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
		rootPass := secretPassword(cluster + "-root")

		By("verifying the schema exists on the primary")
		out, err := mysqlExec(primary, "root", rootPass, "",
			fmt.Sprintf("SHOW DATABASES LIKE '%s';", dbSchema))
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring(dbSchema), "schema was not created")

		By("verifying the schema-scoped user can write to its schema")
		Eventually(func(g Gomega) {
			_, err := mysqlExec(primary, dbUser, dbUserPass, dbSchema,
				"CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY); REPLACE INTO t VALUES (7);")
			g.Expect(err).NotTo(HaveOccurred(), "schema user cannot write to its schema")
			read, err := mysqlExec(primary, dbUser, dbUserPass, dbSchema, "SELECT id FROM t WHERE id = 7;")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(read).To(ContainSubstring("7"))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("does not correct out-of-band drift when driftDetection is disabled", func() {
		const driftCR, driftSchema, driftUser, driftPass, driftSec = "driftdb", "driftschema", "driftuser", "drift-secret", "driftdb-user"
		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")

		By("creating a Database with driftDetection disabled")
		applyManifest(driftSec, passwordSecretManifest(driftSec, driftPass))
		applyManifest(driftCR, databaseDriftManifest(driftCR, cluster, driftSchema, driftUser, driftSec, false))
		DeferCleanup(func() {
			_, _ = kubectl("delete", "database", driftCR, "-n", testNamespace, "--ignore-not-found", "--wait=false")
			_, _ = kubectl("delete", "secret", driftSec, "-n", testNamespace, "--ignore-not-found")
		})

		By("waiting for the Database to be applied")
		Eventually(func(g Gomega) {
			applied, err := kubectl("get", "database", driftCR, "-n", testNamespace,
				"-o", "jsonpath={.status.applied}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(applied).To(Equal("true"), "drift-disabled database is not applied yet")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("dropping the schema-scoped user out of band")
		_, err := mysqlExec(primary, "root", rootPass, "",
			fmt.Sprintf("DROP USER '%s'@'%%';", driftUser))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the operator does not re-create it on a timer (drift correction is off)")
		Consistently(func(g Gomega) {
			out, err := mysqlExec(primary, "root", rootPass, "",
				fmt.Sprintf("SELECT User FROM mysql.user WHERE User = '%s';", driftUser))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(BeEmpty(), "drift-disabled user must not be re-applied without a change")
		}, 45*time.Second, 5*time.Second).Should(Succeed())

		By("editing the spec to trigger a reconcile")
		_, err = kubectl("patch", "database", driftCR, "-n", testNamespace,
			"--type=merge", "-p", `{"spec":{"characterSet":"utf8mb4"}}`)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the user is re-applied on the spec change")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, "root", rootPass, "",
				fmt.Sprintf("SELECT User FROM mysql.user WHERE User = '%s';", driftUser))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(driftUser), "a spec change must re-apply the database users")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("drops the schema when the Database is deleted under reclaimPolicy delete", func() {
		primary := clusterPrimary(cluster)
		rootPass := secretPassword(cluster + "-root")

		By("deleting the Database CR")
		_, err := kubectl("delete", "database", dbCR, "-n", testNamespace, "--wait=true", "--timeout=2m")
		Expect(err).NotTo(HaveOccurred(), "failed to delete Database CR")

		By("verifying the schema is dropped from the primary")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(primary, "root", rootPass, "",
				fmt.Sprintf("SHOW DATABASES LIKE '%s';", dbSchema))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(BeEmpty(), "schema was not reclaimed on delete")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// secretPassword decodes the "password" key from a Secret in the test namespace.
func secretPassword(secretName string) string {
	output, err := kubectl("get", "secret", secretName, "-n", testNamespace,
		"-o", "jsonpath={.data.password}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read password from secret %s", secretName)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(output))
	Expect(err).NotTo(HaveOccurred(), "Failed to decode password from secret %s", secretName)
	return string(decoded)
}

func managedRoleClusterManifest(name, role string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
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
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, role)
}

// databaseDriftManifest builds a Database carrying an explicit driftDetection
// setting, used to verify the opt-out of periodic re-apply. Its schema is
// retained on deletion so namespace teardown does not need a live primary.
func databaseDriftManifest(name, cluster, schema, dbUser, userSecret string, drift bool) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Database
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  name: %s
  reclaimPolicy: retain
  driftDetection: %t
  users:
    - name: %s
      passwordSecret:
        name: %s
        key: password
      grants:
        - privileges: ["ALL"]
`, name, testNamespace, cluster, schema, drift, dbUser, userSecret)
}

func passwordSecretManifest(name, password string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
type: Opaque
stringData:
  password: %s
`, name, testNamespace, password)
}

func databaseManifest(name, cluster, schema, dbUser, userSecret string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Database
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  name: %s
  reclaimPolicy: delete
  users:
    - name: %s
      passwordSecret:
        name: %s
        key: password
      grants:
        - privileges: ["ALL"]
`, name, testNamespace, cluster, schema, dbUser, userSecret)
}
