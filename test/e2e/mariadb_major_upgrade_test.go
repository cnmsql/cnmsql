//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The MariaDB counterpart of the MySQL major-version upgrade *rollout* specs. The
// admission guard (which series hops are allowed) is covered by
// mariadb_upgrade_test.go; this spec exercises the reconcile that actually
// performs an in-place series upgrade, rolling a single-instance cluster through
// every adjacent MariaDB series and asserting the server version advances and the
// application data survives each irreversible hop.
//
// It runs only in the dedicated mariadb-major-upgrade lane, which sets both
// E2E_MARIADB_VERSION and E2E_MAJOR_UPGRADE so the suite co-loads the adjacent
// MariaDB series images (mariadbUpgradeSeries) into the single Kind cluster.
var _ = Describe("MariaDB major-version upgrade rollout", Ordered, Label("disruptive", "major-upgrade", "mariadb"), func() {
	const (
		cluster = "mdb-major-upgrade"
		catalog = "mdb-major-upgrade-images"
	)

	var ns, prevNS, password string

	BeforeAll(func() {
		if os.Getenv("E2E_MAJOR_UPGRADE") != trueEnvValue {
			Skip("requires the dedicated mariadb-major-upgrade job (co-loads adjacent MariaDB series images)")
		}
		prevNS = testNamespace
		ns = createTestNamespace("mdb-major-upgrade")

		By("creating the multi-series MariaDB image catalog")
		applyManifest(catalog, mariadbMajorUpgradeCatalogManifest(catalog, ns))

		DeferCleanup(func() {
			deleteManifest(catalog, mariadbMajorUpgradeCatalogManifest(catalog, ns))
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("upgrades a single-instance cluster through every adjacent series", func() {
		By("bootstrapping a single-instance cluster on the oldest series")
		applyManifest(cluster, mariadbMajorUpgradeClusterManifest(cluster, ns, catalog, mariadbUpgradeSeries[0]))
		DeferCleanup(func() { deleteCluster(cluster) })
		expectClusterReady(cluster, 1, e2eTimeout(20*time.Minute))
		password = appPassword(cluster)

		By("writing durable data before the first irreversible upgrade")
		_, err := mariadbExec(cluster+"-1", "app", password, "app",
			"CREATE TABLE mdb_major_upgrade (id INT PRIMARY KEY, value VARCHAR(32)); "+
				"INSERT INTO mdb_major_upgrade VALUES (1, 'before-upgrade');")
		Expect(err).NotTo(HaveOccurred())

		for _, series := range mariadbUpgradeSeries[1:] {
			serverPrefix := series + "."
			By(fmt.Sprintf("upgrading to MariaDB %s", series))
			applyManifest(cluster, mariadbMajorUpgradeClusterManifest(cluster, ns, catalog, series))

			expectClusterReady(cluster, 1, e2eTimeout(20*time.Minute))

			By(fmt.Sprintf("verifying the server advanced to %s", series))
			Eventually(func(g Gomega) {
				out, err := mariadbExec(cluster+"-1", "app", password, "", "SELECT VERSION();")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(HavePrefix(serverPrefix),
					"server version must advance to the %s series", series)
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

			By("verifying the application data survived the upgrade")
			out, err := mariadbExec(cluster+"-1", "app", password, "app",
				"SELECT value FROM mdb_major_upgrade WHERE id = 1;")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("before-upgrade"),
				"application data must survive the %s upgrade", series)
		}
	})
})

// mariadbMajorUpgradeCatalogManifest is an ImageCatalog mapping each adjacent
// MariaDB series to its own real instance image, so the rollout performs a genuine
// image swap per hop (unlike the admission catalog, which points every series at
// the same image because the webhook never pulls).
func mariadbMajorUpgradeCatalogManifest(name, ns string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `apiVersion: mysql.cnmsql.co/v1alpha1
kind: ImageCatalog
metadata:
  name: %s
  namespace: %s
spec:
  images:
`, name, ns)
	for _, series := range mariadbUpgradeSeries {
		fmt.Fprintf(&b, "    - series: \"%s\"\n      image: %s\n", series, mariadbInstanceImageFor(series))
	}
	return b.String()
}

func mariadbMajorUpgradeClusterManifest(name, ns, catalog, series string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  flavor: mariadb
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %[3]s
    series: "%[4]s"
  upgrade:
    backupBeforeUpgrade: false
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, ns, catalog, series, e2eInstanceResources, e2eMySQLParameters)
}
