//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("MariaDB major-version upgrade admission", Ordered, Label("flavor", "mariadb"), func() {
	const cluster = "mdb-upgrade-guard"

	BeforeAll(func() {
		applyManifest("mdb-upgrade-catalog", mariadbUpgradeCatalogManifest("mdb-upgrade-catalog", testNamespace))
	})

	AfterAll(func() {
		deleteManifest("mdb-upgrade-catalog", mariadbUpgradeCatalogManifest("mdb-upgrade-catalog", testNamespace))
	})

	It("rejects a skipped series and allows a single hop", func() {
		By("creating a MariaDB cluster pinned to series 10.6")
		applyManifest(cluster, mariadbCatalogClusterManifest(cluster, testNamespace, "10.6"))
		DeferCleanup(func() { deleteCluster(cluster) })

		By("rejecting a skip straight to 11.4")
		expectApplyRejected(cluster, mariadbCatalogClusterManifest(cluster, testNamespace, "11.4"), "10.11")

		By("rejecting a skip straight to 12.3")
		expectApplyRejected(cluster, mariadbCatalogClusterManifest(cluster, testNamespace, "12.3"), "10.11")

		By("allowing the adjacent hop to 10.11")
		applyManifest(cluster, mariadbCatalogClusterManifest(cluster, testNamespace, "10.11"))

		By("allowing the next hop to 11.4")
		applyManifest(cluster, mariadbCatalogClusterManifest(cluster, testNamespace, "11.4"))

		By("allowing the final hop to 12.3")
		applyManifest(cluster, mariadbCatalogClusterManifest(cluster, testNamespace, "12.3"))

		By("rejecting a downgrade from 12.3")
		expectApplyRejected(cluster, mariadbCatalogClusterManifest(cluster, testNamespace, "11.4"), "downgrade")
	})

	It("rejects a cluster that sets both imageName and imageCatalogRef on create", func() {
		manifest := strings.Replace(
			mariadbCatalogClusterManifest("mdb-upgrade-both", testNamespace, "10.11"),
			"  storage:",
			"  imageName: "+mariadbImage+"\n  storage:", 1)
		expectApplyRejected("mdb-upgrade-both", manifest, "mutually exclusive")
	})

	It("rejects an invalid flavor transition", func() {
		By("creating a MariaDB cluster")
		applyManifest("mdb-flavor-test", mariadbClusterManifest("mdb-flavor-test"))
		DeferCleanup(func() { deleteCluster("mdb-flavor-test") })

		By("rejecting a flavor change to mysql")
		manifest := strings.Replace(
			mariadbClusterManifest("mdb-flavor-test"),
			"flavor: mariadb", "flavor: mysql", 1)
		expectApplyRejected("mdb-flavor-test", manifest, "immutable")
	})

	It("rejects a MariaDB cluster with groupReplication", func() {
		manifest := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: mdb-gr-reject
  namespace: %s
spec:
  flavor: mariadb
  instances: 1
  imageName: %s
  replication:
    mode: groupReplication
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
  bootstrap:
    initdb: {}
`, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters)
		expectApplyRejected("mdb-gr-reject", manifest, "group replication")
	})
})

func mariadbUpgradeCatalogManifest(name, ns string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: ImageCatalog
metadata:
  name: %s
  namespace: %s
spec:
  images:
    - series: "10.6"
      image: %s
    - series: "10.11"
      image: %s
    - series: "11.4"
      image: %s
    - series: "12.3"
      image: %s
`, name, ns, mariadbImage, mariadbImage, mariadbImage, mariadbImage)
}

func mariadbCatalogClusterManifest(name, ns, series string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  flavor: mariadb
  instances: 1
  imageCatalogRef:
    kind: ImageCatalog
    name: mdb-upgrade-catalog
    series: "%s"
  upgrade:
    backupBeforeUpgrade: false
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
`, name, ns, series, e2eInstanceResources, e2eMySQLParameters)
}
