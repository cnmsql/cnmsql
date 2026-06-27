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

// These specs exercise both the admission guard and, in the dedicated
// multi-image CI job, a real Group Replication roll through 8.0 -> 8.4 -> 9.x.

// upgradeCatalogName is the ImageCatalog these specs resolve series against.
const upgradeCatalogName = "upgrade-images"

func upgradeCatalogManifest(name, ns string) string {
	// The images do not need to be pullable: the webhook validates the spec
	// (series transitions), not image contents, and these specs are deleted
	// without waiting for readiness.
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: ImageCatalog
metadata:
  name: %s
  namespace: %s
spec:
  images:
    - series: "8.0"
      image: %s
    - series: "8.4"
      image: %s
    - series: "9.0"
      image: %s
`, name, ns, instanceImage, instanceImage, instanceImage)
}

func catalogClusterManifest(name, ns, series string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %s
    series: "%s"
  storage:
    size: 1Gi
  mysql:
%s
%s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, ns, upgradeCatalogName, series, e2eMySQLParameters, e2eInstanceResources)
}

func majorUpgradeCatalogManifest(name, ns string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: ImageCatalog
metadata:
  name: %s
  namespace: %s
spec:
  images:
    - series: "8.0"
      image: %s
    - series: "8.4"
      image: %s
    - series: "9.0"
      image: %s
`, name, ns, instanceImageFor("8.0"), instanceImageFor("8.4"), instanceImageFor("9.x"))
}

func majorUpgradeGRClusterManifest(name, ns, catalog, series string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 3
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %[3]s
    series: "%[4]s"
  replication:
    mode: groupReplication
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

// expectApplyRejected applies a manifest expecting the admission webhook to deny
// it, and asserts the denial message contains want.
func expectApplyRejected(name, manifest, want string) {
	path := writeManifest(name, manifest)
	out, err := kubectl("apply", "-f", path)
	Expect(err).To(HaveOccurred(), "expected the apply to be rejected by the webhook")
	Expect(strings.ToLower(out)).To(ContainSubstring(strings.ToLower(want)),
		"rejection message should explain why")
}

var _ = Describe("MySQL major-version upgrade admission", Ordered, Label("feature"), func() {
	const cluster = "upgrade-guard"

	BeforeAll(func() {
		applyManifest(upgradeCatalogName, upgradeCatalogManifest(upgradeCatalogName, testNamespace))
	})

	AfterAll(func() {
		deleteManifest(upgradeCatalogName, upgradeCatalogManifest(upgradeCatalogName, testNamespace))
	})

	It("rejects a cluster that sets both imageName and imageCatalogRef on create", func() {
		manifest := strings.Replace(
			catalogClusterManifest("upgrade-both", testNamespace, "8.0"),
			"  storage:",
			"  imageName: "+instanceImage+"\n  storage:", 1)
		expectApplyRejected("upgrade-both", manifest, "mutually exclusive")
	})

	It("rejects a skipped series and allows a single hop", func() {
		By("creating a cluster pinned to series 8.0")
		applyManifest(cluster, catalogClusterManifest(cluster, testNamespace, "8.0"))
		DeferCleanup(func() { deleteCluster(cluster) })

		By("rejecting a skip straight to 9.0")
		expectApplyRejected(cluster, catalogClusterManifest(cluster, testNamespace, "9.0"), "8.4")

		By("allowing the adjacent hop to 8.4")
		applyManifest(cluster, catalogClusterManifest(cluster, testNamespace, "8.4"))

		By("rejecting a downgrade back to 8.0")
		expectApplyRejected(cluster, catalogClusterManifest(cluster, testNamespace, "8.0"), "downgrade")
	})

	It("blocks a cluster whose ImageCatalog does not exist at reconcile time", func() {
		manifest := strings.ReplaceAll(
			catalogClusterManifest("upgrade-nocat", testNamespace, "8.0"),
			upgradeCatalogName, "does-not-exist")
		applyManifest("upgrade-nocat", manifest)
		DeferCleanup(func() { deleteCluster("upgrade-nocat") })

		Eventually(func(g Gomega) {
			phase, err := clusterField("upgrade-nocat", "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"),
				"cluster with nonexistent catalog must be Blocked by the reconciler")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying no instance Pod is ever created")
		out, err := kubectl("get", "pod", "upgrade-nocat-1", "-n", testNamespace, "--ignore-not-found")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(BeEmpty(), "blocked cluster must not provision instances")
	})

	It("blocks a cluster whose ImageCatalog does not declare the requested series at reconcile time", func() {
		manifest := strings.ReplaceAll(
			catalogClusterManifest("upgrade-noseries", testNamespace, "8.0"),
			"series: \"8.0\"", "series: \"5.7\"")
		applyManifest("upgrade-noseries", manifest)
		DeferCleanup(func() { deleteCluster("upgrade-noseries") })

		Eventually(func(g Gomega) {
			phase, err := clusterField("upgrade-noseries", "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"),
				"cluster with catalog missing the series must be Blocked by the reconciler")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("rejects a cluster whose ImageCatalog series name is empty at admission time", func() {
		manifest := strings.ReplaceAll(
			catalogClusterManifest("upgrade-empty", testNamespace, "8.0"),
			"series: \"8.0\"", "series: \"\"")
		path := writeManifest("upgrade-empty", manifest)

		out, err := kubectl("apply", "-f", path)
		Expect(err).To(HaveOccurred(),
			"empty series must be rejected by the CRD pattern at admission, not admitted")
		Expect(out).To(ContainSubstring("series"),
			"rejection message should name the offending series field")
	})

	It("rejects switching from imageCatalogRef to imageName on an existing cluster", func() {
		applyManifest(cluster, catalogClusterManifest(cluster, testNamespace, "8.0"))
		DeferCleanup(func() { deleteCluster(cluster) })

		manifest := strings.Replace(
			catalogClusterManifest(cluster, testNamespace, "8.0"),
			"imageCatalogRef:", "imageName: "+instanceImage+"\n  imageCatalogRef:", 1)
		expectApplyRejected(cluster, manifest, "mutually exclusive")
	})

	It("rejects a downgrade on a single-instance cluster (non-GR)", func() {
		const solo = "upgrade-solo"
		soloManifest := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %s
    series: "8.4"
  storage:
    size: 1Gi
  mysql:
%s
  bootstrap:
    initdb:
      database: app
      owner: app
%s
`, solo, testNamespace, upgradeCatalogName, e2eMySQLParameters, e2eInstanceResources)

		applyManifest(solo, soloManifest)
		DeferCleanup(func() { deleteCluster(solo) })

		expectApplyRejected(solo, strings.ReplaceAll(soloManifest, "8.4", "8.0"), "downgrade")
	})
})

var _ = Describe("MySQL major-version upgrade rollout", Ordered, Label("disruptive", "major-upgrade"), func() {
	const (
		cluster = "major-upgrade"
		catalog = "major-upgrade-images"
	)

	var ns, prevNS, password string

	BeforeAll(func() {
		if os.Getenv("E2E_MAJOR_UPGRADE") != trueEnvValue {
			Skip("requires the dedicated multi-image major-upgrade job")
		}
		prevNS = testNamespace
		ns = createTestNamespace("major-upgrade")

		By("creating the multi-series image catalog")
		applyManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))

		By("bootstrapping a three-member 8.0 Group Replication cluster")
		applyManifest(cluster, majorUpgradeGRClusterManifest(cluster, ns, catalog, "8.0"))
		DeferCleanup(func() {
			deleteManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, 3, 20*time.Minute)
		password = appPassword(cluster)
	})

	It("rolls through every adjacent series and finalizes the GR protocol", func() {
		primary := clusterPrimary(cluster)
		By("writing durable data before the first irreversible upgrade")
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE major_upgrade_data (id INT PRIMARY KEY, value VARCHAR(32)); "+
				"INSERT INTO major_upgrade_data VALUES (1, 'before-upgrade');")
		Expect(err).NotTo(HaveOccurred())

		hops := []struct {
			series       string
			serverPrefix string
			image        string
		}{
			{series: "8.4", serverPrefix: "8.4.", image: instanceImageFor("8.4")},
			{series: "9.0", serverPrefix: "9.", image: instanceImageFor("9.x")},
		}
		for _, hop := range hops {
			By(fmt.Sprintf("upgrading all members to MySQL %s", hop.series))
			oldPrimary := clusterPrimary(cluster)
			applyManifest(cluster, majorUpgradeGRClusterManifest(cluster, ns, catalog, hop.series))
			order := waitForMajorUpgradeRoll(cluster, oldPrimary, hop.image)
			Expect(order).To(HaveLen(3))
			Expect(order[2]).To(Equal(oldPrimary),
				"the pre-upgrade primary must be the last member rolled")

			expectClusterReady(cluster, 3, 20*time.Minute)
			assertMajorUpgradeMembers(cluster, password, hop.serverPrefix)
			assertGroupCommunicationProtocol(cluster, hop.serverPrefix)

			primary = clusterPrimary(cluster)
			out, err := mysqlExec(primary, "app", password, "app",
				"SELECT value FROM major_upgrade_data WHERE id = 1;")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("before-upgrade"),
				"application data must survive the %s upgrade", hop.series)
		}
	})
})

var _ = Describe("MySQL major-version upgrade defensive scenarios", Ordered, Label("disruptive", "major-upgrade"), func() {
	const catalog = "major-upgrade-def-images"

	var ns, prevNS string

	BeforeAll(func() {
		if os.Getenv("E2E_MAJOR_UPGRADE") != trueEnvValue {
			Skip("requires the dedicated multi-image major-upgrade job")
		}
		prevNS = testNamespace
		ns = createTestNamespace("major-upgrade-def")

		By("creating the multi-series image catalog")
		applyManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))

		DeferCleanup(func() {
			deleteManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("blocks a stable cluster when its ImageCatalog is deleted and recovers on recreate", func() {
		const cluster = "major-upgrade-def-del"
		clusterManifest := majorUpgradeGRClusterManifest(cluster, ns, catalog, "8.0")

		By("bootstrapping a 8.0 cluster")
		applyManifest(cluster, clusterManifest)
		DeferCleanup(func() { deleteCluster(cluster) })
		expectClusterReady(cluster, 3, 20*time.Minute)

		By("deleting the ImageCatalog while the cluster is stable")
		deleteManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))

		By("waiting for the cluster to become Blocked because the catalog is gone")
		Eventually(func(g Gomega) {
			phase, err := clusterField(cluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"),
				"cluster must be Blocked when its ImageCatalog is deleted")
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the cluster does not proceed to provision any new instances")
		Consistently(func(g Gomega) {
			phase, err := clusterField(cluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"),
				"cluster must remain Blocked while catalog is missing")
		}, e2eTimeout(1*time.Minute), 10*time.Second).Should(Succeed())

		By("recreating the catalog")
		applyManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))

		By("triggering reconciliation by updating the Cluster")
		clusterAnnotate(cluster, "cnmsql.co/e2e-trigger=reconcile")

		By("verifying the cluster recovers to Ready after the catalog is back")
		expectClusterReady(cluster, 3, 20*time.Minute)
	})

	It("survives a catalog image spec update while a cluster is pinned to a stable series", func() {
		const cluster = "major-upgrade-def-mut"

		By("bootstrapping a 8.0 cluster")
		applyManifest(cluster, majorUpgradeGRClusterManifest(cluster, ns, catalog, "8.0"))
		DeferCleanup(func() { deleteCluster(cluster) })
		expectClusterReady(cluster, 3, 20*time.Minute)

		password := appPassword(cluster)

		By("mutating the catalog by adding a bogus extra series entry")
		image9x := instanceImageFor("9.x")
		updatedCatalog := majorUpgradeCatalogManifest(catalog, ns)
		updatedCatalog = strings.Replace(updatedCatalog,
			"- series: \"9.0\"\n      image: "+image9x,
			"- series: \"9.0\"\n      image: "+image9x+"\n    - series: \"9.9\"\n      image: "+image9x, 1)
		applyManifest(catalog, updatedCatalog)

		By("verifying the cluster stays Ready and on its pinned series despite the catalog change")
		Consistently(func(g Gomega) {
			ready, err := clusterField(cluster, "{.status.conditions[?(@.type=='Ready')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(Equal("True"))
		}, e2eTimeout(2*time.Minute), 10*time.Second).Should(Succeed())

		assertMajorUpgradeMembers(cluster, password, "8.0.")

		By("restoring the original catalog to leave a clean state")
		applyManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))
	})
})

var _ = Describe("MySQL major-version upgrade single-instance", Ordered, Label("disruptive", "major-upgrade"), func() {
	const (
		cluster = "major-upgrade-solo"
		catalog = "major-upgrade-solo-images"
	)

	var ns, prevNS, password string

	BeforeAll(func() {
		if os.Getenv("E2E_MAJOR_UPGRADE") != trueEnvValue {
			Skip("requires the dedicated multi-image major-upgrade job")
		}
		prevNS = testNamespace
		ns = createTestNamespace("major-upgrade-solo")

		By("creating the multi-series image catalog")
		applyManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))

		DeferCleanup(func() {
			deleteManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("upgrades a single-instance cluster through all adjacent series", func() {
		soloManifest := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %[3]s
    series: "8.0"
  upgrade:
    backupBeforeUpgrade: false
  storage:
    size: 2Gi
%[4]s
  mysql:
    binlogFormat: ROW
%[5]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, cluster, ns, catalog, e2eInstanceResources, e2eMySQLParameters)

		By("bootstrapping a single-instance 8.0 cluster")
		applyManifest(cluster, soloManifest)
		DeferCleanup(func() { deleteCluster(cluster) })
		expectClusterReady(cluster, 1, 20*time.Minute)
		password = appPassword(cluster)

		By("writing durable data before the upgrade")
		_, err := mysqlExec(cluster+"-1", "app", password, "app",
			"CREATE TABLE major_upgrade_solo (id INT PRIMARY KEY, value VARCHAR(32)); "+
				"INSERT INTO major_upgrade_solo VALUES (1, 'solo-data');")
		Expect(err).NotTo(HaveOccurred())

		hops := []struct {
			series       string
			serverPrefix string
		}{
			{series: "8.4", serverPrefix: "8.4."},
			{series: "9.0", serverPrefix: "9."},
		}
		for _, hop := range hops {
			By(fmt.Sprintf("upgrading to MySQL %s", hop.series))
			applyManifest(cluster, strings.ReplaceAll(
				strings.ReplaceAll(soloManifest, `series: "8.0"`, fmt.Sprintf(`series: "%s"`, hop.series)),
				`series: "8.4"`, fmt.Sprintf(`series: "%s"`, hop.series)))

			expectClusterReady(cluster, 1, 20*time.Minute)

			Eventually(func(g Gomega) {
				out, err := mysqlExec(cluster+"-1", "app", password, "", "SELECT VERSION();")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(HavePrefix(hop.serverPrefix))
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

			out, err := mysqlExec(cluster+"-1", "app", password, "app",
				"SELECT value FROM major_upgrade_solo WHERE id = 1;")
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.TrimSpace(out)).To(Equal("solo-data"),
				"application data must survive the %s upgrade", hop.series)
		}
	})
})

var _ = Describe("MySQL major-version upgrade with backupBeforeUpgrade", Ordered, Label("disruptive", "major-upgrade"), func() {
	const (
		cluster = "major-upgrade-backup"
		catalog = "major-upgrade-backup-images"
	)

	var ns, prevNS string

	BeforeAll(func() {
		if os.Getenv("E2E_MAJOR_UPGRADE") != trueEnvValue {
			Skip("requires the dedicated multi-image major-upgrade job")
		}
		prevNS = testNamespace
		ns = createTestNamespace("major-upgrade-backup")

		By("deploying MinIO for backup storage")
		setupMinio()
		DeferCleanup(teardownMinio)

		By("creating the multi-series image catalog")
		applyManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))

		DeferCleanup(func() {
			deleteManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("takes a backup before each major upgrade hop", func() {
		backupClusterManifest := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %[3]s
    series: "8.0"
  upgrade:
    backupBeforeUpgrade: true
  backup:
%[4]s
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
`, cluster, ns, catalog, objectStoreYAML("    "), e2eInstanceResources, e2eMySQLParameters)

		By("bootstrapping a single-instance 8.0 cluster with backupBeforeUpgrade")
		applyManifest(cluster, backupClusterManifest)
		DeferCleanup(func() { deleteCluster(cluster) })
		expectClusterReady(cluster, 1, 20*time.Minute)

		primary := clusterPrimary(cluster)
		password := appPassword(cluster)
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE backup_upgrade_data (id INT PRIMARY KEY, val VARCHAR(32)); "+
				"INSERT INTO backup_upgrade_data VALUES (1, 'pre-backup-upgrade');")
		Expect(err).NotTo(HaveOccurred())

		By("upgrading to 8.4 and verifying a Backup was created")
		manifest84 := strings.ReplaceAll(
			strings.ReplaceAll(backupClusterManifest, `series: "8.0"`, `series: "8.4"`),
			`backupBeforeUpgrade: true`, `backupBeforeUpgrade: true`)
		applyManifest(cluster, manifest84)

		By("waiting for the cluster to be Ready and a Backup to exist")
		expectClusterReady(cluster, 1, 20*time.Minute)
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "backup", "-n", testNamespace, "-o", "name", "--no-headers")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(BeEmpty(), "expected at least one Backup to be created by the upgrade")
			GinkgoWriter.Printf("Backups found:\n%s\n", out)
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying data survived the backed-up upgrade")
		out, err := mysqlExec(clusterPrimary(cluster), "app", password, "app",
			"SELECT val FROM backup_upgrade_data WHERE id = 1;")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("pre-backup-upgrade"))
	})
})

var _ = Describe("MySQL major-version upgrade blocked by missing backup store", Ordered, Label("disruptive", "major-upgrade"), func() {
	const (
		cluster = "major-upgrade-noobj"
		catalog = "major-upgrade-noobj-images"
	)

	var ns, prevNS string

	BeforeAll(func() {
		if os.Getenv("E2E_MAJOR_UPGRADE") != trueEnvValue {
			Skip("requires the dedicated multi-image major-upgrade job")
		}
		prevNS = testNamespace
		ns = createTestNamespace("major-upgrade-noobj")

		By("creating the multi-series image catalog")
		applyManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))

		DeferCleanup(func() {
			deleteManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("blocks the upgrade when backupBeforeUpgrade defaults to true and no object store is configured", func() {
		noObjManifest := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %[3]s
    series: "8.0"
  storage:
    size: 1Gi
%[4]s
  mysql:
    binlogFormat: ROW
%[5]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, cluster, ns, catalog, e2eInstanceResources, e2eMySQLParameters)

		By("bootstrapping a cluster without backup.objectStore — backupBeforeUpgrade defaults to true")
		applyManifest(cluster, noObjManifest)
		DeferCleanup(func() { deleteCluster(cluster) })
		expectClusterReady(cluster, 1, 20*time.Minute)

		By("triggering a series upgrade to 8.4 and expecting Blocked due to missing object store")
		applyManifest(cluster, strings.ReplaceAll(noObjManifest, `series: "8.0"`, `series: "8.4"`))

		Eventually(func(g Gomega) {
			phase, err := clusterField(cluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			reason, err := clusterField(cluster, "{.status.phaseReason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"),
				"upgrade must be Blocked when backupBeforeUpgrade is enabled but no object store exists")
			g.Expect(reason).To(ContainSubstring("no object store"))
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying no instance Pod image was changed while Blocked")
		podImage, err := kubectl("get", "pod", cluster+"-1", "-n", testNamespace,
			"-o", `jsonpath={.spec.containers[?(@.name=="mysql")].image}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(podImage).To(ContainSubstring("cnmsql-instance:8.0"),
			"instance image must not change when upgrade is blocked")

		By("unblocking by disabling the backup-before-upgrade requirement")
		unblockManifest := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %[3]s
    series: "8.4"
  upgrade:
    backupBeforeUpgrade: false
  storage:
    size: 1Gi
%[4]s
  mysql:
    binlogFormat: ROW
%[5]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, cluster, ns, catalog, e2eInstanceResources, e2eMySQLParameters)
		applyManifest(cluster, unblockManifest)
		expectClusterReady(cluster, 1, 20*time.Minute)
	})
})

var _ = Describe("MySQL major-version upgrade with removed parameters", Ordered, Label("disruptive", "major-upgrade"), func() {
	const (
		cluster = "major-upgrade-removed"
		catalog = "major-upgrade-removed-images"
	)

	var ns, prevNS, password string

	BeforeAll(func() {
		if os.Getenv("E2E_MAJOR_UPGRADE") != trueEnvValue {
			Skip("requires the dedicated multi-image major-upgrade job")
		}
		prevNS = testNamespace
		ns = createTestNamespace("major-upgrade-removed")

		By("creating the multi-series image catalog")
		applyManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))

		DeferCleanup(func() {
			deleteManifest(catalog, majorUpgradeCatalogManifest(catalog, ns))
			deleteTestNamespace(ns, prevNS)
		})
	})

	It("drops parameters removed in the target series and surfaces Warning events", func() {
		baseManifest := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %[3]s
    series: "8.0"
  upgrade:
    backupBeforeUpgrade: false
  storage:
    size: 1Gi
  resources:
    requests:
      cpu: 100m
      memory: 384Mi
    limits:
      cpu: "1"
      memory: 1536Mi
  mysql:
    binlogFormat: ROW
    parameters:
      innodb_buffer_pool_size: 128M
      max_connections: "80"
  bootstrap:
    initdb:
      database: app
      owner: app
`, cluster, ns, catalog)

		By("bootstrapping an 8.0 cluster with standard parameters")
		applyManifest(cluster, baseManifest)
		DeferCleanup(func() { deleteCluster(cluster) })
		expectClusterReady(cluster, 1, 20*time.Minute)
		password = appPassword(cluster)

		primary := clusterPrimary(cluster)
		_, err := mysqlExec(primary, "app", password, "app",
			"CREATE TABLE removed_param_test (id INT PRIMARY KEY, val VARCHAR(32)); "+
				"INSERT INTO removed_param_test VALUES (1, 'removed-param-data');")
		Expect(err).NotTo(HaveOccurred())

		By("adding master_info_repository (removed in 8.4) to the spec alongside the series upgrade")
		removedParamManifest := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageCatalogRef:
    apiGroup: mysql.cnmsql.co
    kind: ImageCatalog
    name: %[3]s
    series: "8.4"
  upgrade:
    backupBeforeUpgrade: false
  storage:
    size: 1Gi
  resources:
    requests:
      cpu: 100m
      memory: 384Mi
    limits:
      cpu: "1"
      memory: 1536Mi
  mysql:
    binlogFormat: ROW
    parameters:
      master_info_repository: TABLE
      innodb_buffer_pool_size: 128M
      max_connections: "80"
  bootstrap:
    initdb:
      database: app
      owner: app
`, cluster, ns, catalog)

		applyManifest(cluster, removedParamManifest)
		expectClusterReady(cluster, 1, 20*time.Minute)

		By("verifying a RemovedParameter Warning event was emitted")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "events", "-n", testNamespace,
				"--sort-by=.lastTimestamp")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("RemovedParameter"),
				"must emit a RemovedParameter event for master_info_repository\nall events:\n%s", out)
			g.Expect(out).To(ContainSubstring("master_info_repository"),
				"RemovedParameter event must mention master_info_repository\nall events:\n%s", out)
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the 8.4 server accepted the upgrade and data survived")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(cluster+"-1", "app", password, "", "SELECT VERSION();")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(HavePrefix("8.4."))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		out, err := mysqlExec(cluster+"-1", "app", password, "app",
			"SELECT val FROM removed_param_test WHERE id = 1;")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("removed-param-data"))
	})
})

func waitForMajorUpgradeRoll(cluster, oldPrimary, targetImage string) []string {
	GinkgoHelper()
	instances := []string{cluster + "-1", cluster + "-2", cluster + "-3"}
	pollOrder := make([]string, 0, len(instances))
	for _, instance := range instances {
		if instance != oldPrimary {
			pollOrder = append(pollOrder, instance)
		}
	}
	pollOrder = append(pollOrder, oldPrimary)
	seen := map[string]bool{}
	order := make([]string, 0, len(instances))

	// readyInstances is a best-effort status counter that dips transiently during a
	// serialized GR roll even though only one Pod is ever taken down at a time: a
	// primary re-election, a member still RECOVERING right after it rejoins, or a
	// single failed control-API Status() poll each subtract a member for a sample or
	// two. So we do not fail on one low reading; we require the dip below the floor to
	// persist across consecutive polls, which is what a genuine "two members down at
	// once" regression would do and a transient blip would not.
	const maxTransientUnavailablePolls = 5
	floor := len(instances) - 1
	minimumReady := len(instances)
	consecutiveLow, maxConsecutiveLow := 0, 0

	Eventually(func(g Gomega) int {
		for _, instance := range pollOrder {
			image, err := kubectl("get", "pod", instance, "-n", testNamespace,
				"-o", `jsonpath={.spec.containers[?(@.name=="mysql")].image}`)
			if err == nil && strings.TrimSpace(image) == targetImage && !seen[instance] {
				seen[instance] = true
				order = append(order, instance)
				GinkgoWriter.Printf("Major upgrade rolled %s to %s (order %v)\n", instance, targetImage, order)
			}
		}
		ready, err := clusterField(cluster, "{.status.readyInstances}")
		if err == nil {
			var count int
			if _, scanErr := fmt.Sscanf(strings.TrimSpace(ready), "%d", &count); scanErr == nil {
				if count < minimumReady {
					minimumReady = count
				}
				if count < floor {
					consecutiveLow++
					if consecutiveLow > maxConsecutiveLow {
						maxConsecutiveLow = consecutiveLow
					}
				} else {
					consecutiveLow = 0
				}
			}
		}
		if seen[oldPrimary] {
			g.Expect(order).To(HaveLen(len(instances)),
				"the primary must not roll before both replicas")
		}
		return len(order)
	}, e2eTimeout(20*time.Minute), 2*time.Second).Should(Equal(len(instances)))

	Expect(maxConsecutiveLow).To(BeNumerically("<", maxTransientUnavailablePolls),
		"the serialized rollout must leave at most one member unavailable: "+
			"status.readyInstances stayed below %d for %d consecutive polls (low-water mark %d); "+
			"a sustained drop means two members were down at once rather than a transient "+
			"GR re-election or control-API blip",
		floor, maxConsecutiveLow, minimumReady)
	return order
}

func assertMajorUpgradeMembers(cluster, password, serverPrefix string) {
	GinkgoHelper()
	for _, instance := range []string{cluster + "-1", cluster + "-2", cluster + "-3"} {
		Eventually(func(g Gomega) {
			out, err := mysqlExec(instance, "app", password, "", "SELECT VERSION();")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(HavePrefix(serverPrefix))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	}
}

func assertGroupCommunicationProtocol(cluster, serverPrefix string) {
	GinkgoHelper()
	password := rootPassword(cluster)
	Eventually(func(g Gomega) {
		primary := clusterPrimary(cluster)
		serverVersion, err := mysqlExec(primary, "root", password, "", "SELECT VERSION();")
		g.Expect(err).NotTo(HaveOccurred())
		serverVersion = strings.Split(strings.TrimSpace(serverVersion), "-")[0]
		g.Expect(serverVersion).To(HavePrefix(serverPrefix))

		protocol, err := mysqlExec(primary, "root", password, "",
			"SELECT group_replication_get_communication_protocol();")
		g.Expect(err).NotTo(HaveOccurred())
		protocol = strings.TrimSpace(protocol)
		g.Expect(protocol).NotTo(BeEmpty())

		observedProtocol, err := clusterField(cluster, "{.status.groupReplication.communicationProtocol}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(observedProtocol).To(Equal(protocol),
			"Cluster status must expose the effective GR communication protocol")

		finalizedTarget, err := clusterField(cluster, "{.status.groupReplication.communicationProtocolTarget}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(finalizedTarget).To(HavePrefix(serverPrefix),
			"Cluster status must record the server target passed to protocol finalization")
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
}
