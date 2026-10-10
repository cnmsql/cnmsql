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

// The MariaDB counterpart of the "Volume resize" suite. Online expansion and the
// offline recycle roll are storage-plane operations the operator drives
// identically for either engine; these specs pin that a MariaDB cluster's PVCs
// grow (and, when in-use expansion is disabled, that the Pod is recycled) the
// same way. It reuses the flavor-agnostic PVC/StorageClass helpers.
var _ = Describe("MariaDB volume resize", Ordered, Label("feature", "mariadb"), func() {
	var ns, prevNS, scName string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mdb-volresize")
		scName = fmt.Sprintf("cnmsql-mdb-expandable-%d", GinkgoParallelProcess())
		createExpandableStorageClass(scName)
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
			_, _ = kubectl("delete", "storageclass", scName, "--ignore-not-found")
		})
	})

	It("grows the instance PVC on a size increase without recycling the Pod", func() {
		const cluster = "mdb-resize"
		instance := cluster + "-1"

		By("creating a single-instance MariaDB cluster on the expandable storage class")
		applyManifest(cluster, mariadbExpandableClusterManifest(cluster, scName, "1Gi"))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 1, e2eTimeout(20*time.Minute))

		uid := podUID(instance)
		Expect(uid).NotTo(BeEmpty(), "instance Pod %s has no UID", instance)

		By("increasing spec.storage.size to 2Gi")
		patchClusterStorageSize(cluster, "2Gi")

		By("verifying the operator grows the PVC request to 2Gi")
		Eventually(func() (string, error) {
			out, err := kubectl("get", "pvc", instance, "-n", testNamespace,
				"-o", "jsonpath={.spec.resources.requests.storage}")
			return strings.TrimSpace(out), err
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Equal("2Gi"),
			"PVC %s request was not grown to 2Gi", instance)

		By("verifying the Pod was not recycled (online expansion needs no roll)")
		Consistently(func() string {
			return podUID(instance)
		}, 30*time.Second, 5*time.Second).Should(Equal(uid),
			"instance Pod %s was recycled during an online resize", instance)
	})

	It("recycles the Pod to finish a resize when in-use expansion is disabled", func() {
		const (
			cluster   = "mdb-resize-offline"
			instances = 2
		)
		// Target a replica so the primary stays writable through the roll.
		replica := cluster + "-2"

		By("creating a MariaDB cluster with resizeInUseVolumes disabled")
		applyManifest(cluster, mariadbOfflineResizeClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))

		uid := podUID(replica)
		Expect(uid).NotTo(BeEmpty(), "replica Pod %s has no UID", replica)

		By("marking the replica PVC as pending a node-side resize and waiting for the recycle")
		Eventually(func() bool {
			setPVCResizePending(replica)
			return podTerminatingOrReplaced(replica, uid)
		}, e2eTimeout(5*time.Minute), 2*time.Second).Should(BeTrue(),
			"operator did not recycle replica Pod %s after a pending resize", replica)
		clearPVCConditions(replica)

		By("verifying the replica comes back with a fresh Pod and the cluster converges")
		Eventually(func() string {
			return podUID(replica)
		}, e2eTimeout(5*time.Minute), 5*time.Second).ShouldNot(Or(Equal(uid), BeEmpty()),
			"replica Pod %s was not recreated", replica)
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})
})

// mariadbExpandableClusterManifest is a single-instance MariaDB Cluster pinned to
// the given expandable storage class and initial size.
func mariadbExpandableClusterManifest(name, storageClass, size string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  flavor: mariadb
  instances: 1
  imageName: %[3]s
  storage:
    size: %[7]s
    storageClass: %[4]s
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, mariadbImage, storageClass, e2eInstanceResources, e2eMySQLParameters, size)
}

// mariadbOfflineResizeClusterManifest is a MariaDB Cluster whose storage backend
// cannot expand a mounted volume (resizeInUseVolumes: false), so the operator
// must recycle Pods to finish a resize.
func mariadbOfflineResizeClusterManifest(name string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  flavor: mariadb
  instances: %[3]d
  imageName: %[4]s
  maxStopDelay: 30
  storage:
    size: 2Gi
    resizeInUseVolumes: false
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instances, mariadbImage, e2eInstanceResources, e2eMySQLParameters)
}
