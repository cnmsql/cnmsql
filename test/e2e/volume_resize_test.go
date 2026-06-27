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

// These specs exercise online volume expansion end to end: increasing
// spec.storage.size must grow the instance's PVC request, and because the
// default (storage.resizeInUseVolumes) lets the backend expand the volume in
// place, the operator must not recycle the Pod to do it.
//
// The offline path — recycling the Pod to complete a resize when the backend
// cannot expand a mounted volume — is covered by unit tests, since reliably
// forcing a node-side FileSystemResizePending requires a CSI driver that the
// Kind default (rancher local-path) does not provide.
var _ = Describe("Volume resize", Ordered, func() {
	var ns, prevNS, scName string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("volresize")
		scName = fmt.Sprintf("cnmsql-expandable-%d", GinkgoParallelProcess())
		createExpandableStorageClass(scName)
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
			_, _ = kubectl("delete", "storageclass", scName, "--ignore-not-found")
		})
	})

	It("grows the instance PVC on a size increase without recycling the Pod", func() {
		const cluster = "resize"
		instance := cluster + "-1"

		By("creating a single-instance cluster on the expandable storage class")
		applyManifest(cluster, expandableClusterManifest(cluster, scName, "1Gi"))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 1, 20*time.Minute)

		// Capturing the Pod UID lets us prove the resize did not roll the Pod: a
		// recreated Pod gets a new UID, an in-place online expansion keeps it.
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
})

// createExpandableStorageClass provisions a StorageClass that allows volume
// expansion, cloning the cluster's default StorageClass provisioner and binding
// mode so it works on whatever backend the Kind cluster uses.
func createExpandableStorageClass(name string) {
	GinkgoHelper()
	def, err := kubectl("get", "storageclass", "-o",
		`jsonpath={.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}`)
	Expect(err).NotTo(HaveOccurred(), "failed to read storage classes")
	def = strings.TrimSpace(def)
	Expect(def).NotTo(BeEmpty(), "no default StorageClass found in the cluster")

	provisioner, err := kubectl("get", "storageclass", def, "-o", "jsonpath={.provisioner}")
	Expect(err).NotTo(HaveOccurred(), "failed to read default StorageClass provisioner")
	provisioner = strings.TrimSpace(provisioner)
	Expect(provisioner).NotTo(BeEmpty(), "default StorageClass %s has no provisioner", def)

	mode, _ := kubectl("get", "storageclass", def, "-o", "jsonpath={.volumeBindingMode}")
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = "Immediate"
	}

	applyManifest("sc-"+name, fmt.Sprintf(`apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %[1]s
provisioner: %[2]s
allowVolumeExpansion: true
volumeBindingMode: %[3]s
reclaimPolicy: Delete
`, name, provisioner, mode))
}

// expandableClusterManifest is a single-instance Cluster pinned to the given
// expandable storage class and initial size.
func expandableClusterManifest(name, storageClass, size string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
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
`, name, testNamespace, instanceImage, storageClass, e2eInstanceResources, e2eMySQLParameters, size)
}

// patchClusterStorageSize requests a new spec.storage.size, retrying on
// transient admission-webhook connectivity errors.
func patchClusterStorageSize(name, size string) {
	GinkgoHelper()
	Eventually(func() error {
		out, err := kubectl("patch", "cluster", name, "-n", testNamespace,
			"--type=merge", "-p", fmt.Sprintf(`{"spec":{"storage":{"size":%q}}}`, size))
		if err != nil && isTransientWebhookError(out, err) {
			return err
		}
		if err != nil {
			StopTrying("storage resize rejected").Wrap(err).Now()
		}
		return nil
	}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed(),
		"failed to resize cluster %s storage to %s", name, size)
}

// podUID returns a Pod's UID, or "" if it cannot be read.
func podUID(name string) string {
	out, err := kubectl("get", "pod", name, "-n", testNamespace, "-o", "jsonpath={.metadata.uid}")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
