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

	It("recycles the Pod to finish a resize when in-use expansion is disabled", func() {
		const (
			cluster   = "resize-offline"
			instances = 2
		)
		// Target a replica so the primary stays writable through the roll, exercising
		// the replica-first ordering of the resize roll.
		replica := cluster + "-2"

		By("creating a cluster with resizeInUseVolumes disabled")
		applyManifest(cluster, offlineResizeClusterManifest(cluster, instances))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, instances, 20*time.Minute)

		uid := podUID(replica)
		Expect(uid).NotTo(BeEmpty(), "replica Pod %s has no UID", replica)

		// The Kind default backend has no resizer, so a node-side resize never becomes
		// pending on its own. Inject the condition the operator keys on
		// (FileSystemResizePending) — the same signal a real offline-expand backend
		// sets — to drive the recycle. We re-inject until the roll starts (in case a
		// controller strips it) and clear it the moment the roll is observed, so the
		// operator completes exactly one recycle instead of looping.
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
		expectClusterReady(cluster, instances, 20*time.Minute)
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

// offlineResizeClusterManifest is a Cluster whose storage backend cannot expand a
// mounted volume (resizeInUseVolumes: false), so the operator must recycle Pods
// to finish a resize. maxStopDelay is lowered so the recycle is quick.
func offlineResizeClusterManifest(name string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
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
`, name, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters)
}

// setPVCResizePending injects a FileSystemResizePending condition into a PVC's
// status — the signal a real offline-expand backend sets when a node-side
// filesystem resize is waiting on the volume being remounted. It patches the
// status subresource so the operator sees the pending resize.
func setPVCResizePending(pvcName string) {
	GinkgoHelper()
	const patch = `{"status":{"conditions":[{` +
		`"type":"FileSystemResizePending","status":"True",` +
		`"lastProbeTime":"2026-01-01T00:00:00Z","lastTransitionTime":"2026-01-01T00:00:00Z",` +
		`"reason":"E2EInjected","message":"injected by e2e to exercise the offline resize roll"}]}}`
	_, _ = kubectl("patch", "pvc", pvcName, "-n", testNamespace,
		"--subresource=status", "--type=merge", "-p", patch)
}

// clearPVCConditions removes any injected conditions from a PVC's status so the
// operator stops treating the resize as pending and does not roll the Pod again.
func clearPVCConditions(pvcName string) {
	GinkgoHelper()
	_, _ = kubectl("patch", "pvc", pvcName, "-n", testNamespace,
		"--subresource=status", "--type=merge", "-p", `{"status":{"conditions":[]}}`)
}

// podTerminatingOrReplaced reports whether the named Pod has either started
// terminating (a DeletionTimestamp is set) or already been recreated with a
// different UID — i.e. the operator has begun recycling it.
func podTerminatingOrReplaced(name, originalUID string) bool {
	out, err := kubectl("get", "pod", name, "-n", testNamespace,
		"-o", "jsonpath={.metadata.uid}{'|'}{.metadata.deletionTimestamp}")
	if err != nil {
		// A NotFound (Pod deleted, not yet recreated) is part of the recycle.
		return true
	}
	parts := strings.SplitN(strings.TrimSpace(out), "|", 2)
	uid := parts[0]
	deletionTimestamp := ""
	if len(parts) == 2 {
		deletionTimestamp = parts[1]
	}
	if uid == "" {
		return true
	}
	return uid != originalUID || deletionTimestamp != ""
}
