//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs exercise the bootstrap Jobs that initialise instance data
// directories (design 031): one one-shot Job per instance volume, named
// <instance>-<mode>, that runs before the instance Pod exists. They watch the
// Jobs appear while a cluster provisions, verify the bookkeeping they leave
// behind (the pvc-status annotation, deleted succeeded Jobs, instance Pods
// without a bootstrap init container or object-store credentials), and drive
// the failure path: a restore Job that misses its activeDeadline surfaces as a
// BootstrapFailed condition with a BootstrapJobFailed event, and is replaced
// once the spec that shaped it changes. A final spec re-initialises a replica
// and watches it re-clone through a fresh join Job.
var _ = Describe("Instance Bootstrap Jobs", Ordered, Label("feature"), func() {
	const (
		sourceCluster   = "boot-src"
		restoredCluster = "boot-restored"
		deadlineCluster = "boot-deadline"
		backupName      = "boot-backup"
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("bootstrap-jobs")

		setupObjectStore()
		DeferCleanup(teardownObjectStore)

		By("creating the 2-instance source cluster the later specs back up and re-clone against")
		// The cluster is created here, while the first It still watches it
		// provision, so it survives across the whole Ordered container: a
		// DeferCleanup registered inside an It runs at that It's end.
		applyManifest(sourceCluster, bootstrapSourceManifest(sourceCluster))
		DeferCleanup(func() {
			deleteManifest(sourceCluster, bootstrapSourceManifest(sourceCluster))
		})
	})

	It("bootstraps the source cluster's volumes with Jobs", func() {
		By("watching the primary's volume bootstrap through an initdb Job")
		expectBootstrapJob(sourceCluster+"-1-initdb", 5*time.Minute)

		By("watching the replica's volume bootstrap through a join Job")
		expectBootstrapJob(sourceCluster+"-2-join", 10*time.Minute)

		expectClusterReady(sourceCluster, 2, 20*time.Minute)

		By("verifying the succeeded bootstrap Jobs are deleted")
		jobs, err := kubectl("get", "jobs", "-l", "mysql.cnmsql.co/bootstrap-instance",
			"-n", testNamespace, "-o", "jsonpath={.items[*].metadata.name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(jobs).To(BeEmpty(), "succeeded bootstrap Jobs must be deleted once their volume is ready")

		By("verifying both volumes are marked ready")
		for _, pvc := range []string{sourceCluster + "-1", sourceCluster + "-2"} {
			status, err := kubectl("get", "pvc", pvc, "-n", testNamespace,
				"-o", "jsonpath={.metadata.annotations.mysql\\.cnmsql\\.co/pvc-status}")
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal("ready"), "PVC %s is not marked ready after bootstrap", pvc)
		}

		By("verifying the instance Pod no longer carries a bootstrap init container")
		initContainers, err := kubectl("get", "pod", sourceCluster+"-1", "-n", testNamespace,
			"-o", "jsonpath={.spec.initContainers[*].name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(initContainers).To(Equal("bootstrap-controller"),
			"the instance Pod must only carry the bootstrap-controller init container")
	})

	It("recovers from a backup and survives its deletion", func() {
		By("taking a physical backup of the source cluster")
		applyManifest(backupName, backupManifest(backupName, sourceCluster))
		DeferCleanup(func() {
			deleteManifest(backupName, backupManifest(backupName, sourceCluster))
		})
		expectBackupCompleted(backupName, 8*time.Minute)

		By("creating a cluster that recovers from the backup")
		applyManifest(restoredCluster, recoveryClusterManifest(restoredCluster, backupName))
		DeferCleanup(func() {
			deleteManifest(restoredCluster, recoveryClusterManifest(restoredCluster, backupName))
		})

		By("watching the restored volume bootstrap through a restore Job")
		expectBootstrapJob(restoredCluster+"-1-restore", 5*time.Minute)

		expectClusterReady(restoredCluster, 1, 20*time.Minute)

		By("verifying the restored instance Pod carries no object-store credentials")
		podJSON, err := kubectl("get", "pod", restoredCluster+"-1", "-n", testNamespace, "-o", "json")
		Expect(err).NotTo(HaveOccurred())
		Expect(podJSON).NotTo(ContainSubstring(objectStoreCredsSecret),
			"the restored instance Pod must not reference the object-store credentials Secret")

		By("deleting the source Backup and the restored instance Pod")
		_, err = kubectl("delete", "backup", backupName, "-n", testNamespace)
		Expect(err).NotTo(HaveOccurred())
		_, err = kubectl("delete", "pod", restoredCluster+"-1", "-n", testNamespace, "--wait=false")
		Expect(err).NotTo(HaveOccurred())

		By("verifying the recovered cluster rebuilds the instance without the Backup")
		expectClusterReady(restoredCluster, 1, 10*time.Minute)

		phase, err := clusterField(restoredCluster, "{.status.phase}")
		Expect(err).NotTo(HaveOccurred())
		Expect(phase).To(Equal("Ready"), "a recovered cluster must stay Ready after its source Backup is deleted")
	})

	It("reports a failed restore and retries after a spec change", func() {
		By("creating the Backup for the deadline case")
		applyManifest(backupName, backupManifest(backupName, sourceCluster))
		expectBackupCompleted(backupName, 8*time.Minute)

		By("creating a recovery cluster whose restore Job gets a 5s deadline")
		applyManifest(deadlineCluster, recoveryClusterWithDeadlineManifest(deadlineCluster, backupName, "5s"))
		DeferCleanup(func() {
			deleteManifest(deadlineCluster, recoveryClusterWithDeadlineManifest(deadlineCluster, backupName, "1h"))
		})

		By("waiting for the restore Job to fail on its deadline and surface the failure")
		Eventually(func(g Gomega) {
			phase, err := clusterField(deadlineCluster, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Blocked"), "a failed restore Job must block the cluster")

			status, err := clusterField(deadlineCluster,
				"{.status.conditions[?(@.type=='BootstrapFailed')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(status).To(Equal("True"), "BootstrapFailed must be True while the restore Job is failed")

			reason, err := clusterField(deadlineCluster,
				"{.status.conditions[?(@.type=='BootstrapFailed')].reason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("DeadlineExceeded"),
				"the BootstrapFailed reason must be the Job's DeadlineExceeded")

			events, err := kubectl("get", "events", "-n", testNamespace,
				"--field-selector", "reason=BootstrapJobFailed", "-o", "jsonpath={.items[*].reason}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(events).NotTo(BeEmpty(), "the failed bootstrap Job must raise a BootstrapJobFailed event")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("capturing the failed Job's spec hash")
		hash, err := kubectl("get", "job", deadlineCluster+"-1-restore", "-n", testNamespace,
			"-o", "jsonpath={.metadata.annotations.mysql\\.cnmsql\\.co/bootstrap-spec-hash}")
		Expect(err).NotTo(HaveOccurred())
		Expect(hash).NotTo(BeEmpty(), "the failed Job carries no bootstrap-spec-hash")

		By("extending the deadline so the operator replaces the failed Job")
		applyManifest(deadlineCluster, recoveryClusterWithDeadlineManifest(deadlineCluster, backupName, "1h"))
		Eventually(func(g Gomega) {
			next, err := kubectl("get", "job", deadlineCluster+"-1-restore", "-n", testNamespace,
				"-o", "jsonpath={.metadata.annotations.mysql\\.cnmsql\\.co/bootstrap-spec-hash}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(next).NotTo(Equal(hash), "the failed Job must be replaced once its spec changes")
		}, e2eTimeout(5*time.Minute), 2*time.Second).Should(Succeed())

		expectClusterReady(deadlineCluster, 1, 20*time.Minute)

		By("verifying the BootstrapFailed condition is cleared after the retry")
		status, err := clusterField(deadlineCluster,
			"{.status.conditions[?(@.type=='BootstrapFailed')].status}")
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal("False"), "BootstrapFailed must be False once the restore succeeds")
	})

	It("re-clones a replica through a join Job", func() {
		By("requesting the replica's re-initialisation")
		clusterAnnotate(sourceCluster, reinitAnnotationKey+"="+sourceCluster+"-2")

		By("watching the replica's new volume bootstrap through a join Job")
		expectBootstrapJob(sourceCluster+"-2-join", 5*time.Minute)

		expectClusterReady(sourceCluster, 2, 20*time.Minute)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

// bootstrapSourceManifest renders the 2-instance source cluster the other specs
// back up, recover from and re-clone against. It is archivingClusterManifest
// with a second instance, so the replica's volume bootstraps through a join Job.
func bootstrapSourceManifest(name string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 2
  imageName: %s
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
  backup:
%s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

func recoveryClusterWithDeadlineManifest(name, backup, deadline string) string {
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
  bootstrap:
    recovery:
      backup:
        name: %s
  backup:
%s
    jobTemplate:
      activeDeadline: %s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, backup, objectStoreYAML("    "), deadline)
}

// expectBootstrapJob waits for the named bootstrap Job to exist. Bootstrap Jobs
// are short-lived — the operator deletes a succeeded one before creating the
// instance Pod — so the wait must start as soon as the cluster is applied.
func expectBootstrapJob(name string, timeout time.Duration) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		out, err := kubectl("get", "job", name, "-n", testNamespace,
			"-o", "jsonpath={.metadata.name}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal(name), "bootstrap Job %s not observed", name)
	}, e2eTimeout(timeout), 2*time.Second).Should(Succeed())
}
