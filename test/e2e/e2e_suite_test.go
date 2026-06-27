//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

const trueEnvValue = "true"

// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
var shouldCleanupCertManager = false

// sharedSetupEnabled reports whether the suite should deploy the shared operator,
// MinIO and cert-manager on the suite cluster. Disruptive lanes set
// E2E_SHARED_SETUP=false because every disruptive spec provisions its OWN
// ephemeral cluster — so deploying them on the shared cluster is unused overhead,
// and running a second cluster's control plane alongside it on the single runner
// risks resource/inotify exhaustion. Defaults to enabled when unset.
func sharedSetupEnabled() bool {
	return os.Getenv("E2E_SHARED_SETUP") != "false"
}

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To enable kubectl kuberc (use custom kubectl configurations), set: KUBECTL_KUBERC=true
// By default, kuberc is disabled to ensure consistent test behavior across different environments.
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting cnmsql e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

// SynchronizedBeforeSuite runs the one-time shared setup (image builds, operator
// deployment) on Ginkgo parallel process 1 only, then runs the per-process wait
// on every process so they all see a ready operator before any spec runs.
var _ = SynchronizedBeforeSuite(func() []byte {
	doSuiteSetup()
	return nil
}, func([]byte) {
	// Disruptive lanes deploy no shared operator (each spec provisions its own
	// cluster), so there is nothing to wait for here.
	if !sharedSetupEnabled() {
		return
	}

	By("waiting for the operator to be ready")
	_, err := kubectl("wait", "deployment", "-l", "control-plane=controller-manager",
		"-n", namespace, "--for=condition=Available", "--timeout="+e2eTimeout(5*time.Minute).String())
	Expect(err).NotTo(HaveOccurred(), "controller-manager did not become available")

	By("waiting for the Cluster admission webhook to accept requests")
	probePath := "/tmp/cnmsql-e2e-webhook-readiness.yaml"
	probe := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: webhook-readiness
  namespace: default
spec:
  instances: 1
  imageName: %s
  storage:
    size: 1Gi
`, instanceImage)
	Expect(os.WriteFile(probePath, []byte(probe), 0o644)).To(Succeed())
	Eventually(func() error {
		_, err := kubectl("apply", "--dry-run=server", "-f", probePath)
		return err
	}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed(),
		"Cluster admission webhook did not become ready")
})

// SynchronizedAfterSuite tears down the operator and cert-manager on process 1
// only, after every parallel process has finished its specs.
var _ = SynchronizedAfterSuite(func() {
	// Per-process teardown (no-op — each spec handles its own namespace cleanup).
}, func() {
	if sharedSetupEnabled() {
		teardownSharedMinio()
		undeployOperator()
	}
	teardownCertManager()
	restoreManagerKustomization()
})

// doSuiteSetup is the one-time shared setup that runs on Ginkgo parallel
// process 1 only, via SynchronizedBeforeSuite.
func doSuiteSetup() {
	// `make deploy` rewrites config/manager/kustomization.yaml in the working tree
	// (kustomize edit set image). Snapshot it now and restore it in
	// SynchronizedAfterSuite so an e2e run leaves the tree clean.
	snapshotManagerKustomization()

	// hack/e2e.sh builds and loads the manager image once, outside Ginkgo, and
	// sets E2E_SKIP_IMAGE_BUILD so the suite does not rebuild it. A bare `ginkgo`
	// run (no env set) still builds in-suite, so this stays backward compatible.
	if os.Getenv("E2E_SKIP_IMAGE_BUILD") != trueEnvValue {
		By("building the manager image")
		cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
		_, err := utils.Run(cmd)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

		By("loading the manager image on Kind")
		err = utils.LoadImageToKindClusterWithName(managerImage)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")
	} else {
		By("skipping manager image build/load (E2E_SKIP_IMAGE_BUILD=true; prebuilt by hack/e2e.sh)")
	}

	for _, version := range neededInstanceVersions() {
		pullAndLoadInstanceImage(version)
	}

	configureKubectlKubeRC()

	// Disruptive lanes (E2E_SHARED_SETUP=false) skip the shared operator/MinIO:
	// every disruptive spec provisions its own ephemeral cluster, so this is pure
	// overhead and a second running control plane on the single runner. Instance
	// images are still pulled above so those dedicated clusters can load them.
	if sharedSetupEnabled() {
		setupCertManager()
		deployOperator()
		deploySharedMinio()
	}
}

// managerKustomizationPath is the operator overlay that `make deploy` mutates via
// `kustomize edit set image`. The suite snapshots it at setup and restores it at
// teardown so an e2e run leaves the working tree clean — mirroring how the
// namespaced-mode spec handles config/namespaced/kustomization.yaml.
const managerKustomizationPath = "config/manager/kustomization.yaml"

var managerKustomizationSnapshot []byte

func snapshotManagerKustomization() {
	data, err := os.ReadFile(managerKustomizationPath)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to snapshot %s: %v\n", managerKustomizationPath, err)
		return
	}
	managerKustomizationSnapshot = data
}

func restoreManagerKustomization() {
	if managerKustomizationSnapshot == nil {
		return
	}
	if err := os.WriteFile(managerKustomizationPath, managerKustomizationSnapshot, 0o644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to restore %s: %v\n", managerKustomizationPath, err)
	}
	managerKustomizationSnapshot = nil
}

// pullAndLoadInstanceImage pulls this version's published slim instance image
// (built and pushed from the separate containers repo) and loads it into the
// Kind cluster, so a Cluster pinned to that image boots without each Pod pulling
// from the registry.
func pullAndLoadInstanceImage(version string) {
	image := instanceImageFor(version)

	By(fmt.Sprintf("pulling the instance image (%s)", image))
	cmd := exec.Command("docker", "pull", image)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to pull the instance image %s", image)

	By(fmt.Sprintf("loading the instance image on Kind (%s)", image))
	err = utils.LoadImageToKindClusterWithName(image)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the instance image %s into Kind", image)
}

// neededInstanceVersions is the deduplicated set of instance versions the suite
// loads: the sample Cluster version plus every archiving-matrix version. The
// dedicated major-upgrade job additionally co-loads every supported series so a
// single Kind cluster can perform real adjacent-series rolls.
func neededInstanceVersions() []string {
	seen := map[string]bool{}
	var out []string
	versions := append([]string{sampleVersion()}, archiveVersions()...)
	if os.Getenv("E2E_MAJOR_UPGRADE") == trueEnvValue {
		versions = append(versions, "8.0", "8.4", "9.x")
	}
	for _, v := range versions {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

// deployOperator installs the CRDs and deploys the controller-manager once for
// the whole suite, so every Describe can exercise it without re-deploying.
func deployOperator() {
	By("creating manager namespace")
	cmd := exec.Command("kubectl", "create", "ns", namespace)
	_, _ = utils.Run(cmd)

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install CRDs")

	By("deploying the controller-manager")
	cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
}

// undeployOperator tears down the controller-manager and CRDs installed by
// deployOperator.
func undeployOperator() {
	By("undeploying the controller-manager")
	cmd := exec.Command("make", "undeploy")
	_, _ = utils.Run(cmd)

	By("uninstalling CRDs")
	cmd = exec.Command("make", "uninstall")
	_, _ = utils.Run(cmd)

	By("removing manager namespace")
	cmd = exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found")
	_, _ = utils.Run(cmd)
}

// Disable kubectl kuberc by default for test isolation.
// This prevents local kubectl configurations from affecting test behavior.
// To enable kuberc, set: KUBECTL_KUBERC=true
func configureKubectlKubeRC() {
	if os.Getenv("KUBECTL_KUBERC") != trueEnvValue {
		By("disabling kubectl kuberc for test isolation")
		err := os.Setenv("KUBECTL_KUBERC", "false")
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to disable kubectl kuberc")
		_, _ = fmt.Fprintf(GinkgoWriter,
			"kubectl kuberc disabled for consistent test behavior (override with KUBECTL_KUBERC=true)\n")
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "kubectl kuberc enabled (KUBECTL_KUBERC=true)\n")
	}
}

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == trueEnvValue {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}
