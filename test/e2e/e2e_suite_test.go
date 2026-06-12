//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/yyewolf/cnmysql/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "example.com/cnmysql:v0.0.1"
	// instanceImage is the local M3 instance image consumed by the sample Cluster.
	instanceImage = "cnmysql-instance:8.4"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To enable kubectl kuberc (use custom kubectl configurations), set: KUBECTL_KUBERC=true
// By default, kuberc is disabled to ensure consistent test behavior across different environments.
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting cnmysql e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	// TODO(user): If you want to change the e2e test vendor from Kind,
	// ensure the image is built and available, then remove the following block.
	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	// Build and load every instance image the suite needs: 8.4 for the sample
	// cluster spec plus each version the archiving matrix exercises. Building all
	// four Percona flavors is deliberate — continuous archiving must be proven
	// broadly compatible, so the e2e runs the same catastrophic scenarios on each.
	for _, version := range neededInstanceVersions() {
		buildAndLoadInstanceImage(version)
	}

	configureKubectlKubeRC()
	setupCertManager()
	deployOperator()
})

var _ = AfterSuite(func() {
	undeployOperator()
	teardownCertManager()
})

// buildAndLoadInstanceImage builds this version's slim instance image and loads
// it into the Kind cluster, so a Cluster pinned to cnmysql-instance:<version>
// boots without pulling from a registry.
func buildAndLoadInstanceImage(version string) {
	By(fmt.Sprintf("building the instance image (%s)", version))
	cmd := exec.Command("make", "docker-build-instance", "INSTANCE_VERSION="+version)
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the instance image for %s", version)

	By(fmt.Sprintf("loading the instance image on Kind (%s)", version))
	err = utils.LoadImageToKindClusterWithName(instanceImageFor(version))
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the instance image for %s into Kind", version)
}

// neededInstanceVersions is the deduplicated set of instance versions the suite
// builds: 8.4 (the sample Cluster spec) plus every archiving-matrix version.
func neededInstanceVersions() []string {
	seen := map[string]bool{}
	var out []string
	for _, v := range append([]string{"8.4"}, archiveVersions()...) {
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
	if os.Getenv("KUBECTL_KUBERC") != "true" {
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
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
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
