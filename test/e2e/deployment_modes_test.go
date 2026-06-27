//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

// These specs prove both supported operator topologies (design/021) bring a
// real cluster to Ready. A 2-instance cluster is enough: electing and recording
// the primary drives a status.currentPrimary write from an instance identity,
// which is exactly what the validating webhook (design/020) gates, so reaching
// Ready exercises the webhook in each mode.

const (
	// Two cohabiting namespaced operators, one per namespace.
	nsModeA    = "e2e-nsmode-a"
	nsModeB    = "e2e-nsmode-b"
	nsModeAPfx = "nsa-"
	nsModeBPfx = "nsb-"
	// clusterWideWebhook and clusterWideDeployment identify the shared suite
	// operator, stood down for the duration of the namespaced specs.
	clusterWideWebhook    = "cnmsql-validating-webhook-configuration"
	clusterWideDeployment = "cnmsql-controller-manager"
)

// twoInstanceClusterManifest renders a minimal 2-instance Cluster in ns.
func twoInstanceClusterManifest(name, ns string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 2
  imageName: %s
  storage:
    size: 1Gi
  mysql:
%s
  bootstrap:
    initdb:
      database: app
      owner: app
%s
`, name, ns, instanceImage, e2eMySQLParameters, e2eInstanceResources)
}

var _ = Describe("Cluster-wide deployment mode", Ordered, Label("feature"), func() {
	It("brings a 2-instance cluster to Ready using the shared cluster-wide operator", func() {
		ns := createTestNamespace("clusterwide")
		defer deleteTestNamespace(ns, defaultTestNamespace)

		By("applying a 2-instance Cluster in a user namespace")
		applyManifest("cw-cluster", twoInstanceClusterManifest("cw", ns))

		By("waiting for both instances to become Ready (webhook exercised on primary election)")
		expectClusterReady("cw", 2, 20*time.Minute)
	})
})

// Namespaced mode runs Serial against its OWN ephemeral Kind cluster: it stands
// the cluster-wide operator down (scales it to zero and removes its cluster-wide
// webhook) and runs two namespaced operators side by side. Using a dedicated
// cluster means it never disturbs the shared suite operator; teardown deletes the
// whole cluster. See design/025-e2e-testing-overhaul.md.
var _ = Describe("Namespaced deployment mode", Ordered, Serial, Label("disruptive"), func() {
	var dc *dedicated

	BeforeAll(func() {
		dc = provisionDedicated("nsmode", "")

		By("standing down the dedicated cluster's cluster-wide operator")
		// Scale to zero so it cannot reconcile the namespaced Clusters, and remove
		// its cluster-wide webhook so its Fail policy does not block status writes
		// in the namespaced operators' namespaces.
		_, _ = kubectl("scale", "deployment", clusterWideDeployment, "-n", namespace, "--replicas=0")
		_, _ = kubectl("delete", "validatingwebhookconfiguration", clusterWideWebhook, "--ignore-not-found")

		By("deploying two namespaced operators, one per namespace")
		snapshotNamespacedKustomization()
		deployNamespacedOperator(nsModeA, nsModeAPfx)
		deployNamespacedOperator(nsModeB, nsModeBPfx)

		By("waiting for both operators to be Running")
		waitOperatorRunning(nsModeA)
		waitOperatorRunning(nsModeB)

		By("waiting for each operator's webhook CA bundle to be injected")
		waitWebhookCAInjected(webhookConfigName(nsModeAPfx))
		waitWebhookCAInjected(webhookConfigName(nsModeBPfx))

		By("waiting for each namespaced webhook to accept requests")
		// The CA bundle may be injected before the webhook Pod's TLS
		// listener is ready. Poll the admission endpoint directly so
		// applyManifest does not fail with "connection refused".
		//
		// The probe manifest must live in the operator's own namespace
		// because a namespaced webhook's namespaceSelector gates only
		// its own namespace.
		waitForWebhookReady(nsModeA)
		waitForWebhookReady(nsModeB)
	})

	AfterAll(func() {
		// The whole dedicated cluster is destroyed below, so there is no finalizer
		// dance or operator to restore — only the local working-tree file the
		// `make deploy-namespaced` kustomize-edit mutated needs reverting.
		By("restoring config/namespaced/kustomization.yaml")
		restoreNamespacedKustomization()
		dc.teardown()
	})

	It("brings up a 2-instance cluster in each namespace, each reconciled by its own operator", func() {
		By("applying a 2-instance Cluster in each operator's namespace")
		applyManifest("ns-a-cluster", twoInstanceClusterManifest("ca", nsModeA))
		applyManifest("ns-b-cluster", twoInstanceClusterManifest("cb", nsModeB))

		By("waiting for the Cluster in namespace A to become Ready")
		prev := useNamespace(nsModeA)
		defer func() { testNamespace = prev }()
		expectClusterReady("ca", 2, 20*time.Minute)

		By("waiting for the Cluster in namespace B to become Ready")
		testNamespace = nsModeB
		expectClusterReady("cb", 2, 20*time.Minute)
	})

	It("scopes each operator's webhook to its own namespace", func() {
		// Cross-namespace isolation is the core promise of namespaced mode: one
		// operator's Fail-policy webhook must not gate another namespace. With the
		// per-namespace namespaceSelector, taking operator A down (so its webhook
		// endpoint is unreachable) must block status writes only in A's namespace,
		// not in B's.
		By("scaling operator A down so its webhook endpoint is unreachable")
		_, err := kubectl("scale", "deployment", operatorDeploymentName(nsModeAPfx),
			"-n", nsModeA, "--replicas=0")
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() (string, error) {
			return kubectl("get", "pods", "-l", "control-plane=controller-manager",
				"-n", nsModeA, "-o", "jsonpath={.items[*].metadata.name}")
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(BeEmpty(), "operator A pod did not terminate")

		By("a status write in namespace A is rejected (A's Fail-policy webhook is down)")
		Eventually(func() error {
			return probeClusterStatusWebhook("ca", nsModeA)
		}, e2eTimeout(90*time.Second), 5*time.Second).Should(HaveOccurred(),
			"A's webhook should gate its own namespace while A is down")

		By("a status write in namespace B still succeeds (B's webhook is unaffected)")
		Consistently(func() error {
			return probeClusterStatusWebhook("cb", nsModeB)
		}, e2eTimeout(20*time.Second), 5*time.Second).Should(Succeed(),
			"operator B's namespace must be unaffected by operator A being down")

		By("restoring operator A")
		_, err = kubectl("scale", "deployment", operatorDeploymentName(nsModeAPfx),
			"-n", nsModeA, "--replicas=1")
		Expect(err).NotTo(HaveOccurred())
		waitOperatorRunning(nsModeA)
	})
})

// deployNamespacedOperator installs a namespaced operator into ns with the given
// resource name prefix via the config/namespaced overlay.
func deployNamespacedOperator(ns, prefix string) {
	cmd := exec.Command("make", "deploy-namespaced",
		fmt.Sprintf("IMG=%s", managerImage),
		"NAMESPACE="+ns,
		"NAME_PREFIX="+prefix)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to deploy the namespaced operator in %s", ns)
}

func waitOperatorRunning(ns string) {
	GinkgoHelper()
	Eventually(func() (string, error) {
		return kubectl("get", "pods", "-l", "control-plane=controller-manager",
			"-n", ns, "-o", "jsonpath={.items[*].status.phase}")
	}, e2eTimeout(5*time.Minute), 5*time.Second).Should(ContainSubstring("Running"),
		"operator in %s did not become ready", ns)
}

func waitWebhookCAInjected(config string) {
	GinkgoHelper()
	Eventually(func() (string, error) {
		return kubectl("get", "validatingwebhookconfigurations.admissionregistration.k8s.io",
			config, "-o", "jsonpath={.webhooks[0].clientConfig.caBundle}")
	}, e2eTimeout(3*time.Minute), 5*time.Second).ShouldNot(BeEmpty(), "webhook %s CA not injected", config)
}

func webhookConfigName(prefix string) string      { return prefix + "validating-webhook-configuration" }
func operatorDeploymentName(prefix string) string { return prefix + "controller-manager" }

// probeClusterStatusWebhook issues a status update to a Cluster as the test's
// admin identity. A non-instance caller is always allowed by the handler, so the
// result depends only on whether the namespace's webhook endpoint is reachable:
// it returns an error when the matching webhook (Fail policy) cannot be called.
// The value is unique per call so the API server always sees a real update.
func probeClusterStatusWebhook(name, ns string) error {
	payload := fmt.Sprintf(`{"status":{"phaseReason":"e2e-webhook-probe-%d"}}`, time.Now().UnixNano())
	_, err := kubectl("patch", "cluster", name, "-n", ns,
		"--subresource=status", "--type=merge", "-p", payload)
	return err
}

// namespacedKustomizationSnapshot holds config/namespaced/kustomization.yaml as
// it was before `make deploy-namespaced` mutated its namespace/namePrefix via
// `kustomize edit`, so AfterAll can leave the working tree clean.
var namespacedKustomizationSnapshot []byte

const namespacedKustomizationPath = "config/namespaced/kustomization.yaml"

func snapshotNamespacedKustomization() {
	data, err := os.ReadFile(namespacedKustomizationPath)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to snapshot %s: %v\n", namespacedKustomizationPath, err)
		return
	}
	namespacedKustomizationSnapshot = data
}

func restoreNamespacedKustomization() {
	if namespacedKustomizationSnapshot == nil {
		return
	}
	if err := os.WriteFile(namespacedKustomizationPath, namespacedKustomizationSnapshot, 0o644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to restore %s: %v\n", namespacedKustomizationPath, err)
	}
	namespacedKustomizationSnapshot = nil
}
