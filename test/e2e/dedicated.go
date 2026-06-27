//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

// This file is the ephemeral-cluster harness for disruptive specs. A disruptive
// spec mutates the operator (rolls/redeploys it, scales it down, deletes its
// cluster-wide webhook) or the cluster topology, so it must never run against the
// shared suite operator the rest of the suite depends on. Instead each disruptive
// spec provisions its OWN throwaway Kind cluster with its own operator, exercises
// it in isolation, and deletes the whole cluster on teardown.
//
// This generalizes the pattern the node-failure spec proved (one dedicated
// multi-node cluster) into a single API every Label("disruptive") spec uses.
//
// See design/025-e2e-testing-overhaul.md.

// dedicated is a private, ephemeral Kind cluster with the operator deployed.
type dedicated struct {
	name        string
	prevContext string
}

// provisionDedicated creates an ephemeral Kind cluster for a disruptive spec and
// deploys the operator into it. kindConfigPath selects the topology ("" => Kind's
// default single node; node-failure passes its multi-node config). Kind switches
// the active kube-context to the new cluster, so every kubectl/make call that
// follows targets it until teardown restores the previous context.
//
// The caller must arrange teardown (typically via DeferCleanup). It Skips the
// spec when the kind binary is unavailable, so a developer without kind gets a
// clean skip rather than a failure.
func provisionDedicated(slug, kindConfigPath string) *dedicated {
	GinkgoHelper()
	if !kindAvailable() {
		Skip("kind binary not available; this disruptive spec provisions its own Kind cluster")
	}

	d := &dedicated{name: dedicatedClusterName(slug), prevContext: currentKubeContext()}

	By(fmt.Sprintf("creating dedicated Kind cluster %s", d.name))
	createArgs := []string{"create", "cluster", "--name", d.name}
	if kindConfigPath != "" {
		createArgs = append(createArgs, "--config", kindConfigPath)
	}
	if v := os.Getenv("K8S_VERSION"); v != "" {
		createArgs = append(createArgs, "--image", "kindest/node:"+v)
	}
	_, err := utils.Run(exec.Command(kindBinary(), createArgs...))
	Expect(err).NotTo(HaveOccurred(), "failed to create dedicated Kind cluster %s", d.name)

	By("loading the operator and instance images into the dedicated cluster")
	d.loadImage(managerImage)
	d.loadImage(instanceImage)

	By("installing cert-manager on the dedicated cluster")
	Expect(utils.InstallCertManager()).To(Succeed(), "failed to install cert-manager on the dedicated cluster")

	By("deploying the operator on the dedicated cluster")
	deployOperator()

	By("waiting for the operator and admission webhook to become ready")
	_, err = kubectl("wait", "deployment", "-l", "control-plane=controller-manager",
		"-n", namespace, "--for=condition=Available", "--timeout="+e2eTimeout(5*time.Minute).String())
	Expect(err).NotTo(HaveOccurred(), "controller-manager did not become available on the dedicated cluster")
	waitForWebhookReady("default")

	return d
}

// loadImage loads a locally-built image into the dedicated cluster. Disruptive
// specs use it to load the alternate-hash manager images they build (v2/v3) into
// their own cluster instead of the shared one.
func (d *dedicated) loadImage(image string) {
	GinkgoHelper()
	_, err := utils.Run(exec.Command(kindBinary(), "load", "docker-image", image, "--name", d.name))
	Expect(err).NotTo(HaveOccurred(), "failed to load image %s into %s", image, d.name)
}

// teardown restores the previously active kube-context and deletes the dedicated
// cluster. It is best-effort: a failure here must not mask the spec result. Safe
// to call on a nil receiver (e.g. when provisioning was skipped).
func (d *dedicated) teardown() {
	if d == nil {
		return
	}
	if d.prevContext != "" {
		_, _ = kubectl("config", "use-context", d.prevContext)
	}
	By(fmt.Sprintf("deleting dedicated Kind cluster %s", d.name))
	_, _ = utils.Run(exec.Command(kindBinary(), "delete", "cluster", "--name", d.name))
}

// dedicatedClusterName derives a Kind cluster name distinct from the shared suite
// cluster (and from every other disruptive spec), so they never collide.
func dedicatedClusterName(slug string) string {
	base := os.Getenv("KIND_CLUSTER")
	if base == "" {
		base = "cnmsql-test-e2e"
	}
	return base + "-" + slug
}

// kindBinary returns the kind executable, honoring the KIND override the Makefile
// passes through to the suite.
func kindBinary() string {
	if v := os.Getenv("KIND"); v != "" {
		return v
	}
	return "kind"
}

// kindAvailable reports whether the kind binary can be found, so a disruptive
// spec can skip cleanly rather than fail when it cannot provision its cluster.
func kindAvailable() bool {
	_, err := exec.LookPath(kindBinary())
	return err == nil
}

// currentKubeContext returns the active kube-context, or "" if none is set.
func currentKubeContext() string {
	out, err := kubectl("config", "current-context")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
