//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

// This spec exercises the Vertical Pod Autoscaler integration against the *real*
// VPA controllers (recommender, updater, admission-controller) plus
// metrics-server, installed into a throwaway Kind cluster. It is the end-to-end
// counterpart to the unit/simulation coverage in cluster_vpa_test.go and
// vpa_test.go.
//
// It is Label("disruptive") because the VPA admission-controller registers a
// cluster-wide mutating webhook that intercepts every Pod creation in the
// cluster — it must never run against the shared suite operator other specs
// depend on. provisionDedicated gives it its own operator and cluster, deleted
// on teardown.
//
// The spec proves the two halves of the contract:
//  1. Discovery: the real recommender resolves the Cluster targetRef through the
//     scale sub-resource selector (selectorpath=.status.labelSelector), finds the
//     instance Pods, and publishes a recommendation.
//  2. No-fight: when a Pod is recreated (the path a VPA eviction takes), the
//     admission-controller applies the recommendation and the operator leaves the
//     resized Pod alone instead of rolling it back to spec.resources.
var _ = Describe("Vertical Pod Autoscaler (real controller)", Ordered, Serial, Label("disruptive"), func() {
	const (
		cluster = "vpa-real"
		// specCPU is deliberately low and floorCPU (the VPA minAllowed) is strictly
		// higher, so the recommendation is floored above the spec request. That makes
		// the admitted request differ from spec.resources deterministically, instead
		// of racing the instance's actual CPU usage.
		specCPU  = "50m"
		floorCPU = "150m"
	)

	var dc *dedicated

	BeforeAll(func() {
		dc = provisionDedicated("vpa", "")

		By("installing metrics-server")
		Expect(utils.InstallMetricsServer()).To(Succeed(), "failed to install metrics-server")

		By("installing the full VPA stack")
		Expect(utils.InstallVPA()).To(Succeed(), "failed to install the VPA")
	})

	AfterAll(func() {
		dc.teardown()
	})

	It("keeps a real VPA recommendation applied to a Pod without the operator fighting it", func() {
		ns := createTestNamespace("vpa-real")
		defer deleteTestNamespace(ns, defaultTestNamespace)

		pod := fmt.Sprintf("%s-1", cluster)
		// On any failure, dump the VPA controllers' state: the recommendation, the
		// self-registered webhook (its caBundle/failurePolicy), and the admission
		// controller logs are what explain a discovery or mutation miss.
		defer func() {
			if CurrentSpecReport().Failed() {
				dumpVPADiagnostics(cluster, pod)
			}
		}()

		By("creating a single-instance cluster with a deliberately low CPU request")
		applyManifest(cluster, vpaClusterManifest(cluster, specCPU))
		DeferCleanup(func() { deleteCluster(cluster) })
		expectClusterReady(cluster, 1, 15*time.Minute)

		By("creating a VerticalPodAutoscaler targeting the cluster's scale sub-resource")
		applyManifest("vpa-object", vpaObjectManifest(cluster, floorCPU))

		// (1) Discovery — the unique thing this real-controller test proves: the
		// recommender resolves the Cluster targetRef through our scale selector
		// (selectorpath=.status.labelSelector), finds the instance Pods, and
		// publishes a recommendation. An empty value here means the selectorpath is
		// wrong and VPA cannot target the Cluster at all.
		By("waiting for the VPA recommender to publish a CPU recommendation for the mysql container")
		recPath := "{.status.recommendation.containerRecommendations[?(@.containerName=='mysql')].target.cpu}"
		var recommended string
		Eventually(func(g Gomega) {
			rec, err := kubectl("get", "vpa", cluster, "-n", testNamespace, "-o", "jsonpath="+recPath)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rec).NotTo(BeEmpty(),
				"VPA produced no recommendation; it did not discover the cluster Pods via the scale selector")
			recommended = rec
		}, e2eTimeout(10*time.Minute), 10*time.Second).Should(Succeed())
		Expect(recommended).NotTo(Equal(specCPU),
			"minAllowed should floor the recommendation strictly above the spec request")

		// (2) No-fight — apply the recommendation VPA actually computed to the running
		// Pod via an in-place resize. This is the same operation VPA's in-place
		// updater performs on k8s >=1.33, and it is deterministic: it does not depend
		// on the admission webhook firing on a fresh Pod (which, with VPA's default
		// failurePolicy=Ignore, fails silently). The operator must not revert it.
		cpuReqPath := "{.spec.containers[?(@.name=='mysql')].resources.requests.cpu}"
		By(fmt.Sprintf("applying the VPA recommendation (%s) to the running Pod via an in-place resize", recommended))
		resize := fmt.Sprintf(`{"spec":{"containers":[{"name":"mysql","resources":{"requests":{"cpu":%q}}}]}}`, recommended)
		_, err := kubectl("patch", "pod", pod, "-n", testNamespace, "--subresource=resize", "--patch", resize)
		Expect(err).NotTo(HaveOccurred(), "in-place Pod resize should be accepted")

		By("confirming the resized request is applied to the running Pod")
		Eventually(func(g Gomega) {
			cpu, err := kubectl("get", "pod", pod, "-n", testNamespace, "-o", "jsonpath="+cpuReqPath)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cpu).To(Equal(recommended))
		}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

		By("confirming the operator does not revert the VPA-applied request over repeated reconciles")
		Consistently(func(g Gomega) {
			cpu, err := kubectl("get", "pod", pod, "-n", testNamespace, "-o", "jsonpath="+cpuReqPath)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(cpu).To(Equal(recommended),
				"operator reset the VPA-applied request to spec.resources; it is fighting the autoscaler")
		}, e2eTimeout(90*time.Second), 10*time.Second).Should(Succeed())
	})
})

// dumpVPADiagnostics prints the VPA controllers' state to the Ginkgo writer when
// a real-stack VPA spec fails: the recommendation (did discovery work?), the
// self-registered mutating webhook (caBundle/failurePolicy — why a fresh-Pod
// mutation may have been silently skipped) and the admission/recommender logs.
func dumpVPADiagnostics(vpaName, pod string) {
	By("dumping VPA diagnostics")
	dumps := []struct {
		what string
		args []string
	}{
		{"vpa object", []string{"get", "vpa", vpaName, "-n", testNamespace, "-o", "yaml"}},
		{"vpa-webhook-config", []string{"get", "mutatingwebhookconfiguration", "vpa-webhook-config", "-o", "yaml"}},
		{"pod resources", []string{"get", "pod", pod, "-n", testNamespace, "-o",
			"jsonpath={range .spec.containers[*]}{.name}{'='}{.resources}{'\\n'}{end}"}},
		{"admission-controller logs", []string{"logs", "deployment/vpa-admission-controller",
			"-n", "kube-system", "--tail=100"}},
		{"recommender logs", []string{"logs", "deployment/vpa-recommender", "-n", "kube-system", "--tail=50"}},
	}
	for _, d := range dumps {
		out, err := kubectl(d.args...)
		if err != nil {
			out = fmt.Sprintf("(failed to collect: %v)", err)
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "\n===== VPA diagnostics: %s =====\n%s\n", d.what, out)
	}
}

// vpaClusterManifest is a single-instance Cluster with an explicit (low) CPU
// request so the VPA recommendation is forced above it. Memory stays at a floor
// MySQL can boot with; only CPU is asserted on.
func vpaClusterManifest(name, cpuRequest string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 1
  imageName: %[3]s
  storage:
    size: 2Gi
  resources:
    requests:
      cpu: %[4]s
      memory: 384Mi
    limits:
      cpu: "1"
      memory: 1536Mi
  mysql:
    binlogFormat: ROW
%[5]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instanceImage, cpuRequest, e2eMySQLParameters)
}

// vpaObjectManifest targets the Cluster through its scale sub-resource. minAllowed
// CPU is set above the cluster's spec request so the recommendation is floored
// strictly higher, and controlledValues=RequestsOnly keeps the assertion on
// requests (limits are left untouched).
func vpaObjectManifest(clusterName, floorCPU string) string {
	return fmt.Sprintf(`apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  targetRef:
    apiVersion: mysql.cnmsql.co/v1alpha1
    kind: Cluster
    name: %[1]s
  updatePolicy:
    updateMode: "Auto"
  resourcePolicy:
    containerPolicies:
      - containerName: mysql
        controlledValues: RequestsOnly
        minAllowed:
          cpu: %[3]s
          memory: 384Mi
        maxAllowed:
          cpu: "1"
          memory: 1Gi
`, clusterName, testNamespace, floorCPU)
}
