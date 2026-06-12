//go:build e2e
// +build e2e

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/yyewolf/cnmysql/test/utils"
)

// namespace where the project is deployed in
const namespace = "cnmysql-system"

// serviceAccountName created for the project
const serviceAccountName = "cnmysql-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "cnmysql-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "cnmysql-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// The CRDs and controller-manager are deployed once for the whole suite in
	// BeforeSuite. This Describe only cleans up the cluster-scoped objects it
	// creates for the metrics checks.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("removing the metrics ClusterRoleBinding")
		cmd = exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=cnmysql-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		It("should bootstrap a replicated MySQL cluster", func() {
			By("applying the sample Cluster")
			cmd := exec.Command("kubectl", "apply", "-f", "config/samples/mysql_v1alpha1_cluster.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply sample Cluster")

			DeferCleanup(func() {
				cmd := exec.Command("kubectl", "delete", "-f", "config/samples/mysql_v1alpha1_cluster.yaml",
					"--ignore-not-found")
				_, _ = utils.Run(cmd)
			})

			By("waiting for all instances to become ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "cluster", "cluster-sample",
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"))

				cmd = exec.Command("kubectl", "get", "cluster", "cluster-sample",
					"-o", "jsonpath={.status.currentPrimary}")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("cluster-sample-1"))

				cmd = exec.Command("kubectl", "get", "cluster", "cluster-sample",
					"-o", "jsonpath={.status.readyInstances}")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("3"))
			}, 12*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the primary's role label and that it accepts writes")
			password := applicationPassword()
			writeSQL := "CREATE TABLE IF NOT EXISTS e2e (id INT PRIMARY KEY); " +
				"REPLACE INTO e2e VALUES (42);"
			cmd = exec.Command("kubectl", "exec", "cluster-sample-1", "--",
				"mysql", "-uapp", "-p"+password, "app", "-e", writeSQL)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to write on the primary")

			By("verifying the write replicates and is readable through the ro Service")
			readSQL := "SELECT id FROM app.e2e WHERE id = 42;"
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-2", "--",
					"mysql", "-uapp", "-p"+password, "-e", readSQL)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("42"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the rw and ro Services route to the right roles")
			cmd = exec.Command("kubectl", "get", "endpointslice",
				"-l", "kubernetes.io/service-name=cluster-sample-rw",
				"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(Equal("cluster-sample-1"), "rw must route only to the primary")

			By("requesting a planned switchover to a replica")
			requestSwitchover("cluster-sample", "cluster-sample-2")

			By("waiting for the promoted replica to become the current primary")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "cluster", "cluster-sample",
					"-o", "jsonpath={.status.currentPrimary}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("cluster-sample-2"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying rw routes only to the promoted primary")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpointslice",
					"-l", "kubernetes.io/service-name=cluster-sample-rw",
					"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("cluster-sample-2"))
			}, 5*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying writes on the promoted primary replicate to the old primary")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-2", "--",
					"mysql", "-uapp", "-p"+password, "app", "-e", "REPLACE INTO e2e VALUES (43);")
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Promoted primary is not writable yet")
			}, 5*time.Minute, 5*time.Second).Should(Succeed())
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-1", "--",
					"mysql", "-uapp", "-p"+password, "-e", "SELECT id FROM app.e2e WHERE id = 43;")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("43"))
			}, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("restarting a replica Pod and verifying it rejoins the current primary (dynamic role/source)")
			cmd = exec.Command("kubectl", "delete", "pod", "cluster-sample-3", "--wait=false")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete a replica Pod")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "cluster", "cluster-sample",
					"-o", "jsonpath={.status.readyInstances}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("3"))
			}, 6*time.Minute, 5*time.Second).Should(Succeed())
			// The restarted replica carries no baked --source-host; it must follow
			// the current primary (cluster-sample-2) and receive its writes.
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-3", "--",
					"mysql", "-uapp", "-p"+password, "-e", "SELECT id FROM app.e2e WHERE id = 43;")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("43"))
			}, 4*time.Minute, 5*time.Second).Should(Succeed())

			By("deleting the current primary Pod to trigger automatic failover")
			cmd = exec.Command("kubectl", "delete", "pod", "cluster-sample-2", "--wait=false")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete the primary Pod")

			By("waiting for a surviving replica to be promoted as the new primary")
			var newPrimary string
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "cluster", "cluster-sample",
					"-o", "jsonpath={.status.currentPrimary}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).NotTo(Equal("cluster-sample-2"), "primary must move off the failed instance")
				g.Expect(output).NotTo(BeEmpty())
				newPrimary = output
			}, 6*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying rw routes only to the failed-over primary and it accepts writes")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpointslice",
					"-l", "kubernetes.io/service-name=cluster-sample-rw",
					"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal(newPrimary))

				cmd = exec.Command("kubectl", "exec", newPrimary, "--",
					"mysql", "-uapp", "-p"+password, "app", "-e", "REPLACE INTO e2e VALUES (44);")
				_, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "failed-over primary is not writable yet")
			}, 6*time.Minute, 5*time.Second).Should(Succeed())

			By("verifying the recovered instance rejoins and catches up as a replica")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-2", "--",
					"mysql", "-uapp", "-p"+password, "-e", "SELECT id FROM app.e2e WHERE id = 44;")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("44"))
			}, 6*time.Minute, 5*time.Second).Should(Succeed())
		})
	})
})

func requestSwitchover(clusterName, targetPrimary string) {
	payload := fmt.Sprintf(
		`{"status":{"targetPrimary":%q,"targetPrimaryTimestamp":%q,"phase":"Switchover","phaseReason":"Switching over to %s"}}`,
		targetPrimary,
		time.Now().UTC().Format(time.RFC3339),
		targetPrimary,
	)
	cmd := exec.Command("kubectl", "patch", "cluster", clusterName,
		"--subresource=status",
		"--type=merge",
		"-p", payload)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to request switchover")
}

func applicationPassword() string {
	cmd := exec.Command("kubectl", "get", "secret", "cluster-sample-app",
		"-o", "jsonpath={.data.password}")
	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to read application password")
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(output))
	Expect(err).NotTo(HaveOccurred(), "Failed to decode application password")
	return string(decoded)
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
