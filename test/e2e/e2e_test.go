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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/CloudNative-MySQL/cloudnative-mysql/test/utils"
)

// namespace where the project is deployed in
const namespace = "cloudnative-mysql-system"

// serviceAccountName created for the project
const serviceAccountName = "cloudnative-mysql-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "cloudnative-mysql-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "cloudnative-mysql-metrics-binding"

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

	SetDefaultEventuallyTimeout(e2eTimeout(2 * time.Minute))
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
				"--clusterrole=cloudnative-mysql-metrics-reader",
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
			Eventually(verifyControllerPodReady, e2eTimeout(3*time.Minute), time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, e2eTimeout(3*time.Minute), time.Second).Should(Succeed())

			By("waiting for the webhook service endpoints to be ready")
			verifyWebhookEndpointsReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpointslices.discovery.k8s.io", "-n", namespace,
					"-l", "kubernetes.io/service-name=cloudnative-mysql-webhook-service",
					"-o", "jsonpath={range .items[*]}{range .endpoints[*]}{.addresses[*]}{end}{end}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Webhook endpoints should exist")
				g.Expect(output).ShouldNot(BeEmpty(), "Webhook endpoints not yet ready")
			}
			Eventually(verifyWebhookEndpointsReady, e2eTimeout(3*time.Minute), time.Second).Should(Succeed())

			By("verifying the validating webhook server is ready")
			verifyValidatingWebhookReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "validatingwebhookconfigurations.admissionregistration.k8s.io",
					"cloudnative-mysql-validating-webhook-configuration",
					"-o", "jsonpath={.webhooks[0].clientConfig.caBundle}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "ValidatingWebhookConfiguration should exist")
				g.Expect(output).ShouldNot(BeEmpty(), "Validating webhook CA bundle not yet injected")
			}
			Eventually(verifyValidatingWebhookReady, e2eTimeout(3*time.Minute), time.Second).Should(Succeed())

			By("waiting additional time for webhook server to stabilize")
			time.Sleep(5 * time.Second)

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
			Eventually(verifyCurlUp, e2eTimeout(5*time.Minute)).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, e2eTimeout(2*time.Minute)).Should(Succeed())
		})

		It("should provisioned cert-manager", func() {
			By("validating that cert-manager has the certificate Secret")
			verifyCertManager := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "secrets", "webhook-server-cert", "-n", namespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}
			Eventually(verifyCertManager).Should(Succeed())
		})

		It("should have CA injection for validating webhooks", func() {
			By("checking CA injection for validating webhooks")
			verifyCAInjection := func(g Gomega) {
				cmd := exec.Command("kubectl", "get",
					"validatingwebhookconfigurations.admissionregistration.k8s.io",
					"cloudnative-mysql-validating-webhook-configuration",
					"-o", "go-template={{ range .webhooks }}{{ .clientConfig.caBundle }}{{ end }}")
				vwhOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(len(vwhOutput)).To(BeNumerically(">", 10))
			}
			Eventually(verifyCAInjection).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		It("should bootstrap a replicated MySQL cluster", func() {
			By("applying the sample Cluster")
			cmd := exec.Command("kubectl", "apply", "-f", "config/samples/mysql_v1alpha1_cluster.yaml")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply sample Cluster")

			DeferCleanup(func() {
				deleteCluster("cluster-sample")
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
			}, e2eTimeout(12*time.Minute), 5*time.Second).Should(Succeed())

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
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

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
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

			By("verifying rw routes only to the promoted primary")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpointslice",
					"-l", "kubernetes.io/service-name=cluster-sample-rw",
					"-o", "jsonpath={.items[*].endpoints[*].targetRef.name}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("cluster-sample-2"))
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

			By("verifying writes on the promoted primary replicate to the old primary")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-2", "--",
					"mysql", "-uapp", "-p"+password, "app", "-e", "REPLACE INTO e2e VALUES (43);")
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Promoted primary is not writable yet")
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-1", "--",
					"mysql", "-uapp", "-p"+password, "-e", "SELECT id FROM app.e2e WHERE id = 43;")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("43"))
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

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
			}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())
			// The restarted replica carries no baked --source-host; it must follow
			// the current primary (cluster-sample-2) and receive its writes.
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-3", "--",
					"mysql", "-uapp", "-p"+password, "-e", "SELECT id FROM app.e2e WHERE id = 43;")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("43"))
			}, e2eTimeout(4*time.Minute), 5*time.Second).Should(Succeed())

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
			}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

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
			}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())

			By("verifying the recovered instance rejoins and catches up as a replica")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "exec", "cluster-sample-2", "--",
					"mysql", "-uapp", "-p"+password, "-e", "SELECT id FROM app.e2e WHERE id = 44;")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("44"))
			}, e2eTimeout(6*time.Minute), 5*time.Second).Should(Succeed())
		})

		It("should block a cluster that sets a denied my.cnf parameter", func() {
			By("applying a Cluster whose spec.mysql.parameters overrides datadir")
			manifest := fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: denied-param
spec:
  instances: 1
  imageName: %s
  storage:
    size: 1Gi
  mysql:
    parameters:
      datadir: /evil
  bootstrap:
    initdb:
      database: app
      owner: app
`, instanceImage)
			applyManifest("denied-param", manifest)
			DeferCleanup(func() {
				deleteCluster("denied-param")
			})

			By("verifying the cluster is Blocked with a reason naming the denied key")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "cluster", "denied-param",
					"-o", "jsonpath={.status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Blocked"))

				cmd = exec.Command("kubectl", "get", "cluster", "denied-param",
					"-o", "jsonpath={.status.phaseReason}")
				output, err = utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("datadir"))
			}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())

			By("verifying no instance Pod is ever created for the blocked cluster")
			cmd := exec.Command("kubectl", "get", "pod", "denied-param-1", "--ignore-not-found")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(BeEmpty(), "blocked cluster must not provision instances")
		})

		It("should report executable hash in status for operator upgrades", func() {
			By("applying a single-instance Cluster")
			manifest := fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: exec-hash
spec:
  instances: 1
  imageName: %s
  storage:
    size: 1Gi
  bootstrap:
    initdb:
      database: app
      owner: app
`, instanceImage)
			applyManifest("exec-hash", manifest)
			DeferCleanup(func() {
				deleteCluster("exec-hash")
			})

			By("waiting for the cluster to become ready")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "cluster", "exec-hash",
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"))
			}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

			var operatorHash, instanceHash string

			By("reading the operator executable hash from Cluster status")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "cluster", "exec-hash",
					"-o", "jsonpath={.status.operatorExecutableHash}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).NotTo(BeEmpty())
				operatorHash = output
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

			By("reading the instance's executable hash from Cluster status")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "cluster", "exec-hash",
					"-o", `go-template={{index .status.executableHashByInstance "exec-hash-1"}}`)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).NotTo(BeEmpty())
				instanceHash = output
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

			By("verifying the instance hash matches the operator hash")
			Expect(instanceHash).To(Equal(operatorHash),
				"instance manager hash must equal operator hash (same binary)")
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
	secretName := fmt.Sprintf("%s-token-request-%d", serviceAccountName, GinkgoParallelProcess())
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

var _ = Describe("Operator Upgrade", Ordered, func() {
	const v2Image = "example.com/cloudnative-mysql:v0.0.2"

	BeforeAll(func() {
		By("building v2 manager image with a different binary hash")
		insertE2EMarker()
		cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", v2Image))
		_, err := utils.Run(cmd)
		restoreE2EMarker()
		Expect(err).NotTo(HaveOccurred(), "Failed to build v2 manager image")

		By("loading v2 manager image on Kind")
		err = utils.LoadImageToKindClusterWithName(v2Image)
		Expect(err).NotTo(HaveOccurred(), "Failed to load v2 manager image into Kind")
	})

	AfterAll(func() {
		restoreE2EMarker()
	})

	It("should upgrade the operator with a serialized, primary-last rollout", func() {
		ns := createTestNamespace("op-upgrade")
		defer deleteTestNamespace(ns, defaultTestNamespace)

		By("applying a 3-instance Cluster")
		manifest := fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
kind: Cluster
metadata:
  name: upgrade
  namespace: %s
spec:
  instances: 3
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
`, ns, instanceImage, e2eMySQLParameters, e2eInstanceResources)
		applyManifest("upgrade", manifest)
		DeferCleanup(func() {
			deleteCluster("upgrade")
		})

		By("waiting for the cluster to become ready with the initial operator")
		expectClusterReady("upgrade", 3, 12*time.Minute)

		initialPrimary := clusterPrimary("upgrade")
		GinkgoWriter.Printf("Initial primary: %s\n", initialPrimary)

		By("reading the initial operator executable hash")
		cmd := exec.Command("kubectl", "get", "cluster", "upgrade",
			"-n", testNamespace, "-o", "jsonpath={.status.operatorExecutableHash}")
		initialHash, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		Expect(initialHash).NotTo(BeEmpty())
		GinkgoWriter.Printf("Initial operator hash: %s\n", initialHash)

		By("deploying the v2 operator")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", v2Image))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy v2 operator")

		By("waiting for the v2 operator pod to become ready")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "pods", "-l", "control-plane=controller-manager",
				"-n", namespace, "-o", "jsonpath={.items[0].status.phase}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).To(Equal("Running"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying the operator hash changed after upgrade")
		Eventually(func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "cluster", "upgrade",
				"-n", testNamespace, "-o", "jsonpath={.status.operatorExecutableHash}")
			output, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(output).NotTo(BeEmpty())
			g.Expect(output).NotTo(Equal(initialHash), "operator hash should change after v2 deploy")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("verifying replicas are upgraded one at a time during the rollout")
		phaseSeen := false
		minReadySeen := 3
		checkSerialized := func(g Gomega) {
			cmd := exec.Command("kubectl", "get", "cluster", "upgrade",
				"-n", testNamespace, "-o", "jsonpath={.status.phase}")
			phase, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			if phase == "Ready" {
				return
			}
			phaseSeen = true
			cmd = exec.Command("kubectl", "get", "cluster", "upgrade",
				"-n", testNamespace, "-o", "jsonpath={.status.phaseReason}")
			reason, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			GinkgoWriter.Printf("Upgrade phase: %s — %s\n", phase, reason)
			g.Expect(phase).To(Or(Equal("Upgrading"), Equal("Switchover"), Equal("WaitingForUser")))
			if phase == "Upgrading" {
				g.Expect(reason).To(Or(
					ContainSubstring(initialPrimary+"-2"),
					ContainSubstring(initialPrimary+"-3"),
				), "first upgraded instance should be a replica, not the primary %s", initialPrimary)
			}
			cmd = exec.Command("kubectl", "get", "cluster", "upgrade",
				"-n", testNamespace, "-o", "jsonpath={.status.readyInstances}")
			readyStr, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			if ready, err := strconv.Atoi(strings.TrimSpace(readyStr)); err == nil && ready < minReadySeen {
				minReadySeen = ready
			}
		}
		Eventually(checkSerialized, e2eTimeout(8*time.Minute), 5*time.Second).Should(Succeed())
		Expect(phaseSeen).To(BeTrue(), "Expected Upgrading or Switchover phase to appear during rollout")
		Expect(minReadySeen).To(BeNumerically(">=", 2),
			"at most one instance should be down during serialized rollout (min ready seen: %d)", minReadySeen)

		By("waiting for the upgrade to complete and the cluster to return to Ready")
		expectClusterReady("upgrade", 3, 15*time.Minute)

		By("verifying the primary changed after switchover-based upgrade")
		finalPrimary := clusterPrimary("upgrade")
		Expect(finalPrimary).NotTo(Equal(initialPrimary),
			"primary should change after switchover-based upgrade with 3 instances")

		By("verifying all instance hashes match the new operator hash after upgrade")
		var newHash string
		cmd = exec.Command("kubectl", "get", "cluster", "upgrade",
			"-n", testNamespace, "-o", "jsonpath={.status.operatorExecutableHash}")
		newHash, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
		GinkgoWriter.Printf("New operator hash: %s\n", newHash)

		for _, inst := range []string{"upgrade-1", "upgrade-2", "upgrade-3"} {
			cmd := exec.Command("kubectl", "get", "cluster", "upgrade",
				"-n", testNamespace, "-o", fmt.Sprintf(`go-template={{index .status.executableHashByInstance "%s"}}`, inst))
			instHash, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(instHash).To(Equal(newHash),
				"upgrade instance %s hash should match new operator hash", inst)
		}

		By("verifying the final primary is reachable and writable")
		password := appPassword("upgrade")
		writeSQL := "CREATE TABLE IF NOT EXISTS e2e_upgrade (id INT PRIMARY KEY); " +
			"REPLACE INTO e2e_upgrade VALUES (99);"
		cmd = exec.Command("kubectl", "exec", finalPrimary, "-n", testNamespace, "-c", "mysql", "--",
			"env", "MYSQL_PWD="+password, "mysql", "-uapp", "app", "-e", writeSQL)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Final primary must accept writes")
	})
})

// mainGoSnapshot holds the exact bytes of cmd/main.go captured before a marker
// was appended. restoreE2EMarker rewrites these bytes back, so the file is
// restored without touching unrelated (possibly uncommitted) changes — unlike a
// `git checkout`, which would discard any working-tree edits to cmd/main.go.
var mainGoSnapshot []byte

// appendMainGoMarker snapshots cmd/main.go and appends a unique top-level
// declaration so the next build produces a binary with a distinct hash. The
// marker line is guaranteed to be present in the rebuilt source, so the upgrade
// image can never collide with the baseline image's binary hash.
func appendMainGoMarker(marker string) {
	orig, err := os.ReadFile("cmd/main.go")
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to read cmd/main.go for marker insert: %v\n", err)
		return
	}
	mainGoSnapshot = orig
	updated := append(append([]byte{}, orig...), []byte("\n"+marker+"\n")...)
	if err := os.WriteFile("cmd/main.go", updated, 0o644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to append marker to cmd/main.go: %v\n", err)
	}
}

// insertE2EMarker adds a line to cmd/main.go so the next build produces a
// different binary hash.
func insertE2EMarker() {
	appendMainGoMarker(`var _ = "e2e-upgrade-v2"`)
}

// restoreE2EMarker restores cmd/main.go to the bytes captured by the last marker
// insert, leaving any other working-tree changes intact.
func restoreE2EMarker() {
	if mainGoSnapshot == nil {
		return
	}
	if err := os.WriteFile("cmd/main.go", mainGoSnapshot, 0o644); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to restore cmd/main.go: %v\n", err)
	}
	mainGoSnapshot = nil
}
