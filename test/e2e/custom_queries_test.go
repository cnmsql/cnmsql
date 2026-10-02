//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs exercise custom monitoring queries end to end: the operator
// grants the instance Role get on the referenced ConfigMaps and Secrets, the
// instance manager reads them through the API, runs them as the unprivileged
// cnmsql_metrics account and publishes the results on :9187, and edits apply
// without restarting a Pod.

const (
	customQueriesConfigMap = "custom-queries"
	customQueriesSecret    = "custom-queries-secret"
)

var _ = Describe("Custom monitoring queries", Ordered, Label("feature"), func() {
	const (
		cluster   = "custom-queries"
		instances = 2
	)
	pods := []string{cluster + "-1", cluster + "-2"}

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("custom-queries")
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})

		By("creating the query ConfigMap and Secret before the cluster")
		applyCustomQueries(customQueryDoc(42, false))
		applyManifest(customQueriesSecret, customQueriesSecretManifest())

		By("creating a cluster referencing them")
		applyManifest(cluster, customQueriesClusterManifest(cluster, instances, mysqlFlavor))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, instances, 20*time.Minute)
	})

	It("publishes ConfigMap and Secret queries on every instance", func() {
		for _, pod := range pods {
			By("scraping " + pod)
			Eventually(func(g Gomega) {
				body := scrapeInstanceMetricsProxy(pod)
				g.Expect(sampleValue(body, `mysql_e2e_static_answer{source="configmap"}`)).To(Equal(42.0))
				g.Expect(sampleValue(body, "mysql_e2e_secret_answer")).To(Equal(7.0))
				g.Expect(sampleValue(body, "mysql_exporter_last_scrape_error")).To(Equal(0.0))
				g.Expect(body).To(ContainSubstring("mysql_global_status_uptime"),
					"the built-in queries still run alongside the custom ones")
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		}
	})

	It("lets the instances read only the referenced ConfigMap and Secret", func() {
		sa := fmt.Sprintf("system:serviceaccount:%s:%s-instance", ns, pods[0])
		canI := func(args ...string) string {
			out, _ := kubectl(append([]string{"auth", "can-i", "-n", ns, "--as", sa}, args...)...)
			return strings.TrimSpace(out)
		}
		Expect(canI("get", "configmap/"+customQueriesConfigMap)).To(Equal("yes"))
		Expect(canI("get", "secret/"+customQueriesSecret)).To(Equal("yes"))
		Expect(canI("get", "configmap/unrelated")).To(Equal("no"))
		Expect(canI("list", "configmaps")).To(Equal("no"))
	})

	It("applies an edited ConfigMap without restarting the Pods", func() {
		before := podIdentities(pods)

		By("changing the published value")
		applyCustomQueries(customQueryDoc(43, false))
		expectStaticAnswer(pods, 43)

		Expect(podIdentities(pods)).To(Equal(before), "a query edit must not restart or replace a Pod")
	})

	It("runs the queries as the unprivileged metrics account", func() {
		primary := clusterPrimary(cluster)

		By("creating an application table the metrics account cannot read yet")
		_, err := mysqlExec(primary, "app", appPassword(cluster), "app",
			"CREATE TABLE items (id INT PRIMARY KEY); INSERT INTO items VALUES (1), (2), (3)")
		Expect(err).NotTo(HaveOccurred())

		By("adding a query on that table and one that tries to create a user")
		applyCustomQueries(customQueryDoc(44, true))
		expectStaticAnswer(pods, 44)
		for _, pod := range pods {
			body := scrapeInstanceMetricsProxy(pod)
			Expect(body).NotTo(ContainSubstring("mysql_e2e_items_total"),
				"cnmsql_metrics must not read app tables without a grant")
			Expect(sampleValue(body, "mysql_exporter_last_scrape_error")).To(Equal(1.0))
		}
		out, err := mysqlExec(primary, "root", rootPassword(cluster), "",
			"SELECT COUNT(*) FROM mysql.user WHERE user = 'e2e_intruder'")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("0"), "a custom query must not be able to create users")

		By("declaring read access in spec.monitoring.privileges")
		patchMonitoring(cluster, `{"privileges": [{"privileges": ["SELECT"], "on": "app.*"}]}`)
		expectMetricsAccountReady(cluster)
		for _, pod := range pods {
			Eventually(func(g Gomega) {
				g.Expect(sampleValue(scrapeInstanceMetricsProxy(pod), "mysql_e2e_items_total")).To(Equal(3.0))
			}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed(),
				"the grant must replicate and let %s read app.items", pod)
		}

		By("removing the failing query clears the scrape error")
		applyCustomQueries(customQueryDoc(45, false))
		expectStaticAnswer(pods, 45)
		for _, pod := range pods {
			Expect(sampleValue(scrapeInstanceMetricsProxy(pod), "mysql_exporter_last_scrape_error")).To(Equal(0.0))
		}
	})

	It("keeps the declared grants in place", func() {
		primary := clusterPrimary(cluster)
		applyCustomQueries(customQueryDoc(46, false) + itemsQuery)
		expectStaticAnswer(pods, 46)

		By("revoking the declared grant by hand")
		_, err := mysqlExec(primary, "root", rootPassword(cluster), "",
			"REVOKE SELECT ON app.* FROM 'cnmsql_metrics'@'localhost'")
		Expect(err).NotTo(HaveOccurred())
		for _, pod := range pods {
			Eventually(func() string {
				return metricsAccountGrants(pod, rootPassword(cluster))
			}, e2eTimeout(2*time.Minute), 5*time.Second).Should(ContainSubstring("`app`"),
				"the revoked grant must come back on %s", pod)
		}
		expectItemsTotal(pods, 3)

		By("granting an undeclared privilege by hand")
		_, err = mysqlExec(primary, "root", rootPassword(cluster), "",
			"GRANT INSERT ON app.* TO 'cnmsql_metrics'@'localhost'")
		Expect(err).NotTo(HaveOccurred())
		for _, pod := range pods {
			Eventually(func() string {
				return metricsAccountGrants(pod, rootPassword(cluster))
			}, e2eTimeout(2*time.Minute), 5*time.Second).ShouldNot(ContainSubstring("INSERT"),
				"the extra grant must be revoked on %s", pod)
		}
	})

	It("recreates a dropped metrics account", func() {
		primary := clusterPrimary(cluster)
		_, err := mysqlExec(primary, "root", rootPassword(cluster), "",
			"DROP USER 'cnmsql_metrics'@'localhost'")
		Expect(err).NotTo(HaveOccurred())
		for _, pod := range pods {
			Eventually(func() string {
				return metricsAccountGrants(pod, rootPassword(cluster))
			}, e2eTimeout(2*time.Minute), 5*time.Second).Should(
				And(ContainSubstring("PROCESS"), ContainSubstring("`app`")),
				"the account and its grants must come back on %s", pod)
		}
		expectItemsTotal(pods, 3)
	})

	It("revokes a removed entry and keeps the built-in grants", func() {
		patchMonitoring(cluster, `{"privileges": null}`)
		for _, pod := range pods {
			Eventually(func() string {
				return metricsAccountGrants(pod, rootPassword(cluster))
			}, e2eTimeout(2*time.Minute), 5*time.Second).ShouldNot(ContainSubstring("`app`"))
			grants := metricsAccountGrants(pod, rootPassword(cluster))
			Expect(grants).To(ContainSubstring("PROCESS"))
			Expect(grants).To(ContainSubstring("`performance_schema`"))
			Eventually(func(g Gomega) {
				body := scrapeInstanceMetricsProxy(pod)
				g.Expect(body).NotTo(ContainSubstring("mysql_e2e_items_total"))
				g.Expect(body).To(ContainSubstring("mysql_global_status_uptime"))
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		}
		applyCustomQueries(customQueryDoc(45, false))
		expectStaticAnswer(pods, 45)
	})

	It("rejects write privileges and unsafe targets", func() {
		for patch, field := range map[string]string{
			`{"privileges": [{"privileges": ["INSERT"], "on": "app.*"}]}`:   "spec.monitoring.privileges[0].privileges",
			`{"privileges": [{"privileges": ["SELECT"], "on": "*.*"}]}`:     "spec.monitoring.privileges[0].on",
			`{"privileges": [{"privileges": ["SELECT"], "on": "mysql.*"}]}`: "spec.monitoring.privileges[0].on",
		} {
			out, err := kubectl("patch", "cluster", cluster, "-n", testNamespace, "--type", "merge",
				"-p", `{"spec": {"monitoring": `+patch+`}}`)
			Expect(err).To(HaveOccurred(), "the webhook must reject %s", patch)
			Expect(out + err.Error()).To(ContainSubstring(field))
		}
	})

	It("honors disableDefaultQueries and metricsQueriesTTL", func() {
		pod := pods[0]
		patchMonitoring(cluster, `{"disableDefaultQueries": true, "metricsQueriesTTL": "10m"}`)

		By("waiting for the built-in queries to stop")
		Eventually(func(g Gomega) {
			body := scrapeInstanceMetricsProxy(pod)
			g.Expect(body).NotTo(ContainSubstring("mysql_global_status_"))
			g.Expect(sampleValue(body, `mysql_e2e_static_answer{source="configmap"}`)).To(Equal(45.0))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("checking a scrape after the minute resync still replays the cached run")
		first := sampleValue(scrapeInstanceMetricsProxy(pod), "mysql_e2e_clock_now")
		Expect(first).To(BeNumerically(">", 0))
		time.Sleep(75 * time.Second)
		Expect(sampleValue(scrapeInstanceMetricsProxy(pod), "mysql_e2e_clock_now")).To(Equal(first),
			"inside metricsQueriesTTL the queries must not run again")
	})

	It("stops the custom queries when the references are removed", func() {
		patchMonitoring(cluster, `{"customQueriesConfigMap": null, "customQueriesSecret": null,
			"disableDefaultQueries": null, "metricsQueriesTTL": null}`)

		for _, pod := range pods {
			Eventually(func(g Gomega) {
				body := scrapeInstanceMetricsProxy(pod)
				g.Expect(body).NotTo(ContainSubstring("mysql_e2e_"))
				g.Expect(body).To(ContainSubstring("mysql_global_status_uptime"))
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		}

		By("checking the instance Role no longer grants the ConfigMap")
		Eventually(func() string {
			out, _ := kubectl("get", "role", cluster+"-instance", "-n", ns, "-o", "jsonpath={.rules[*].resources}")
			return out
		}, e2eTimeout(2*time.Minute), 5*time.Second).ShouldNot(ContainSubstring("configmaps"))
	})
})

var _ = Describe("MariaDB custom monitoring queries", Ordered, Label("flavor", "mariadb"), func() {
	const cluster = "mariadb-custom-queries"
	pod := cluster + "-1"

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("mariadb-custom-queries")
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		applyCustomQueries(customQueryDoc(42, false))
		applyManifest(customQueriesSecret, customQueriesSecretManifest())
		applyManifest(cluster, customQueriesClusterManifest(cluster, 1, mariadbFlavor))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 1, 20*time.Minute)
	})

	It("publishes the queries through the passwordless metrics account", func() {
		Eventually(func(g Gomega) {
			body := scrapeInstanceMetricsProxy(pod)
			g.Expect(sampleValue(body, `mysql_e2e_static_answer{source="configmap"}`)).To(Equal(42.0))
			g.Expect(sampleValue(body, "mysql_e2e_secret_answer")).To(Equal(7.0))
			g.Expect(sampleValue(body, "mysql_exporter_last_scrape_error")).To(Equal(0.0))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("reads application tables through a declared grant", func() {
		_, err := mariadbExec(pod, "app", appPassword(cluster), "app",
			"CREATE TABLE items (id INT PRIMARY KEY); INSERT INTO items VALUES (1), (2), (3)")
		Expect(err).NotTo(HaveOccurred())
		patchMonitoring(cluster, `{"privileges": [{"privileges": ["SELECT"], "on": "app.*"}]}`)
		expectMetricsAccountReady(cluster)

		applyCustomQueries(customQueryDoc(43, true))
		Eventually(func(g Gomega) {
			body := scrapeInstanceMetricsProxy(pod)
			g.Expect(sampleValue(body, "mysql_e2e_items_total")).To(Equal(3.0))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		out, err := mariadbExec(pod, "root", rootPassword(cluster), "",
			"SELECT COUNT(*) FROM mysql.user WHERE user = 'e2e_intruder'")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("0"), "a custom query must not be able to create users")
	})
})

const (
	mysqlFlavor   = ""
	mariadbFlavor = "mariadb"
)

// customQueryDoc renders the ConfigMap queries. answer is published as
// mysql_e2e_static_answer so a spec can tell when an edit has been loaded.
// withAppQueries adds a query on app.items and one that tries to create a user.
func customQueryDoc(answer int, withAppQueries bool) string {
	doc := fmt.Sprintf(`e2e_static:
  query: "SELECT 'configmap' AS source, %d AS answer"
  metrics:
    - source:
        usage: LABEL
    - answer:
        usage: GAUGE
e2e_clock:
  query: "SELECT UNIX_TIMESTAMP(NOW(6)) AS now"
  metrics:
    - now:
        usage: GAUGE
`, answer)
	if withAppQueries {
		doc += `e2e_items:
  query: "SELECT COUNT(*) AS total FROM app.items"
  metrics:
    - total:
        usage: GAUGE
e2e_intruder:
  query: "CREATE USER 'e2e_intruder'@'%'"
  metrics:
    - v:
        usage: GAUGE
`
	}
	return doc
}

// applyCustomQueries creates or replaces the query ConfigMap.
func applyCustomQueries(doc string) {
	GinkgoHelper()
	indented := "    " + strings.ReplaceAll(strings.TrimRight(doc, "\n"), "\n", "\n    ")
	applyManifest(customQueriesConfigMap, fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %s
  namespace: %s
data:
  queries.yaml: |
%s
`, customQueriesConfigMap, testNamespace, indented))
}

func customQueriesSecretManifest() string {
	return fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
stringData:
  queries.yaml: |
    e2e_secret:
      query: "SELECT 7 AS answer"
      metrics:
        - answer:
            usage: GAUGE
`, customQueriesSecret, testNamespace)
}

func customQueriesClusterManifest(name string, instances int, flavor string) string {
	image, flavorLine := instanceImage, ""
	if flavor == mariadbFlavor {
		image, flavorLine = mariadbImage, "\n  flavor: mariadb"
	}
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:%[8]s
  instances: %[3]d
  imageName: %[4]s
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  monitoring:
    customQueriesConfigMap:
      - name: %[7]s
        key: queries.yaml
    customQueriesSecret:
      - name: %[9]s
        key: queries.yaml
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instances, image, e2eInstanceResources, e2eMySQLParameters,
		customQueriesConfigMap, flavorLine, customQueriesSecret)
}

// patchMonitoring merge-patches spec.monitoring of a Cluster.
func patchMonitoring(cluster, monitoring string) {
	GinkgoHelper()
	_, err := kubectl("patch", "cluster", cluster, "-n", testNamespace, "--type", "merge",
		"-p", `{"spec": {"monitoring": `+monitoring+`}}`)
	Expect(err).NotTo(HaveOccurred())
}

// expectStaticAnswer waits until every Pod publishes the given static answer,
// which shows the latest ConfigMap edit has been loaded.
func expectStaticAnswer(pods []string, answer float64) {
	GinkgoHelper()
	for _, pod := range pods {
		Eventually(func(g Gomega) {
			g.Expect(sampleValue(scrapeInstanceMetricsProxy(pod), `mysql_e2e_static_answer{source="configmap"}`)).
				To(Equal(answer))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed(),
			"%s did not pick up the ConfigMap edit", pod)
	}
}

// podIdentities returns each Pod's UID and container restart counts, which
// change if the Pod is replaced or restarted.
func podIdentities(pods []string) map[string]string {
	GinkgoHelper()
	ids := map[string]string{}
	for _, pod := range pods {
		out, err := kubectl("get", "pod", pod, "-n", testNamespace,
			"-o", "jsonpath={.metadata.uid} {.status.containerStatuses[*].restartCount}")
		Expect(err).NotTo(HaveOccurred())
		ids[pod] = strings.TrimSpace(out)
	}
	return ids
}

// scrapeInstanceMetricsProxy reads an instance's /metrics through the API
// server's Pod proxy, which is cheaper than a curl Pod when a spec scrapes
// repeatedly.
func scrapeInstanceMetricsProxy(pod string) string {
	GinkgoHelper()
	var body string
	Eventually(func() error {
		var err error
		body, err = kubectl("get", "--raw",
			fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:9187/proxy/metrics", testNamespace, pod))
		return err
	}, e2eTimeout(time.Minute), 2*time.Second).Should(Succeed(), "failed to scrape %s", pod)
	return body
}

// sampleValue returns the value of the series written exactly as series (name
// plus its label set, if any) in a metrics exposition, or -1 when absent.
func sampleValue(body, series string) float64 {
	for _, line := range strings.Split(body, "\n") {
		name, value, ok := strings.Cut(line, " ")
		if !ok || name != series {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return -1
		}
		return v
	}
	return -1
}

// itemsQuery publishes the row count of app.items, which needs a grant.
const itemsQuery = `e2e_items:
  query: "SELECT COUNT(*) AS total FROM app.items"
  metrics:
    - total:
        usage: GAUGE
`

// expectItemsTotal waits until every Pod publishes mysql_e2e_items_total.
func expectItemsTotal(pods []string, want float64) {
	GinkgoHelper()
	for _, pod := range pods {
		Eventually(func(g Gomega) {
			g.Expect(sampleValue(scrapeInstanceMetricsProxy(pod), "mysql_e2e_items_total")).To(Equal(want))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed(),
			"%s did not publish mysql_e2e_items_total", pod)
	}
}

// metricsAccountGrants returns SHOW GRANTS for the metrics account on a Pod,
// or the error text when the account is missing.
func metricsAccountGrants(pod, password string) string {
	out, err := mysqlExec(pod, "root", password, "", "SHOW GRANTS FOR 'cnmsql_metrics'@'localhost'")
	if err != nil {
		return err.Error()
	}
	return out
}

// expectMetricsAccountReady waits for the MetricsAccountReady condition.
func expectMetricsAccountReady(cluster string) {
	GinkgoHelper()
	Eventually(func() string {
		out, _ := kubectl("get", "cluster", cluster, "-n", testNamespace, "-o",
			`jsonpath={.status.conditions[?(@.type=="MetricsAccountReady")].status}`)
		return strings.TrimSpace(out)
	}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Equal("True"))
}
