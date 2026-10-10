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

// The replica readiness lag gate (spec.replication.maxReadyLag). Readiness on a
// replica checks that the IO and SQL threads are running, so a replica that
// comes back far behind — scaled up from a PVC cloned hours earlier, or
// restarted after a long outage — becomes Ready at once, joins the -ro/-r
// Services, and the Cluster reports Ready while reads served from it are hours
// old (issue #147).
//
// These specs reproduce that state deterministically with SOURCE_DELAY: the
// applier deliberately lags the IO thread by a fixed number of seconds, both
// replication threads report running, and the heartbeat lag sits above the
// configured bound for as long as the test needs it. A fence cannot do this: a
// fenced instance has mysqld down and fails readiness for the wrong reason.
//
// The assertions mirror the issue's symptoms: a replica whose lag exceeds the
// bound must stay out of -ro/-r, the readiness failure must name the current
// lag, and once the replica is within the bound it must rejoin the read
// Services.
func readinessLagGateSpec(
	cluster, flavor, image string,
	exec func(pod, user, password, database, sql string) (string, error),
	applyDelaySQL func(seconds int) string,
) {
	const (
		instances = 3
		// The bound and the deliberate applier delay. The delay sits three times
		// past the bound so no plausible measurement slack can bring the lag back
		// inside while the "behind" assertions run.
		maxReadyLag = "15s"
		delay       = 45
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace(cluster)

		By(fmt.Sprintf("creating a %d-instance %s cluster with maxReadyLag=%s", instances, flavor, maxReadyLag))
		applyManifest(cluster, readinessLagGateClusterManifest(cluster, flavor, image, instances, maxReadyLag))
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
		expectClusterReady(cluster, instances, e2eTimeout(20*time.Minute))
	})

	It("keeps a replica past maxReadyLag out of the read Services until it catches up", func() {
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, instances, primary)

		By(fmt.Sprintf("verifying the caught-up replica %s serves the ro Service", replica))
		Eventually(func(g Gomega) {
			g.Expect(serviceEndpoints(cluster+"-ro")).To(ContainElement(replica),
				"a caught-up replica must be routed by the read Services")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		// Even if a later assertion leaves the applier delayed, the namespace
		// teardown discards the cluster. Reset the delay anyway so a passing run
		// ends with the instance in its normal state.
		DeferCleanup(func() {
			_, _ = exec(replica, "root", rootPassword(cluster), "", "STOP REPLICA; "+applyDelaySQL(0)+"; START REPLICA;")
		})

		By(fmt.Sprintf("delaying %s's applier by %ds while both replication threads keep running", replica, delay))
		_, err := exec(replica, "root", rootPassword(cluster), "",
			"STOP REPLICA; "+applyDelaySQL(delay)+"; START REPLICA;")
		Expect(err).NotTo(HaveOccurred(), "failed to delay the replica's applier")

		By("waiting for the replica's heartbeat lag to cross the bound")
		Eventually(func(g Gomega) {
			lag, lerr := instanceLagMillis(cluster, replica)
			g.Expect(lerr).NotTo(HaveOccurred())
			g.Expect(lag).To(BeNumerically(">", 15000),
				"the delayed replica's heartbeat lag must exceed the %s bound", maxReadyLag)
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		// The status lag can cross the bound before the Pod leaves the Services:
		// the kubelet needs the readiness probe's failureThreshold of consecutive
		// failures, then the EndpointSlice controller has to drop the endpoint.
		By("waiting for the kubelet to take the behind replica out of -ro/-r")
		Eventually(func(g Gomega) {
			g.Expect(serviceEndpoints(cluster + "-ro")).NotTo(ContainElement(replica),
				"a replica over maxReadyLag must leave the ro Service")
			g.Expect(serviceEndpoints(cluster + "-r")).NotTo(ContainElement(replica),
				"a replica over maxReadyLag must leave the r Service")
		}, e2eTimeout(time.Minute), 2*time.Second).Should(Succeed())

		By("holding the behind replica out of -ro/-r while its threads are nominally healthy")
		Consistently(func(g Gomega) {
			g.Expect(serviceEndpoints(cluster + "-ro")).NotTo(ContainElement(replica),
				"a replica over maxReadyLag must not serve the ro Service")
			g.Expect(serviceEndpoints(cluster + "-r")).NotTo(ContainElement(replica),
				"a replica over maxReadyLag must not serve the r Service")
			g.Expect(serviceEndpoints(cluster+"-rw")).To(ContainElement(primary),
				"the lag gate must not disturb the rw Service")
		}, e2eTimeout(45*time.Second), 3*time.Second).Should(Succeed())

		// The kubelet's probe event carries only the HTTP status code, never the
		// /readyz body, so the attribution lives in the Cluster status.
		By("attributing the not-ready replica to the lag gate in the Cluster status")
		Eventually(func(g Gomega) {
			reason, rerr := clusterField(cluster, "{.status.phaseReason}")
			g.Expect(rerr).NotTo(HaveOccurred())
			g.Expect(reason).To(ContainSubstring("behind maxReadyLag: "+replica),
				"the Cluster must name the replica the lag gate holds back")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("restoring the replica's applier so it catches up")
		_, err = exec(replica, "root", rootPassword(cluster), "", "STOP REPLICA; "+applyDelaySQL(0)+"; START REPLICA;")
		Expect(err).NotTo(HaveOccurred())

		By("letting the caught-up replica back into the read Services")
		Eventually(func(g Gomega) {
			lag, lerr := instanceLagMillis(cluster, replica)
			g.Expect(lerr).NotTo(HaveOccurred())
			g.Expect(lag).To(BeNumerically("<=", 15000),
				"the replica must be within the bound again after the delay is lifted")
			g.Expect(serviceEndpoints(cluster+"-ro")).To(ContainElement(replica),
				"a replica back within maxReadyLag must rejoin the ro Service")
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		expectClusterReady(cluster, instances, e2eTimeout(10*time.Minute))
	})
}

var _ = Describe("Replica readiness lag gate", Ordered, Label("feature"), func() {
	readinessLagGateSpec("readylag", "mysql", instanceImage, mysqlExec, func(seconds int) string {
		return fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_DELAY=%d", seconds)
	})
})

// The MariaDB counterpart. The gate itself is flavor-agnostic instance-manager
// logic, but the delayed-applier scenario is set up through each flavor's own
// replication SQL, so the proof needs its own Describe.
var _ = Describe("MariaDB replica readiness lag gate", Ordered, Label("feature", "mariadb"), func() {
	readinessLagGateSpec("mdb-readylag", "mariadb", mariadbImage, mariadbExec, func(seconds int) string {
		return fmt.Sprintf("CHANGE MASTER TO MASTER_DELAY=%d", seconds)
	})
})

// readinessLagGateClusterManifest renders the Cluster for the lag gate specs.
func readinessLagGateClusterManifest(name, flavor, image string, instances int, maxReadyLag string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  flavor: %[3]s
  instances: %[4]d
  imageName: %[5]s
  storage:
    size: 2Gi
%[6]s
  replication:
    maxReadyLag: %[7]s
  mysql:
    binlogFormat: ROW
%[8]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, flavor, instances, image, e2eInstanceResources, maxReadyLag, e2eMySQLParameters)
}

// serviceEndpoints returns the target Pod names currently routed by the named
// Service in the test namespace.
func serviceEndpoints(service string) []string {
	// An EndpointSlice keeps a not-ready Pod as an endpoint with
	// conditions.ready=false, and kube-proxy does not route to it. Only ready
	// endpoints serve the Service; an unset ready condition means ready.
	out, err := kubectl("get", "endpointslice",
		"-l", "kubernetes.io/service-name="+service,
		"-n", testNamespace,
		"-o", `jsonpath={range .items[*].endpoints[*]}{.targetRef.name}={.conditions.ready}{"\n"}{end}`)
	Expect(err).NotTo(HaveOccurred(), "failed to read endpoints for service %s", service)
	var ready []string
	for line := range strings.Lines(out) {
		name, state, _ := strings.Cut(strings.TrimSpace(line), "=")
		if name != "" && state != "false" {
			ready = append(ready, name)
		}
	}
	return ready
}

// instanceLagMillis reads the heartbeat lag the operator last mirrored into
// status.replicationLagByInstance for an instance, in milliseconds.
func instanceLagMillis(cluster, instance string) (int64, error) {
	out, err := clusterField(cluster, fmt.Sprintf("{.status.replicationLagByInstance['%s']}", instance))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(out), 10, 64)
}
