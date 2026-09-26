//go:build e2e
// +build e2e

package e2e

import (
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/test/utils"
)

// e2eTimeoutMultiplier scales all Eventually timeout values when the test suite
// runs on slower infrastructure (e.g. self-hosted GitHub Actions runners).
// Set E2E_TIMEOUT_MULTIPLIER=2 to double every timeout; the default is 1.
var e2eTimeoutMultiplier = func() float64 {
	if v := os.Getenv("E2E_TIMEOUT_MULTIPLIER"); v != "" {
		if m, err := strconv.ParseFloat(v, 64); err == nil && m > 0 {
			return m
		}
	}
	return 1.0
}()

func e2eTimeout(d time.Duration) time.Duration {
	return time.Duration(float64(d) * e2eTimeoutMultiplier)
}

// testNamespace is the namespace that hosts the test Clusters and their
// supporting objects. The controller runs in `namespace` (cnmsql-system) and
// watches all namespaces, so user-facing resources live here, mirroring how a
// real user would deploy a Cluster outside the operator's namespace.
// defaultTestNamespace is the fallback namespace for tests that do not create
// their own. It is kept for backward compatibility; new parallel-friendly tests
// call createTestNamespace instead.
const defaultTestNamespace = "default"

// testNamespace is the namespace currently targeted by helpers. Each parallel
// test group sets this to its own namespace before creating resources.
var testNamespace = defaultTestNamespace

// objectStoreNamespace is the shared namespace where the in-cluster S3 store runs
// once for the whole suite, avoiding per-Describe deploy/teardown cycles.
const objectStoreNamespace = "e2e-objectstore"

// currentObjectStoreNamespace is the namespace of the S3 store the helpers
// currently target: the shared store by default, or a Describe's private store
// between setupPrivateObjectStore and teardownPrivateObjectStore. Like
// testNamespace it is process-local, so a private store never leaks into specs
// running on other parallel processes.
var currentObjectStoreNamespace = objectStoreNamespace

// objectStoreName is the Deployment/Service name of the in-cluster S3 store. The
// specs that simulate an object-store outage scale this Deployment, which must
// be a private store (see setupPrivateObjectStore).
const objectStoreName = "seaweedfs"

// objectStorePort is the port the in-cluster S3 store serves the S3 API on.
const objectStorePort = 8333

// objectStoreBucket is the bucket pre-created in the in-cluster S3 store and
// targeted by the backup/recovery specs.
const objectStoreBucket = "cnmsql-backups"

// objectStoreAccessKey and objectStoreSecretKey are the static credentials the
// in-cluster store is configured with. They are test-only and never leave Kind.
const (
	objectStoreAccessKey = "cnmsqladmin"
	objectStoreSecretKey = "cnmsqladmin"
)

// objectStoreCredsSecret is the Secret holding the store's access credentials
// consumed by Clusters and Backups through their object-store configuration.
const objectStoreCredsSecret = "objectstore-creds"

// s3Remote is the rclone remote name the toolbox is configured with. Object keys
// the specs pass to the toolbox are addressed as "<s3Remote>:<bucket>/<key>".
const s3Remote = "local"

// objectKey builds a toolbox-addressable path for a key in the suite's bucket.
func objectKey(format string, args ...any) string {
	return fmt.Sprintf("%s:%s/%s", s3Remote, objectStoreBucket, fmt.Sprintf(format, args...))
}

// generateTestNamespace returns a unique namespace name using the current
// Ginkgo parallel process id, so parallel nodes never collide.
func generateTestNamespace(prefix string) string {
	return fmt.Sprintf("e2e-%s-%d", prefix, GinkgoParallelProcess())
}

// createTestNamespace creates a Kubernetes namespace for test resources and
// returns the name. The caller is responsible for calling deleteTestNamespace.
// It sets testNamespace to the result so downstream helpers target it.
//
// Before creating, it deletes any stale namespace left behind from a previous
// run. CRs are deleted first, then Pods with a 10s grace period so mysqld gets
// a brief SIGTERM for clean shutdown before the namespace is deleted.
func createTestNamespace(prefix string) string {
	ns := generateTestNamespace(prefix)
	By(fmt.Sprintf("creating test namespace %s", ns))
	_, _ = kubectl("delete",
		"logicalrestores,clusters,backups,scheduledbackups,databases,databaseusers,imagecatalogs",
		"--all", "-n", ns, "--ignore-not-found", "--wait=false")
	_, _ = kubectl("delete", "pods", "--all", "-n", ns, "--ignore-not-found",
		"--grace-period=10", "--wait=false")
	_, _ = kubectl("delete", "ns", ns, "--ignore-not-found", "--wait=true", "--timeout=120s")
	_, err := kubectl("create", "ns", ns)
	Expect(err).NotTo(HaveOccurred(), "Failed to create namespace %s", ns)
	testNamespace = ns
	return ns
}

// deleteTestNamespace removes a Kubernetes namespace and waits for it to fully
// terminate. It restores the testNamespace global to the caller-provided prev.
//
// Managed CRs are deleted first with --wait=false so the operator starts tearing
// down finalizer chains. Pods are then deleted with --grace-period=10, which
// overrides the Pod's 3600s terminationGracePeriodSeconds and caps the preStop
// hook + SIGTERM + SIGKILL sequence to 10s — fast enough for e2e cleanup, but
// gives mysqld a few seconds of SIGTERM for a clean flush before the SIGKILL.
func deleteTestNamespace(ns, prev string) {
	By(fmt.Sprintf("deleting test namespace %s", ns))
	_, _ = kubectl("delete",
		"logicalrestores,clusters,backups,scheduledbackups,databases,databaseusers,imagecatalogs",
		"--all", "-n", ns, "--ignore-not-found", "--wait=false")
	_, _ = kubectl("delete", "pods", "--all", "-n", ns, "--ignore-not-found",
		"--grace-period=10", "--wait=false")
	_, _ = kubectl("delete", "ns", ns, "--ignore-not-found", "--wait=true", "--timeout=120s")
	testNamespace = prev
}

// useNamespace temporarily sets the global testNamespace. Call with a
// defer-recovered pattern:
//
//	prev := useNamespace(myNs)
//	defer func() { testNamespace = prev }()
func useNamespace(ns string) string {
	prev := testNamespace
	testNamespace = ns
	return prev
}

func dumpE2EDiagnostics() {
	dumps := []struct {
		name string
		args []string
	}{
		{name: "all clusters", args: []string{"get", "clusters", "-A", "-o", "yaml"}},
		{name: "all pods", args: []string{"get", "pods", "-A", "-o", "wide"}},
		{name: "events in test namespace", args: []string{"get", "events", "-n", testNamespace, "--sort-by=.lastTimestamp"}},
		{name: "all events", args: []string{"get", "events", "-A", "--sort-by=.lastTimestamp"}},
		{name: "operator pod logs", args: []string{"logs", "-l", "control-plane=controller-manager", "-n", namespace, "--tail=300"}},
		{name: "node capacity and pressure", args: []string{"describe", "nodes"}},
	}
	for _, dump := range dumps {
		out, err := kubectl(dump.args...)
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "\nFailed to collect %s: %v\n%s\n", dump.name, err, out)
			continue
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "\n%s:\n%s\n", dump.name, out)
	}
	dumpInstanceLogs()
}

// dumpInstanceLogs prints recent logs from every MySQL instance Pod in the
// current test namespace. When a cluster stalls (e.g. replicas never join), the
// instance-runner and mysqld output here is what explains why; the operator log
// alone does not show it.
func dumpInstanceLogs() {
	pods, err := kubectl("get", "pods", "-n", testNamespace,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}")
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "\nFailed to list pods in %s: %v\n%s\n", testNamespace, err, pods)
		return
	}
	for _, pod := range strings.Fields(pods) {
		out, err := kubectl("logs", pod, "-n", testNamespace, "-c", "mysql", "--tail=80")
		if err != nil {
			// Pod may still be in an init container; surface that too.
			out, _ = kubectl("logs", pod, "-n", testNamespace, "--all-containers", "--tail=80")
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "\ninstance %s logs:\n%s\n", pod, out)
	}
}

// kubectl runs a kubectl command from the project directory and returns its
// combined output.
func kubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

// waitForWebhookReady blocks until the Cluster admission webhook accepts
// dry-run requests in the given namespace. When ns is empty, the probe
// manifest uses the current testNamespace (for namespaced operators).
// Use after deploying or restoring an operator to guarantee the webhook
// endpoint is actually reachable before tests proceed.
func waitForWebhookReady(ns string) {
	GinkgoHelper()
	if ns == "" {
		ns = testNamespace
	}
	probePath := "/tmp/cnmsql-e2e-webhook-readiness.yaml"
	probe := fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: webhook-readiness
  namespace: %s
spec:
  instances: 1
  imageName: %s
  storage:
    size: 1Gi
`, ns, instanceImage)
	Expect(os.WriteFile(probePath, []byte(probe), 0o644)).To(Succeed())
	Eventually(func() error {
		_, err := kubectl("apply", "--dry-run=server", "-f", probePath)
		return err
	}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed(),
		"Cluster admission webhook did not become ready")
}

// applyManifest writes the given manifest to a temporary file and applies it,
// returning the file path so callers can delete it later with deleteManifest.
//
// The apply is retried while it fails with a transient admission-webhook
// connectivity error. The operator's validating webhook can briefly become
// unreachable mid-spec (e.g. the operator Pod is rescheduled or rolled), which
// surfaces as "failed calling webhook ... connection refused" from the API
// server rather than a real validation rejection. A genuine validation error
// (a bad spec) is not transient and fails fast.
func applyManifest(name, manifest string) {
	GinkgoHelper()
	path := writeManifest(name, manifest)
	Eventually(func() error {
		out, err := kubectl("apply", "-f", path)
		if err != nil && isTransientWebhookError(out, err) {
			return err
		}
		// Wrap non-transient errors in StopTrying so a real validation
		// failure aborts immediately instead of retrying until timeout.
		if err != nil {
			StopTrying("apply rejected").Wrap(err).Now()
		}
		return nil
	}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed(),
		"Failed to apply manifest %s", name)
}

// isTransientWebhookError reports whether a failed kubectl apply was rejected
// because the admission webhook endpoint was momentarily unreachable, as
// opposed to a genuine validation rejection. These are safe to retry.
func isTransientWebhookError(out string, err error) bool {
	msg := out + err.Error()
	if !strings.Contains(msg, "failed calling webhook") &&
		!strings.Contains(msg, "failed to call webhook") {
		return false
	}
	for _, s := range []string{
		"connection refused",
		"no endpoints available",
		"EOF",
		"i/o timeout",
		"context deadline exceeded",
		"Timeout: request did not complete",
		"connect: connection reset by peer",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// clusterAnnotate annotates a Cluster resource, retrying on transient
// admission-webhook connectivity errors (e.g. webhook Pod restarting). The
// webhook uses failurePolicy: Fail, so a momentarily unreachable endpoint
// must not fail the test.
func clusterAnnotate(name, annotation string) {
	GinkgoHelper()
	Eventually(func() error {
		out, err := kubectl("annotate", "cluster", name, "-n", testNamespace,
			annotation, "--overwrite")
		if err != nil && isTransientWebhookError(out, err) {
			return err
		}
		if err != nil {
			StopTrying("annotate rejected").Wrap(err).Now()
		}
		return nil
	}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed(),
		"Failed to annotate Cluster %s", name)
}

// deleteManifest deletes the resources described by the named manifest, ignoring
// any that are already gone. It is safe to call from DeferCleanup.
func deleteManifest(name, manifest string) {
	path := writeManifest(name, manifest)
	_, _ = kubectl("delete", "-f", path, "--ignore-not-found", "--wait=false")
}

// deleteCluster removes a Cluster and blocks until its instance Pods and PVCs
// are gone, so the next spec's instances aren't scheduled against a node still
// pinned by the previous cluster's resources. A bounded timeout keeps a stuck
// finalizer from hanging cleanup indefinitely.
func deleteCluster(name string) {
	_, _ = kubectl("delete", "cluster", name, "-n", testNamespace,
		"--ignore-not-found", "--wait=true", "--timeout=120s")
}

func writeManifest(name, manifest string) string {
	path := filepath.Join("/tmp", fmt.Sprintf("cnmsql-e2e-%s-%d.yaml", name, GinkgoParallelProcess()))
	Expect(os.WriteFile(path, []byte(manifest), 0o644)).To(Succeed(), "Failed to write manifest %s", name)
	return path
}

// clusterField returns a single jsonpath field from a Cluster's status/spec.
func clusterField(name, jsonpath string) (string, error) {
	return kubectl("get", "cluster", name, "-n", testNamespace, "-o", "jsonpath="+jsonpath)
}

// terminalStateGrace is how long a terminal reason must hold, unchanged, before
// expectClusterReady stops waiting for the cluster and fails on it.
//
// The Blocked phase is not only reached by misconfiguration. A cluster reaches it
// whenever the operator declines a failover, and it stays there until the operator
// re-observes the instance that is coming back: unfencing an instance has to restart
// its mysqld, which takes minutes on a loaded CI node, and the phase carries the old
// refusal for all of that time. A grace period long enough to cover that restart
// keeps the fast-fail useful — a misconfigured cluster reports the same reason
// forever, so it still surfaces in minutes rather than at the full timeout — while a
// cluster that is on its way back is given the time it needs.
const terminalStateGrace = 5 * time.Minute

// expectClusterReady blocks until the named Cluster reports Ready with the
// expected number of ready instances and an elected primary. On timeout it dumps
// cluster-wide diagnostics (Cluster status, Pod states, events, operator logs,
// node capacity) before failing: a cluster that never converges is the most
// common e2e failure, and without this dump the CI logs reveal nothing about why.
func expectClusterReady(name string, instances int, timeout time.Duration) {
	GinkgoHelper()
	awaitClusterReady(name, instances, timeout, true)
}

// expectClusterRecovers is expectClusterReady for a cluster that is known to be
// parked in Blocked right now, having been driven there on purpose by a guard spec
// that has since removed the cause. Blocked is the state such a spec just finished
// asserting, so it is the expected starting point of the recovery and never a reason
// to fail. Image-pull errors remain terminal.
func expectClusterRecovers(name string, instances int, timeout time.Duration) {
	GinkgoHelper()
	awaitClusterReady(name, instances, timeout, false)
}

// awaitClusterReady polls the cluster to readiness. blockedIsTerminal decides
// whether a persistent Blocked phase is grounds to give up early.
func awaitClusterReady(name string, instances int, timeout time.Duration, blockedIsTerminal bool) {
	GinkgoHelper()
	timeout = e2eTimeout(timeout)
	check := func() error {
		ready, err := clusterField(name, "{.status.conditions[?(@.type=='Ready')].status}")
		if err != nil {
			return fmt.Errorf("reading Ready condition: %w", err)
		}
		if ready != "True" {
			return fmt.Errorf("cluster %s is not Ready yet", name)
		}
		readyInstances, err := clusterField(name, "{.status.readyInstances}")
		if err != nil {
			return fmt.Errorf("reading readyInstances: %w", err)
		}
		if want := fmt.Sprintf("%d", instances); readyInstances != want {
			return fmt.Errorf("cluster %s has %q ready instances, want %d", name, readyInstances, instances)
		}
		primary, err := clusterField(name, "{.status.currentPrimary}")
		if err != nil {
			return fmt.Errorf("reading currentPrimary: %w", err)
		}
		if primary == "" {
			return fmt.Errorf("cluster %s has no elected primary yet", name)
		}
		return nil
	}

	deadline := time.Now().Add(timeout)
	var (
		prevTerminal  string
		terminalSince time.Time
	)
	for {
		lastErr := check()
		if lastErr == nil {
			return
		}
		// Fail early on a state the cluster cannot leave on its own, so a
		// misconfigured cluster surfaces with diagnostics instead of burning the whole
		// timeout. clusterTerminalState is deliberately conservative (Blocked phase +
		// image-pull errors only), so a transient CrashLoopBackOff during bootstrap
		// never trips a false failure.
		//
		// Blocked is also parked while the operator waits out a switchover, a failover
		// it has declined, or an instance that is restarting, none of which the cluster
		// needs help to leave. Require the same reason to hold for terminalStateGrace
		// before failing on it, so that only a reason the cluster is genuinely stuck on
		// counts.
		reason := clusterTerminalState(name, blockedIsTerminal)
		switch {
		case reason == "":
			terminalSince = time.Time{}
		case reason != prevTerminal:
			terminalSince = time.Now()
		case time.Since(terminalSince) >= terminalStateGrace:
			By(fmt.Sprintf("cluster %s held a terminal state for %s (%s); dumping diagnostics",
				name, terminalStateGrace, reason))
			dumpE2EDiagnostics()
			Fail(fmt.Sprintf("cluster %s cannot become ready: %s (last check: %v)", name, reason, lastErr))
		}
		prevTerminal = reason
		if time.Now().After(deadline) {
			By(fmt.Sprintf("cluster %s did not become ready within %s; dumping diagnostics", name, timeout))
			dumpE2EDiagnostics()
			Fail(fmt.Sprintf("cluster %s did not become ready within %s: %v", name, timeout, lastErr))
		}
		time.Sleep(5 * time.Second)
	}
}

// clusterTerminalState reports a non-empty reason when a cluster is in a state it
// cannot recover from without intervention, so expectClusterReady can give up
// instead of polling to the deadline. It is intentionally conservative: only an
// operator-declared Blocked phase and image-pull failures count. Because every
// instance image is preloaded into Kind, an image-pull error is a real
// misconfiguration, never transient; a CrashLoopBackOff is NOT treated as
// terminal, since instances may crash-loop briefly during bootstrap. Any read
// error degrades to "" (no fast-fail), so a jsonpath/label mismatch only loses
// the optimization, never causes a false failure.
//
// Callers recovering a cluster from a block they induced themselves pass
// blockedIsTerminal=false: for them the phase is the expected starting state.
func clusterTerminalState(name string, blockedIsTerminal bool) string {
	if phase, err := clusterField(name, "{.status.phase}"); blockedIsTerminal &&
		err == nil && strings.TrimSpace(phase) == "Blocked" {
		reason, _ := clusterField(name, "{.status.phaseReason}")
		return fmt.Sprintf("cluster phase Blocked: %s", strings.TrimSpace(reason))
	}
	out, err := kubectl("get", "pods", "-n", testNamespace,
		"-l", "mysql.cnmsql.co/cluster="+name,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{'='}"+
			"{.status.initContainerStatuses[*].state.waiting.reason}{','}"+
			"{.status.containerStatuses[*].state.waiting.reason}{'\\n'}{end}")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(out, "\n") {
		for _, bad := range []string{"ImagePullBackOff", "ErrImagePull", "InvalidImageName"} {
			if strings.Contains(line, bad) {
				return fmt.Sprintf("pod image error: %s", strings.TrimSpace(line))
			}
		}
	}
	return ""
}

// clusterPrimary returns the current primary Pod name for a cluster.
func clusterPrimary(name string) string {
	primary, err := clusterField(name, "{.status.currentPrimary}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read currentPrimary for %s", name)
	Expect(primary).NotTo(BeEmpty(), "Cluster %s has no primary", name)
	return primary
}

// scaleInstances patches a Cluster's spec.instances to request a different
// number of instances.
func scaleInstances(name string, instances int) {
	GinkgoHelper()
	Eventually(func() error {
		out, err := kubectl("patch", "cluster", name, "-n", testNamespace,
			"--type=merge", "-p", fmt.Sprintf(`{"spec":{"instances":%d}}`, instances))
		if err != nil && isTransientWebhookError(out, err) {
			return err
		}
		if err != nil {
			StopTrying("scale rejected").Wrap(err).Now()
		}
		return nil
	}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed(),
		"failed to scale cluster %s to %d instances", name, instances)
}

// appPassword returns the decoded application password for a Cluster, read from
// the operator-generated `<cluster>-app` Secret.
func appPassword(cluster string) string {
	output, err := kubectl("get", "secret", cluster+"-app", "-n", testNamespace,
		"-o", "jsonpath={.data.password}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read application password for %s", cluster)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(output))
	Expect(err).NotTo(HaveOccurred(), "Failed to decode application password for %s", cluster)
	return string(decoded)
}

// mysqlExec runs a SQL statement inside an instance Pod as the given user and
// returns the command output. The password is passed through the MYSQL_PWD
// environment variable to suppress the MySQL "Using a password on the command
// line" warning from contaminating result parsing. Column headers are suppressed
// with -N so parsed output is always just the raw values. The target container
// is pinned with -c mysql because the instance Pod also carries a bootstrap init
// container; without it kubectl prints a "Defaulted container" notice into the
// combined output that would corrupt single-value parsing.
func mysqlExec(pod, user, password, database, sql string) (string, error) {
	args := []string{"exec", pod, "-n", testNamespace, "-c", "mysql", "--",
		"env", "MYSQL_PWD=" + password, "mysql", "-u" + user, "-N"}
	if database != "" {
		args = append(args, database)
	}
	args = append(args, "-e", sql)
	return kubectl(args...)
}

// mariadbExec runs a SQL statement on a MariaDB pod using the mariadb binary
// (which does not print a deprecation warning like the mysql symlink).
func mariadbExec(pod, user, password, database, sql string) (string, error) {
	args := []string{"exec", pod, "-n", testNamespace, "-c", "mysql", "--",
		"env", "MYSQL_PWD=" + password, "mariadb", "-u" + user, "-N"}
	if database != "" {
		args = append(args, database)
	}
	args = append(args, "-e", sql)
	return kubectl(args...)
}

// mariadbExecCols is mariadbExec without -N (--skip-column-names). It is needed
// for vertical (\G) queries whose output is parsed by column name: under -N the
// client omits the "Column_name:" labels in \G output, leaving bare values, so a
// caller that greps for e.g. "Slave_IO_Running: Yes" would never match. Use this
// (not mariadbExec) whenever the assertion depends on the column labels.
func mariadbExecCols(pod, user, password, database, sql string) (string, error) {
	args := []string{"exec", pod, "-n", testNamespace, "-c", "mysql", "--",
		"env", "MYSQL_PWD=" + password, "mariadb", "-u" + user}
	if database != "" {
		args = append(args, database)
	}
	args = append(args, "-e", sql)
	return kubectl(args...)
}

// createUserViaControlAPI POSTs a user-create request to an instance's mTLS
// control API using a one-shot curl Pod that mounts the cluster client cert and
// CA (the same material the operator authenticates with). The DB container image
// does not necessarily ship curl — the MariaDB instance image does not — so we
// cannot `kubectl exec ... -- curl` inside it; the curl Pod is
// engine-agnostic. It blocks until the Pod reports the call returned HTTP 200.
func createUserViaControlAPI(cluster, instance, jsonBody string) {
	GinkgoHelper()
	name := "user-create-" + instance
	url := fmt.Sprintf("https://%s.%s.svc:8080/user/create", instance, testNamespace)
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  restartPolicy: Never
  containers:
  - name: curl
    image: %[6]s
    command: ["sh", "-c"]
    args:
    - >
      code=$(curl -sS -X POST --retry 10 --retry-connrefused --retry-delay 2
      -H 'Content-Type: application/json' -d '%[3]s'
      --cert /client/tls.crt --key /client/tls.key --cacert /ca/ca.crt
      -o /dev/null -w '%%{http_code}' %[4]s);
      echo "control API returned $code"; test "$code" = "200"
    volumeMounts:
    - {name: client, mountPath: /client, readOnly: true}
    - {name: ca, mountPath: /ca, readOnly: true}
  volumes:
  - name: client
    secret:
      secretName: %[5]s-client-tls
  - name: ca
    secret:
      secretName: %[5]s-ca
`, name, testNamespace, jsonBody, url, cluster, curlImage)

	applyManifest(name, manifest)
	DeferCleanup(func() {
		_, _ = kubectl("delete", "pod", name, "-n", testNamespace, "--ignore-not-found", "--wait=false")
	})

	Eventually(func(g Gomega) {
		phase, err := kubectl("get", "pod", name, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
		g.Expect(err).NotTo(HaveOccurred())
		phase = strings.TrimSpace(phase)
		if phase == "Failed" {
			logs, _ := kubectl("logs", name, "-n", testNamespace)
			Fail(fmt.Sprintf("user-create Pod failed: %s", logs))
		}
		g.Expect(phase).To(Equal("Succeeded"), "user-create Pod has not reported a 200 yet")
	}, e2eTimeout(2*time.Minute), 3*time.Second).Should(Succeed())
}

// objectStoreEndpoint returns the HTTP endpoint for the current in-cluster S3
// store: the shared one in objectStoreNamespace, reachable from every test
// namespace, unless a private store is active.
func objectStoreEndpoint() string {
	return fmt.Sprintf("http://%s.%s.svc:%d", objectStoreName, currentObjectStoreNamespace, objectStorePort)
}

// deploySharedObjectStore creates the shared object-store namespace and deploys a
// single-node S3 store once for the whole suite. This avoids per-Describe
// deploy/teardown cycles (each ~6 minutes), saving significant wall-clock time in
// parallel runs since every Describe that needs an object store would otherwise
// stand up and tear down its own instance.
func deploySharedObjectStore() {
	By("creating shared object-store namespace")
	_, _ = kubectl("create", "ns", objectStoreNamespace)

	By("deploying the shared in-cluster object store")
	applyManifest("objectstore-shared", objectStoreManifest(objectStoreNamespace))

	By("waiting for the shared object store to become available")
	_, err := kubectl("wait", "deployment/"+objectStoreName, "-n", objectStoreNamespace,
		"--for=condition=Available", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "Shared object store did not become available")

	By("waiting for the shared bucket-creation Job to complete")
	_, err = kubectl("wait", "job/"+objectStoreName+"-mkbucket", "-n", objectStoreNamespace,
		"--for=condition=Complete", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "Shared bucket-creation Job did not complete")
}

// teardownSharedObjectStore removes the shared object-store namespace and all its
// resources.
func teardownSharedObjectStore() {
	_, _ = kubectl("delete", "ns", objectStoreNamespace, "--ignore-not-found", "--wait=false")
}

// ensureObjectStoreCreds creates the credentials Secret in the current
// testNamespace so Cluster CRs can reference it locally. The shared store runs in
// objectStoreNamespace and this Secret mirrors its credentials in each test
// namespace.
func ensureObjectStoreCreds() {
	_, _ = kubectl("delete", "secret", objectStoreCredsSecret, "-n", testNamespace, "--ignore-not-found")
	_, err := kubectl("create", "secret", "generic", objectStoreCredsSecret, "-n", testNamespace,
		"--from-literal=ACCESS_KEY_ID="+objectStoreAccessKey,
		"--from-literal=SECRET_ACCESS_KEY="+objectStoreSecretKey)
	Expect(err).NotTo(HaveOccurred(), "Failed to create %s secret in %s", objectStoreCredsSecret, testNamespace)
}

// setupObjectStore ensures the shared store is ready and creates the credentials
// Secret in the current test namespace. The shared store is deployed once by the
// suite, so this is a fast idempotent operation per Describe.
func setupObjectStore() {
	// The shared store is a single replica; if it is mid-restart when this
	// Describe begins, re-gate on its readiness so the first object-store call
	// does not race a connection-refused window.
	_, err := kubectl("wait", "deployment/"+objectStoreName, "-n", objectStoreNamespace,
		"--for=condition=Available", "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "shared object store not available at setup")
	ensureObjectStoreCreds()
}

// setupPrivateObjectStore deploys a dedicated S3 store into the current test
// namespace and points every object-store helper at it until
// teardownPrivateObjectStore. A Describe that takes the store down (outage
// simulation) must use one: scaling the shared store to zero fails the backups
// and binlog archivers of every spec running on the other parallel processes,
// and the restarted store serves errors until its volume server re-registers.
// It returns the previous store namespace to hand to teardownPrivateObjectStore.
func setupPrivateObjectStore() string {
	By("deploying a private object store into " + testNamespace)
	// The manifest also carries the credentials Secret, so no
	// ensureObjectStoreCreds is needed.
	applyManifest("objectstore-private", objectStoreManifest(testNamespace))

	_, err := kubectl("wait", "deployment/"+objectStoreName, "-n", testNamespace,
		"--for=condition=Available", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "Private object store did not become available")
	_, err = kubectl("wait", "job/"+objectStoreName+"-mkbucket", "-n", testNamespace,
		"--for=condition=Complete", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "Private bucket-creation Job did not complete")

	prev := currentObjectStoreNamespace
	currentObjectStoreNamespace = testNamespace
	return prev
}

// teardownPrivateObjectStore points the helpers back at the previous store. The
// private store's resources live in the test namespace and go with it.
func teardownPrivateObjectStore(prev string) {
	currentObjectStoreNamespace = prev
}

// teardownObjectStore removes the per-namespace credentials Secret. The shared
// store survives across Describes and is torn down by SynchronizedAfterSuite.
func teardownObjectStore() {
	_, _ = kubectl("delete", "secret", objectStoreCredsSecret, "-n", testNamespace, "--ignore-not-found")
}

// s3ClientEnvYAML renders the rclone remote configuration as Pod env entries, at
// the given indentation. rclone reads a remote wholly from the environment
// (RCLONE_CONFIG_<REMOTE>_*), so the toolbox needs no config file or mounted
// secret.
func s3ClientEnvYAML(indent, endpoint string) string {
	prefix := "RCLONE_CONFIG_" + strings.ToUpper(s3Remote) + "_"
	vars := [][2]string{
		{"TYPE", "s3"},
		{"PROVIDER", "Other"},
		{"ENDPOINT", endpoint},
		{"ACCESS_KEY_ID", objectStoreAccessKey},
		{"SECRET_ACCESS_KEY", objectStoreSecretKey},
		{"REGION", "us-east-1"},
		{"FORCE_PATH_STYLE", "true"},
		// rclone otherwise writes a NOTICE about the absent config file to
		// stderr, which lands in the combined output the specs parse.
		{"", ""},
	}
	var b strings.Builder
	for _, v := range vars {
		if v[0] == "" {
			fmt.Fprintf(&b, "%s- name: RCLONE_CONFIG\n%s  value: /dev/null\n", indent, indent)
			continue
		}
		fmt.Fprintf(&b, "%s- name: %s%s\n%s  value: %q\n", indent, prefix, v[0], indent, v[1])
	}
	return strings.TrimRight(b.String(), "\n")
}

// seedObjectStoreMarker writes a small object at the given key in the bucket via
// a one-shot toolbox Job and waits for it to complete. It is used to make a
// destination prefix non-empty deterministically.
func seedObjectStoreMarker(key string) {
	name := "seed-" + strings.NewReplacer("/", "-", ".", "-", "_", "-").Replace(key)
	manifest := fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  backoffLimit: 20
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: s3client
        image: %[3]s
        command:
        - /bin/sh
        - -c
        - |
          set -e
          echo cnmsql-guard-marker | rclone rcat %[4]s
        env:
%[5]s
`, name, testNamespace, s3ClientImage, objectKey("%s", key), s3ClientEnvYAML("        ", objectStoreEndpoint()))
	applyManifest(name, manifest)
	DeferCleanup(func() {
		deleteManifest(name, manifest)
	})
	_, err := kubectl("wait", "job/"+name, "-n", testNamespace,
		"--for=condition=Complete", "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "Failed to seed object %s", key)
}

// objectStoreYAML returns the indented spec.backup.objectStore block pointing at
// the shared in-cluster object store. indent is the leading whitespace for the
// `objectStore` key so the snippet can be embedded under spec.backup.
func objectStoreYAML(indent string) string {
	lines := []string{
		"objectStore:",
		"  endpoint: " + objectStoreEndpoint(),
		"  region: us-east-1",
		"  bucket: " + objectStoreBucket,
		"  forcePathStyle: true",
		"  credentials:",
		"    accessKeyId:",
		"      name: " + objectStoreCredsSecret,
		"      key: ACCESS_KEY_ID",
		"    secretAccessKey:",
		"      name: " + objectStoreCredsSecret,
		"      key: SECRET_ACCESS_KEY",
	}
	for i, l := range lines {
		lines[i] = indent + l
	}
	return strings.Join(lines, "\n")
}

// objectStoreManifest renders the single-node S3 store (SeaweedFS) plus its PVC,
// Service, credentials Secret and bucket-creation Job into the given namespace.
//
// The namespace is an explicit parameter rather than read from testNamespace:
// the suite renders this for the shared object-store namespace, and a rendered
// manifest cannot be retargeted by string replacement without also rewriting any
// unrelated occurrence of the old namespace name.
func objectStoreManifest(namespace string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %[1]s-config
  namespace: %[2]s
data:
  s3.json: |
    {
      "identities": [
        {
          "name": "cnmsql",
          "credentials": [{"accessKey": "%[6]s", "secretKey": "%[7]s"}],
          "actions": ["Admin", "Read", "Write", "List", "Tagging"]
        }
      ]
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app: %[1]s
spec:
  replicas: 1
  # The store owns a ReadWriteOnce PVC, so a rolling update would deadlock on the
  # old Pod still holding the volume. Recreate detaches first.
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
      containers:
      - name: %[1]s
        image: %[3]s
        args:
        - server
        - -dir=/data
        - -s3
        - -s3.port=%[4]d
        - -s3.config=/etc/seaweedfs/s3.json
        ports:
        - containerPort: %[4]d
        volumeMounts:
        - name: data
          mountPath: /data
        - name: config
          mountPath: /etc/seaweedfs
        # Reserve headroom so the store is not evicted under node memory pressure
        # mid-suite; a single-replica restart otherwise refuses connections for
        # the whole detach/reattach + boot window and flakes the backup specs.
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            memory: 768Mi
        readinessProbe:
          httpGet:
            path: /healthz
            port: %[4]d
          initialDelaySeconds: 5
          periodSeconds: 3
        livenessProbe:
          httpGet:
            path: /healthz
            port: %[4]d
          # The store brings up master, volume, filer and S3 in one process and
          # replays its metadata on restart; that takes appreciably longer than
          # the readiness gate, so the liveness probe must not reap it mid-boot.
          initialDelaySeconds: 60
          periodSeconds: 10
          failureThreshold: 6
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: %[1]s-data
      - name: config
        configMap:
          name: %[1]s-config
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[1]s-data
  namespace: %[2]s
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 2Gi
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app: %[1]s
  ports:
  - port: %[4]d
    targetPort: %[4]d
---
apiVersion: v1
kind: Secret
metadata:
  name: %[5]s
  namespace: %[2]s
stringData:
  ACCESS_KEY_ID: %[6]s
  SECRET_ACCESS_KEY: %[7]s
---
apiVersion: batch/v1
kind: Job
metadata:
  name: %[1]s-mkbucket
  namespace: %[2]s
spec:
  backoffLimit: 20
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: s3client
        image: %[8]s
        command:
        - /bin/sh
        - -c
        - |
          set -e
          until rclone lsd %[10]s: >/dev/null 2>&1; do sleep 2; done
          rclone mkdir %[10]s:%[9]s
          rclone lsd %[10]s:
        env:
%[11]s
`, objectStoreName, namespace, objectStoreImage, objectStorePort, objectStoreCredsSecret,
		objectStoreAccessKey, objectStoreSecretKey, s3ClientImage, objectStoreBucket,
		s3Remote, s3ClientEnvYAML("        ",
			fmt.Sprintf("http://%s.%s.svc:%d", objectStoreName, namespace, objectStorePort)))
}
