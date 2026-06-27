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

// minioNamespace is the shared namespace where MinIO runs once for the whole
// suite, avoiding per-Describe deploy/teardown cycles.
const minioNamespace = "e2e-minio"

// minioBucket is the bucket pre-created in the in-cluster MinIO and targeted by
// the backup/recovery specs.
const minioBucket = "cnmsql-backups"

// minioCredsSecret is the Secret holding the MinIO access credentials consumed
// by Clusters and Backups through their object-store configuration.
const minioCredsSecret = "minio-creds"

const e2eInstanceResources = `  resources:
    requests:
      cpu: 100m
      memory: 384Mi
    limits:
      cpu: "1"
      memory: 1536Mi
`

const e2eMySQLParameters = `    parameters:
      innodb_buffer_pool_size: 128M
      max_connections: "80"
`

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
		"clusters,backups,scheduledbackups,databases,databaseusers,imagecatalogs",
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
		"clusters,backups,scheduledbackups,databases,databaseusers,imagecatalogs",
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

// expectClusterReady blocks until the named Cluster reports Ready with the
// expected number of ready instances and an elected primary. On timeout it dumps
// cluster-wide diagnostics (Cluster status, Pod states, events, operator logs,
// node capacity) before failing: a cluster that never converges is the most
// common e2e failure, and without this dump the CI logs reveal nothing about why.
func expectClusterReady(name string, instances int, timeout time.Duration) {
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
	for {
		lastErr := check()
		if lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			By(fmt.Sprintf("cluster %s did not become ready within %s; dumping diagnostics", name, timeout))
			dumpE2EDiagnostics()
			Fail(fmt.Sprintf("cluster %s did not become ready within %s: %v", name, timeout, lastErr))
		}
		time.Sleep(5 * time.Second)
	}
}

// clusterPrimary returns the current primary Pod name for a cluster.
func clusterPrimary(name string) string {
	primary, err := clusterField(name, "{.status.currentPrimary}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read currentPrimary for %s", name)
	Expect(primary).NotTo(BeEmpty(), "Cluster %s has no primary", name)
	return primary
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

// minioEndpoint returns the HTTP endpoint for the shared in-cluster MinIO, which
// runs once in minioNamespace and is reachable from every test namespace.
func minioEndpoint() string {
	return fmt.Sprintf("http://minio.%s.svc:9000", minioNamespace)
}

// deploySharedMinio creates the shared MinIO namespace and deploys a single-node
// MinIO instance once for the whole suite. This avoids per-Describe deploy/teardown
// cycles (each ~6 minutes), saving significant wall-clock time in parallel runs
// since every Describe that needs an object store stands up and tears down its own
// MinIO instance.
func deploySharedMinio() {
	By("creating shared MinIO namespace")
	_, _ = kubectl("create", "ns", minioNamespace)

	By("deploying shared in-cluster MinIO")
	applyManifest("minio-shared", sharedMinioManifest())

	By("waiting for shared MinIO to become available")
	_, err := kubectl("wait", "deployment/minio", "-n", minioNamespace,
		"--for=condition=Available", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "Shared MinIO did not become available")

	By("waiting for the shared MinIO bucket-creation Job to complete")
	_, err = kubectl("wait", "job/minio-mkbucket", "-n", minioNamespace,
		"--for=condition=Complete", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "Shared MinIO bucket-creation Job did not complete")
}

// teardownSharedMinio removes the shared MinIO namespace and all its resources.
func teardownSharedMinio() {
	_, _ = kubectl("delete", "ns", minioNamespace, "--ignore-not-found", "--wait=false")
}

// ensureMinioCreds creates the minio-creds Secret in the current testNamespace
// so Cluster CRs can reference it locally. The shared MinIO runs in minioNamespace
// and this Secret mirrors its credentials in each test namespace.
func ensureMinioCreds() {
	_, _ = kubectl("delete", "secret", minioCredsSecret, "-n", testNamespace, "--ignore-not-found")
	_, err := kubectl("create", "secret", "generic", minioCredsSecret, "-n", testNamespace,
		"--from-literal=ACCESS_KEY_ID=minioadmin",
		"--from-literal=SECRET_ACCESS_KEY=minioadmin")
	Expect(err).NotTo(HaveOccurred(), "Failed to create %s secret in %s", minioCredsSecret, testNamespace)
}

// setupMinio ensures the shared MinIO is ready and creates the credentials Secret
// in the current test namespace. The shared MinIO is deployed once by the suite,
// so this is a fast idempotent operation per Describe.
func setupMinio() {
	ensureMinioCreds()
}

// teardownMinio removes the per-namespace credentials Secret. The shared MinIO
// instance survives across Describes and is torn down by SynchronizedAfterSuite.
func teardownMinio() {
	_, _ = kubectl("delete", "secret", minioCredsSecret, "-n", testNamespace, "--ignore-not-found")
}

// seedObjectStoreMarker writes a small object at the given key in the MinIO
// bucket via a one-shot mc Job and waits for it to complete. It is used to make
// a destination prefix non-empty deterministically. The Job targets the shared
// MinIO in minioNamespace.
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
      - name: mc
        image: minio/mc:latest
        command:
        - sh
        - -c
        - |
          until mc alias set local %[3]s minioadmin minioadmin; do sleep 2; done
          echo cnmsql-guard-marker | mc pipe local/%[4]s/%[5]s
`, name, testNamespace, minioEndpoint(), minioBucket, key)
	applyManifest(name, manifest)
	DeferCleanup(func() {
		deleteManifest(name, manifest)
	})
	_, err := kubectl("wait", "job/"+name, "-n", testNamespace,
		"--for=condition=Complete", "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "Failed to seed object %s", key)
}

// objectStoreYAML returns the indented spec.backup.objectStore block pointing at
// the shared in-cluster MinIO. indent is the leading whitespace for the
// `objectStore` key so the snippet can be embedded under spec.backup.
func objectStoreYAML(indent string) string {
	lines := []string{
		"objectStore:",
		"  endpoint: " + minioEndpoint(),
		"  region: us-east-1",
		"  bucket: " + minioBucket,
		"  forcePathStyle: true",
		"  credentials:",
		"    accessKeyId:",
		"      name: " + minioCredsSecret,
		"      key: ACCESS_KEY_ID",
		"    secretAccessKey:",
		"      name: " + minioCredsSecret,
		"      key: SECRET_ACCESS_KEY",
	}
	for i, l := range lines {
		lines[i] = indent + l
	}
	return strings.Join(lines, "\n")
}

func minioManifest() string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: %[1]s
  labels:
    app: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args: ["server", "/data", "--console-address", ":9001"]
        env:
        - name: MINIO_ROOT_USER
          value: minioadmin
        - name: MINIO_ROOT_PASSWORD
          value: minioadmin
        ports:
        - containerPort: 9000
        volumeMounts:
        - name: data
          mountPath: /data
        readinessProbe:
          httpGet:
            path: /minio/health/ready
            port: 9000
          initialDelaySeconds: 5
          periodSeconds: 3
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: minio-data
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-data
  namespace: %[1]s
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: %[1]s
spec:
  selector:
    app: minio
  ports:
  - port: 9000
    targetPort: 9000
---
apiVersion: v1
kind: Secret
metadata:
  name: %[2]s
  namespace: %[1]s
stringData:
  ACCESS_KEY_ID: minioadmin
  SECRET_ACCESS_KEY: minioadmin
---
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-mkbucket
  namespace: %[1]s
spec:
  backoffLimit: 20
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: minio/mc:latest
        command:
        - sh
        - -c
        - |
          until mc alias set local http://minio.%[1]s.svc:9000 minioadmin minioadmin; do sleep 2; done
          mc mb --ignore-existing local/%[3]s
          mc ls local
`, testNamespace, minioCredsSecret, minioBucket)
}

// sharedMinioManifest returns the MinIO manifest for the shared namespace. It is
// identical to minioManifest but uses minioNamespace instead of testNamespace so
// the suite deploys MinIO once in a dedicated namespace reachable from all tests.
func sharedMinioManifest() string {
	m := minioManifest()
	// Replace every occurrence of the per-test namespace with the shared one.
	// minioManifest uses %[1]s=testNamespace — the formatted string contains the
	// actual namespace name, so we do a plain string replace.
	return strings.ReplaceAll(m, testNamespace, minioNamespace)
}
