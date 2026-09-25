//go:build e2e
// +build e2e

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// s3ToolboxName is the Deployment name of the long-lived S3 client toolbox.
const s3ToolboxName = "s3-toolbox"

// setupS3Client deploys a long-lived S3 client toolbox Pod with the bucket
// credentials pre-wired through the rclone remote environment, so the archiving
// specs can read archive objects synchronously with `kubectl exec` (fast enough
// to poll inside Eventually, unlike a per-poll Job).
func setupS3Client() {
	By("deploying the S3 client toolbox pod")
	applyManifest(s3ToolboxName, s3ToolboxManifest())
	_, err := kubectl("wait", "deployment/"+s3ToolboxName, "-n", testNamespace,
		"--for=condition=Available", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "S3 client toolbox did not become available")
}

func teardownS3Client() {
	deleteManifest(s3ToolboxName, s3ToolboxManifest())
}

// s3Exec runs a command in the toolbox pod and returns its combined output.
func s3Exec(args ...string) (string, error) {
	full := append([]string{"exec", "deploy/" + s3ToolboxName, "-n", testNamespace, "--"}, args...)
	return kubectl(full...)
}

// rcloneExec runs an rclone subcommand in the toolbox pod. rclone writes progress
// and retry chatter to stderr, which kubectl folds into the combined output the
// specs parse, so every call is quiet by default.
func rcloneExec(args ...string) (string, error) {
	return s3Exec(append([]string{"rclone", "--quiet", "--retries=1"}, args...)...)
}

// dumpBackupWorkerLogs prints recent logs from the backup worker Job's Pod(s) for
// the given Backup. When a backup reaches terminal phase=failed the worker's own
// output is what explains why (object-store auth, xtrabackup, connectivity); the
// operator log only reports that the Job failed. The worker Job is named
// "<backup>-backup", so its Pods carry the job-name label set by the Job
// controller.
func dumpBackupWorkerLogs(backup string) {
	selector := "job-name=" + backup + "-backup"
	out, err := kubectl("logs", "-n", testNamespace, "-l", selector,
		"--all-containers", "--tail=200", "--prefix")
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "\nFailed to collect backup worker logs (%s): %v\n%s\n",
			selector, err, out)
		return
	}
	_, _ = fmt.Fprintf(GinkgoWriter, "\nbackup worker logs (%s):\n%s\n", selector, out)
}

// expectBackupCompleted waits for a Backup to reach phase=completed. The worker
// Job runs with backoffLimit 1, so phase=failed is terminal and stops the wait
// at once instead of burning the timeout. Either way out, it dumps what explains
// the failure: the Backup's own error, the worker Job and its logs, and the
// object store's logs, none of which the cluster-level dump collects.
func expectBackupCompleted(name string, timeout time.Duration) {
	GinkgoHelper()
	var phase, failure string
	// A local Gomega records the failure instead of aborting the spec, so the
	// diagnostics below still get to run.
	waiter := NewGomega(func(message string, _ ...int) { failure = message })
	waiter.Eventually(func(g Gomega) {
		var err error
		phase, err = kubectl("get", "backup", name, "-n", testNamespace,
			"-o", "jsonpath={.status.phase}")
		g.Expect(err).NotTo(HaveOccurred())
		if phase == "failed" {
			StopTrying("backup " + name + " reached terminal phase=failed").Now()
		}
		g.Expect(phase).To(Equal("completed"), "backup %s not completed yet", name)
	}, e2eTimeout(timeout), 5*time.Second).Should(Succeed())
	if failure != "" {
		By(fmt.Sprintf("backup %s did not complete (phase %q); dumping diagnostics", name, phase))
		dumpBackupDiagnostics(name)
		Fail(fmt.Sprintf("backup %s did not complete (last phase %q): %s", name, phase, failure))
	}
}

// dumpBackupDiagnostics prints everything needed to tell a worker failure (auth,
// xtrabackup, a stalled source stream) from an object-store failure, followed by
// the suite-wide cluster diagnostics.
func dumpBackupDiagnostics(backup string) {
	job := backup + "-backup"
	store := "deployment/" + objectStoreName
	dumps := []struct {
		name string
		args []string
	}{
		{name: "backup", args: []string{"get", "backup", backup, "-n", testNamespace, "-o", "yaml"}},
		{name: "backup worker job", args: []string{"describe", "job", job, "-n", testNamespace}},
		{name: "backup worker pods", args: []string{"describe", "pods", "-n", testNamespace, "-l", "job-name=" + job}},
		{name: "object store pods", args: []string{"get", "pods", "-n", currentObjectStoreNamespace, "-o", "wide"}},
		{name: "object store logs", args: []string{"logs", store, "-n", currentObjectStoreNamespace, "--tail=200"}},
		{name: "object store previous logs",
			args: []string{"logs", store, "-n", currentObjectStoreNamespace, "--previous", "--tail=100"}},
	}
	for _, dump := range dumps {
		out, err := kubectl(dump.args...)
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "\nFailed to collect %s: %v\n%s\n", dump.name, err, out)
			continue
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "\n%s:\n%s\n", dump.name, out)
	}
	dumpBackupWorkerLogs(backup)
	dumpE2EDiagnostics()
}

// readArchiveIndex fetches and decodes the cluster-level binlog archive index
// (`<cluster>/binlogs/_index.json`) from object storage. A missing index (the
// archiver has not written one yet) surfaces as an error so callers can poll.
func readArchiveIndex(cluster string) (objectstore.ArchiveIndex, error) {
	var idx objectstore.ArchiveIndex
	key := objectKey("%s/binlogs/_index.json", cluster)
	out, err := rcloneExec("cat", key)
	if err != nil {
		return idx, fmt.Errorf("reading archive index %s: %w (%s)", key, err, out)
	}
	if err := json.Unmarshal([]byte(out), &idx); err != nil {
		return idx, fmt.Errorf("decoding archive index %s: %w (%s)", key, err, out)
	}
	return idx, nil
}

// flushBinaryLogs returns the primary's gtid_executed, then rotates its active
// binary log so every transaction up to that position lands in an immutable,
// archivable file.
//
// The position is read before the rotation, not after. The primary is never
// quiescent — the instance manager stamps the heartbeat table once a second — so
// a position read after the flush already includes transactions that landed in
// the new, still-open file, which the archiver cannot ship until something closes
// it. Such a target is unreachable: the recovery guard rejects it as beyond the
// archived coverage. Reading first pins the target inside the file the flush is
// about to close, which is the next file the archiver ships.
func flushBinaryLogs(cluster, primary, password string) string {
	executed := gtidExecuted(primary, password)
	_, err := mysqlExec(primary, "root", rootPassword(cluster), "",
		"FLUSH BINARY LOGS")
	Expect(err).NotTo(HaveOccurred(), "FLUSH BINARY LOGS failed on %s", primary)
	return executed
}

// gtidExecuted reads @@GLOBAL.gtid_executed from an instance as the app user.
func gtidExecuted(pod, password string) string {
	out, err := mysqlExec(pod, "app", password, "",
		"SELECT @@GLOBAL.gtid_executed")
	Expect(err).NotTo(HaveOccurred(), "reading gtid_executed from %s", pod)
	return parseSingleValue(out)
}

// parseSingleValue extracts the value of the first column from mysql -e output.
// Since mysqlExec always passes -N (skip column names), there is no header line
// to discard. The function joins every non-empty line after filtering out stderr
// noise that CombinedOutput folds in: MySQL password warnings and kubectl's
// "Defaulted container" notice.
//
// MySQL's batch mode (-e on a non-tty) escapes control characters embedded in a
// value as two-character literals: gtid_executed wraps its UUID sets across lines
// as "...,\n..." which arrives as a backslash followed by 'n', not a real
// newline. Left in place that bogus "\n"-prefixed token parses as a phantom UUID
// that the real archive coverage can never contain, so strip the literal \n and
// \t escapes to reconstruct a contiguous, valid GTID set.
func parseSingleValue(out string) string {
	var parts []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" ||
			strings.HasPrefix(line, "mysql:") ||
			strings.HasPrefix(line, "[Warning]") ||
			strings.HasPrefix(line, "Defaulted container") ||
			strings.Contains(line, "can be insecure") {
			continue
		}
		parts = append(parts, line)
	}
	joined := strings.Join(parts, "")
	joined = strings.ReplaceAll(joined, `\n`, "")
	joined = strings.ReplaceAll(joined, `\t`, "")
	return joined
}

// rootPassword returns the decoded root password for a Cluster, read from the
// operator-generated `<cluster>-root` Secret.
func rootPassword(cluster string) string {
	out, err := kubectl("get", "secret", cluster+"-root", "-n", testNamespace,
		"-o", "jsonpath={.data.password}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read root password for %s", cluster)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(out))
	Expect(err).NotTo(HaveOccurred(), "Failed to decode root password for %s", cluster)
	return string(decoded)
}

// expectArchiveCovers blocks until the cluster's archive index reports a covered
// GTID set that is a superset of want — i.e. every committed transaction up to
// want has been durably shipped to object storage with no gap.
func expectArchiveCovers(cluster, want string, timeout time.Duration) {
	GinkgoHelper()
	expectFlavorArchiveCovers(cluster, engine.FlavorMySQL, want, timeout)
}

// expectMariadbArchiveCovers is expectArchiveCovers for a MariaDB source, whose
// GTID sets are domain-server-sequence triples rather than MySQL's UUID form and
// so need the MariaDB containment model. This is the same comparison the
// operator's recovery guard makes before it admits a targetGTID, so waiting on it
// is what makes a PITR spec's target admissible rather than merely likely to be.
func expectMariadbArchiveCovers(cluster, want string, timeout time.Duration) {
	GinkgoHelper()
	expectFlavorArchiveCovers(cluster, engine.FlavorMariaDB, want, timeout)
}

func expectFlavorArchiveCovers(cluster string, flavor engine.Flavor, want string, timeout time.Duration) {
	GinkgoHelper()
	// An empty want would make containment trivially true: the assertion would
	// pass without proving anything. Every caller captures a GTID position after a
	// seed+flush, so it must be non-empty; refuse the vacuous case loudly.
	Expect(want).NotTo(BeEmpty(),
		"refusing to assert archive coverage of an empty GTID set (position parsed empty?)")
	eng, err := engine.ForFlavor(flavor)
	Expect(err).NotTo(HaveOccurred())
	timeout = e2eTimeout(timeout)
	Eventually(func(g Gomega) {
		idx, err := readArchiveIndex(cluster)
		// The archiver writes the index only after it ships a rotated file, so a
		// missing index usually means it has shipped nothing. Its own reported
		// failure reason is far more actionable than the client's "not found", so
		// fold it into the message — computed only on failure to spare the happy
		// path three kubectl calls per poll.
		if err != nil {
			g.Expect(err).NotTo(HaveOccurred(), "archive index unreadable: %s", archivingDiagnostics(cluster))
		}
		covers, err := eng.GTID().Contains(idx.CoveredGTIDSet, want)
		g.Expect(err).NotTo(HaveOccurred())
		if !covers {
			g.Expect(covers).To(BeTrue(),
				"archive covered=%q does not yet contain executed=%q (%s)",
				idx.CoveredGTIDSet, want, archivingDiagnostics(cluster))
		}
	}, timeout, 5*time.Second).Should(Succeed())
}

// archivingDiagnostics returns a compact one-line summary of the cluster's
// self-reported continuous-archiving status, so a coverage timeout explains why
// the archiver is stuck instead of only reporting a missing object.
func archivingDiagnostics(cluster string) string {
	cond, _ := clusterField(cluster, "{.status.conditions[?(@.type=='ContinuousArchiving')].status}")
	last, _ := clusterField(cluster, "{.status.continuousArchiving.lastArchivedBinlog}")
	reason, _ := clusterField(cluster, "{.status.continuousArchiving.lastFailureReason}")
	return fmt.Sprintf("condition=%q lastArchivedBinlog=%q lastFailureReason=%q", cond, last, reason)
}

// continuousArchivingClusterManifest renders a Cluster with continuous binlog
// archiving enabled, pinned to a specific instance image version. A tight RPO
// and small max binlog size keep the archiving loop active during the short
// lifetime of an e2e spec.
func continuousArchivingClusterManifest(name, version string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
%[7]s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
`, name, testNamespace, instances, instanceImageFor(version), e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}

func s3ToolboxManifest() string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app: %[1]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
      containers:
      - name: s3client
        image: %[3]s
        command: ["/bin/sh", "-c", "sleep infinity"]
        env:
%[4]s
`, s3ToolboxName, testNamespace, s3ClientImage, s3ClientEnvYAML("        ", objectStoreEndpoint()))
}
