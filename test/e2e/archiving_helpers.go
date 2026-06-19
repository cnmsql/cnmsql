//go:build e2e
// +build e2e

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/objectstore"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
)

// archiveVersions is the set of Percona versions the continuous-archiving specs
// run against. Precedence:
//   - E2E_MYSQL_VERSION (single version): the CI matrix model — each job pins
//     one MySQL version and the whole suite runs against it.
//   - E2E_ARCHIVE_VERSIONS (comma-separated list): explicit local override to
//     exercise several versions in one cluster.
//   - default: the full supported matrix, so a bare local run proves archiving
//     broadly compatible.
func archiveVersions() []string {
	if v := strings.TrimSpace(os.Getenv("E2E_MYSQL_VERSION")); v != "" {
		return []string{v}
	}
	if raw := strings.TrimSpace(os.Getenv("E2E_ARCHIVE_VERSIONS")); raw != "" {
		var out []string
		for v := range strings.SplitSeq(raw, ",") {
			if v = strings.TrimSpace(v); v != "" {
				out = append(out, v)
			}
		}
		return out
	}
	return []string{"8.0", "8.4", "9.x"}
}

// sampleVersion is the MySQL version used by the non-archiving sample Clusters
// (the bulk of the suite). Under the CI matrix model it follows
// E2E_MYSQL_VERSION so every spec runs against the job's pinned version;
// otherwise it defaults to 8.4 (the historical sample version).
func sampleVersion() string {
	if v := strings.TrimSpace(os.Getenv("E2E_MYSQL_VERSION")); v != "" {
		return v
	}
	return "8.4"
}

// instanceImageFor returns the published slim instance image reference for a
// version. The images are built and pushed from the separate containers repo;
// the suite pulls them and loads them into Kind (see pullAndLoadInstanceImage).
func instanceImageFor(version string) string {
	return instanceImageRepo + ":" + version
}

// instanceImageRepo is the GHCR repository the containers repo publishes the
// slim instance images to. Override with E2E_INSTANCE_IMAGE_REPO to test against
// a fork or a private mirror.
var instanceImageRepo = func() string {
	if v := strings.TrimSpace(os.Getenv("E2E_INSTANCE_IMAGE_REPO")); v != "" {
		return v
	}
	return "ghcr.io/cloudnative-mysql/cloudnative-mysql-instance"
}()

// setupMC deploys a long-lived mc (MinIO client) toolbox Pod with the bucket
// credentials pre-wired through MC_HOST_local, so the archiving specs can read
// archive objects synchronously with `kubectl exec` (fast enough to poll inside
// Eventually, unlike a per-poll Job).
func setupMC() {
	By("deploying the mc toolbox pod")
	applyManifest("mc-toolbox", mcToolboxManifest())
	_, err := kubectl("wait", "deployment/mc-toolbox", "-n", testNamespace,
		"--for=condition=Available", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "mc toolbox did not become available")
}

func teardownMC() {
	deleteManifest("mc-toolbox", mcToolboxManifest())
}

// mcExec runs an mc command in the toolbox pod and returns its combined output.
func mcExec(args ...string) (string, error) {
	full := append([]string{"exec", "deploy/mc-toolbox", "-n", testNamespace, "--"}, args...)
	return kubectl(full...)
}

// readArchiveIndex fetches and decodes the cluster-level binlog archive index
// (`<cluster>/binlogs/_index.json`) from object storage. A missing index (the
// archiver has not written one yet) surfaces as an error so callers can poll.
func readArchiveIndex(cluster string) (objectstore.ArchiveIndex, error) {
	var idx objectstore.ArchiveIndex
	key := fmt.Sprintf("local/%s/%s/binlogs/_index.json", minioBucket, cluster)
	out, err := mcExec("mc", "--quiet", "cat", key)
	if err != nil {
		return idx, fmt.Errorf("reading archive index %s: %w (%s)", key, err, out)
	}
	if err := json.Unmarshal([]byte(out), &idx); err != nil {
		return idx, fmt.Errorf("decoding archive index %s: %w (%s)", key, err, out)
	}
	return idx, nil
}

// flushBinaryLogs forces the primary to rotate its active binary log so every
// committed transaction lands in an immutable, archivable file. Returns the
// gtid_executed captured immediately after the flush.
func flushBinaryLogs(cluster, primary, password string) string {
	_, err := mysqlExec(primary, "root", rootPassword(cluster), "",
		"FLUSH BINARY LOGS")
	Expect(err).NotTo(HaveOccurred(), "FLUSH BINARY LOGS failed on %s", primary)
	return gtidExecuted(primary, password)
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
	// An empty want would make GTIDContains trivially true: the assertion would
	// pass without proving anything. Every caller captures gtid_executed after a
	// seed+flush, so it must be non-empty; refuse the vacuous case loudly.
	ExpectWithOffset(1, want).NotTo(BeEmpty(),
		"refusing to assert archive coverage of an empty GTID set (gtid_executed parsed empty?)")
	timeout = e2eTimeout(timeout)
	Eventually(func(g Gomega) {
		idx, err := readArchiveIndex(cluster)
		// The archiver writes the index only after it ships a rotated file, so a
		// missing index usually means it has shipped nothing. Its own reported
		// failure reason is far more actionable than mc's "object not found", so
		// fold it into the message — computed only on failure to spare the happy
		// path three kubectl calls per poll.
		if err != nil {
			g.Expect(err).NotTo(HaveOccurred(), "archive index unreadable: %s", archivingDiagnostics(cluster))
		}
		covers, err := replication.GTIDContains(idx.CoveredGTIDSet, want)
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
	return fmt.Sprintf(`apiVersion: mysql.cloudnative-mysql.io/v1alpha1
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

func mcToolboxManifest() string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: mc-toolbox
  namespace: %[1]s
  labels:
    app: mc-toolbox
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mc-toolbox
  template:
    metadata:
      labels:
        app: mc-toolbox
    spec:
      containers:
      - name: mc
        image: minio/mc:latest
        command: ["/bin/sh", "-c", "sleep infinity"]
        env:
        - name: MC_HOST_local
          value: http://minioadmin:minioadmin@minio.%[1]s.svc:9000
`, testNamespace)
}
