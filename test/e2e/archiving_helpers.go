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

	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
)

// archiveVersions is the set of Percona versions the continuous-archiving specs
// run against. It defaults to the full supported matrix so archiving is proven
// broadly compatible; override with E2E_ARCHIVE_VERSIONS (comma-separated) to
// narrow it for a faster local run.
func archiveVersions() []string {
	if raw := strings.TrimSpace(os.Getenv("E2E_ARCHIVE_VERSIONS")); raw != "" {
		var out []string
		for v := range strings.SplitSeq(raw, ",") {
			if v = strings.TrimSpace(v); v != "" {
				out = append(out, v)
			}
		}
		return out
	}
	return []string{"5.6", "8.0", "8.4", "9.x"}
}

// instanceImageFor returns the locally-built slim instance image tag for a
// version. It mirrors images/build.sh's default REGISTRY (cnmysql-instance).
func instanceImageFor(version string) string {
	return "cnmysql-instance:" + version
}

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

// parseSingleValue extracts the value row from a tab/newline mysql -e result,
// dropping the column header line.
func parseSingleValue(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return ""
	}
	return strings.TrimSpace(strings.Join(lines[1:], ""))
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
	Eventually(func(g Gomega) {
		idx, err := readArchiveIndex(cluster)
		g.Expect(err).NotTo(HaveOccurred())
		covers, err := replication.GTIDContains(idx.CoveredGTIDSet, want)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(covers).To(BeTrue(),
			"archive covered=%q does not yet contain executed=%q", idx.CoveredGTIDSet, want)
	}, timeout, 5*time.Second).Should(Succeed())
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
  mysql:
    binlogFormat: ROW
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
%[5]s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
`, name, testNamespace, instances, instanceImageFor(version), objectStoreYAML("    "))
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
