//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The MariaDB counterpart of "Reinit union PITR": the same re-clone-and-promote
// dance that forces recovery to union cross-incarnation segments, but on MariaDB.
// It is the end-to-end proof of the range-based segment selection
// (SelectMariadbSegments) and merge-by-sequence positional replay
// (PlanMariadbPositional) — MariaDB's GTID position is a contiguous per-domain
// frontier that cannot express the gap a re-clone leaves in any single server's
// history, so recovery must stitch the fresh-identity segment onto the earlier ones
// by sequence. Runs in the dedicated mariadb-heavy lane; SQL goes through the
// mariadb client and the target is a MariaDB GTID (@@gtid_current_pos).
var _ = Describe("MariaDB reinit union PITR", Ordered, Label("flavor", "mariadb", "heavy"), func() {
	const (
		sourceCluster   = "mdb-reinit-union-src"
		restoredCluster = "mdb-reinit-union-restored"
		backupName      = "mdb-reinit-union-base"
	)

	f := reinitUnionFlavor{
		writeRows:    writeMariadbStressRows,
		flushCapture: flushMariadbBinaryLogs,
		verifySanity: verifyMariadbStressDataSanity,
		createTable: func(pod, password string) {
			GinkgoHelper()
			_, err := mariadbExec(pod, "app", password, "app",
				"CREATE TABLE stress_test (id INT PRIMARY KEY, phase VARCHAR(16), ts BIGINT);")
			Expect(err).NotTo(HaveOccurred(), "failed to create stress table")
		},
		probeWritable: func(g Gomega, pod, password string) {
			_, err := mariadbExec(pod, "app", password, "app",
				"INSERT INTO stress_test VALUES (-1, 'probe', UNIX_TIMESTAMP()); DELETE FROM stress_test WHERE id = -1;")
			g.Expect(err).NotTo(HaveOccurred(), "new primary is not writable yet")
		},
		sourceManifest: mariadbReinitUnionClusterManifest,
		pitrManifest: func(name, backup, targetGTID string) string {
			return mariadbPITRClusterManifest(name, backup, sourceCluster, targetGTID)
		},
		awaitArchive: func(cluster, target string) {
			expectMariadbArchiveCovers(cluster, target, 8*time.Minute)
		},
	}

	runReinitUnionPITR(f, sourceCluster, restoredCluster, backupName)
})

// mariadbReinitUnionClusterManifest is a 2-instance MariaDB cluster with continuous
// binlog archiving enabled, the source of the reinit-union PITR run. It mirrors
// mariadbStressArchivingClusterManifest but with instances: 2, so that after a
// re-init the sole surviving replica is deterministically promoted.
func mariadbReinitUnionClusterManifest(name string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  flavor: mariadb
  instances: 2
  imageName: %s
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
%s
    continuousArchiving:
      enabled: true
      targetRPOSeconds: 10
      maxBinlogSizeMB: 1
`, name, testNamespace, mariadbImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
}
