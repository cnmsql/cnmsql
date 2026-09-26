//go:build e2e

/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// Logical backups (design 028, phase 1): a SQL dump taken on a replica through
// the instance manager as cnmsql_dump, stored next to the physical backups
// without ever being mistaken for one. The same specs run on both flavors, since
// the dump tool, the account's grants and the snapshot position differ.
var _ = Describe("Logical backups", Ordered, Label("feature", "flavor"), func() {
	logicalBackupSpecs(logicalFlavor{
		prefix:        "lb",
		image:         instanceImage,
		tool:          "mysqldump",
		flavor:        "mysql",
		exec:          mysqlExec,
		operatorSpecs: true,
	})
})

var _ = Describe("MariaDB logical backups", Ordered, Label("flavor", "mariadb"), func() {
	logicalBackupSpecs(logicalFlavor{
		prefix:      "mlb",
		image:       mariadbImage,
		tool:        "mariadb-dump",
		flavor:      "mariadb",
		flavorYAML:  "  flavor: mariadb\n",
		exec:        mariadbExec,
		recordsGTID: true,
	})
})

// logicalFlavor is what the logical backup specs need to know about a flavor.
type logicalFlavor struct {
	// prefix names the flavor's namespace and resources.
	prefix string
	image  string
	// tool and flavor are what the dump's manifest must record.
	tool   string
	flavor string
	// flavorYAML is the Cluster's spec.flavor line, empty for the default.
	flavorYAML string
	// exec runs SQL through the image's client.
	exec func(pod, user, password, database, sql string) (string, error)
	// recordsGTID is whether the dump records its snapshot GTID: MariaDB
	// writes it in the dump, MySQL dumps run with --set-gtid-purged=OFF.
	recordsGTID bool
	// operatorSpecs runs the specs that exercise only operator logic
	// (schedules, retention), which one flavor is enough to cover.
	operatorSpecs bool
}

func logicalBackupSpecs(f logicalFlavor) {
	var (
		cluster      = f.prefix + "-src"
		physical     = f.prefix + "-physical"
		fullDump     = f.prefix + "-full"
		partialDump  = f.prefix + "-billing"
		missingDB    = f.prefix + "-missing"
		recovered    = f.prefix + "-recovered"
		imported     = f.prefix + "-imported"
		billingOnly  = f.prefix + "-billing-only"
		schedule     = f.prefix + "-nightly"
		again        = f.prefix + "-after-migration"
		refused      = f.prefix + "-restore-refused"
		restoreShop  = f.prefix + "-restore-shop"
		stalePrefix  = cluster + "/stale-dump/stale-id"
		dumpSecret   = cluster + "-dump"
		dumpReadyJSP = `{.status.conditions[?(@.type=="DumpAccountReady")].status}`
	)
	systemSchemas := []string{"mysql", "sys", "performance_schema", "information_schema", "heartbeat"}

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace(f.prefix + "-logical")

		setupObjectStore()
		DeferCleanup(teardownObjectStore)
		setupS3Client()
		DeferCleanup(teardownS3Client)

		By("creating a two-instance archiving cluster")
		applyManifest(cluster, logicalClusterManifest(f, cluster, 2))
		DeferCleanup(func() { deleteManifest(cluster, logicalClusterManifest(f, cluster, 2)) })
		expectClusterReady(cluster, 2, 20*time.Minute)

		By("seeding two application schemas")
		_, err := f.exec(clusterPrimary(cluster), "root", rootPassword(cluster), "",
			"CREATE DATABASE IF NOT EXISTS shop; "+
				"CREATE TABLE IF NOT EXISTS shop.items (id INT PRIMARY KEY, name VARCHAR(32)); "+
				"REPLACE INTO shop.items VALUES (1, 'widget'); "+
				"CREATE DATABASE IF NOT EXISTS billing; "+
				"CREATE TABLE IF NOT EXISTS billing.invoices (id INT PRIMARY KEY, total INT); "+
				"REPLACE INTO billing.invoices VALUES (1, 100);")
		Expect(err).NotTo(HaveOccurred())
	})

	It("creates the dump account without touching the instance Pods", func() {
		Eventually(func(g Gomega) {
			out, err := clusterField(cluster, dumpReadyJSP)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"), "DumpAccountReady is not true yet")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		out, err := kubectl("get", "pods", "-n", testNamespace, "-l", instanceSelector(cluster),
			"-o", "jsonpath={.items[*].spec.containers[*].env[*].valueFrom.secretKeyRef.name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).NotTo(ContainSubstring(dumpSecret), "the dump password must stay out of the instance Pods")
	})

	It("takes a physical backup first", func() {
		applyManifest(physical, backupManifest(physical, cluster))
		expectBackupCompleted(physical, 8*time.Minute)
	})

	It("takes a logical backup from the replica", func() {
		applyManifest(fullDump, logicalBackupManifest(fullDump, cluster, nil))
		expectBackupCompleted(fullDump, 8*time.Minute)

		status := backupStatus(fullDump)
		Expect(status.Method).To(Equal("logical"))
		Expect(status.InstanceName).NotTo(Equal(clusterPrimary(cluster)), "prefer-standby should dump a replica")
		Expect(status.Databases).To(ContainElements("app", "billing", "shop"))
		Expect(status.Databases).NotTo(ContainElement(BeElementOf(systemSchemas)),
			"system and operator schemas must never be dumped")
		Expect(status.SHA256).NotTo(BeEmpty())
		Expect(status.BeginBinlog).To(ContainSubstring(":"), "the snapshot binlog position should be recorded")
		if f.recordsGTID {
			Expect(status.BeginGTID).NotTo(BeEmpty(), "the snapshot GTID should be recorded")
		} else {
			Expect(status.BeginGTID).To(BeEmpty(), "this flavor's dumps carry no snapshot GTID")
		}
		Expect(status.DestinationPath).To(HaveSuffix("/dump.sql.zst"))

		By("checking the objects and the manifest in the store")
		prefix := fmt.Sprintf("%s/%s/%s", cluster, fullDump, status.BackupID)
		Expect(s3ObjectExists(objectKey("%s/dump.sql.zst", prefix))).To(BeTrue())
		Expect(s3ObjectExists(objectKey("%s/metadata.json", prefix))).To(BeFalse(),
			"a dump must not carry a base-backup manifest")
		raw, err := rcloneExec("cat", objectKey("%s/logical.json", prefix))
		Expect(err).NotTo(HaveOccurred())
		var meta objectstore.LogicalBackupMetadata
		Expect(json.Unmarshal([]byte(raw), &meta)).To(Succeed(), "logical.json: %s", raw)
		Expect(meta.Method).To(Equal("logical"))
		Expect(meta.Compression).To(Equal("zstd"))
		Expect(meta.SHA256).To(Equal(status.SHA256))
		Expect(meta.UncompressedBytes).To(BeNumerically(">", meta.SizeBytes))
		// The manifest must describe the dump well enough to load it from the
		// store alone.
		Expect(meta.Databases).To(Equal(status.Databases), "the manifest must record the dumped databases")
		Expect(meta.Tool).To(Equal(f.tool))
		Expect(meta.Flavor).To(Equal(f.flavor))
		Expect(meta.ServerVersion).NotTo(BeEmpty())
		Expect(meta.SnapshotGTID).To(Equal(status.BeginGTID))
		Expect(meta.ArchiveKey).To(Equal(fmt.Sprintf("%s/dump.sql.zst", prefix)))
		Expect(meta.FormatVersion).To(Equal(objectstore.LogicalFormatVersion))

		By("checking the archive is a real zstd stream")
		Expect(s3ObjectHead(objectKey("%s/dump.sql.zst", prefix), 4)).
			To(Equal([]byte{0x28, 0xB5, 0x2F, 0xFD}), "the dump archive must start with the zstd magic bytes")
	})

	It("dumps only the selected databases", func() {
		applyManifest(partialDump, logicalBackupManifest(partialDump, cluster, []string{"billing"}))
		expectBackupCompleted(partialDump, 8*time.Minute)
		Expect(backupStatus(partialDump).Databases).To(Equal([]string{"billing"}))
	})

	It("fails with a precise reason for a database that does not exist", func() {
		applyManifest(missingDB, logicalBackupManifest(missingDB, cluster, []string{"nope"}))
		Eventually(func(g Gomega) {
			reason, err := kubectl("get", "backup", missingDB, "-n", testNamespace,
				"-o", `jsonpath={.status.conditions[?(@.type=="Degraded")].reason}`)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(reason).To(Equal("InvalidDumpRequest"))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		// The instance refuses the request before streaming, so the worker
		// never starts an upload and nothing lands under the Backup's prefix.
		status := backupStatus(missingDB)
		Expect(status.BackupID).NotTo(BeEmpty(), "a failed backup still records its backupId")
		prefix := fmt.Sprintf("%s/%s/%s", cluster, missingDB, status.BackupID)
		Expect(s3ObjectExists(objectKey("%s/dump.sql.zst", prefix))).To(BeFalse())
		Expect(s3ObjectExists(objectKey("%s/logical.json", prefix))).To(BeFalse())
	})

	It("still recovers from the physical backup when newer dumps share the prefix", func() {
		applyManifest(recovered, rawRecoveryClusterManifest(f, recovered, cluster))
		DeferCleanup(func() { deleteManifest(recovered, rawRecoveryClusterManifest(f, recovered, cluster)) })
		expectClusterReady(recovered, 1, 20*time.Minute)
		Eventually(func(g Gomega) {
			// Recovery resets root to the recovered cluster's own Secret.
			out, err := f.exec(clusterPrimary(recovered), "root", rootPassword(recovered), "",
				"SELECT name FROM shop.items WHERE id = 1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("widget"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("imports the dump into a new cluster whose replicas clone it", func() {
		manifest := importClusterManifest(f, imported, 2, fmt.Sprintf(`      import:
        backup:
          name: %s
`, fullDump), "")
		applyManifest(imported, manifest)
		DeferCleanup(func() { deleteManifest(imported, manifest) })
		expectClusterReady(imported, 2, 20*time.Minute)

		primary := clusterPrimary(imported)
		replica := imported + "-1"
		if replica == primary {
			replica = imported + "-2"
		}
		By("checking the data on the primary and on the replica that cloned it")
		for _, pod := range []string{primary, replica} {
			Eventually(func(g Gomega) {
				out, err := f.exec(pod, "root", rootPassword(imported), "",
					"SELECT CONCAT((SELECT name FROM shop.items WHERE id = 1), '/', (SELECT SUM(total) FROM billing.invoices))")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("widget/100"), "on %s", pod)
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		}

		By("checking only the bootstrap primary ran the import")
		exitCode, err := kubectl("get", "pod", imported+"-1", "-n", testNamespace, "-o",
			`jsonpath={.status.initContainerStatuses[?(@.name=="import")].state.terminated.exitCode}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(exitCode).To(Equal("0"), "the import init container should have succeeded")
		containers, err := kubectl("get", "pod", imported+"-2", "-n", testNamespace, "-o",
			"jsonpath={.spec.initContainers[*].name}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Fields(containers)).NotTo(ContainElement("import"), "a replica clones, it does not import")
	})

	It("imports only the selected databases from a raw object-store source", func() {
		manifest := importClusterManifest(f, billingOnly, 1, fmt.Sprintf(`      import:
        source: %[1]s
        backupID: %[2]s
        databases:
          - billing
        postImportSQL:
          - CREATE TABLE billing.imported (id INT PRIMARY KEY)
          - INSERT INTO billing.imported VALUES (7)
`, cluster, backupStatus(fullDump).BackupID), fmt.Sprintf(`  externalClusters:
    - name: %s
%s
`, cluster, objectStoreYAML("      ")))
		applyManifest(billingOnly, manifest)
		DeferCleanup(func() { deleteManifest(billingOnly, manifest) })
		expectClusterReady(billingOnly, 1, 20*time.Minute)

		Eventually(func(g Gomega) {
			out, err := f.exec(clusterPrimary(billingOnly), "root", rootPassword(billingOnly), "",
				"SELECT CONCAT((SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'shop'), '/', "+
					"(SELECT SUM(total) FROM billing.invoices), '/', (SELECT id FROM billing.imported))")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("0/100/7"),
				"shop must be left out, billing and the post-import SQL must be there")
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("refuses to restore over a database that holds objects", func() {
		manifest := logicalRestoreManifest(refused, cluster, fullDump, "shop", "FailIfExists")
		applyManifest(refused, manifest)
		DeferCleanup(func() { deleteManifest(refused, manifest) })
		status := expectRestoreFinished(refused, 5*time.Minute)
		Expect(status.Phase).To(Equal("failed"))
		Expect(status.Reason).To(Equal("DatabaseNotEmpty"))
		Expect(status.Error).To(ContainSubstring("Nothing was changed on the cluster"))
		out, err := f.exec(clusterPrimary(cluster), "root", rootPassword(cluster), "", "SELECT name FROM shop.items WHERE id = 1")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("widget"))
	})

	It("restores one database into the running cluster, and the replica follows", func() {
		By("damaging both databases after the dump")
		_, err := f.exec(clusterPrimary(cluster), "root", rootPassword(cluster), "",
			"DELETE FROM shop.items; UPDATE billing.invoices SET total = 0;")
		Expect(err).NotTo(HaveOccurred())

		manifest := logicalRestoreManifest(restoreShop, cluster, fullDump, "shop", "DropAndRecreate")
		applyManifest(restoreShop, manifest)
		DeferCleanup(func() { deleteManifest(restoreShop, manifest) })
		status := expectRestoreFinished(restoreShop, 8*time.Minute)
		Expect(status.Phase).To(Equal("completed"), "restore failed: %s", status.Error)
		Expect(status.TargetInstance).To(Equal(clusterPrimary(cluster)))
		Expect(status.Databases).To(Equal([]string{"shop"}))

		By("checking shop is back on every instance and billing kept its damage")
		for _, pod := range []string{cluster + "-1", cluster + "-2"} {
			Eventually(func(g Gomega) {
				out, err := f.exec(pod, "root", rootPassword(cluster), "",
					"SELECT CONCAT((SELECT name FROM shop.items WHERE id = 1), '/', (SELECT SUM(total) FROM billing.invoices))")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(out)).To(Equal("widget/0"), "on %s", pod)
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
		}

		_, err = f.exec(clusterPrimary(cluster), "root", rootPassword(cluster), "",
			"UPDATE billing.invoices SET total = 100 WHERE id = 1;")
		Expect(err).NotTo(HaveOccurred())
	})

	if f.operatorSpecs {
		It("runs logical ScheduledBackups", func() {
			applyManifest(schedule, logicalScheduledBackupManifest(schedule, cluster))
			DeferCleanup(func() { deleteManifest(schedule, logicalScheduledBackupManifest(schedule, cluster)) })
			var generated string
			Eventually(func(g Gomega) {
				out, err := kubectl("get", "backups", "-n", testNamespace, "-o",
					`jsonpath={range .items[?(@.metadata.ownerReferences[0].name=="`+schedule+`")]}{.metadata.name}{"\n"}{end}`)
				g.Expect(err).NotTo(HaveOccurred())
				generated = strings.TrimSpace(strings.Split(out, "\n")[0])
				g.Expect(generated).NotTo(BeEmpty(), "the schedule has not created a Backup yet")
			}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
			expectBackupCompleted(generated, 8*time.Minute)
			Expect(backupStatus(generated).Method).To(Equal("logical"))
		})

		It("expires old dumps and leaves the base backups alone", func() {
			old := time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)
			s3Pipe(fmt.Sprintf(`{"formatVersion":1,"backupID":"stale-id","clusterName":"%s","backupName":"stale-dump",`+
				`"method":"logical","compression":"zstd","archiveKey":"%s/dump.sql.zst","databases":["shop"],`+
				`"startedAt":"%s","completedAt":"%s"}`, cluster, stalePrefix, old, old),
				objectKey("%s/logical.json", stalePrefix))
			s3Pipe("stale", objectKey("%s/dump.sql.zst", stalePrefix))
			// The throttle still holds from the pass the cluster ran when it went
			// ready, so nothing can reap the seeded dump before the patch below.
			Expect(s3ObjectExists(objectKey("%s/logical.json", stalePrefix))).To(BeTrue(),
				"the stale dump should exist before the retention pass")

			_, err := kubectl("patch", "cluster", cluster, "-n", testNamespace, "--subresource=status",
				"--type=merge", "-p", `{"status":{"lastRetentionRunTime":null}}`)
			Expect(err).NotTo(HaveOccurred())
			clusterAnnotate(cluster, "cnmsql.co/retention-nudge="+fmt.Sprint(time.Now().Unix()))

			Eventually(func(g Gomega) {
				g.Expect(s3ObjectExists(objectKey("%s/logical.json", stalePrefix))).To(BeFalse())
			}, e2eTimeout(5*time.Minute), 10*time.Second).Should(Succeed())

			out, err := rcloneExec("lsf", "-R", "--files-only", objectKey("%s/", cluster))
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(ContainSubstring(physical+"/"), "the base backup must survive")
			Expect(out).To(ContainSubstring(fullDump+"/"), "recent dumps must survive")
		})
	}

	It("recreates a lost dump account without restarting instances", func() {
		restartsBefore := instanceRestarts(cluster)
		accountCount := "SELECT COUNT(*) FROM mysql.user WHERE User = 'cnmsql_dump' AND Host = 'localhost'"

		By("dropping the account and its Secret, as on a cluster older than logical backups")
		_, err := f.exec(clusterPrimary(cluster), "root", rootPassword(cluster), "",
			"DROP USER IF EXISTS 'cnmsql_dump'@'localhost'")
		Expect(err).NotTo(HaveOccurred())
		// The operator only re-applies the account when its Secret changes, so the
		// account stays gone until the Secret is deleted below.
		out, err := f.exec(clusterPrimary(cluster), "root", rootPassword(cluster), "", accountCount)
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("0"), "the account should be gone before the Secret is deleted")
		_, err = kubectl("delete", "secret", dumpSecret, "-n", testNamespace)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the operator to recreate both")
		Eventually(func(g Gomega) {
			out, err := f.exec(clusterPrimary(cluster), "root", rootPassword(cluster), "", accountCount)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("1"))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("taking a logical backup with the new password")
		applyManifest(again, logicalBackupManifest(again, cluster, nil))
		expectBackupCompleted(again, 8*time.Minute)

		restartsAfter := instanceRestarts(cluster)
		Expect(restartsAfter).To(Equal(restartsBefore),
			"no instance may restart\nbefore: %s\nafter:  %s", restartsBefore, restartsAfter)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
}

type e2eBackupStatus struct {
	Method          string   `json:"method"`
	InstanceName    string   `json:"instanceName"`
	BackupID        string   `json:"backupId"`
	DestinationPath string   `json:"destinationPath"`
	SHA256          string   `json:"sha256"`
	BeginBinlog     string   `json:"beginBinlog"`
	BeginGTID       string   `json:"beginGTID"`
	Databases       []string `json:"databases"`
}

type e2eRestoreStatus struct {
	Phase          string   `json:"phase"`
	TargetInstance string   `json:"targetInstance"`
	Databases      []string `json:"databases"`
	Error          string   `json:"error"`
	// Reason is the Ready condition's reason.
	Reason string `json:"-"`
}

// expectRestoreFinished waits for a LogicalRestore to complete or fail and
// returns its status.
func expectRestoreFinished(name string, timeout time.Duration) e2eRestoreStatus {
	GinkgoHelper()
	var status e2eRestoreStatus
	Eventually(func(g Gomega) {
		out, err := kubectl("get", "logicalrestore", name, "-n", testNamespace, "-o", "jsonpath={.status}")
		g.Expect(err).NotTo(HaveOccurred())
		status = e2eRestoreStatus{}
		g.Expect(json.Unmarshal([]byte(out), &status)).To(Succeed(), "status: %s", out)
		g.Expect(status.Phase).To(BeElementOf("completed", "failed"), "restore %s is %q", name, status.Phase)
	}, e2eTimeout(timeout), 5*time.Second).Should(Succeed())
	reason, err := kubectl("get", "logicalrestore", name, "-n", testNamespace, "-o",
		`jsonpath={.status.conditions[?(@.type=="Ready")].reason}`)
	Expect(err).NotTo(HaveOccurred())
	status.Reason = reason
	return status
}

func logicalRestoreManifest(name, cluster, backup, database, policy string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: LogicalRestore
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  backup:
    name: %s
  databases:
    - %s
  policy: %s
`, name, testNamespace, cluster, backup, database, policy)
}

func backupStatus(name string) e2eBackupStatus {
	GinkgoHelper()
	out, err := kubectl("get", "backup", name, "-n", testNamespace, "-o", "jsonpath={.status}")
	Expect(err).NotTo(HaveOccurred())
	var status e2eBackupStatus
	Expect(json.Unmarshal([]byte(out), &status)).To(Succeed(), "backup status: %s", out)
	return status
}

// s3ObjectHead returns the first n bytes of an object in the store. The bytes
// travel base64-encoded so a binary archive survives the kubectl transport
// intact; a missing or short object fails the assertion.
func s3ObjectHead(key string, n int) []byte {
	GinkgoHelper()
	out, err := s3Exec("sh", "-c",
		fmt.Sprintf("rclone --quiet --retries=1 cat %s 2>/dev/null | head -c %d | base64 | tr -d '\\n'", key, n))
	Expect(err).NotTo(HaveOccurred(), "reading %s: %s", key, out)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(out))
	Expect(err).NotTo(HaveOccurred(), "decoding the head of %s: %q", key, out)
	Expect(decoded).To(HaveLen(n), "object %s holds fewer than %d bytes", key, n)
	return decoded
}

// instanceSelector matches a cluster's instance Pods only. Backup worker Pods
// carry the cluster label too, but not the mysql component label.
func instanceSelector(cluster string) string {
	return mysqlv1alpha1.ClusterLabelName + "=" + cluster + ",app.kubernetes.io/component=mysql"
}

// instanceRestarts returns every instance Pod's name, UID and container restart
// counts, which change if a Pod is recreated or a container restarts.
func instanceRestarts(cluster string) string {
	GinkgoHelper()
	out, err := kubectl("get", "pods", "-n", testNamespace, "-l", instanceSelector(cluster),
		"-o", `jsonpath={range .items[*]}{.metadata.name}/{.metadata.uid}={.status.containerStatuses[*].restartCount}; {end}`)
	Expect(err).NotTo(HaveOccurred())
	return out
}

// logicalFlavorCluster renders a Cluster of flavor f; specTail follows the
// shared spec fields: its bootstrap block and anything after it.
func logicalFlavorCluster(f logicalFlavor, name string, instances int, specTail string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
%s  instances: %d
  imageName: %s
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
%s`, name, testNamespace, f.flavorYAML, instances, f.image, e2eInstanceResources, e2eMySQLParameters, specTail)
}

func logicalClusterManifest(f logicalFlavor, name string, instances int) string {
	return logicalFlavorCluster(f, name, instances, `  bootstrap:
    initdb:
      database: app
      owner: app
  backup:
    retentionPolicy: 7d
`+objectStoreYAML("    ")+"\n")
}

func logicalBackupManifest(name, cluster string, databases []string) string {
	logical := ""
	if len(databases) > 0 {
		logical = "  logical:\n    databases:\n"
		for _, db := range databases {
			logical += "      - " + db + "\n"
		}
	}
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Backup
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  method: logical
%s`, name, testNamespace, cluster, logical)
}

func logicalScheduledBackupManifest(name, cluster string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: ScheduledBackup
metadata:
  name: %s
  namespace: %s
spec:
  cluster:
    name: %s
  schedule: "0 0 3 * * *"
  immediate: true
  method: logical
  logical:
    databases:
      - shop
`, name, testNamespace, cluster)
}

// rawRecoveryClusterManifest recovers from the source cluster's object-store
// prefix without a Backup object, which is the path that lists base backups.
func rawRecoveryClusterManifest(f logicalFlavor, name, source string) string {
	return logicalFlavorCluster(f, name, 1, fmt.Sprintf(`  bootstrap:
    recovery:
      source: %s
  externalClusters:
    - name: %s
%s
`, source, source, objectStoreYAML("      ")))
}

// importClusterManifest bootstraps a cluster from a logical backup. importYAML
// is the initdb.import block, extraYAML is appended to the spec. The cluster
// has no backup store of its own: the dump is read from the source's.
func importClusterManifest(f logicalFlavor, name string, instances int, importYAML, extraYAML string) string {
	return logicalFlavorCluster(f, name, instances, `  bootstrap:
    initdb:
      database: app
      owner: app
`+importYAML+extraYAML)
}
