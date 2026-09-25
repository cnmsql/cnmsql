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
// without ever being mistaken for one.
var _ = Describe("Logical backups", Ordered, Label("flavor"), func() {
	const (
		cluster      = "lb-src"
		physical     = "lb-physical"
		fullDump     = "lb-full"
		partialDump  = "lb-billing"
		missingDB    = "lb-missing"
		recovered    = "lb-recovered"
		schedule     = "lb-nightly"
		stalePrefix  = "lb-src/stale-dump/stale-id"
		dumpSecret   = cluster + "-dump"
		dumpReadyJSP = `{.status.conditions[?(@.type=="DumpAccountReady")].status}`
	)

	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("logical")

		setupObjectStore()
		DeferCleanup(teardownObjectStore)
		setupS3Client()
		DeferCleanup(teardownS3Client)

		By("creating a two-instance archiving cluster")
		applyManifest(cluster, logicalClusterManifest(cluster, 2))
		DeferCleanup(func() { deleteManifest(cluster, logicalClusterManifest(cluster, 2)) })
		expectClusterReady(cluster, 2, 20*time.Minute)

		By("seeding two application schemas")
		_, err := mysqlExec(clusterPrimary(cluster), "root", rootPassword(cluster), "",
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
		Expect(status.Databases).NotTo(ContainElements("mysql", "sys", "heartbeat"))
		Expect(status.SHA256).NotTo(BeEmpty())
		Expect(status.BeginBinlog).To(ContainSubstring(":"), "the snapshot binlog position should be recorded")
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
	})

	It("still recovers from the physical backup when newer dumps share the prefix", func() {
		applyManifest(recovered, rawRecoveryClusterManifest(recovered, cluster))
		DeferCleanup(func() { deleteManifest(recovered, rawRecoveryClusterManifest(recovered, cluster)) })
		expectClusterReady(recovered, 1, 20*time.Minute)
		Eventually(func(g Gomega) {
			// Recovery resets root to the recovered cluster's own Secret.
			out, err := mysqlExec(clusterPrimary(recovered), "root", rootPassword(recovered), "",
				"SELECT name FROM shop.items WHERE id = 1")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("widget"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())
	})

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

	It("recreates a lost dump account without restarting instances", func() {
		restartsBefore := instanceRestarts(cluster)

		By("dropping the account and its Secret, as on a cluster older than logical backups")
		_, err := mysqlExec(clusterPrimary(cluster), "root", rootPassword(cluster), "",
			"DROP USER IF EXISTS 'cnmsql_dump'@'localhost'")
		Expect(err).NotTo(HaveOccurred())
		// The operator only re-applies the account when its Secret changes, so the
		// account stays gone until the Secret is deleted below.
		out, err := mysqlExec(clusterPrimary(cluster), "root", rootPassword(cluster), "",
			"SELECT COUNT(*) FROM mysql.user WHERE User = 'cnmsql_dump' AND Host = 'localhost'")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("0"), "the account should be gone before the Secret is deleted")
		_, err = kubectl("delete", "secret", dumpSecret, "-n", testNamespace)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the operator to recreate both")
		Eventually(func(g Gomega) {
			out, err := mysqlExec(clusterPrimary(cluster), "root", rootPassword(cluster), "",
				"SELECT COUNT(*) FROM mysql.user WHERE User = 'cnmsql_dump' AND Host = 'localhost'")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("1"))
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("taking a logical backup with the new password")
		const again = "lb-after-migration"
		applyManifest(again, logicalBackupManifest(again, cluster, nil))
		expectBackupCompleted(again, 8*time.Minute)

		restartsAfter := instanceRestarts(cluster)
		Expect(restartsAfter).To(Equal(restartsBefore),
			"no instance may restart\nbefore: %s\nafter:  %s", restartsBefore, restartsAfter)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})

type e2eBackupStatus struct {
	Method          string   `json:"method"`
	InstanceName    string   `json:"instanceName"`
	BackupID        string   `json:"backupId"`
	DestinationPath string   `json:"destinationPath"`
	SHA256          string   `json:"sha256"`
	BeginBinlog     string   `json:"beginBinlog"`
	Databases       []string `json:"databases"`
}

func backupStatus(name string) e2eBackupStatus {
	GinkgoHelper()
	out, err := kubectl("get", "backup", name, "-n", testNamespace, "-o", "jsonpath={.status}")
	Expect(err).NotTo(HaveOccurred())
	var status e2eBackupStatus
	Expect(json.Unmarshal([]byte(out), &status)).To(Succeed(), "backup status: %s", out)
	return status
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

func logicalClusterManifest(name string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: %d
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
    retentionPolicy: 7d
%s
`, name, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters, objectStoreYAML("    "))
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
func rawRecoveryClusterManifest(name, source string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %s
  namespace: %s
spec:
  instances: 1
  imageName: %s
  storage:
    size: 2Gi
%s
  mysql:
    binlogFormat: ROW
%s
  bootstrap:
    recovery:
      source: %s
  externalClusters:
    - name: %s
%s
`, name, testNamespace, instanceImage, e2eInstanceResources, e2eMySQLParameters, source, source,
		objectStoreYAML("      "))
}
