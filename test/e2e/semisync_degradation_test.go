//go:build e2e
// +build e2e

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
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Semi-sync degradation (design 044): with every replica gone, the primary
// falls back to asynchronous replication once a commit's acknowledgement wait
// times out. The operator must report that through SemiSyncDegraded, and in
// "required" mode with readOnlyAfterDegradedSeconds the primary must stop
// accepting writes instead of continuing unprotected. Bringing the replica
// back must restore both.
var _ = Describe("Semi-sync degradation", Ordered, Label("feature"), func() {
	const (
		cluster   = "semideg"
		instances = 2
	)

	var ns, prevNS, rootPass string

	manifest := func() string {
		return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: %[3]d
  imageName: %[4]s
  minSyncReplicas: 1
  maxSyncReplicas: 1
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
    semiSync:
      enabled: true
      timeoutMillis: 2000
      dataDurability: required
      degradedAfterSeconds: 4
      readOnlyAfterDegradedSeconds: 15
  bootstrap:
    initdb:
      database: app
      owner: app
`, cluster, testNamespace, instances, instanceImage, e2eInstanceResources, e2eMySQLParameters)
	}

	semiSyncCondition := func(g Gomega) (status, reason string) {
		status, err := clusterField(cluster, "{.status.conditions[?(@.type=='SemiSyncDegraded')].status}")
		g.Expect(err).NotTo(HaveOccurred())
		reason, err = clusterField(cluster, "{.status.conditions[?(@.type=='SemiSyncDegraded')].reason}")
		g.Expect(err).NotTo(HaveOccurred())
		return strings.TrimSpace(status), strings.TrimSpace(reason)
	}

	write := func(primary string, id int) error {
		_, err := mysqlExec(primary, "root", rootPass, "",
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS app.semideg (id INT PRIMARY KEY); "+
				"REPLACE INTO app.semideg VALUES (%d);", id))
		return err
	}

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("semideg")

		By("creating a 2-instance cluster with required semi-sync and the read-only fence")
		applyManifest(cluster, manifest())
		DeferCleanup(func() { deleteManifest(cluster, manifest()) })
		expectClusterReady(cluster, instances, 20*time.Minute)
		rootPass = secretPassword(cluster + "-root")
	})

	It("reports semi-sync as healthy and pins wait-no-replica ON", func() {
		primary := clusterPrimary(cluster)
		Expect(write(primary, 1)).To(Succeed())
		Eventually(func(g Gomega) {
			status, reason := semiSyncCondition(g)
			g.Expect(status).To(Equal("False"))
			g.Expect(reason).To(Equal("SemiSyncActive"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		out, err := mysqlExec(primary, "root", rootPass, "",
			"SHOW GLOBAL VARIABLES WHERE Variable_name IN "+
				"('rpl_semi_sync_source_wait_no_replica', 'rpl_semi_sync_master_wait_no_slave');")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("ON"), "wait-no-replica must be ON in required mode: %q", out)
	})

	It("reports the fallback, then pauses writes, while the replica is gone", func() {
		primary := clusterPrimary(cluster)
		replica := otherInstance(cluster, instances, primary)

		By(fmt.Sprintf("fencing replica %s so no replica acknowledges", replica))
		_, err := kubectl("annotate", "pod", replica, "-n", testNamespace,
			fencingAnnotation+"=true", "--overwrite")
		Expect(err).NotTo(HaveOccurred())

		By("committing on the primary: the acknowledgement wait times out and it falls back to async")
		Eventually(func(g Gomega) {
			g.Expect(write(primary, 2)).To(Succeed())
			out, err := mysqlExec(primary, "root", rootPass, "",
				"SHOW GLOBAL STATUS WHERE Variable_name IN "+
					"('Rpl_semi_sync_source_status', 'Rpl_semi_sync_master_status');")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("OFF"))
		}, e2eTimeout(3*time.Minute), 5*time.Second).Should(Succeed())

		By("waiting for SemiSyncDegraded=True")
		Eventually(func(g Gomega) {
			status, _ := semiSyncCondition(g)
			g.Expect(status).To(Equal("True"))
		}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed())

		By("waiting for the read-only fence: writes are refused instead of going async")
		Eventually(func(g Gomega) {
			_, reason := semiSyncCondition(g)
			g.Expect(reason).To(Equal("PrimaryReadOnly"))
			err := write(primary, 3)
			g.Expect(err).To(HaveOccurred(), "the fenced primary must refuse writes")
			g.Expect(err.Error()).To(ContainSubstring("read-only"))
		}, e2eTimeout(2*time.Minute), 2*time.Second).Should(Succeed())

		By("checking the primary was not failed over for it")
		Expect(clusterPrimary(cluster)).To(Equal(primary))

		By("unfencing the replica")
		_, err = kubectl("annotate", "pod", replica, "-n", testNamespace, fencingAnnotation+"-")
		Expect(err).NotTo(HaveOccurred())

		By("waiting for writes to resume and the condition to clear")
		Eventually(func(g Gomega) {
			g.Expect(write(primary, 4)).To(Succeed())
			status, reason := semiSyncCondition(g)
			g.Expect(status).To(Equal("False"))
			g.Expect(reason).To(Equal("SemiSyncActive"))
		}, e2eTimeout(10*time.Minute), 5*time.Second).Should(Succeed())

		By("checking both transitions were recorded as events")
		Eventually(func(g Gomega) {
			out, err := kubectl("get", "events", "-n", testNamespace,
				"--field-selector", "involvedObject.name="+cluster, "-o", "jsonpath={.items[*].reason}")
			g.Expect(err).NotTo(HaveOccurred())
			for _, reason := range []string{"SemiSyncDegraded", "SemiSyncReadOnly", "SemiSyncRestored"} {
				g.Expect(out).To(ContainSubstring(reason))
			}
		}, e2eTimeout(time.Minute), 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})
})
