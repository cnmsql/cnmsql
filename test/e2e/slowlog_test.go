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
)

// Design 037: a slow query on an instance reaches the instance container's
// logs as one record, read through the Pods API without exec, and the run
// volume holding the slow log is a capped tmpfs.
var _ = Describe("Slow query log", Ordered, Label("feature"), func() {
	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("slowlog")
	})
	AfterAll(func() { deleteTestNamespace(ns, prevNS) })

	It("reaches the instance logs on MySQL", func() {
		expectSlowQueryInLogs("slowlog-mysql", "mysql", instanceImage, mysqlExec)
	})
	It("reaches the instance logs on MariaDB", func() {
		expectSlowQueryInLogs("slowlog-mariadb", "mariadb", mariadbImage, mariadbExec)
	})
})

func expectSlowQueryInLogs(name, flavor, image string, exec func(pod, user, password, database, sql string) (string, error)) {
	manifest := slowLogClusterManifest(name, flavor, image)
	applyManifest(name, manifest)
	DeferCleanup(func() { deleteManifest(name, manifest) })
	expectClusterReady(name, 1, 15*time.Minute)
	pod := clusterPrimary(name)

	By("checking the run volume is a capped tmpfs")
	out, err := kubectl("get", "pod", pod, "-n", testNamespace, "-o",
		`jsonpath={.spec.volumes[?(@.name=="run")].emptyDir}`)
	Expect(err).NotTo(HaveOccurred())
	Expect(out).To(ContainSubstring(`"medium":"Memory"`))
	Expect(out).To(ContainSubstring(`"sizeLimit":"32Mi"`))

	By("running a slow query as the application user")
	marker := "cnmsql-slowlog-e2e-" + flavor
	_, err = exec(pod, "app", appPassword(name), "app", fmt.Sprintf("SELECT SLEEP(0.5), '%s'", marker))
	Expect(err).NotTo(HaveOccurred())

	By("reading its record from the Pod logs")
	Eventually(func(g Gomega) {
		logs, err := kubectl("logs", pod, "-n", testNamespace, "-c", "mysql")
		g.Expect(err).NotTo(HaveOccurred())
		r := slowQueryRecordFor(logs, marker)
		g.Expect(r).NotTo(BeNil(), "no slow query record for %s", marker)
		g.Expect(r["user"]).To(Equal("app"))
		g.Expect(r["query_time"]).To(BeNumerically(">=", 0.5))
	}, e2eTimeout(2*time.Minute), 5*time.Second).Should(Succeed())
}

// slowQueryRecordFor returns the "Slow query" record whose query names marker.
func slowQueryRecordFor(logs, marker string) map[string]any {
	for line := range strings.SplitSeq(logs, "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil || m["msg"] != "Slow query" {
			continue
		}
		if q, _ := m["query"].(string); strings.Contains(q, marker) {
			return m
		}
	}
	return nil
}

func slowLogClusterManifest(name, flavor, image string) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  flavor: %[3]s
  instances: 1
  imageName: %[4]s
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
    parameters:
      innodb_buffer_pool_size: 128M
      max_connections: "80"
      slow_query_log: "ON"
      long_query_time: "0.2"
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, flavor, image, e2eInstanceResources)
}
