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
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The runtime-parameter suite proves design 043: a spec.mysql.parameters
// change the server accepts with SET GLOBAL never rolls a Pod, a change the
// server refuses rolls exactly as before, and a mixed change does both. The
// restart-required probe variable is innodb_flush_method: it is read-only in
// every supported engine, harmless to change mid-life, and nothing the
// instance manager depends on.
var _ = Describe("Runtime parameter changes", Ordered, Label("feature"), func() {
	var ns, prevNS string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("paramreload")
		DeferCleanup(func() {
			deleteTestNamespace(ns, prevNS)
		})
	})

	parametersClusterManifest := func(name, params string) string {
		return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  instances: 2
  imageName: %[3]s
  storage:
    size: 2Gi
%[4]s
  mysql:
    parameters:
%[5]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instanceImage, e2eInstanceResources, params)
	}

	patchClusterParameters := func(name, params string) {
		_, err := kubectl("patch", "cluster", name, "-n", testNamespace, "--type=merge",
			"-p", fmt.Sprintf(`{"spec":{"mysql":{"parameters":%s}}}`, params))
		Expect(err).NotTo(HaveOccurred(), "failed to patch spec.mysql.parameters on %s", name)
	}

	clusterEventReasons := func(name string) string {
		out, err := kubectl("get", "events", "-n", testNamespace,
			"--field-selector", "involvedObject.name="+name,
			"-o", "jsonpath={.items[*].reason}")
		Expect(err).NotTo(HaveOccurred(), "failed to read events for %s", name)
		return out
	}

	// globalVariableG reads a global variable with a soft failure: inside an
	// Eventually the poll is retried, which is what a Pod mid-roll needs (the
	// mysql socket disappears while mysqld restarts).
	globalVariableG := func(g Gomega, pod, password, variable string) string {
		out, err := mysqlExec(pod, "root", password, "",
			fmt.Sprintf("SELECT @@GLOBAL.%s;", variable))
		g.Expect(err).NotTo(HaveOccurred(), "failed to read %s on %s", variable, pod)
		return strings.TrimSpace(out)
	}

	// globalVariable is the hard-failing read for post-Ready assertions, where
	// a failure is a real result and not a Pod mid-roll.
	globalVariable := func(pod, password, variable string) string {
		out, err := mysqlExec(pod, "root", password, "",
			fmt.Sprintf("SELECT @@GLOBAL.%s;", variable))
		Expect(err).NotTo(HaveOccurred(), "failed to read %s on %s", variable, pod)
		return strings.TrimSpace(out)
	}

	It("applies a dynamic-only parameter change without recreating Pods", func() {
		const cluster = "paramreload"
		applyManifest(cluster, parametersClusterManifest(cluster, `      max_connections: "80"`))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 2, 20*time.Minute)

		password := rootPassword(cluster)
		uid1 := podUID(cluster + "-1")
		uid2 := podUID(cluster + "-2")

		By("changing max_connections to 120")
		patchClusterParameters(cluster, `{"max_connections":"120"}`)

		By("verifying the new value is live on both instances")
		for _, pod := range []string{cluster + "-1", cluster + "-2"} {
			Eventually(func(g Gomega) {
				g.Expect(globalVariableG(g, pod, password, "max_connections")).To(Equal("120"),
					"max_connections was not applied at runtime on %s", pod)
			}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
		}

		By("verifying no Pod was recreated")
		Consistently(func() string {
			return podUID(cluster + "-1")
		}, 60*time.Second, 5*time.Second).Should(Equal(uid1),
			"the primary was rolled for a runtime-applied parameter change")
		Consistently(func() string {
			return podUID(cluster + "-2")
		}, 60*time.Second, 5*time.Second).Should(Equal(uid2),
			"the replica was rolled for a runtime-applied parameter change")

		By("verifying the operator surfaced the runtime application")
		Eventually(clusterEventReasons(cluster), e2eTimeout(2*time.Minute), 5*time.Second).
			Should(ContainSubstring("ParametersAppliedAtRuntime"))
		Expect(clusterEventReasons(cluster)).NotTo(ContainSubstring("ParametersRequireRestart"),
			"a dynamic-only change must not warn about restarts")
	})

	It("applies the dynamic part of a mixed change immediately and rolls for the rest", func() {
		const cluster = "paramreload-mixed"
		applyManifest(cluster, parametersClusterManifest(cluster, `      max_connections: "80"`))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 2, 20*time.Minute)

		password := rootPassword(cluster)
		uid1 := podUID(cluster + "-1")
		uid2 := podUID(cluster + "-2")

		By("changing max_connections (dynamic) and innodb_flush_method (restart-required) together")
		patchClusterParameters(cluster, `{"max_connections":"130","innodb_flush_method":"O_DSYNC"}`)

		By("verifying the dynamic value is applied before the roll starts")
		Eventually(func(g Gomega) {
			g.Expect(globalVariableG(g, cluster+"-1", password, "max_connections")).To(Equal("130"),
				"the dynamic part of a mixed change must be applied at runtime")
			g.Expect(podUID(cluster + "-1")).To(Equal(uid1),
				"the dynamic part must land before the Pod is recreated")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("waiting for the roll to apply the restart-required value")
		Eventually(func(g Gomega) {
			g.Expect(podUID(cluster + "-1")).NotTo(Equal(uid1),
				"the restart-required part must roll the primary")
			g.Expect(podUID(cluster + "-2")).NotTo(Equal(uid2),
				"the restart-required part must roll the replica")
		}, e2eTimeout(20*time.Minute), 10*time.Second).Should(Succeed())
		expectClusterReady(cluster, 2, 20*time.Minute)

		By("verifying both values are live after the roll")
		for _, pod := range []string{cluster + "-1", cluster + "-2"} {
			Expect(globalVariable(pod, password, "max_connections")).To(Equal("130"),
				"the dynamic value must survive the roll on %s", pod)
			Expect(globalVariable(pod, password, "innodb_flush_method")).To(Equal("O_DSYNC"),
				"the restart-required value must come from my.cnf on %s", pod)
		}
		Eventually(clusterEventReasons(cluster), e2eTimeout(2*time.Minute), 5*time.Second).
			Should(ContainSubstring("ParametersRequireRestart"))
	})

	It("rolls Pods for a restart-required-only change exactly as before", func() {
		const cluster = "paramreload-static"
		applyManifest(cluster, parametersClusterManifest(cluster, `      max_connections: "80"`))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 2, 20*time.Minute)

		password := rootPassword(cluster)
		uid1 := podUID(cluster + "-1")

		By("changing only innodb_flush_method")
		patchClusterParameters(cluster, `{"innodb_flush_method":"O_DSYNC"}`)

		By("waiting for the Pods to be recreated")
		Eventually(func(g Gomega) {
			g.Expect(podUID(cluster + "-1")).NotTo(Equal(uid1),
				"a restart-required-only change must roll the primary")
		}, e2eTimeout(20*time.Minute), 10*time.Second).Should(Succeed())
		expectClusterReady(cluster, 2, 20*time.Minute)

		By("verifying the value comes from my.cnf after the roll")
		Eventually(func(g Gomega) {
			g.Expect(globalVariableG(g, cluster+"-1", password, "innodb_flush_method")).To(Equal("O_DSYNC"),
				"the value must come from my.cnf after the roll")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())
	})

	It("rejects operator-managed keys in spec.mysql.parameters", func() {
		const cluster = "paramreload-managed"
		applyManifest(cluster, parametersClusterManifest(cluster, `      max_connections: "80"`))
		DeferCleanup(func() {
			deleteCluster(cluster)
		})
		expectClusterReady(cluster, 2, 20*time.Minute)

		By("attempting to set the operator-managed server_id")
		_, err := kubectl("patch", "cluster", cluster, "-n", testNamespace, "--type=merge",
			"-p", `{"spec":{"mysql":{"parameters":{"server_id":"7"}}}}`)
		Expect(err).To(HaveOccurred(),
			"the webhook must reject operator-managed keys in spec.mysql.parameters")
		Expect(clusterEventReasons(cluster)).NotTo(ContainSubstring("ParametersAppliedAtRuntime"),
			"a refused spec change must not trigger a parameter pass")
	})
})
