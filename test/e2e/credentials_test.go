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

// Instance credentials (design 030): the instance manager reads its account
// passwords from the cluster's credential Secrets through the Kubernetes API,
// so no password rides on a Pod env var or a backup worker Job, the
// per-instance Role admits only the named reads the manager needs, and
// rotating the dump Secret reaches the managers through their watches without
// restarting an instance.
var _ = Describe("Instance credentials", Ordered, Label("core"), func() {
	const (
		cluster    = "cred"
		dumpSecret = cluster + "-dump"
		instanceSA = cluster + "-1-instance"

		// instanceEnvJSP renders every env var name of an instance Pod: the
		// bootstrap init containers first, then the run container.
		instanceEnvJSP = `{range .spec.initContainers[*]}{.env[*].name}{" "}{end}{range .spec.containers[*]}{.env[*].name}{end}`

		// workerEnvJSP renders every env source the worker Job template has:
		// names, secretKeyRefs and envFrom secretRefs of the init containers
		// and the worker container. A dump password could hide in any of them.
		workerEnvJSP = `{range .spec.template.spec.initContainers[*]}{.env[*].name}{" "}` +
			`{.env[*].valueFrom.secretKeyRef.name}{" "}{.envFrom[*].secretRef.name}{" "}{end}` +
			`{range .spec.template.spec.containers[*]}{.env[*].name}{" "}` +
			`{.env[*].valueFrom.secretKeyRef.name}{" "}{.envFrom[*].secretRef.name}{end}`

		dumpReadyJSP   = `{.status.conditions[?(@.type=="DumpAccountReady")].status}`
		dumpVersionJSP = `{.status.dumpAccountSecretVersion}`
	)

	var ns, prevNS, rotatedBackup string

	BeforeAll(func() {
		prevNS = testNamespace
		ns = createTestNamespace("credentials")

		setupObjectStore()
		DeferCleanup(teardownObjectStore)

		By("creating a two-instance cluster with a backup store")
		manifest := logicalClusterManifest(logicalFlavor{image: instanceImage}, cluster, 2)
		applyManifest(cluster, manifest)
		DeferCleanup(func() { deleteManifest(cluster, manifest) })
		expectClusterReady(cluster, 2, 20*time.Minute)
	})

	AfterAll(func() {
		deleteTestNamespace(ns, prevNS)
	})

	It("keeps every password env var off the instance Pods", func() {
		out, err := kubectl("get", "pods", "-n", testNamespace, "-l", instanceSelector(cluster),
			"-o", `jsonpath={range .items[*]}{.metadata.name}{'\n'}{end}`)
		Expect(err).NotTo(HaveOccurred())
		pods := strings.Fields(out)
		Expect(pods).To(HaveLen(2), "expected the two instance Pods via %q, got %q",
			instanceSelector(cluster), out)

		for _, pod := range pods {
			env, err := kubectl("get", "pod", pod, "-n", testNamespace, "-o", "jsonpath="+instanceEnvJSP)
			Expect(err).NotTo(HaveOccurred())
			Expect(env).NotTo(ContainSubstring("_PASSWORD"),
				"instance Pod %s must read its passwords through the api, not env vars (env: %s)", pod, env)
		}
	})

	It("scopes the instance Role to its own credential Secrets", func() {
		as := "--as=system:serviceaccount:" + testNamespace + ":" + instanceSA
		cani := func(verb, resource string) string {
			out, err := kubectl("auth", "can-i", verb, resource, "-n", testNamespace, as)
			Expect(err).NotTo(HaveOccurred(), "auth can-i %s %s", verb, resource)
			return strings.TrimSpace(out)
		}

		Expect(cani("get", "secret/"+cluster+"-control")).To(Equal("yes"),
			"the instance manager reads its control account by name")
		Expect(cani("watch", "secret/"+dumpSecret)).To(Equal("yes"),
			"the instance manager follows dump password rotations through a watch")
		Expect(cani("list", "secrets")).To(Equal("no"),
			"list would expose every Secret in the namespace")
		Expect(cani("get", "secret/"+cluster+"-replication")).To(Equal("no"),
			"replication authenticates with mTLS only; its Secret is not for instances")
		Expect(cani("get", "secret/"+objectStoreCredsSecret)).To(Equal("no"),
			"object-store credentials never enter the instance Pods")
	})

	It("applies a rotated dump password without restarting an instance", func() {
		before := instanceRestarts(cluster)
		rotated := "rotated-" + fmt.Sprint(time.Now().UnixNano())

		By("patching the " + dumpSecret + " Secret with a new password")
		_, err := kubectl("patch", "secret", dumpSecret, "-n", testNamespace, "--type=merge",
			"-p", fmt.Sprintf(`{"stringData":{"password":%q}}`, rotated))
		Expect(err).NotTo(HaveOccurred())
		version, err := kubectl("get", "secret", dumpSecret, "-n", testNamespace,
			"-o", "jsonpath={.metadata.resourceVersion}")
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the operator to re-apply the account from the rotated Secret")
		Eventually(func(g Gomega) {
			ready, err := clusterField(cluster, dumpReadyJSP)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(Equal("True"), "DumpAccountReady is not true after the rotation")
			applied, err := clusterField(cluster, dumpVersionJSP)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(applied).To(Equal(version),
				"status.dumpAccountSecretVersion must match the rotated Secret's resourceVersion")
		}, e2eTimeout(5*time.Minute), 5*time.Second).Should(Succeed())

		By("taking a logical backup with the rotated password")
		rotatedBackup = cluster + "-rotated"
		applyManifest(rotatedBackup, logicalBackupManifest(rotatedBackup, cluster, nil))
		expectBackupCompleted(rotatedBackup, 8*time.Minute)

		Expect(instanceRestarts(cluster)).To(Equal(before),
			"the rotation must not restart or recreate an instance\nbefore: %s\nafter:  %s",
			before, instanceRestarts(cluster))

		By("checking the source manager picked the rotation up through its watch")
		// The worker carries no dump password (design 030 C5), so a completed
		// backup already proves the source manager read the new value; the log
		// line makes the pickup visible. The manager logs in production JSON:
		// "msg":"Picked up rotated credential Secret","account":"dump",...
		source := backupStatus(rotatedBackup).InstanceName
		logs, err := kubectl("logs", source, "-n", testNamespace, "-c", "mysql", "--tail=800")
		Expect(err).NotTo(HaveOccurred())
		pickedUp := false
		for _, line := range strings.Split(logs, "\n") {
			if strings.Contains(line, "Picked up rotated credential Secret") &&
				strings.Contains(line, `"account":"dump"`) {
				pickedUp = true
				break
			}
		}
		Expect(pickedUp).To(BeTrue(),
			"the source manager %s must log picking up the rotated credential Secret for account=dump", source)
	})

	It("keeps the dump Secret out of the backup worker Job", func() {
		Expect(rotatedBackup).NotTo(BeEmpty(), "the rotation backup must exist first")

		env, err := kubectl("get", "job", rotatedBackup+"-backup", "-n", testNamespace,
			"-o", "jsonpath="+workerEnvJSP)
		Expect(err).NotTo(HaveOccurred())
		Expect(env).NotTo(ContainSubstring(dumpSecret),
			"the worker reads the dump from the source manager over mTLS, never from the %s Secret (env: %s)",
			dumpSecret, env)
	})
})
