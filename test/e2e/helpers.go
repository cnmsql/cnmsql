//go:build e2e
// +build e2e

package e2e

import (
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/yyewolf/cnmysql/test/utils"
)

// testNamespace is the namespace that hosts the test Clusters and their
// supporting objects. The controller runs in `namespace` (cnmysql-system) and
// watches all namespaces, so user-facing resources live here, mirroring how a
// real user would deploy a Cluster outside the operator's namespace.
const testNamespace = "default"

// minioBucket is the bucket pre-created in the in-cluster MinIO and targeted by
// the backup/recovery specs.
const minioBucket = "cnmysql-backups"

// minioCredsSecret is the Secret holding the MinIO access credentials consumed
// by Clusters and Backups through their object-store configuration.
const minioCredsSecret = "minio-creds"

// kubectl runs a kubectl command from the project directory and returns its
// combined output.
func kubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

// applyManifest writes the given manifest to a temporary file and applies it,
// returning the file path so callers can delete it later with deleteManifest.
func applyManifest(name, manifest string) {
	path := writeManifest(name, manifest)
	_, err := kubectl("apply", "-f", path)
	Expect(err).NotTo(HaveOccurred(), "Failed to apply manifest %s", name)
}

// deleteManifest deletes the resources described by the named manifest, ignoring
// any that are already gone. It is safe to call from DeferCleanup.
func deleteManifest(name, manifest string) {
	path := writeManifest(name, manifest)
	_, _ = kubectl("delete", "-f", path, "--ignore-not-found", "--wait=false")
}

func writeManifest(name, manifest string) string {
	path := filepath.Join("/tmp", "cnmysql-e2e-"+name+".yaml")
	Expect(os.WriteFile(path, []byte(manifest), 0o644)).To(Succeed(), "Failed to write manifest %s", name)
	return path
}

// clusterField returns a single jsonpath field from a Cluster's status/spec.
func clusterField(name, jsonpath string) (string, error) {
	return kubectl("get", "cluster", name, "-n", testNamespace, "-o", "jsonpath="+jsonpath)
}

// expectClusterReady blocks until the named Cluster reports Ready with the
// expected number of ready instances and an elected primary.
func expectClusterReady(name string, instances int, timeout time.Duration) {
	Eventually(func(g Gomega) {
		ready, err := clusterField(name, "{.status.conditions[?(@.type=='Ready')].status}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(ready).To(Equal("True"), "Cluster %s is not Ready yet", name)

		readyInstances, err := clusterField(name, "{.status.readyInstances}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(readyInstances).To(Equal(fmt.Sprintf("%d", instances)),
			"Cluster %s does not have %d ready instances yet", name, instances)

		primary, err := clusterField(name, "{.status.currentPrimary}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(primary).NotTo(BeEmpty(), "Cluster %s has no elected primary yet", name)
	}, timeout, 5*time.Second).Should(Succeed())
}

// clusterPrimary returns the current primary Pod name for a cluster.
func clusterPrimary(name string) string {
	primary, err := clusterField(name, "{.status.currentPrimary}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read currentPrimary for %s", name)
	Expect(primary).NotTo(BeEmpty(), "Cluster %s has no primary", name)
	return primary
}

// appPassword returns the decoded application password for a Cluster, read from
// the operator-generated `<cluster>-app` Secret.
func appPassword(cluster string) string {
	output, err := kubectl("get", "secret", cluster+"-app", "-n", testNamespace,
		"-o", "jsonpath={.data.password}")
	Expect(err).NotTo(HaveOccurred(), "Failed to read application password for %s", cluster)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(output))
	Expect(err).NotTo(HaveOccurred(), "Failed to decode application password for %s", cluster)
	return string(decoded)
}

// mysqlExec runs a SQL statement inside an instance Pod as the given user and
// returns the command output.
func mysqlExec(pod, user, password, database, sql string) (string, error) {
	args := []string{"exec", pod, "-n", testNamespace, "--",
		"mysql", "-u" + user, "-p" + password}
	if database != "" {
		args = append(args, database)
	}
	args = append(args, "-e", sql)
	return kubectl(args...)
}

// setupMinio deploys a single-node MinIO, waits for a bucket-creation Job to
// finish, and creates the credentials Secret referenced by object stores. It is
// idempotent enough to run once per backup-focused Describe.
func setupMinio() {
	By("deploying in-cluster MinIO and credentials")
	applyManifest("minio", minioManifest())

	By("waiting for MinIO to become available")
	_, err := kubectl("wait", "deployment/minio", "-n", testNamespace,
		"--for=condition=Available", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "MinIO did not become available")

	By("waiting for the MinIO bucket-creation Job to complete")
	_, err = kubectl("wait", "job/minio-mkbucket", "-n", testNamespace,
		"--for=condition=Complete", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "MinIO bucket-creation Job did not complete")
}

// teardownMinio removes the MinIO deployment and its supporting objects.
func teardownMinio() {
	deleteManifest("minio", minioManifest())
}

// seedObjectStoreMarker writes a small object at the given key in the MinIO
// bucket via a one-shot mc Job and waits for it to complete. It is used to make
// a destination prefix non-empty deterministically.
func seedObjectStoreMarker(key string) {
	name := "seed-" + strings.NewReplacer("/", "-", ".", "-", "_", "-").Replace(key)
	manifest := fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  backoffLimit: 20
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: minio/mc:latest
        command:
        - sh
        - -c
        - |
          until mc alias set local http://minio.%[2]s.svc:9000 minioadmin minioadmin; do sleep 2; done
          echo cnmysql-guard-marker | mc pipe local/%[3]s/%[4]s
`, name, testNamespace, minioBucket, key)
	applyManifest(name, manifest)
	DeferCleanup(func() {
		deleteManifest(name, manifest)
	})
	_, err := kubectl("wait", "job/"+name, "-n", testNamespace,
		"--for=condition=Complete", "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "Failed to seed object %s", key)
}

// objectStoreYAML returns the indented spec.backup.objectStore block pointing at
// the in-cluster MinIO. indent is the leading whitespace for the `objectStore`
// key so the snippet can be embedded under spec.backup.
func objectStoreYAML(indent string) string {
	lines := []string{
		"objectStore:",
		"  endpoint: http://minio." + testNamespace + ".svc:9000",
		"  region: us-east-1",
		"  bucket: " + minioBucket,
		"  forcePathStyle: true",
		"  credentials:",
		"    accessKeyId:",
		"      name: " + minioCredsSecret,
		"      key: ACCESS_KEY_ID",
		"    secretAccessKey:",
		"      name: " + minioCredsSecret,
		"      key: SECRET_ACCESS_KEY",
	}
	for i, l := range lines {
		lines[i] = indent + l
	}
	return strings.Join(lines, "\n")
}

func minioManifest() string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: %[1]s
  labels:
    app: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args: ["server", "/data", "--console-address", ":9001"]
        env:
        - name: MINIO_ROOT_USER
          value: minioadmin
        - name: MINIO_ROOT_PASSWORD
          value: minioadmin
        ports:
        - containerPort: 9000
        volumeMounts:
        - name: data
          mountPath: /data
        readinessProbe:
          httpGet:
            path: /minio/health/ready
            port: 9000
          initialDelaySeconds: 5
          periodSeconds: 3
      volumes:
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: %[1]s
spec:
  selector:
    app: minio
  ports:
  - port: 9000
    targetPort: 9000
---
apiVersion: v1
kind: Secret
metadata:
  name: %[2]s
  namespace: %[1]s
stringData:
  ACCESS_KEY_ID: minioadmin
  SECRET_ACCESS_KEY: minioadmin
---
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-mkbucket
  namespace: %[1]s
spec:
  backoffLimit: 20
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: mc
        image: minio/mc:latest
        command:
        - sh
        - -c
        - |
          until mc alias set local http://minio.%[1]s.svc:9000 minioadmin minioadmin; do sleep 2; done
          mc mb --ignore-existing local/%[3]s
          mc ls local
`, testNamespace, minioCredsSecret, minioBucket)
}
