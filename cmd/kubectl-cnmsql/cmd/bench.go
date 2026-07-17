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

package cmd

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

const (
	// defaultSysbenchImage ships sysbench 1.1 with a MySQL 8 client. A MySQL-8
	// client is required because modern servers default to the
	// caching_sha2_password auth plugin, which the older MariaDB-based sysbench
	// images cannot speak. Override with --image for air-gapped mirrors.
	defaultSysbenchImage = "perconalab/sysbench"
	// defaultFioImage ships the fio binary with the libaio engine.
	defaultFioImage = "ljishen/fio"

	// benchManagedByLabel marks resources the bench command creates so they are
	// easy to spot and, if a run is interrupted, clean up by hand.
	benchManagedByLabel = "app.kubernetes.io/managed-by"
	benchManagedByValue = "kubectl-cnmsql"
	benchClusterLabel   = "cnmsql.cnmsql.co/bench"

	// secretPasswordKey is the data key holding the password in a credential
	// Secret (the cluster root Secret and the bench user Secret both use it).
	secretPasswordKey = "password"

	// benchUser is the dedicated, network-capable account the mysql benchmark
	// runs as. The operator's root user is socket-only (root@localhost), so it
	// cannot be used from a Job connecting over the -rw service; the plugin
	// provisions this user before the run and drops it afterwards.
	benchUser = "cnmsql_bench"

	defaultBenchTimeout = 15 * time.Minute
)

// dbNamePattern restricts --db-name to a plain SQL identifier so it is safe to
// interpolate into the provisioning statements.
var dbNamePattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// knownSysbenchTests is the set of OLTP workloads --tests may reference. They
// all share the same table schema, so prepare/cleanup with any one of them
// covers the rest.
var knownSysbenchTests = map[string]bool{
	"oltp_read_write":       true,
	"oltp_read_only":        true,
	"oltp_write_only":       true,
	"oltp_point_select":     true,
	"oltp_insert":           true,
	"oltp_update_index":     true,
	"oltp_update_non_index": true,
	"oltp_delete":           true,
	"bulk_insert":           true,
	"select_random_points":  true,
	"select_random_ranges":  true,
}

// newBenchCommand builds the `bench` subtree: performance benchmarks that run as
// short-lived Jobs against a cluster and stream the tool's output to stdout.
func newBenchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Run performance benchmarks against a cluster",
		Long: "Run performance benchmarks against a cnmsql cluster. Each benchmark " +
			"runs as a short-lived Kubernetes Job in the cluster's namespace; its " +
			"output is streamed to your terminal and the Job (and any scratch PVC) " +
			"is cleaned up on completion.\n\n" +
			"Two benchmarks are available:\n" +
			"  mysql  — sysbench OLTP workload against the cluster read-write endpoint\n" +
			"  fio    — storage/filesystem benchmark against a fresh PVC",
		Example: `  # Benchmark MySQL throughput with sysbench
  kubectl cnmsql bench mysql cluster-sample

  # Benchmark the underlying storage with fio
  kubectl cnmsql bench fio cluster-sample`,
	}
	cmd.AddCommand(newBenchMySQLCommand(), newBenchFioCommand())
	return cmd
}

// mysqlBenchOptions carries the resolved flags for `bench mysql`.
type mysqlBenchOptions struct {
	tables    int
	tableSize int
	threads   int
	timeSec   int
	tests     []string
	dbName    string
	image     string
	keep      bool
	dryRun    bool
	timeout   time.Duration
}

func newBenchMySQLCommand() *cobra.Command {
	o := mysqlBenchOptions{}
	var testsCSV string
	cmd := &cobra.Command{
		Use:   "mysql [CLUSTER]",
		Short: "Run a sysbench OLTP benchmark against a cluster",
		Long: "Run a sysbench OLTP benchmark against the cluster's read-write " +
			"endpoint (<cluster>-rw). A Job prepares a scratch schema, runs each " +
			"selected workload, and drops the schema again on completion.\n\n" +
			"The benchmark connects as root using the cluster's root password " +
			"Secret (injected into the Job via secretKeyRef, never passed on the " +
			"command line).",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		Example: `  # Run the default OLTP suite against the sole cluster in the namespace
  kubectl cnmsql bench mysql

  # A short, small read-write benchmark against a named cluster
  kubectl cnmsql bench mysql cluster-sample --time=30 --tables=4 --threads=8

  # Only the point-select workload
  kubectl cnmsql bench mysql cluster-sample --tests=oltp_point_select

  # Print the Job that would run, without creating it
  kubectl cnmsql bench mysql cluster-sample --dry-run`,
		RunE: func(cmd *cobra.Command, args []string) error {
			o.tests = splitCSV(testsCSV)
			return runBenchMySQL(cmd.Context(), firstArg(args), o)
		},
	}
	f := cmd.Flags()
	f.IntVar(&o.tables, "tables", 10, "number of tables to create")
	f.IntVar(&o.tableSize, "table-size", 10000, "rows per table")
	f.IntVar(&o.threads, "threads", 4, "number of client threads")
	f.IntVar(&o.timeSec, "time", 60, "duration of each workload in seconds")
	f.StringVar(&testsCSV, "tests", "oltp_read_write,oltp_read_only,oltp_write_only,oltp_point_select",
		"comma-separated sysbench OLTP workloads to run")
	f.StringVar(&o.dbName, "db-name", "sbtest", "scratch database to create for the benchmark")
	f.StringVar(&o.image, "image", defaultSysbenchImage, "sysbench container image")
	f.BoolVar(&o.keep, "keep", false, "keep the Job after completion instead of deleting it")
	f.BoolVar(&o.dryRun, "dry-run", false, "print the Job that would run and exit")
	f.DurationVar(&o.timeout, "timeout", defaultBenchTimeout, "overall timeout for the benchmark")
	return cmd
}

func runBenchMySQL(ctx context.Context, clusterName string, o mysqlBenchOptions) error {
	if err := validateMySQLBenchOptions(o); err != nil {
		return err
	}
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	password, err := randHex(16)
	if err != nil {
		return err
	}
	secretName := cluster.Name + "-bench-mysql"
	secret := buildBenchSecret(cluster, secretName, password)
	job := buildSysbenchJob(cluster, benchUser, secretName, o)

	// The benchmark connects over the -rw service, but the operator's root user
	// is socket-only, so we provision a network-capable user (and the scratch
	// database) over an exec into the primary, then drop the user afterwards.
	if !o.dryRun {
		fmt.Printf("[bench] provisioning scratch database %q and user %q\n", o.dbName, benchUser)
		if err := execRootSQL(ctx, env, cluster, benchSetupSQL(o.dbName, benchUser, password)); err != nil {
			return fmt.Errorf("provisioning benchmark user: %w", err)
		}
		defer func() {
			if err := execRootSQL(context.WithoutCancel(ctx), env, cluster, benchTeardownSQL(benchUser)); err != nil {
				fmt.Fprintf(os.Stderr, "warning: dropping benchmark user %q: %v\n", benchUser, err)
			}
		}()
	}
	return runBenchJob(ctx, env, job, []client.Object{secret}, o.keep, o.dryRun, o.timeout)
}

// validateMySQLBenchOptions checks the flags that would otherwise fail deep
// inside the Job. Kept pure so it is unit-testable.
func validateMySQLBenchOptions(o mysqlBenchOptions) error {
	if len(o.tests) == 0 {
		return fmt.Errorf("--tests must list at least one workload")
	}
	for _, t := range o.tests {
		if !knownSysbenchTests[t] {
			return fmt.Errorf("unknown sysbench test %q", t)
		}
	}
	if o.tables < 1 || o.tableSize < 1 || o.threads < 1 || o.timeSec < 1 {
		return fmt.Errorf("--tables, --table-size, --threads and --time must all be positive")
	}
	if !dbNamePattern.MatchString(o.dbName) {
		return fmt.Errorf("--db-name %q must be a plain identifier (letters, digits, underscore)", o.dbName)
	}
	if o.image == "" {
		return fmt.Errorf("--image must not be empty")
	}
	return nil
}

// buildSysbenchJob assembles the Job that runs the sysbench workloads. The bench
// user's password is injected via secretKeyRef; every other parameter travels as
// a plain env var so the rendered Job is easy to inspect with --dry-run.
func buildSysbenchJob(cluster *mysqlv1alpha1.Cluster, user, secretName string, o mysqlBenchOptions) *batchv1.Job {
	env := []corev1.EnvVar{
		{Name: "MYSQL_HOST", Value: cluster.Name + "-rw"},
		{Name: "MYSQL_PORT", Value: "3306"},
		{Name: "BENCH_USER", Value: user},
		{Name: "DB_NAME", Value: o.dbName},
		{Name: "SB_TABLES", Value: fmt.Sprint(o.tables)},
		{Name: "SB_TABLE_SIZE", Value: fmt.Sprint(o.tableSize)},
		{Name: "SB_THREADS", Value: fmt.Sprint(o.threads)},
		{Name: "SB_TIME", Value: fmt.Sprint(o.timeSec)},
		{Name: "SB_TESTS", Value: strings.Join(o.tests, " ")},
		{
			Name: "MYSQL_PWD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
					Key:                  secretPasswordKey,
				},
			},
		},
	}
	return benchJob(cluster, cluster.Name+"-bench-mysql", o.image, sysbenchScript, env, volumesWithMounts{}, "")
}

// sysbenchScript prepares the shared OLTP schema with the first selected
// workload, runs each workload, and drops the schema. The scratch database and
// the connecting user are provisioned by the plugin beforehand.
const sysbenchScript = `set -eu
COMMON="--db-driver=mysql --mysql-host=$MYSQL_HOST --mysql-port=$MYSQL_PORT --mysql-user=$BENCH_USER"
COMMON="$COMMON --mysql-password=$MYSQL_PWD --mysql-db=$DB_NAME --mysql-ssl=REQUIRED"
COMMON="$COMMON --tables=$SB_TABLES --table-size=$SB_TABLE_SIZE --threads=$SB_THREADS --time=$SB_TIME"
FIRST=""
for t in $SB_TESTS; do [ -z "$FIRST" ] && FIRST=$t; done
echo "[bench] preparing schema (tables=$SB_TABLES table-size=$SB_TABLE_SIZE)"
sysbench $COMMON "$FIRST" prepare
for t in $SB_TESTS; do
  echo "[bench] ===== $t ====="
  sysbench $COMMON "$t" run
done
echo "[bench] cleaning up scratch schema"
sysbench $COMMON "$FIRST" cleanup
echo "[bench] done"
`

// fioBenchOptions carries the resolved flags for `bench fio`.
type fioBenchOptions struct {
	storageClass string
	size         string
	fileSize     string
	node         string
	ioengine     string
	runtimeSec   int
	image        string
	keep         bool
	dryRun       bool
	yes          bool
	timeout      time.Duration
}

func newBenchFioCommand() *cobra.Command {
	o := fioBenchOptions{}
	cmd := &cobra.Command{
		Use:   "fio [CLUSTER]",
		Short: "Run an fio storage benchmark on the cluster's storage class",
		Long: "Benchmark storage performance (IOPS, throughput, latency) by running " +
			"fio against a freshly provisioned scratch PVC. By default the PVC uses " +
			"the cluster's storage class, so the numbers reflect the storage MySQL " +
			"actually runs on.\n\n" +
			"fio can be I/O-intensive: on shared/local disks it may impact a live " +
			"cluster co-scheduled on the same volume, so the command prompts for " +
			"confirmation unless --yes is given. The scratch PVC and Job are deleted " +
			"on completion unless --keep is set.",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		Example: `  # Benchmark storage using the cluster's storage class
  kubectl cnmsql bench fio cluster-sample

  # Pin the benchmark to a specific node and skip the prompt
  kubectl cnmsql bench fio cluster-sample --node=worker-2 --yes

  # Use a different storage class and a larger scratch volume
  kubectl cnmsql bench fio cluster-sample --storage-class=fast --size=5Gi --file-size=4Gi`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBenchFio(cmd.Context(), firstArg(args), o)
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.storageClass, "storage-class", "",
		"storage class for the scratch PVC (default: cluster's storage class)")
	f.StringVar(&o.size, "size", "2Gi", "size of the scratch PVC")
	f.StringVar(&o.fileSize, "file-size", "1Gi", "size of the fio test file (must be smaller than --size)")
	f.StringVar(&o.node, "node", "", "pin the benchmark to a specific node")
	f.StringVar(&o.ioengine, "ioengine", "libaio", "fio I/O engine")
	f.IntVar(&o.runtimeSec, "runtime", 30, "duration of each fio workload in seconds")
	f.StringVar(&o.image, "image", defaultFioImage, "fio container image")
	f.BoolVar(&o.keep, "keep", false, "keep the Job and scratch PVC after completion")
	f.BoolVar(&o.dryRun, "dry-run", false, "print the PVC and Job that would run and exit")
	f.BoolVarP(&o.yes, "yes", "y", false, "skip the confirmation prompt")
	f.DurationVar(&o.timeout, "timeout", defaultBenchTimeout, "overall timeout for the benchmark")
	return cmd
}

func runBenchFio(ctx context.Context, clusterName string, o fioBenchOptions) error {
	if err := validateFioBenchOptions(o); err != nil {
		return err
	}
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}
	prompt := fmt.Sprintf(
		"fio can be I/O-intensive and may affect workloads sharing the disk. Benchmark storage for %q?",
		cluster.Name)
	if !o.dryRun && !plugin.Confirm(prompt, o.yes) {
		fmt.Println("aborted")
		return nil
	}

	storageClass := o.storageClass
	if storageClass == "" && cluster.Spec.Storage.StorageClass != nil {
		storageClass = *cluster.Spec.Storage.StorageClass
	}
	pvc := buildFioPVC(cluster, storageClass, o.size)
	job := buildFioJob(cluster, pvc.Name, o)
	return runBenchJob(ctx, env, job, []client.Object{pvc}, o.keep, o.dryRun, o.timeout)
}

// validateFioBenchOptions checks flags that would otherwise fail inside the Job.
func validateFioBenchOptions(o fioBenchOptions) error {
	size, err := resource.ParseQuantity(o.size)
	if err != nil {
		return fmt.Errorf("invalid --size %q: %w", o.size, err)
	}
	fileSize, err := resource.ParseQuantity(o.fileSize)
	if err != nil {
		return fmt.Errorf("invalid --file-size %q: %w", o.fileSize, err)
	}
	if fileSize.Cmp(size) >= 0 {
		return fmt.Errorf("--file-size (%s) must be smaller than --size (%s)", o.fileSize, o.size)
	}
	if o.ioengine == "" {
		return fmt.Errorf("--ioengine must not be empty")
	}
	if o.runtimeSec < 1 {
		return fmt.Errorf("--runtime must be positive")
	}
	if o.image == "" {
		return fmt.Errorf("--image must not be empty")
	}
	return nil
}

// buildFioPVC provisions the scratch volume fio writes to. An empty storageClass
// leaves the field nil so the cluster's default storage class is used.
func buildFioPVC(cluster *mysqlv1alpha1.Cluster, storageClass, size string) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-bench-fio",
			Namespace: cluster.Namespace,
			Labels:    benchLabels(cluster),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
	if storageClass != "" {
		pvc.Spec.StorageClassName = &storageClass
	}
	return pvc
}

// buildFioJob assembles the Job that mounts the scratch PVC and runs fio.
func buildFioJob(cluster *mysqlv1alpha1.Cluster, pvcName string, o fioBenchOptions) *batchv1.Job {
	env := []corev1.EnvVar{
		{Name: "IOENGINE", Value: o.ioengine},
		{Name: "FIO_FILE_SIZE", Value: o.fileSize},
		{Name: "FIO_RUNTIME", Value: fmt.Sprint(o.runtimeSec)},
	}
	volumes := []corev1.Volume{{
		Name: "scratch",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
		},
	}}
	mounts := []corev1.VolumeMount{{Name: "scratch", MountPath: "/data"}}
	storage := volumesWithMounts{volumes: volumes, mounts: mounts}
	return benchJob(cluster, cluster.Name+"-bench-fio", o.image, fioScript, env, storage, o.node)
}

// fioScript runs a small suite of workloads (random read, write and mixed) and
// removes the test files afterwards so a --keep'd PVC starts clean next time.
const fioScript = `set -eu
cd /data
for mode in randread randwrite randrw; do
  echo "[bench] ===== fio $mode ====="
  fio --name="$mode" --directory=/data --rw="$mode" --ioengine="$IOENGINE" \
    --bs=4k --size="$FIO_FILE_SIZE" --numjobs=1 --iodepth=32 \
    --time_based --runtime="$FIO_RUNTIME" --group_reporting
  rm -f /data/"$mode".* 2>/dev/null || true
done
echo "[bench] done"
`

// volumesWithMounts bundles a Job's volumes and their in-container mounts.
type volumesWithMounts struct {
	volumes []corev1.Volume
	mounts  []corev1.VolumeMount
}

// benchJob is the shared Job skeleton for both benchmarks: a single container
// running the given script, RestartPolicy Never, no retries, and identifying
// labels. storage is optional (fio mounts a scratch PVC); node pins the pod.
func benchJob(
	cluster *mysqlv1alpha1.Cluster, name, image, script string,
	env []corev1.EnvVar, storage volumesWithMounts, node string,
) *batchv1.Job {
	backoff := int32(0)
	podSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers: []corev1.Container{{
			Name:         "bench",
			Image:        image,
			Command:      []string{"sh", "-c", script},
			Env:          env,
			VolumeMounts: storage.mounts,
		}},
		Volumes: storage.volumes,
	}
	if node != "" {
		podSpec.NodeName = node
	}
	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    benchLabels(cluster),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: benchLabels(cluster)},
				Spec:       podSpec,
			},
		},
	}
}

func benchLabels(cluster *mysqlv1alpha1.Cluster) map[string]string {
	return map[string]string{
		benchManagedByLabel: benchManagedByValue,
		benchClusterLabel:   cluster.Name,
	}
}

// rootSecretName resolves the Secret holding the cluster's root password,
// honoring an explicit spec.rootPasswordSecret and otherwise defaulting to the
// operator-generated <cluster>-root Secret (mirrors shell.go).
func rootSecretName(cluster *mysqlv1alpha1.Cluster) string {
	if cluster.Spec.RootPasswordSecret != nil && cluster.Spec.RootPasswordSecret.Name != "" {
		return cluster.Spec.RootPasswordSecret.Name
	}
	return cluster.Name + "-root"
}

// buildBenchSecret holds the generated password for the ephemeral bench user.
// It is created before the Job and deleted with it (unless --keep).
func buildBenchSecret(cluster *mysqlv1alpha1.Cluster, name, password string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    benchLabels(cluster),
		},
		StringData: map[string]string{secretPasswordKey: password},
	}
}

// benchSetupSQL creates the scratch database and a network-capable user scoped
// to it. dbName is validated as an identifier and password is random hex, so
// both are safe to interpolate.
func benchSetupSQL(dbName, user, password string) string {
	return fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s`;\n"+
			"CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';\n"+
			"ALTER USER '%s'@'%%' IDENTIFIED BY '%s';\n"+
			"GRANT ALL PRIVILEGES ON `%s`.* TO '%s'@'%%';\n",
		dbName, user, password, user, password, dbName, user)
}

// benchTeardownSQL drops the ephemeral bench user. The scratch database is left
// in place (sysbench cleanup already dropped its tables) so we never risk
// dropping a database a caller pointed --db-name at deliberately.
func benchTeardownSQL(user string) string {
	return fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%';\n", user)
}

// execRootSQL runs SQL as root over the primary's local socket via `kubectl
// exec` (the same transport shell.go uses). SQL is fed on stdin so no password
// interpolated into it is ever exposed on a command line.
func execRootSQL(ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster, sql string) error {
	primary := plugin.PrimaryInstance(cluster)
	if primary == "" {
		return fmt.Errorf("cluster %q has no primary yet", cluster.Name)
	}
	password, err := readRootPassword(ctx, env, cluster)
	if err != nil {
		return err
	}
	shellCmd := fmt.Sprintf("MYSQL_PWD='%s' %s --socket=/var/run/mysqld/mysqld.sock --user=root",
		strings.ReplaceAll(password, "'", "'\\''"), mysqlClientBinary(cluster))
	args := []string{"exec", "-i", "-n", cluster.Namespace, primary, "-c", "mysql", "--", "sh", "-c", shellCmd}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = strings.NewReader(sql)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("running SQL on %q: %w: %s", primary, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// mysqlClientBinary returns the client binary name matching the cluster's
// flavor (mysql or mariadb).
func mysqlClientBinary(cluster *mysqlv1alpha1.Cluster) string {
	if cluster.ResolvedFlavor() == mysqlv1alpha1.FlavorMariaDB {
		return string(mysqlv1alpha1.FlavorMariaDB)
	}
	return string(mysqlv1alpha1.FlavorMySQL)
}

// readRootPassword loads the cluster root password from its Secret.
func readRootPassword(ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster) (string, error) {
	name := rootSecretName(cluster)
	secret, err := env.Clientset.CoreV1().Secrets(cluster.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting root password secret %q: %w", name, err)
	}
	password := string(secret.Data[secretPasswordKey])
	if password == "" {
		return "", fmt.Errorf("root password secret %q has empty password", name)
	}
	return password, nil
}

// randHex returns a random hex string of 2*n characters, used for the bench
// user's throwaway password.
func randHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating random password: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// runBenchJob creates any prerequisite objects (e.g. a scratch PVC) and the Job,
// waits for its pod, streams the tool output to stdout, then cleans everything
// up unless keep is set. With dryRun it prints the objects as YAML and returns.
func runBenchJob(
	ctx context.Context, env *plugin.Env, job *batchv1.Job, pre []client.Object,
	keep, dryRun bool, timeout time.Duration,
) error {
	if dryRun {
		for _, obj := range pre {
			if err := plugin.PrintObject(obj, "yaml"); err != nil {
				return err
			}
			fmt.Println("---")
		}
		return plugin.PrintObject(job, "yaml")
	}

	for _, obj := range pre {
		if err := env.Client.Create(ctx, obj); err != nil {
			return fmt.Errorf("creating %T %q: %w", obj, obj.GetName(), err)
		}
	}
	if err := env.Client.Create(ctx, job); err != nil {
		// Best-effort cleanup of any prerequisites we already created.
		cleanupBenchObjects(ctx, env, pre)
		return fmt.Errorf("creating benchmark Job %q: %w", job.Name, err)
	}
	if !keep {
		defer func() {
			cleanupBenchObjects(ctx, env, []client.Object{job})
			cleanupBenchObjects(ctx, env, pre)
		}()
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	podName, err := waitForBenchPod(runCtx, env, job.Namespace, job.Name)
	if err != nil {
		return fmt.Errorf("waiting for benchmark pod: %w", err)
	}
	if err := streamPodLogs(runCtx, env, job.Namespace, podName, &corev1.PodLogOptions{Follow: true}, false); err != nil {
		return fmt.Errorf("streaming benchmark output: %w", err)
	}
	phase, err := benchPodPhase(runCtx, env, job.Namespace, podName)
	if err != nil {
		return err
	}
	if phase == corev1.PodFailed {
		return fmt.Errorf("benchmark pod %q failed", podName)
	}
	return nil
}

// waitForBenchPod blocks until the Job's pod exists and has left the Pending
// phase (so its logs can be streamed), returning the pod name.
func waitForBenchPod(ctx context.Context, env *plugin.Env, namespace, jobName string) (string, error) {
	selector := labels.SelectorFromSet(labels.Set{"job-name": jobName}).String()
	for {
		pods, err := env.Clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return "", err
		}
		for i := range pods.Items {
			switch pods.Items[i].Status.Phase {
			case corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed:
				return pods.Items[i].Name, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// benchPodPhase reports the current phase of the benchmark pod.
func benchPodPhase(ctx context.Context, env *plugin.Env, namespace, podName string) (corev1.PodPhase, error) {
	pod, err := env.Clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting benchmark pod %q: %w", podName, err)
	}
	return pod.Status.Phase, nil
}

// cleanupBenchObjects deletes the given objects best-effort, propagating the
// delete in the background so a Job also removes its pods. It uses a fresh
// context so cleanup still runs when the caller's context was cancelled.
func cleanupBenchObjects(ctx context.Context, env *plugin.Env, objs []client.Object) {
	for _, obj := range objs {
		delCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		if err := env.Client.Delete(delCtx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			fmt.Fprintf(os.Stderr, "warning: cleaning up %T %q: %v\n", obj, obj.GetName(), err)
		}
		cancel()
	}
}
