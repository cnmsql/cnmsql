package cmd

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

func defaultMySQLBenchOptions() mysqlBenchOptions {
	return mysqlBenchOptions{
		tables:    10,
		tableSize: 10000,
		threads:   4,
		timeSec:   60,
		tests:     []string{"oltp_read_write", "oltp_point_select"},
		dbName:    "sbtest",
		image:     defaultSysbenchImage,
		timeout:   defaultBenchTimeout,
	}
}

func defaultFioBenchOptions() fioBenchOptions {
	return fioBenchOptions{
		size:       "2Gi",
		fileSize:   "1Gi",
		ioengine:   "libaio",
		runtimeSec: 30,
		image:      defaultFioImage,
		timeout:    defaultBenchTimeout,
	}
}

func TestValidateMySQLBenchOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*mysqlBenchOptions)
		wantErr string
	}{
		{name: "valid"},
		{name: "no tests", mutate: func(o *mysqlBenchOptions) { o.tests = nil }, wantErr: "at least one workload"},
		{
			name: "unknown test", mutate: func(o *mysqlBenchOptions) { o.tests = []string{"oltp_bogus"} },
			wantErr: "unknown sysbench test",
		},
		{name: "zero tables", mutate: func(o *mysqlBenchOptions) { o.tables = 0 }, wantErr: "must all be positive"},
		{name: "empty db", mutate: func(o *mysqlBenchOptions) { o.dbName = "" }, wantErr: "--db-name"},
		{name: "empty image", mutate: func(o *mysqlBenchOptions) { o.image = "" }, wantErr: "--image"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := defaultMySQLBenchOptions()
			if tt.mutate != nil {
				tt.mutate(&o)
			}
			err := validateMySQLBenchOptions(o)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateMySQLBenchOptions() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateFioBenchOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*fioBenchOptions)
		wantErr string
	}{
		{name: "valid"},
		{name: "bad size", mutate: func(o *fioBenchOptions) { o.size = "big" }, wantErr: "invalid --size"},
		{name: "bad file size", mutate: func(o *fioBenchOptions) { o.fileSize = "big" }, wantErr: "invalid --file-size"},
		{
			name: "file not smaller", mutate: func(o *fioBenchOptions) { o.fileSize = "2Gi" },
			wantErr: "must be smaller than",
		},
		{name: "empty ioengine", mutate: func(o *fioBenchOptions) { o.ioengine = "" }, wantErr: "--ioengine"},
		{name: "zero runtime", mutate: func(o *fioBenchOptions) { o.runtimeSec = 0 }, wantErr: "--runtime"},
		{name: "empty image", mutate: func(o *fioBenchOptions) { o.image = "" }, wantErr: "--image"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := defaultFioBenchOptions()
			if tt.mutate != nil {
				tt.mutate(&o)
			}
			err := validateFioBenchOptions(o)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateFioBenchOptions() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestBuildSysbenchJob(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	job := buildSysbenchJob(&cluster, benchUser, cluster.Name+"-bench-mysql", defaultMySQLBenchOptions())

	if job.Namespace != "test" || job.Name != "demo-bench-mysql" {
		t.Fatalf("job meta = %s/%s", job.Namespace, job.Name)
	}
	if job.Labels[benchClusterLabel] != cluster.Name || job.Labels[benchManagedByLabel] != benchManagedByValue {
		t.Errorf("job labels = %v", job.Labels)
	}
	if *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %d, want 0", *job.Spec.BackoffLimit)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != defaultSysbenchImage {
		t.Errorf("image = %q", container.Image)
	}
	env := envMap(container.Env)
	if env["MYSQL_HOST"] != "demo-rw" || env["DB_NAME"] != "sbtest" || env["BENCH_USER"] != benchUser {
		t.Errorf("env = %v", env)
	}
	if env["SB_TESTS"] != "oltp_read_write oltp_point_select" {
		t.Errorf("SB_TESTS = %q", env["SB_TESTS"])
	}
	pwd := findEnv(container.Env, "MYSQL_PWD")
	if pwd == nil || pwd.ValueFrom == nil || pwd.ValueFrom.SecretKeyRef == nil ||
		pwd.ValueFrom.SecretKeyRef.Name != "demo-bench-mysql" || pwd.ValueFrom.SecretKeyRef.Key != secretPasswordKey {
		t.Errorf("MYSQL_PWD secretKeyRef = %#v", pwd)
	}
	// The password travels as a secretKeyRef env var, never as a literal value.
	if pwd.Value != "" {
		t.Errorf("MYSQL_PWD carries a literal value %q", pwd.Value)
	}
}

func TestBenchSetupSQL(t *testing.T) {
	t.Parallel()
	sql := benchSetupSQL("sbtest", benchUser, "deadbeef")
	for _, want := range []string{
		"CREATE DATABASE IF NOT EXISTS `sbtest`",
		"CREATE USER IF NOT EXISTS 'cnmsql_bench'@'%' IDENTIFIED BY 'deadbeef'",
		"GRANT ALL PRIVILEGES ON `sbtest`.* TO 'cnmsql_bench'@'%'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("setup SQL missing %q in:\n%s", want, sql)
		}
	}
	if teardown := benchTeardownSQL(benchUser); !strings.Contains(teardown, "DROP USER IF EXISTS 'cnmsql_bench'@'%'") {
		t.Errorf("teardown SQL = %q", teardown)
	}
}

func TestRandHexDistinct(t *testing.T) {
	t.Parallel()
	a, err := randHex(16)
	if err != nil {
		t.Fatalf("randHex() error = %v", err)
	}
	b, _ := randHex(16)
	if len(a) != 32 || a == b {
		t.Errorf("randHex() = %q, %q", a, b)
	}
}

func TestBuildFioPVCAndJob(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	o := defaultFioBenchOptions()
	o.node = "worker-2"

	pvc := buildFioPVC(&cluster, "fast", o.size)
	if pvc.Name != "demo-bench-fio" || pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast" {
		t.Fatalf("pvc = %#v", pvc.Spec)
	}
	if got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "2Gi" {
		t.Errorf("pvc size = %s", got.String())
	}

	job := buildFioJob(&cluster, pvc.Name, o)
	if job.Spec.Template.Spec.NodeName != "worker-2" {
		t.Errorf("nodeName = %q", job.Spec.Template.Spec.NodeName)
	}
	vols := job.Spec.Template.Spec.Volumes
	if len(vols) != 1 || vols[0].PersistentVolumeClaim == nil ||
		vols[0].PersistentVolumeClaim.ClaimName != pvc.Name {
		t.Fatalf("volumes = %#v", vols)
	}
	mounts := job.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) != 1 || mounts[0].MountPath != "/data" {
		t.Errorf("mounts = %#v", mounts)
	}
	env := envMap(job.Spec.Template.Spec.Containers[0].Env)
	if env["IOENGINE"] != "libaio" || env["FIO_FILE_SIZE"] != "1Gi" || env["FIO_RUNTIME"] != "30" {
		t.Errorf("env = %v", env)
	}
}

func TestBuildFioPVCDefaultsStorageClass(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	pvc := buildFioPVC(&cluster, "", "1Gi")
	if pvc.Spec.StorageClassName != nil {
		t.Errorf("StorageClassName = %v, want nil (cluster default)", *pvc.Spec.StorageClassName)
	}
}

func TestRootSecretName(t *testing.T) {
	t.Parallel()
	cluster := testCluster()
	if got := rootSecretName(&cluster); got != "demo-root" {
		t.Errorf("rootSecretName() = %q, want demo-root", got)
	}
	cluster.Spec.RootPasswordSecret = &mysqlv1alpha1.LocalObjectReference{Name: "custom"}
	if got := rootSecretName(&cluster); got != "custom" {
		t.Errorf("rootSecretName() = %q, want custom", got)
	}
}

func TestBenchTimeoutDefault(t *testing.T) {
	t.Parallel()
	if defaultBenchTimeout < time.Minute {
		t.Errorf("defaultBenchTimeout = %s, want >= 1m", defaultBenchTimeout)
	}
}

func envMap(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

func findEnv(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}
