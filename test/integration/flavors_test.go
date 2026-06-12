/*
Copyright 2026 The CNMySQL Authors.

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

//go:build integration

package integration

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
)

// flavor describes a supported MySQL/Percona version under test. Each one builds
// our own slim instance image (Dockerfile.instance) from a Debian base plus the
// version's Percona APT repositories — we no longer consume the upstream
// percona/percona-server image. The build args mirror images/versions.json.
type flavor struct {
	// name is the subtest name.
	name string
	// base is the Debian base image used for this flavor.
	base string
	// ps and pxb are the percona-release repository keywords for the server and
	// XtraBackup; pxbPackage is the XtraBackup package to install.
	ps         string
	pxb        string
	pxbPackage string
	// component is the percona-release repository component ("release" for GA,
	// "testing" for versions Percona only ships pre-GA, e.g. 9.x).
	component string
	// version is passed to the manager as --server-version and must match the
	// server actually installed by the image.
	version string
	// hasAdminInterface is true for servers with the administrative interface
	// (8.0.14+); older servers reach the control connection over the socket.
	hasAdminInterface bool
	// joinSupported is false where XtraBackup-based replica provisioning cannot
	// run on the flavor's image (see runJoinTest).
	joinSupported bool
}

// flavors is the matrix of MySQL versions the operator targets. It mirrors
// images/versions.json; keep the two in sync.
var flavors = []flavor{
	{
		name:              "8.0",
		base:              "debian:bookworm-slim",
		ps:                "ps-80",
		pxb:               "pxb-80",
		pxbPackage:        "percona-xtrabackup-80",
		component:         "release",
		version:           "8.0.46",
		hasAdminInterface: true,
		joinSupported:     true,
	},
	{
		name:              "8.4",
		base:              "debian:bookworm-slim",
		ps:                "ps-84-lts",
		pxb:               "pxb-84-lts",
		pxbPackage:        "percona-xtrabackup-84",
		component:         "release",
		version:           "8.4.0",
		hasAdminInterface: true,
		joinSupported:     true,
	},
	{
		name:              "9.x",
		base:              "debian:bookworm-slim",
		ps:                "ps-9x-innovation",
		pxb:               "pxb-9x-innovation",
		pxbPackage:        "percona-xtrabackup-96",
		// Percona Server 9.x is only published in the testing channel so far.
		component:         "testing",
		version:           "9.6.0",
		hasAdminInterface: true,
		joinSupported:     true,
	},
}

// buildResult memoises a single image build per flavor.
type buildResult struct {
	once sync.Once
	tag  string
	err  error
}

// instanceBuilds tracks the per-flavor image build so the initdb/run/join tests
// (which run in parallel) build each flavor's image exactly once.
var instanceBuilds sync.Map // flavor name -> *buildResult

// ensureInstanceImage builds this flavor's slim instance image from the repo's
// Dockerfile.instance (once) and returns its tag. We shell out to `docker build`
// rather than let testcontainers build from the context: testcontainers'
// .dockerignore handling drops Go sources our multi-stage build needs.
func ensureInstanceImage(t *testing.T, f flavor) string {
	t.Helper()
	v, _ := instanceBuilds.LoadOrStore(f.name, &buildResult{})
	br := v.(*buildResult)
	br.once.Do(func() {
		repoRoot, err := filepath.Abs("../..")
		if err != nil {
			br.err = err
			return
		}
		tag := "cnmysql-instance-test:" + f.name
		cmd := exec.Command("docker", "build",
			"-f", "Dockerfile.instance",
			"--build-arg", "BASE_IMAGE="+f.base,
			"--build-arg", "PS_REPO="+f.ps,
			"--build-arg", "PXB_REPO="+f.pxb,
			"--build-arg", "PXB_PACKAGE="+f.pxbPackage,
			"--build-arg", "REPO_COMPONENT="+f.component,
			"-t", tag, repoRoot)
		cmd.Dir = repoRoot
		if out, err := cmd.CombinedOutput(); err != nil {
			br.err = fmt.Errorf("building %s: %w\n%s", tag, err, out)
			return
		}
		br.tag = tag
	})
	if br.err != nil {
		t.Fatal(br.err)
	}
	return br.tag
}

// gtidArgs returns the mysqld command-line flags enabling GTID replication for
// the flavor, accounting for the 8.0 rename of log_slave_updates.
func (f flavor) gtidArgs(t *testing.T) string {
	t.Helper()
	v, err := version.Parse(f.version)
	if err != nil {
		t.Fatal(err)
	}
	updates := "--log-slave-updates"
	if v.HasLogReplicaUpdates() {
		updates = "--log-replica-updates=ON"
	}
	return "--gtid-mode=ON --enforce-gtid-consistency=ON --log-bin=binlog " +
		updates + " --binlog-format=ROW"
}

// myCnf renders a minimal [mysqld] configuration for the flavor: GTID
// replication and, where supported, the administrative interface.
func (f flavor) myCnf(t *testing.T, serverID int) string {
	t.Helper()
	v, err := version.Parse(f.version)
	if err != nil {
		t.Fatal(err)
	}
	updates := "log_slave_updates=ON"
	if v.HasLogReplicaUpdates() {
		updates = "log_replica_updates=ON"
	}
	cfg := fmt.Sprintf(`[mysqld]
server-id=%d
gtid_mode=ON
enforce_gtid_consistency=ON
log_bin=binlog
%s
binlog_format=ROW
`, serverID, updates)
	if f.hasAdminInterface {
		cfg += "admin_address=127.0.0.1\nadmin_port=33062\n"
	}
	return cfg
}
