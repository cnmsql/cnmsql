//go:build integration

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

package integration

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// flavor describes a supported MySQL/Percona version under test. The integration
// suite consumes the published slim instance images (built and pushed from the
// separate containers repo) rather than building them here; testcontainers pulls
// the image on demand. The version matrix mirrors the containers repo's
// images/versions.json — keep the two in sync.
type flavor struct {
	// name is the subtest name and the image tag (the major key, e.g. "8.0").
	name string
	// version is passed to the manager as --server-version and must match the
	// major.minor of the server installed in the image.
	version string
	// hasAdminInterface is true for servers with the administrative interface
	// (8.0.14+); older servers reach the control connection over the socket.
	hasAdminInterface bool
	// joinSupported is false where XtraBackup-based replica provisioning cannot
	// run on the flavor's image (see runJoinTest).
	joinSupported bool
}

// flavors is the matrix of MySQL versions the operator targets. It mirrors the
// containers repo's images/versions.json; keep the two in sync.
var flavors = []flavor{
	{
		name:              "8.0",
		version:           "8.0.46",
		hasAdminInterface: true,
		joinSupported:     true,
	},
	{
		name:              "8.4",
		version:           "8.4.0",
		hasAdminInterface: true,
		joinSupported:     true,
	},
	{
		name:              "9.x",
		version:           "9.6.0",
		hasAdminInterface: true,
		joinSupported:     true,
	},
}

// selectedFlavors returns the flavors to exercise. By default it is the full
// matrix; setting E2E_MYSQL_VERSION pins a single version so a CI matrix job can
// run one flavor per job, mirroring the e2e suite.
func selectedFlavors(t *testing.T) []flavor {
	t.Helper()
	want := strings.TrimSpace(os.Getenv("E2E_MYSQL_VERSION"))
	if want == "" {
		return flavors
	}
	for _, f := range flavors {
		if f.name == want {
			return []flavor{f}
		}
	}
	t.Fatalf("E2E_MYSQL_VERSION=%q matches no known flavor", want)
	return nil
}

// instanceImageRepo is the GHCR repository the containers repo publishes the
// slim instance images to. Override with INSTANCE_IMAGE_REPO to test against a
// fork or a private mirror.
func instanceImageRepo() string {
	if v := strings.TrimSpace(os.Getenv("INSTANCE_IMAGE_REPO")); v != "" {
		return v
	}
	return "ghcr.io/cnmsql/cnmsql-instance"
}

// instanceImage returns the published slim instance image reference for a
// flavor. testcontainers pulls it on demand.
func instanceImage(f flavor) string {
	return instanceImageRepo() + ":" + f.name
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
