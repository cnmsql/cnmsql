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

package controller

import (
	"strings"
	"testing"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// mariadbPlan is a single-instance plan for a MariaDB 11.4 cluster.
func mariadbPlan() clusterPlan {
	plan := testPlan()
	plan.Image = "ghcr.io/cnmsql/cnmsql-mariadb-instance:11.4"
	plan.ServerVersion = "11.4.3"
	plan.Flavor = mysqlv1alpha1.FlavorMariaDB
	return plan
}

// TestRenderMyCnfMariaDBIsValid guards the end-to-end rendering path for a
// MariaDB cluster: the controller must select the MariaDB engine and emit a
// my.cnf mariadbd accepts. The manual smoke test regressed here — mariadbd
// aborts data-dir init on the MySQL-only gtid_mode/enforce_gtid_consistency
// variables.
func TestRenderMyCnfMariaDBIsValid(t *testing.T) {
	cluster := baseCluster()
	cluster.Spec.Flavor = mysqlv1alpha1.FlavorMariaDB

	out, err := (&ClusterReconciler{}).renderMyCnf(
		cluster, mariadbPlan(),
		instancePlan{ServerID: 1, IsPrimary: true, ServiceName: "demo-1"},
		[]string{"demo-1"},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Variables mariadbd rejects as unknown (MySQL-only).
	for _, banned := range []string{
		"gtid_mode",
		"enforce_gtid_consistency",
		"admin_address",
		"admin_port",
		"log_replica_updates",
	} {
		if strings.Contains(out, banned) {
			t.Errorf("rendered MariaDB my.cnf must not contain %q:\n%s", banned, out)
		}
	}

	// MariaDB-appropriate settings.
	for _, needle := range []string{
		"gtid_strict_mode = ON",
		"log_slave_updates = ON",
		"log_bin = binlog",
	} {
		if !strings.Contains(out, needle) {
			t.Errorf("rendered MariaDB my.cnf missing %q:\n%s", needle, out)
		}
	}
}

// TestRenderMyCnfMySQLStillEmitsGTIDMode ensures the flavor split did not
// change MySQL output: the default flavor keeps gtid_mode/enforce.
func TestRenderMyCnfMySQLStillEmitsGTIDMode(t *testing.T) {
	out, err := (&ClusterReconciler{}).renderMyCnf(
		baseCluster(), testPlan(),
		instancePlan{ServerID: 1, IsPrimary: true, ServiceName: "demo-1"},
		[]string{"demo-1"},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{"gtid_mode = ON", "enforce_gtid_consistency = ON"} {
		if !strings.Contains(out, needle) {
			t.Errorf("rendered MySQL my.cnf missing %q:\n%s", needle, out)
		}
	}
	if strings.Contains(out, "gtid_strict_mode") {
		t.Errorf("MySQL my.cnf must not contain gtid_strict_mode:\n%s", out)
	}
}
