//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"strconv"
	"strings"

	. "github.com/onsi/gomega"
)

// Shared building blocks for the MariaDB feature-parity specs. They mirror the
// MySQL suite's helpers but (a) set flavor: mariadb / the MariaDB image and
// (b) run SQL through the `mariadb` client binary via mariadbExec — the MariaDB
// instance image does not ship the `mysql` symlink the mysqlExec helpers rely on.

// mariadbBasicClusterManifest is the MariaDB counterpart of basicClusterManifest:
// an N-instance async-replication cluster with an app database.
func mariadbBasicClusterManifest(name string, instances int) string {
	return fmt.Sprintf(`apiVersion: mysql.cnmsql.co/v1alpha1
kind: Cluster
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  flavor: mariadb
  instances: %[3]d
  imageName: %[4]s
  storage:
    size: 2Gi
%[5]s
  mysql:
    binlogFormat: ROW
%[6]s
  bootstrap:
    initdb:
      database: app
      owner: app
`, name, testNamespace, instances, mariadbImage, e2eInstanceResources, e2eMySQLParameters)
}

// mariadbStatus returns the value column of a single SHOW STATUS row, or "" if
// the variable is absent or unreadable. It is the MariaDB analogue of
// rawMysqlStatus.
func mariadbStatus(pod, rootPass, variable string) string {
	out, err := mariadbExec(pod, "root", rootPass, "",
		fmt.Sprintf("SHOW STATUS LIKE '%s'", variable))
	if err != nil {
		return ""
	}
	fields := strings.Fields(out)
	if len(fields) < 2 {
		return ""
	}
	return strings.Join(fields[1:], " ")
}

// mariadbUptime returns the running server's Uptime (seconds) via SHOW GLOBAL
// STATUS. It never resets while the server stays up, so it is the load-bearing
// signal that an in-place manager swap left the server running.
func mariadbUptime(g Gomega, pod, password string) int {
	out, err := mariadbExec(pod, "app", password, "", "SHOW GLOBAL STATUS LIKE 'Uptime';")
	g.Expect(err).NotTo(HaveOccurred())
	fields := strings.Fields(out)
	g.Expect(len(fields)).To(BeNumerically(">=", 2), "Uptime not found in %q", out)
	uptime, err := strconv.Atoi(fields[len(fields)-1])
	g.Expect(err).NotTo(HaveOccurred(), "could not parse Uptime from %q", out)
	return uptime
}

// mariadbSemiSyncWaitCount reads the primary's semi-sync acknowledgement count
// (rpl_semi_sync_master_wait_for_slave_count) through the mariadb client.
func mariadbSemiSyncWaitCount(g Gomega, pod, rootPass string) int {
	out, err := mariadbExec(pod, "root", rootPass, "",
		"SHOW VARIABLES LIKE 'rpl_semi_sync%wait_for%count';")
	g.Expect(err).NotTo(HaveOccurred())
	fields := strings.Fields(out)
	g.Expect(len(fields)).To(BeNumerically(">=", 2),
		"semi-sync wait count variable not found in %q", out)
	count, err := strconv.Atoi(fields[len(fields)-1])
	g.Expect(err).NotTo(HaveOccurred(), "could not parse wait count from %q", out)
	return count
}
