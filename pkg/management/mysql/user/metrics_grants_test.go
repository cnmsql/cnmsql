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

package user

import (
	"slices"
	"testing"
)

var metricsBase = []Privilege{
	{Privileges: []string{"PROCESS", "REPLICATION CLIENT", "REPLICATION SLAVE"}, On: "*.*"},
	{Privileges: []string{"SELECT"}, On: "performance_schema.*"},
}

const (
	mysqlBaseGlobal = "GRANT PROCESS, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `cnmsql_metrics`@`localhost`"
	mysqlBasePS     = "GRANT SELECT ON `performance_schema`.* TO `cnmsql_metrics`@`localhost`"
	appSelect       = "GRANT SELECT ON `app`.* TO `cnmsql_metrics`@`localhost`"
)

func planMetrics(observed []string, declared ...Privilege) MetricsGrantPlan {
	return PlanMetricsGrants("cnmsql_metrics", "localhost", observed, metricsBase, declared)
}

func TestPlanMetricsGrantsInSync(t *testing.T) {
	t.Parallel()
	for name, observed := range map[string][]string{
		"mysql": {mysqlBaseGlobal, mysqlBasePS, appSelect},
		"mariadb": {
			"GRANT PROCESS, BINLOG MONITOR, REPLICATION SLAVE ON *.* TO `cnmsql_metrics`@`localhost`",
			mysqlBasePS, appSelect,
		},
		"mariadb slave monitor": {
			"GRANT PROCESS, BINLOG MONITOR, SLAVE MONITOR, REPLICATION SLAVE ON *.* TO `cnmsql_metrics`@`localhost`",
			mysqlBasePS, appSelect,
		},
		"ansi quotes": {
			`GRANT PROCESS, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "cnmsql_metrics"@"localhost"`,
			`GRANT SELECT ON "performance_schema".* TO "cnmsql_metrics"@"localhost"`,
			`GRANT SELECT ON "app".* TO "cnmsql_metrics"@"localhost"`,
		},
		"replication replica alias": {
			"GRANT PROCESS, REPLICATION REPLICA, REPLICATION CLIENT ON *.* TO `cnmsql_metrics`@`localhost`",
			mysqlBasePS, appSelect,
		},
	} {
		p := planMetrics(observed, Privilege{Privileges: []string{"SELECT"}, On: "`app`.*"})
		if len(p.Grants) != 0 || len(p.Revokes) != 0 {
			t.Errorf("%s: expected an empty plan, got grants=%q revokes=%q", name, p.Grants, p.Revokes)
		}
	}
}

func TestPlanMetricsGrantsCreatesMissing(t *testing.T) {
	t.Parallel()
	p := planMetrics([]string{"GRANT USAGE ON *.* TO `cnmsql_metrics`@`localhost`"},
		Privilege{Privileges: []string{"SELECT", "SHOW VIEW"}, On: "`app`.*"})
	want := []string{
		"GRANT PROCESS, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'cnmsql_metrics'@'localhost'",
		"GRANT SELECT ON performance_schema.* TO 'cnmsql_metrics'@'localhost'",
		"GRANT SELECT, SHOW VIEW ON `app`.* TO 'cnmsql_metrics'@'localhost'",
	}
	if !slices.Equal(p.Grants, want) {
		t.Fatalf("grants = %q, want %q", p.Grants, want)
	}
	if len(p.Revokes) != 0 {
		t.Fatalf("unexpected revokes %q", p.Revokes)
	}
	if !slices.Contains(p.Granted, "SELECT ON `app`.*") {
		t.Fatalf("granted summary %q lacks the declared grant", p.Granted)
	}
}

func TestPlanMetricsGrantsRevokesExtras(t *testing.T) {
	t.Parallel()
	observed := []string{
		"GRANT PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, SUPER ON *.* TO `cnmsql_metrics`@`localhost`",
		"GRANT BACKUP_ADMIN,FLUSH_TABLES ON *.* TO `cnmsql_metrics`@`localhost`",
		mysqlBasePS,
		"GRANT SELECT, INSERT ON `app`.* TO `cnmsql_metrics`@`localhost` WITH GRANT OPTION",
		"GRANT SELECT (`id`, `name`) ON `app`.`users` TO `cnmsql_metrics`@`localhost`",
		"GRANT EXECUTE ON PROCEDURE `app`.`p` TO `cnmsql_metrics`@`localhost`",
		"GRANT PROXY ON ''@'' TO `cnmsql_metrics`@`localhost`",
		"GRANT `reader`@`%` TO `cnmsql_metrics`@`localhost`",
		"REVOKE SELECT ON `secret`.* FROM `cnmsql_metrics`@`localhost`",
	}
	p := planMetrics(observed, Privilege{Privileges: []string{"SELECT"}, On: "`app`.*"})
	if len(p.Grants) != 0 {
		t.Fatalf("unexpected grants %q", p.Grants)
	}
	want := []string{
		"REVOKE SUPER, BACKUP_ADMIN, FLUSH_TABLES ON *.* FROM 'cnmsql_metrics'@'localhost'",
		"REVOKE INSERT, GRANT OPTION ON `app`.* FROM 'cnmsql_metrics'@'localhost'",
		"REVOKE SELECT (`id`, `name`) ON `app`.`users` FROM 'cnmsql_metrics'@'localhost'",
		"REVOKE EXECUTE ON PROCEDURE `app`.`p` FROM 'cnmsql_metrics'@'localhost'",
		"REVOKE PROXY ON ''@'' FROM 'cnmsql_metrics'@'localhost'",
		"REVOKE `reader`@`%` FROM 'cnmsql_metrics'@'localhost'",
	}
	if !slices.Equal(p.Revokes, want) {
		t.Fatalf("revokes =\n%q\nwant\n%q", p.Revokes, want)
	}
}

func TestPlanMetricsGrantsNeverRevokesBase(t *testing.T) {
	t.Parallel()
	// The declared list named a base grant earlier and no longer does: the
	// observed grant is base, so it stays.
	p := planMetrics([]string{mysqlBaseGlobal, mysqlBasePS})
	if len(p.Revokes) != 0 || len(p.Grants) != 0 {
		t.Fatalf("expected an empty plan, got grants=%q revokes=%q", p.Grants, p.Revokes)
	}
	// Declaring a base grant adds nothing.
	p = planMetrics([]string{mysqlBaseGlobal, mysqlBasePS},
		Privilege{Privileges: []string{"SELECT"}, On: "`performance_schema`.*"})
	if len(p.Revokes) != 0 || len(p.Grants) != 0 {
		t.Fatalf("expected an empty plan, got grants=%q revokes=%q", p.Grants, p.Revokes)
	}
}

func TestPlanMetricsGrantsTargetCase(t *testing.T) {
	t.Parallel()
	// Identifiers are case-sensitive on Linux: App and app are different
	// schemas.
	p := planMetrics([]string{mysqlBaseGlobal, mysqlBasePS, "GRANT SELECT ON `App`.* TO `cnmsql_metrics`@`localhost`"},
		Privilege{Privileges: []string{"SELECT"}, On: "`app`.*"})
	if len(p.Grants) != 1 || len(p.Revokes) != 1 {
		t.Fatalf("grants=%q revokes=%q", p.Grants, p.Revokes)
	}
}

func TestParseShowGrantIgnoresNonGrantLines(t *testing.T) {
	t.Parallel()
	for _, line := range []string{
		"REVOKE SELECT ON `x`.* FROM `cnmsql_metrics`@`localhost`",
		"SET DEFAULT ROLE `r` FOR `cnmsql_metrics`@`localhost`",
		"",
	} {
		if _, ok := parseShowGrant(line); ok {
			t.Errorf("%q: expected to be ignored", line)
		}
	}
}

func TestPlanMetricsGrantsRevokesProxyWithGrantOption(t *testing.T) {
	t.Parallel()
	// REVOKE PROXY takes no other privilege in its list, and removing the
	// proxy row also removes its grant option.
	p := planMetrics([]string{mysqlBaseGlobal, mysqlBasePS,
		"GRANT PROXY ON ``@`` TO `cnmsql_metrics`@`localhost` WITH GRANT OPTION"})
	want := []string{"REVOKE PROXY ON ``@`` FROM 'cnmsql_metrics'@'localhost'"}
	if !slices.Equal(p.Revokes, want) {
		t.Fatalf("revokes = %q, want %q", p.Revokes, want)
	}
}
