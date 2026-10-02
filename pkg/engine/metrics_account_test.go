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

package engine

import (
	"strings"
	"testing"
)

func TestValidateMetricsPrivilegeNames(t *testing.T) {
	t.Parallel()
	for _, ok := range [][]string{{"SELECT"}, {"select", "Show View"}, {" SHOW  VIEW "}} {
		if err := ValidateMetricsPrivilegeNames(ok); err != nil {
			t.Errorf("%q: unexpected error %v", ok, err)
		}
	}
	for _, bad := range [][]string{
		nil, {}, {"INSERT"}, {"SELECT", "UPDATE"}, {"DELETE"}, {"CREATE"}, {"DROP"},
		{"ALTER"}, {"EXECUTE"}, {"SUPER"}, {"GRANT OPTION"}, {"ALL"}, {"ALL PRIVILEGES"},
		{"PROCESS"}, {"FILE"}, {"SYSTEM_VARIABLES_ADMIN"}, {""},
	} {
		if err := ValidateMetricsPrivilegeNames(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestParseMetricsGrantTarget(t *testing.T) {
	t.Parallel()
	ok := map[string]string{
		"app.*":                "`app`.*",
		"app.items":            "`app`.`items`",
		"`app`.*":              "`app`.*",
		"`my-app`.`t 1`":       "`my-app`.`t 1`",
		"sys.*":                "`sys`.*",
		"performance_schema.*": "`performance_schema`.*",
		"App_1$.T":             "`App_1$`.`T`",
		"mysqlx.*":             "`mysqlx`.*",
	}
	for on, want := range ok {
		target, err := ParseMetricsGrantTarget(on)
		if err != nil {
			t.Errorf("%q: unexpected error %v", on, err)
			continue
		}
		if got := target.SQL(); got != want {
			t.Errorf("%q: SQL() = %q, want %q", on, got, want)
		}
	}
	bad := []string{
		"", "*.*", "*", "app", "app.", ".t", "*.t", "app.*.*", "app.t.u",
		"mysql.*", "MySQL.user", "`mysql`.*", "mysq_.*", "my_ql.*", "_____.*",
		"app%.*", "`a%`.*", "`a\\b`.*", "app.`t`x", "`app.*", "app;DROP.*",
		"app.* TO x", "`ap`p`.*",
	}
	for _, on := range bad {
		if _, err := ParseMetricsGrantTarget(on); err == nil {
			t.Errorf("%q: expected an error", on)
		}
	}
}

func TestMetricsAccountBaseGrants(t *testing.T) {
	t.Parallel()
	base := MetricsAccountBaseGrants()
	rendered := make([]string, 0, len(base))
	for _, g := range base {
		rendered = append(rendered, strings.Join(g.Privileges, ", ")+" ON "+g.On)
	}
	want := "PROCESS, REPLICATION CLIENT, REPLICATION SLAVE ON *.*|SELECT ON performance_schema.*"
	if got := strings.Join(rendered, "|"); got != want {
		t.Fatalf("base grants = %q, want %q", got, want)
	}
}
