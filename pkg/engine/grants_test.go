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

import "testing"

func TestParseGrantTarget(t *testing.T) {
	t.Parallel()
	ok := map[string]string{
		"*.*":            "*.*",
		"app.*":          "`app`.*",
		"app.items":      "`app`.`items`",
		"`app`.*":        "`app`.*",
		"`my-app`.`t 1`": "`my-app`.`t 1`",
		"mysql.*":        "`mysql`.*",
		"`app_%`.*":      "`app_%`.*",
	}
	for on, want := range ok {
		target, err := ParseGrantTarget(on)
		if err != nil {
			t.Errorf("%q: unexpected error %v", on, err)
			continue
		}
		if got := target.SQL(); got != want {
			t.Errorf("%q: SQL() = %q, want %q", on, got, want)
		}
	}
	bad := []string{
		"", "*", "app", "app.", ".t", "*.t", "app.*.*", "app.t.u", "* . *", " *.*", "*.* ",
		"`*`.*", "app.`*`", "`app.*", "app.`t`x", "`ap`p`.*", "app;DROP.*",
		"app.* TO 'other'@'%' WITH GRANT OPTION #", "tenant_a.* TO x", "*.* WITH GRANT OPTION",
	}
	for _, on := range bad {
		if _, err := ParseGrantTarget(on); err == nil {
			t.Errorf("%q: expected an error", on)
		}
	}
}

func TestNormalizePrivilegeName(t *testing.T) {
	t.Parallel()
	ok := map[string]string{
		"select":                  "SELECT",
		" Show  View ":            "SHOW VIEW",
		"ALL\tPRIVILEGES":         "ALL PRIVILEGES",
		"create temporary tables": "CREATE TEMPORARY TABLES",
		"system_variables_admin":  "SYSTEM_VARIABLES_ADMIN",
		"READ_ONLY ADMIN":         "READ_ONLY ADMIN",
	}
	for in, want := range ok {
		got, err := NormalizePrivilegeName(in)
		if err != nil {
			t.Errorf("%q: unexpected error %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
	for _, bad := range []string{
		"", "  ", "SELECT, SUPER", "SELECT,SYSTEM_VARIABLES_ADMIN", "SELECT (c1)", "SELECT ON",
		"ALL ON *.* TO x", "SELECT;", "1SELECT", "SELECT-X", "ALL PRIVILEGES WITH GRANT OPTION",
	} {
		if _, err := NormalizePrivilegeName(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestGrantDatabaseMatches(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		db, schema string
		want       bool
	}{
		{"mysql", "mysql", true},
		{"MySQL", "mysql", true},
		{"my_ql", "mysql", true},
		{"_____", "mysql", true},
		{"mysqlx", "mysql", false},
		{"mysq", "mysql", false},
		{"tenant_a", "tenantXa", true},
		{"tenant_a", "tenant_b", false},
	} {
		if got := GrantDatabaseMatches(tc.db, tc.schema); got != tc.want {
			t.Errorf("GrantDatabaseMatches(%q, %q) = %v, want %v", tc.db, tc.schema, got, tc.want)
		}
	}
}
