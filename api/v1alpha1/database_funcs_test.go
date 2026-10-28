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

package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func validDatabase(mutate func(*Database)) *Database {
	d := &Database{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-a"},
		Spec: DatabaseSpec{
			Cluster: LocalObjectReference{Name: "c"},
			Name:    "tenant_a",
			Users: []InlineUser{{
				Name:   "tenant_a_app",
				Grants: []DatabaseGrant{{Privileges: []string{"SELECT", "INSERT", "UPDATE", "DELETE"}}},
			}},
		},
	}
	if mutate != nil {
		mutate(d)
	}
	return d
}

func withGrant(g DatabaseGrant) func(*Database) {
	return func(d *Database) { d.Spec.Users[0].Grants = []DatabaseGrant{g} }
}

func TestDatabaseValidateAccepts(t *testing.T) {
	for name, mutate := range map[string]func(*Database){
		"default target":        nil,
		"ALL on own schema":     withGrant(DatabaseGrant{Privileges: []string{"ALL"}, On: "tenant_a.*"}),
		"quoted own schema":     withGrant(DatabaseGrant{Privileges: []string{"all privileges"}, On: "`tenant_a`.*"}),
		"table in own schema":   withGrant(DatabaseGrant{Privileges: []string{"SELECT"}, On: "tenant_a.orders"}),
		"safe admin privileges": withGrant(DatabaseGrant{Privileges: SafeDBaaSAdminPrivileges()}),
		"normalised spacing":    withGrant(DatabaseGrant{Privileges: []string{"show  view", "Lock Tables"}}),
		"name from metadata": func(d *Database) {
			d.Spec.Name = ""
			d.Spec.Users[0].Grants = []DatabaseGrant{{Privileges: []string{"SELECT"}, On: "`tenant-a`.*"}}
		},
	} {
		if errs := validDatabase(mutate).Validate(); len(errs) != 0 {
			t.Errorf("%s: unexpected errors %v", name, errs)
		}
	}
}

// The GHSA-jpxg-rv6j-7mf4 reproductions and their variants must be refused.
func TestDatabaseValidateRejects(t *testing.T) {
	for name, mutate := range map[string]func(*Database){
		"ALL on *.*":         withGrant(DatabaseGrant{Privileges: []string{"ALL"}, On: "*.*"}),
		"SELECT on *.*":      withGrant(DatabaseGrant{Privileges: []string{"SELECT"}, On: "*.*"}),
		"other tenant":       withGrant(DatabaseGrant{Privileges: []string{"SELECT"}, On: "tenant_b.*"}),
		"mysql schema":       withGrant(DatabaseGrant{Privileges: []string{"SELECT"}, On: "mysql.user"}),
		"injected grantee":   withGrant(DatabaseGrant{Privileges: []string{"SELECT"}, On: "tenant_a.* TO 'other'@'%' WITH GRANT OPTION #"}),
		"spaced global":      withGrant(DatabaseGrant{Privileges: []string{"SELECT"}, On: "* . *"}),
		"dynamic privilege":  withGrant(DatabaseGrant{Privileges: []string{"SYSTEM_VARIABLES_ADMIN"}}),
		"global privilege":   withGrant(DatabaseGrant{Privileges: []string{"CREATE USER"}}),
		"super":              withGrant(DatabaseGrant{Privileges: []string{"SUPER"}}),
		"grant option":       withGrant(DatabaseGrant{Privileges: []string{"GRANT OPTION"}}),
		"list in one entry":  withGrant(DatabaseGrant{Privileges: []string{"SELECT, SUPER"}}),
		"injected privilege": withGrant(DatabaseGrant{Privileges: []string{"ALL ON *.* TO 'x'@'%' #"}}),
		"no privileges":      withGrant(DatabaseGrant{}),
		"schema mysql":       func(d *Database) { d.Spec.Name = "mysql" },
		"schema MySQL":       func(d *Database) { d.Spec.Name = "MySQL" },
		"schema my_ql":       func(d *Database) { d.Spec.Name = "my_ql" },
		"schema sys":         func(d *Database) { d.Spec.Name = "sys" },
		"schema heartbeat":   func(d *Database) { d.Spec.Name = "heartbeat" },
		"schema perf":        func(d *Database) { d.Spec.Name = "performance_schema" },
		"schema wildcard":    func(d *Database) { d.Spec.Name = "tenant%" },
		"schema backslash":   func(d *Database) { d.Spec.Name = `tenant\_a` },
		"schema backtick":    func(d *Database) { d.Spec.Name = "a`b" },
		"metadata name sys":  func(d *Database) { d.Spec.Name, d.Name = "", "sys" },
	} {
		if errs := validDatabase(mutate).Validate(); len(errs) == 0 {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}
