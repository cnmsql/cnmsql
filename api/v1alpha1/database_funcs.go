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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// databaseReservedSchemas are the schemas a Database may not manage: the
// server's system schemas and the operator's heartbeat schema. Managing one
// would hand its inline users that schema, and ALL on mysql.* is root.
var databaseReservedSchemas = []string{
	"mysql", "information_schema", "performance_schema", "sys", "heartbeat",
}

// databaseAllowedPrivileges are the privileges a Database inline user may be
// granted: the schema-level data and DDL privileges, plus ALL, which at
// schema level means exactly those. Global and dynamic privileges, and GRANT
// OPTION, are left out, since a Database is the unit handed to a tenant.
var databaseAllowedPrivileges = func() map[string]bool {
	allowed := map[string]bool{"ALL": true, "ALL PRIVILEGES": true}
	for _, p := range SafeDBaaSAdminPrivileges() {
		allowed[p] = true
	}
	return allowed
}()

// DatabaseName returns the MySQL schema name, defaulting to the resource name.
func (d *Database) DatabaseName() string {
	if d.Spec.Name != "" {
		return d.Spec.Name
	}
	return d.Name
}

// Validate checks a Database: the schema must not be a system or operator
// schema, and every inline user grant must hold only schema-level privileges
// on the managed schema or a table in it.
func (d *Database) Validate() field.ErrorList {
	var allErrs field.ErrorList
	spec := field.NewPath("spec")

	name := d.DatabaseName()
	namePath := spec.Child("name")
	if d.Spec.Name == "" {
		namePath = field.NewPath("metadata", "name")
	}
	if err := validateManagedSchemaName(name); err != nil {
		allErrs = append(allErrs, field.Invalid(namePath, name, err.Error()))
	}

	for i := range d.Spec.Users {
		for j, g := range d.Spec.Users[i].Grants {
			allErrs = append(allErrs,
				validateDatabaseGrant(spec.Child("users").Index(i).Child("grants").Index(j), g, name)...)
		}
	}
	return allErrs
}

// validateManagedSchemaName refuses a schema name that GRANT would read as a
// pattern, or that names (or, through the "_" wildcard, matches) a reserved
// schema.
func validateManagedSchemaName(name string) error {
	if strings.ContainsAny(name, "%\\`") {
		return fmt.Errorf("schema name %q may not contain %%, \\ or `", name)
	}
	for _, schema := range databaseReservedSchemas {
		if engine.GrantDatabaseMatches(name, schema) {
			return fmt.Errorf("schema name %q covers the reserved %s schema", name, schema)
		}
	}
	return nil
}

func validateDatabaseGrant(path *field.Path, g DatabaseGrant, dbName string) field.ErrorList {
	var allErrs field.ErrorList
	if len(g.Privileges) == 0 {
		allErrs = append(allErrs, field.Required(path.Child("privileges"), "a grant must list at least one privilege"))
	}
	for k, priv := range g.Privileges {
		p := path.Child("privileges").Index(k)
		name, err := engine.NormalizePrivilegeName(priv)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(p, priv, err.Error()))
			continue
		}
		if !databaseAllowedPrivileges[name] {
			allErrs = append(allErrs, field.Invalid(p, priv, fmt.Sprintf(
				"a Database can only grant schema-level privileges (%s or ALL); use a DatabaseUser for anything wider",
				strings.Join(SafeDBaaSAdminPrivileges(), ", "))))
		}
	}
	if g.On == "" {
		return allErrs
	}
	target, err := engine.ParseGrantTarget(g.On)
	if err != nil {
		return append(allErrs, field.Invalid(path.Child("on"), g.On, err.Error()))
	}
	if target.Database != dbName {
		allErrs = append(allErrs, field.Invalid(path.Child("on"), g.On,
			fmt.Sprintf("a Database can only grant on its own schema: use %s or a table in it, or omit on",
				engine.GrantTarget{Database: dbName}.SQL())))
	}
	return allErrs
}
