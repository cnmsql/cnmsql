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
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	// tlsNone is the RequireTLS value meaning "no TLS requirement".
	tlsNone = "none"
	// grantTargetAll is the default grant target (all schemas, all tables).
	grantTargetAll = "*.*"
)

// deniedDynamicPrivileges are the cluster-control privileges a DatabaseUser must
// never be granted: they would let a tenant break replication, fencing, server
// configuration, or the operator's own accounts. This is the grant-level
// equivalent of the spec.mysql.parameters denylist and is the real safety net
// behind the "safe DBaaS superuser" recipe (ALL without WITH GRANT OPTION).
var deniedDynamicPrivileges = map[string]bool{
	"replication_slave_admin":              true,
	"replication_applier":                  true,
	"group_replication_admin":              true,
	"group_replication_stream":             true,
	"system_variables_admin":               true,
	"connection_admin":                     true,
	"service_connection_admin":             true,
	"persist_ro_variables_admin":           true,
	"binlog_admin":                         true,
	"binlog_encryption_admin":              true,
	"clone_admin":                          true,
	"super":                                true,
	"shutdown":                             true,
	"file":                                 true,
	"group_replication_flow_control_admin": true,
}

// SafeDBaaSAdminPrivileges is the broad set of static, data-plane privileges for
// a "DBaaS admin": full control over data and schema objects across all
// databases, with none of the global dynamic admin privileges that GRANT ALL ON
// *.* would pull in. Granting these by name (rather than ALL) keeps the tenant
// off the operator's control plane (replication, fencing, server config).
func SafeDBaaSAdminPrivileges() []string {
	return []string{
		"SELECT", "INSERT", "UPDATE", "DELETE",
		"CREATE", "DROP", "ALTER", "INDEX", "REFERENCES",
		"CREATE TEMPORARY TABLES", "LOCK TABLES", "EXECUTE",
		"CREATE VIEW", "SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE",
		"EVENT", "TRIGGER",
	}
}

// SafeDBaaSAdminRevokes is the set of write/DDL privileges to revoke from the
// system schemas for a DBaaS admin built on a broad "*.*" grant. Carving these
// out (with partial_revokes=ON) removes write access to the grant tables, which
// is what otherwise makes a global write grant root-equivalent. It pairs with
// SafeDBaaSAdminPrivileges.
func SafeDBaaSAdminRevokes() []DatabaseUserRevoke {
	writes := []string{
		"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "INDEX",
		"CREATE VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "EVENT", "TRIGGER",
	}
	return []DatabaseUserRevoke{
		{Privileges: writes, On: "mysql.*"},
		{Privileges: writes, On: "sys.*"},
	}
}

// UserName returns the resolved MySQL user name, defaulting to the resource name.
func (u *DatabaseUser) UserName() string {
	if u.Spec.Name != "" {
		return u.Spec.Name
	}
	return u.Name
}

// AdoptRequested reports whether the adopt annotation opts this user into taking
// ownership of a pre-existing MySQL account.
func (u *DatabaseUser) AdoptRequested() bool {
	return strings.EqualFold(u.Annotations[DatabaseUserAdoptAnnotation], "true")
}

// SetDefaults fills in the implicit defaults the API server would otherwise
// apply, so reconciliation and validation see a fully-populated spec.
func (u *DatabaseUser) SetDefaults() {
	if u.Spec.Host == "" {
		u.Spec.Host = "%"
	}
	if u.Spec.Ensure == "" {
		u.Spec.Ensure = EnsurePresent
	}
	if u.Spec.RequireTLS == "" {
		u.Spec.RequireTLS = tlsNone
	}
	if u.Spec.ReclaimPolicy == "" {
		u.Spec.ReclaimPolicy = "retain"
	}
	for i := range u.Spec.Grants {
		if u.Spec.Grants[i].On == "" {
			u.Spec.Grants[i].On = grantTargetAll
		}
	}
}

// Validate checks a DatabaseUser: the name must not be reserved, the host must
// be set, superuser and explicit grants are mutually exclusive, RequireTLS must
// be valid, no grant may request a denied cluster-control privilege, and no grant
// may request ALL on the global *.* target (which would pull in every dynamic
// admin privilege).
func (u *DatabaseUser) Validate() field.ErrorList {
	var allErrs field.ErrorList
	spec := field.NewPath("spec")

	name := u.UserName()
	if name == "" {
		allErrs = append(allErrs, field.Required(spec.Child("name"), "user name is required"))
	} else if isReservedRoleName(name) {
		allErrs = append(allErrs, field.Invalid(spec.Child("name"), name,
			"user name is reserved (root, mysql.*, cnmsql_*)"))
	}
	if u.Spec.Host == "" {
		allErrs = append(allErrs, field.Required(spec.Child("host"), "user host is required"))
	}
	if u.Spec.Superuser && len(u.Spec.Grants) > 0 {
		allErrs = append(allErrs, field.Invalid(spec.Child("grants"), u.Spec.Grants,
			"grants cannot be set when superuser is true"))
	}
	switch u.Spec.RequireTLS {
	case "", tlsNone, "ssl", "x509":
	default:
		allErrs = append(allErrs, field.Invalid(spec.Child("requireTLS"), u.Spec.RequireTLS,
			"requireTLS must be one of none, ssl, x509"))
	}
	for i := range u.Spec.Grants {
		global := isGlobalGrantTarget(u.Spec.Grants[i].On)
		for _, priv := range u.Spec.Grants[i].Privileges {
			p := strings.ToLower(strings.TrimSpace(priv))
			if deniedDynamicPrivileges[p] {
				allErrs = append(allErrs, field.Invalid(
					spec.Child("grants").Index(i).Child("privileges"), priv,
					"privilege is denied: it would let the user break replication, fencing, or operator accounts"))
			}
			// GRANT ALL ON *.* also grants every dynamic privilege (replication,
			// system-variable, group-replication admin, shutdown, ...), so a global
			// ALL is as dangerous as superuser and bypasses the denylist above.
			// Allow ALL only when scoped to a database/table, not the whole instance.
			if global && (p == "all" || p == "all privileges") {
				allErrs = append(allErrs, field.Invalid(
					spec.Child("grants").Index(i).Child("privileges"), priv,
					"ALL on *.* is not allowed: it grants every dynamic admin privilege; "+
						"enumerate data privileges, scope ALL to a database (db.*), or set superuser=true"))
			}
		}
	}
	for i := range u.Spec.Revokes {
		if u.Spec.Revokes[i].On == "" {
			allErrs = append(allErrs, field.Required(
				spec.Child("revokes").Index(i).Child("on"),
				"a revoke must name the target to carve out (e.g. mysql.*)"))
		}
		if len(u.Spec.Revokes[i].Privileges) == 0 {
			allErrs = append(allErrs, field.Required(
				spec.Child("revokes").Index(i).Child("privileges"),
				"a revoke must list at least one privilege"))
		}
	}
	return allErrs
}

// isGlobalGrantTarget reports whether a grant target refers to the whole
// instance ("*.*"), ignoring identifier quoting and case.
func isGlobalGrantTarget(on string) bool {
	t := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(on), "`", ""))
	return t == "" || t == "*.*"
}
