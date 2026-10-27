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
	"errors"
	"fmt"
	"strings"
)

// GlobalGrantTarget is the grant target covering every schema and table.
const GlobalGrantTarget = "*.*"

const (
	// MetricsAccountName is the passwordless account custom monitoring
	// queries run as. Its host is MetricsAccountHost, so it only
	// authenticates over the instance's local socket.
	MetricsAccountName = "cnmsql_metrics"
	// MetricsAccountHost pins the metrics account to socket connections.
	MetricsAccountHost = "localhost"
)

// MetricsAccountBaseGrants are the grants the metrics account holds on every
// cluster. initdb applies them and the metrics account reconciler never
// revokes them.
func MetricsAccountBaseGrants() []AccountGrant {
	return []AccountGrant{
		{Privileges: []string{"PROCESS", privReplicationClient, "REPLICATION SLAVE"}, On: GlobalGrantTarget},
		{Privileges: []string{"SELECT"}, On: "performance_schema.*"},
	}
}

// metricsAllowedPrivileges is what spec.monitoring.privileges may grant.
// Anyone who can edit a query ConfigMap runs SQL as the metrics account, so
// only read access is allowed. EXECUTE is left out because a SQL SECURITY
// DEFINER routine can write.
var metricsAllowedPrivileges = map[string]bool{
	"select":    true,
	"show view": true,
}

// ValidateMetricsPrivilegeNames checks that privileges is a non-empty list of
// privileges the metrics account may be granted.
func ValidateMetricsPrivilegeNames(privileges []string) error {
	if len(privileges) == 0 {
		return errors.New("at least one privilege is required")
	}
	for _, p := range privileges {
		if !metricsAllowedPrivileges[strings.ToLower(strings.Join(strings.Fields(p), " "))] {
			return fmt.Errorf("privilege %q is not allowed: only SELECT and SHOW VIEW can be granted "+
				"to the metrics account, since anyone who can edit a query ConfigMap runs SQL as it", p)
		}
	}
	return nil
}

// ParseMetricsGrantTarget parses and checks a grant target: "db.*" or
// "db.table", each part bare or backtick-quoted. The global "*.*" and the
// mysql schema, which holds password hashes, are refused. GRANT reads "_" and
// "%" in a database name as wildcards, so "%" is refused and a name that "_"
// would let match "mysql" is too.
func ParseMetricsGrantTarget(on string) (GrantTarget, error) {
	target, err := ParseGrantTarget(on)
	if err != nil {
		return GrantTarget{}, err
	}
	db, table := target.Database, target.Table
	if target.IsGlobal() {
		return GrantTarget{}, errors.New("the target must name a database: *.* is not allowed")
	}
	if strings.ContainsAny(db, `%\`) || strings.ContainsAny(table, `\`) {
		return GrantTarget{}, fmt.Errorf(`target %q may not contain %% or \`, on)
	}
	if GrantDatabaseMatches(db, "mysql") {
		return GrantTarget{}, fmt.Errorf("target %q covers the mysql schema, which holds password hashes", on)
	}
	if strings.EqualFold(db, "information_schema") {
		return GrantTarget{}, fmt.Errorf(
			"target %q cannot be granted: information_schema is readable without a grant", on)
	}
	return target, nil
}
