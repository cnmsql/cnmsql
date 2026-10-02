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
	"regexp"
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
		{Privileges: []string{"PROCESS", "REPLICATION CLIENT", "REPLICATION SLAVE"}, On: GlobalGrantTarget},
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

// MetricsGrantTarget is a database (Table empty) or table the metrics account
// may be granted read access on.
type MetricsGrantTarget struct {
	Database string
	Table    string
}

var bareIdentifier = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)

// ParseMetricsGrantTarget parses and checks a grant target: "db.*" or
// "db.table", each part bare or backtick-quoted. The global "*.*" and the
// mysql schema, which holds password hashes, are refused. GRANT reads "_" and
// "%" in a database name as wildcards, so "%" is refused and a name that "_"
// would let match "mysql" is too.
func ParseMetricsGrantTarget(on string) (MetricsGrantTarget, error) {
	db, rest, err := splitIdentifier(on)
	if err != nil {
		return MetricsGrantTarget{}, err
	}
	if db == "*" {
		return MetricsGrantTarget{}, errors.New("the target must name a database: *.* is not allowed")
	}
	if !strings.HasPrefix(rest, ".") {
		return MetricsGrantTarget{}, fmt.Errorf("target %q must be db.* or db.table", on)
	}
	table := strings.TrimPrefix(rest, ".")
	if table == "*" {
		table = ""
	} else {
		var tail string
		table, tail, err = splitIdentifier(table)
		if err != nil {
			return MetricsGrantTarget{}, err
		}
		if tail != "" || table == "*" {
			return MetricsGrantTarget{}, fmt.Errorf("target %q must be db.* or db.table", on)
		}
	}
	if strings.ContainsAny(db, `%\`) || strings.ContainsAny(table, `\`) {
		return MetricsGrantTarget{}, fmt.Errorf(`target %q may not contain %% or \`, on)
	}
	if matchesMySQLSchema(db) {
		return MetricsGrantTarget{}, fmt.Errorf("target %q covers the mysql schema, which holds password hashes", on)
	}
	return MetricsGrantTarget{Database: db, Table: table}, nil
}

// splitIdentifier reads one leading identifier (bare, backticked, or "*")
// from s and returns it unquoted with the rest of s.
func splitIdentifier(s string) (string, string, error) {
	if strings.HasPrefix(s, "*") {
		return "*", s[1:], nil
	}
	if strings.HasPrefix(s, "`") {
		end := strings.IndexByte(s[1:], '`')
		if end <= 0 {
			return "", "", fmt.Errorf("unterminated or empty quoted identifier in %q", s)
		}
		return s[1 : end+1], s[end+2:], nil
	}
	end := strings.IndexByte(s, '.')
	if end < 0 {
		end = len(s)
	}
	ident := s[:end]
	if !bareIdentifier.MatchString(ident) {
		return "", "", fmt.Errorf("invalid identifier %q: quote it with backticks or use letters, digits, _ and $", ident)
	}
	return ident, s[end:], nil
}

// matchesMySQLSchema reports whether a database name, with "_" read as a
// one-character wildcard, matches "mysql" ignoring case.
func matchesMySQLSchema(db string) bool {
	const schema = "mysql"
	if len(db) != len(schema) {
		return false
	}
	for i := range len(schema) {
		if db[i] != '_' && !strings.EqualFold(db[i:i+1], schema[i:i+1]) {
			return false
		}
	}
	return true
}

// SQL renders the target with each part backtick-quoted.
func (t MetricsGrantTarget) SQL() string {
	if t.Table == "" {
		return "`" + t.Database + "`.*"
	}
	return "`" + t.Database + "`.`" + t.Table + "`"
}
