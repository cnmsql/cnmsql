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

// GrantTarget is a parsed grant target. Database is "*" for the global *.*
// target, and Table is empty when the grant covers every table.
type GrantTarget struct {
	Database string
	Table    string
}

// ParseGrantTarget parses a grant target: "*.*", "db.*" or "db.table", each
// name bare or backtick-quoted. It checks the grammar only; callers add the
// rules for their grant source. Every grant target goes through it, so a
// target cannot carry another clause (a second grantee, WITH GRANT OPTION)
// into the statement.
func ParseGrantTarget(on string) (GrantTarget, error) {
	db, rest, err := splitIdentifier(on)
	if err != nil {
		return GrantTarget{}, err
	}
	if !strings.HasPrefix(rest, ".") {
		return GrantTarget{}, fmt.Errorf("target %q must be *.*, db.* or db.table", on)
	}
	rest = strings.TrimPrefix(rest, ".")
	if rest == "*" {
		return GrantTarget{Database: db}, nil
	}
	table, tail, err := splitIdentifier(rest)
	if err != nil {
		return GrantTarget{}, err
	}
	if db == "*" || table == "*" || tail != "" {
		return GrantTarget{}, fmt.Errorf("target %q must be *.*, db.* or db.table", on)
	}
	return GrantTarget{Database: db, Table: table}, nil
}

// IsGlobal reports whether the target is *.*.
func (t GrantTarget) IsGlobal() bool {
	return t.Database == "*"
}

// SQL renders the target with each name backtick-quoted.
func (t GrantTarget) SQL() string {
	switch {
	case t.IsGlobal():
		return GlobalGrantTarget
	case t.Table == "":
		return "`" + t.Database + "`.*"
	default:
		return "`" + t.Database + "`.`" + t.Table + "`"
	}
}

var bareIdentifier = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)

// splitIdentifier reads one leading identifier (bare, backticked, or "*")
// from s and returns it unquoted with the rest of s. A quoted "*" is refused,
// since it would read back as the wildcard.
func splitIdentifier(s string) (string, string, error) {
	if strings.HasPrefix(s, "*") {
		return "*", s[1:], nil
	}
	if strings.HasPrefix(s, "`") {
		end := strings.IndexByte(s[1:], '`')
		if end <= 0 {
			return "", "", fmt.Errorf("unterminated or empty quoted identifier in %q", s)
		}
		if s[1:end+1] == "*" {
			return "", "", fmt.Errorf("invalid identifier in %q: `*` is not a name", s)
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

// GrantDatabaseMatches reports whether a database name in a GRANT, with "_"
// read as the one-character wildcard GRANT treats it as, matches schema
// ignoring case.
func GrantDatabaseMatches(db, schema string) bool {
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

var privilegeWord = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// multiWordPrivileges are the MySQL and MariaDB privilege names that contain a
// space. Every other privilege, static or dynamic, is a single word.
var multiWordPrivileges = map[string]bool{
	"ALL PRIVILEGES":           true,
	"ALTER ROUTINE":            true,
	"CREATE ROLE":              true,
	"CREATE ROUTINE":           true,
	"CREATE TABLESPACE":        true,
	"CREATE TEMPORARY TABLES":  true,
	"CREATE USER":              true,
	"CREATE VIEW":              true,
	"DROP ROLE":                true,
	"GRANT OPTION":             true,
	"LOCK TABLES":              true,
	privReplicationClient:      true,
	"REPLICATION SLAVE":        true,
	"SHOW DATABASES":           true,
	privShowView:               true,
	"BINLOG ADMIN":             true,
	"BINLOG MONITOR":           true,
	"BINLOG REPLAY":            true,
	"CONNECTION ADMIN":         true,
	"DELETE HISTORY":           true,
	"FEDERATED ADMIN":          true,
	"READ_ONLY ADMIN":          true,
	"REPLICA MONITOR":          true,
	"REPLICATION MASTER ADMIN": true,
	"REPLICATION REPLICA":      true,
	"REPLICATION SLAVE ADMIN":  true,
	"SET USER":                 true,
	"SHOW CREATE ROUTINE":      true,
	"SLAVE MONITOR":            true,
}

// NormalizePrivilegeName returns one privilege name upper-cased and
// single-spaced. It refuses anything else, such as a comma-separated list, a
// column list or a trailing clause, so an entry cannot smuggle more into the
// GRANT statement it is joined into, or past a per-name allowlist or denylist.
func NormalizePrivilegeName(p string) (string, error) {
	name := strings.ToUpper(strings.Join(strings.Fields(p), " "))
	if name == "" {
		return "", errors.New("privilege name is empty")
	}
	if !privilegeWord.MatchString(name) && !multiWordPrivileges[name] {
		return "", fmt.Errorf(
			"invalid privilege %q: each entry must be a single privilege name, such as SELECT or SHOW VIEW", p)
	}
	return name, nil
}
