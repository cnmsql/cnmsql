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
	"strconv"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

const (
	mysqlDumpBinary   = "mysqldump"
	mariadbDumpBinary = "mariadb-dump"

	// DumpAccountName is the read-only system account logical backups run as.
	// Its host is DumpAccountHost, so it only authenticates over the instance's
	// local Unix socket.
	DumpAccountName = "cnmsql_dump"
	// DumpAccountHost pins the dump account to socket connections.
	DumpAccountHost = "localhost"

	privSelect = "SELECT"
)

// DumpOpts configures a logical dump.
type DumpOpts struct {
	// DefaultsFile is an option file holding the dump account's user, password
	// and socket. It is passed as --defaults-extra-file so the password never
	// appears in argv.
	DefaultsFile string
	// Databases are the schemas to dump. The caller resolves "all" to an
	// explicit list, so the output always carries the per-database section
	// markers a partial restore filters on.
	Databases []string
	// ExtraArgs are appended after the operator's arguments.
	ExtraArgs []string
	// ServerVersion gates version-specific spellings.
	ServerVersion version.Version
}

// AccountGrant is one privilege list on one target, for the dump account.
type AccountGrant struct {
	Privileges []string
	On         string
}

// LogicalTool exposes the dump client for logical backups: its binary names,
// its arguments, the parser for the snapshot position it writes into the dump,
// and the privileges its account needs.
type LogicalTool interface {
	// DumpBinary is the dump client: mysqldump or mariadb-dump.
	DumpBinary() string
	// LoadBinary is the SQL client that loads a dump: mysql or mariadb.
	LoadBinary() string
	// DumpArgs builds the dump client's arguments.
	DumpArgs(opts DumpOpts) ([]string, error)
	// ParseSnapshotPosition extracts the snapshot position from the dump's
	// comment lines (every line that starts with "-- "). The binlog coordinates
	// come from the --source-data/--master-data comment near the top. MariaDB
	// also writes the matching GTID as a comment near the end; MySQL writes no
	// GTID, because --set-gtid-purged is OFF.
	ParseSnapshotPosition(comments string) (BinlogInfo, error)
	// DumpAccountGrants are the privileges the dump account holds on *.*.
	DumpAccountGrants(v version.Version) []AccountGrant
	// DumpAccountRevokes are carved out of DumpAccountGrants after they are
	// applied. Empty when the engine cannot express a partial revoke.
	DumpAccountRevokes(v version.Version) []AccountGrant
}

// baseDumpArgs are shared by both engines. --single-transaction takes one
// consistent InnoDB snapshot for every selected database, --no-tablespaces
// avoids needing PROCESS, and --hex-blob keeps binary data intact across
// character-set conversions.
var baseDumpArgs = []string{
	"--single-transaction",
	"--quick",
	"--skip-lock-tables",
	"--routines",
	"--events",
	"--triggers",
	"--hex-blob",
	"--default-character-set=utf8mb4",
	"--no-tablespaces",
}

func buildDumpArgs(opts DumpOpts, engineArgs ...string) ([]string, error) {
	if opts.DefaultsFile == "" {
		return nil, errors.New("dump: defaults file is required")
	}
	if len(opts.Databases) == 0 {
		return nil, errors.New("dump: at least one database is required")
	}
	// --defaults-extra-file must be the first argument or the client rejects it.
	args := make([]string, 0, 2+len(baseDumpArgs)+len(engineArgs)+len(opts.ExtraArgs)+len(opts.Databases))
	args = append(args, "--defaults-extra-file="+opts.DefaultsFile)
	args = append(args, baseDumpArgs...)
	args = append(args, engineArgs...)
	args = append(args, opts.ExtraArgs...)
	args = append(args, "--databases")
	return append(args, opts.Databases...), nil
}

// snapshotBinlogRE matches the commented replication coordinates both dump
// clients write with --source-data=2 / --master-data=2. Percona 8.0 prints the
// CHANGE MASTER spelling even for --source-data; 8.4 and later print CHANGE
// REPLICATION SOURCE.
var snapshotBinlogRE = regexp.MustCompile(`(?m)^-- CHANGE (?:MASTER|REPLICATION SOURCE) TO ` +
	`(?:MASTER|SOURCE)_LOG_FILE='([^']+)', (?:MASTER|SOURCE)_LOG_POS=(\d+);`)

// snapshotGTIDRE matches the deferred GTID comment mariadb-dump writes near the
// end of a --master-data dump.
var snapshotGTIDRE = regexp.MustCompile(`(?m)^-- SET GLOBAL gtid_slave_pos='([^']*)';`)

func parseSnapshotBinlog(comments string) (BinlogInfo, error) {
	m := snapshotBinlogRE.FindStringSubmatch(comments)
	if m == nil {
		return BinlogInfo{}, errors.New("dump: no snapshot binlog position in the dump header")
	}
	pos, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return BinlogInfo{}, fmt.Errorf("dump: parsing snapshot binlog position %q: %w", m[2], err)
	}
	return BinlogInfo{File: m[1], Position: pos}, nil
}

type mysqlLogicalTool struct{}

func (mysqlLogicalTool) DumpBinary() string { return mysqlDumpBinary }
func (mysqlLogicalTool) LoadBinary() string { return mysqlSQLClient }

// DumpArgs keeps the dump GTID-neutral with --set-gtid-purged=OFF. COMMENTED is
// not an option: it still writes an uncommented SET @@SESSION.SQL_LOG_BIN=0,
// which would keep a load out of the binlog. --source-data replaced
// --master-data in 8.0.26 and is the only spelling on 8.4 and later.
func (mysqlLogicalTool) DumpArgs(opts DumpOpts) ([]string, error) {
	sourceData := "--source-data=2"
	if !opts.ServerVersion.AtLeast(8, 0, 26) {
		sourceData = "--master-data=2"
	}
	return buildDumpArgs(opts, "--set-gtid-purged=OFF", sourceData)
}

func (mysqlLogicalTool) ParseSnapshotPosition(comments string) (BinlogInfo, error) {
	return parseSnapshotBinlog(comments)
}

// DumpAccountGrants: RELOAD for the brief FLUSH TABLES WITH READ LOCK that
// --source-data takes, REPLICATION CLIENT to read the binlog position.
func (mysqlLogicalTool) DumpAccountGrants(version.Version) []AccountGrant {
	return []AccountGrant{{
		Privileges: []string{privSelect, "SHOW VIEW", "TRIGGER", "EVENT", "RELOAD", "REPLICATION CLIENT"},
		On:         "*.*",
	}}
}

// DumpAccountRevokes keeps the global SELECT off the grant tables. It takes
// effect where partial_revokes is ON; elsewhere REVOKE IF EXISTS is a no-op and
// the localhost host limit is what contains the account.
func (mysqlLogicalTool) DumpAccountRevokes(version.Version) []AccountGrant {
	return []AccountGrant{{Privileges: []string{privSelect}, On: "mysql.*"}}
}

type mariadbLogicalTool struct{}

func (mariadbLogicalTool) DumpBinary() string { return mariadbDumpBinary }
func (mariadbLogicalTool) LoadBinary() string { return mariadbSQLClient }

// DumpArgs: mariadb-dump has no --set-gtid-purged, and without --gtid it writes
// the GTID only as a comment, so the dump stays GTID-neutral.
func (mariadbLogicalTool) DumpArgs(opts DumpOpts) ([]string, error) {
	return buildDumpArgs(opts, "--master-data=2")
}

func (mariadbLogicalTool) ParseSnapshotPosition(comments string) (BinlogInfo, error) {
	info, err := parseSnapshotBinlog(comments)
	if err != nil {
		return BinlogInfo{}, err
	}
	if m := snapshotGTIDRE.FindStringSubmatch(comments); m != nil {
		info.GTIDSet = m[1]
	}
	return info, nil
}

// DumpAccountGrants: BINLOG MONITOR replaced REPLICATION CLIENT in 10.5, and
// 11.3 added SHOW CREATE ROUTINE, which reads routine bodies without SELECT on
// mysql.proc.
func (mariadbLogicalTool) DumpAccountGrants(v version.Version) []AccountGrant {
	privs := []string{privSelect, "SHOW VIEW", "TRIGGER", "EVENT", "RELOAD", "BINLOG MONITOR"}
	if v.AtLeast(11, 3, 0) {
		privs = append(privs, "SHOW CREATE ROUTINE")
	}
	return []AccountGrant{{Privileges: privs, On: "*.*"}}
}

// DumpAccountRevokes is empty: MariaDB has no partial_revokes.
func (mariadbLogicalTool) DumpAccountRevokes(version.Version) []AccountGrant { return nil }
