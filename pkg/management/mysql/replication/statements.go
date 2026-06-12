/*
Copyright 2026 The CNMySQL Authors.

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

// Package replication builds and (later) executes the SQL that configures GTID
// replication, role transitions and semi-synchronous replication. The
// statement builders here are pure and version-aware so they can be unit-tested
// without a running server; MySQL 8.0.23 renamed the MASTER/SLAVE syntax to
// SOURCE/REPLICA.
package replication

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
)

// SourceOptions describes how a replica connects to its replication source.
type SourceOptions struct {
	// Host and Port of the source instance.
	Host string `json:"host"`
	Port int    `json:"port"`
	// User and Password of the replication account.
	User     string `json:"user"`
	Password string `json:"password,omitempty"`
	// AutoPosition enables GTID auto-positioning (SOURCE_AUTO_POSITION=1).
	AutoPosition bool `json:"autoPosition"`
	// SSL turns on TLS for the replication connection.
	SSL bool `json:"ssl"`
	// SSLCA, SSLCert and SSLKey configure mTLS to the source. When all are set
	// they are added to the statement and SSL is implied.
	SSLCA   string `json:"sslCA,omitempty"`
	SSLCert string `json:"sslCert,omitempty"`
	SSLKey  string `json:"sslKey,omitempty"`
	// ConnectRetry and RetryCount tune reconnection behaviour. Zero means the
	// server default.
	ConnectRetry int `json:"connectRetry,omitempty"`
	RetryCount   int `json:"retryCount,omitempty"`
	// GetPublicKey requests the source's public key for caching_sha2_password
	// authentication over a non-TLS connection (MySQL 8.0+). Not needed when
	// using mTLS.
	GetPublicKey bool `json:"getPublicKey,omitempty"`
}

// ChangeSourceStatement builds the CHANGE REPLICATION SOURCE TO (8.0.23+) or
// CHANGE MASTER TO statement for the given server version and options. The
// password is escaped; callers should still avoid logging the result.
func ChangeSourceStatement(v version.Version, opts SourceOptions) string {
	modern := v.UsesReplicaTerminology()

	verb := "CHANGE MASTER TO"
	prefix := "MASTER_"
	if modern {
		verb = "CHANGE REPLICATION SOURCE TO"
		prefix = "SOURCE_"
	}

	var clauses []string
	add := func(suffix, value string) {
		clauses = append(clauses, prefix+suffix+"="+value)
	}

	add("HOST", quote(opts.Host))
	if opts.Port != 0 {
		add("PORT", strconv.Itoa(opts.Port))
	}
	add("USER", quote(opts.User))
	if opts.Password != "" {
		add("PASSWORD", quote(opts.Password))
	}
	if opts.ConnectRetry != 0 {
		add("CONNECT_RETRY", strconv.Itoa(opts.ConnectRetry))
	}
	if opts.RetryCount != 0 {
		add("RETRY_COUNT", strconv.Itoa(opts.RetryCount))
	}

	mtls := opts.SSLCA != "" && opts.SSLCert != "" && opts.SSLKey != ""
	if opts.SSL || mtls {
		add("SSL", "1")
	}
	if mtls {
		add("SSL_CA", quote(opts.SSLCA))
		add("SSL_CERT", quote(opts.SSLCert))
		add("SSL_KEY", quote(opts.SSLKey))
	}

	if opts.AutoPosition {
		add("AUTO_POSITION", "1")
	}
	// caching_sha2_password public-key retrieval (and the clause itself) only
	// exists on 5.7.23 / 8.0.4+; older servers reject it as a syntax error.
	if opts.GetPublicKey && v.HasGetSourcePublicKey() {
		if modern {
			clauses = append(clauses, "GET_SOURCE_PUBLIC_KEY=1")
		} else {
			clauses = append(clauses, "GET_MASTER_PUBLIC_KEY=1")
		}
	}

	return fmt.Sprintf("%s %s", verb, strings.Join(clauses, ", "))
}

// StartReplicaStatement returns START REPLICA / START SLAVE.
func StartReplicaStatement(v version.Version) string {
	if v.UsesReplicaTerminology() {
		return "START REPLICA"
	}
	return "START SLAVE"
}

// StopReplicaStatement returns STOP REPLICA / STOP SLAVE.
func StopReplicaStatement(v version.Version) string {
	if v.UsesReplicaTerminology() {
		return "STOP REPLICA"
	}
	return "STOP SLAVE"
}

// ResetReplicaStatement returns RESET REPLICA [ALL] / RESET SLAVE [ALL].
func ResetReplicaStatement(v version.Version, all bool) string {
	stmt := "RESET SLAVE"
	if v.UsesReplicaTerminology() {
		stmt = "RESET REPLICA"
	}
	if all {
		stmt += " ALL"
	}
	return stmt
}

// ShowReplicaStatusStatement returns SHOW REPLICA STATUS / SHOW SLAVE STATUS.
func ShowReplicaStatusStatement(v version.Version) string {
	if v.UsesReplicaTerminology() {
		return "SHOW REPLICA STATUS"
	}
	return "SHOW SLAVE STATUS"
}

// ResetBinaryLogsStatement clears the binary logs and GTID execution history,
// using the version-appropriate syntax. It is run on a freshly provisioned
// replica before setting gtid_purged.
func ResetBinaryLogsStatement(v version.Version) string {
	if v.UsesResetBinaryLogsAndGtids() {
		return "RESET BINARY LOGS AND GTIDS"
	}
	return "RESET MASTER"
}

// SetGTIDPurgedStatement sets the global gtid_purged variable to the given GTID
// set, telling the server which transactions are already present (e.g. from a
// physical backup) so GTID auto-positioning starts from the right point.
func SetGTIDPurgedStatement(gtidSet string) string {
	return "SET GLOBAL gtid_purged = " + quote(gtidSet)
}

// SetReadOnlyStatement toggles the global read_only variable.
func SetReadOnlyStatement(on bool) string {
	return "SET GLOBAL read_only = " + onOff(on)
}

// SetSuperReadOnlyStatement toggles the global super_read_only variable. Only
// valid on MySQL 5.7.8+.
func SetSuperReadOnlyStatement(on bool) string {
	return "SET GLOBAL super_read_only = " + onOff(on)
}

// SetGlobalStatement builds a SET GLOBAL <name> = <value> statement. The value
// is rendered verbatim (numbers, ON/OFF); use quote for string values.
func SetGlobalStatement(name, value string) string {
	return fmt.Sprintf("SET GLOBAL %s = %s", name, value)
}

// InstallSemiSyncSourceStatement installs the semi-sync source plugin for the
// version's plugin naming.
func InstallSemiSyncSourceStatement(v version.Version) string {
	n := v.SemiSync()
	return fmt.Sprintf("INSTALL PLUGIN %s SONAME %s", n.PluginSource, quote(n.LibSource))
}

// InstallSemiSyncReplicaStatement installs the semi-sync replica plugin for the
// version's plugin naming.
func InstallSemiSyncReplicaStatement(v version.Version) string {
	n := v.SemiSync()
	return fmt.Sprintf("INSTALL PLUGIN %s SONAME %s", n.PluginReplica, quote(n.LibReplica))
}

func onOff(on bool) string {
	if on {
		return "ON"
	}
	return "OFF"
}

// quote single-quotes a string literal for use in a MySQL statement, escaping
// backslashes and single quotes.
func quote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
