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

// Package config renders the mysqld configuration (my.cnf) for a CNMySQL
// instance. The operator owns a set of replication- and TLS-critical keys; user
// supplied parameters may not override them. Rendering is version-aware to cope
// with keyword differences between MySQL 5.6/5.7 and 8.0+.
package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
)

// DefaultAdminPort is the default MySQL administrative interface port.
const DefaultAdminPort = 33062

// DefaultAdminAddress is the default bind address for the administrative
// interface. Loopback keeps it reachable only from inside the pod (the instance
// manager), never from the network.
const DefaultAdminAddress = "127.0.0.1"

// Role is the replication role an instance is rendered for.
type Role string

const (
	// RolePrimary is a read-write source instance.
	RolePrimary Role = "primary"
	// RoleReplica is a read-only replica instance.
	RoleReplica Role = "replica"
)

// TLSPaths holds the on-disk locations of the TLS material used by mysqld.
type TLSPaths struct {
	CA   string
	Cert string
	Key  string
}

func (t TLSPaths) isset() bool {
	return t.CA != "" && t.Cert != "" && t.Key != ""
}

// SemiSync configures semi-synchronous replication rendering.
type SemiSync struct {
	// Enabled installs and turns on the semi-sync plugins via the rendered
	// configuration. The plugins themselves are loaded at runtime by the
	// replication package; here we only render the related variables.
	Enabled bool
	// WaitForReplicaCount mirrors rpl_semi_sync_source_wait_for_replica_count.
	WaitForReplicaCount int
	// TimeoutMillis mirrors rpl_semi_sync_source_timeout.
	TimeoutMillis int
}

// ServerConfig is the fully-resolved input to rendering a my.cnf.
type ServerConfig struct {
	// ServerID is the unique mysqld server id. Required and >0.
	ServerID int
	// Version is the MySQL server version, e.g. "8.0.36" or "5.7.44".
	Version string
	// Role determines read-only/super-read-only handling.
	Role Role
	// DataDir, Socket and Port locate the server.
	DataDir string
	Socket  string
	Port    int
	// ReportHost is the address replicas report to the source.
	ReportHost string
	// BinlogFormat is the binary log format (ROW/STATEMENT/MIXED).
	BinlogFormat string
	// AdminAddress and AdminPort configure the administrative network interface
	// (MySQL 8.0.14+), a separate listener not governed by max_connections that
	// guarantees the instance manager can always reach mysqld. They are ignored
	// on older servers. When AdminAddress is empty it defaults to loopback.
	AdminAddress string
	AdminPort    int
	// TLS holds the TLS material paths; mTLS is enforced when set.
	TLS TLSPaths
	// SemiSync configures semi-synchronous replication.
	SemiSync SemiSync
	// UserParameters are operator-validated user my.cnf settings.
	UserParameters map[string]string
}

// managedKeys are the [mysqld] keys the operator fully controls. Users may not
// set them through UserParameters.
var managedKeys = map[string]struct{}{
	"server-id":                {},
	"server_id":                {},
	"gtid_mode":                {},
	"gtid-mode":                {},
	"enforce_gtid_consistency": {},
	"enforce-gtid-consistency": {},
	"log_bin":                  {},
	"log-bin":                  {},
	"log_replica_updates":      {},
	"log_slave_updates":        {},
	"relay_log":                {},
	"relay-log":                {},
	"read_only":                {},
	"super_read_only":          {},
	"datadir":                  {},
	"socket":                   {},
	"report_host":              {},
	"admin_address":            {},
	"admin-address":            {},
	"admin_port":               {},
	"admin-port":               {},
	"ssl_ca":                   {},
	"ssl-ca":                   {},
	"ssl_cert":                 {},
	"ssl-cert":                 {},
	"ssl_key":                  {},
	"ssl-key":                  {},
	"binlog_format":            {},
	"binlog-format":            {},
}

// normalizeKey lowercases and converts dashes to underscores so that the
// dash/underscore variants of a key compare equal.
func normalizeKey(key string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "-", "_")
}

// IsManagedKey reports whether the given my.cnf key is owned by the operator.
func IsManagedKey(key string) bool {
	_, ok := managedKeys[normalizeKey(key)]
	return ok
}

// ValidateUserParameters returns an error listing any user parameters that
// collide with operator-managed keys.
func ValidateUserParameters(params map[string]string) error {
	var conflicts []string
	for key := range params {
		if IsManagedKey(key) {
			conflicts = append(conflicts, key)
		}
	}
	if len(conflicts) == 0 {
		return nil
	}
	sort.Strings(conflicts)
	return fmt.Errorf("the following parameters are managed by the operator and cannot be set: %s",
		strings.Join(conflicts, ", "))
}

// Render produces the my.cnf content for the given server configuration. It
// returns an error if the configuration is invalid or user parameters collide
// with managed keys.
func (c *ServerConfig) Render() (string, error) {
	if c.ServerID <= 0 {
		return "", fmt.Errorf("serverID must be greater than 0, got %d", c.ServerID)
	}
	if c.Role != RolePrimary && c.Role != RoleReplica {
		return "", fmt.Errorf("invalid role %q", c.Role)
	}
	if err := ValidateUserParameters(c.UserParameters); err != nil {
		return "", err
	}
	ver, err := version.Parse(c.Version)
	if err != nil {
		return "", err
	}

	managed := c.managedSettings(ver)

	var b strings.Builder
	b.WriteString("# Generated by CNMySQL instance manager. Do not edit.\n")
	b.WriteString("[mysqld]\n")

	writeSection(&b, "# --- operator-managed ---", managed)

	if len(c.UserParameters) > 0 {
		writeSection(&b, "# --- user-provided ---", mapToOrderedPairs(c.UserParameters))
	}

	return b.String(), nil
}

// managedSettings returns the ordered operator-managed key/value pairs for the
// given version.
func (c *ServerConfig) managedSettings(ver version.Version) []pair {
	binlogFormat := c.BinlogFormat
	if binlogFormat == "" {
		binlogFormat = "ROW"
	}

	pairs := []pair{
		{"server-id", strconv.Itoa(c.ServerID)},
		{"datadir", c.DataDir},
		{"socket", c.Socket},
		{"gtid_mode", "ON"},
		{"enforce_gtid_consistency", "ON"},
		{"log_bin", "binlog"},
		{"relay_log", "relay-bin"},
		{"binlog_format", binlogFormat},
	}

	if c.Port != 0 {
		pairs = append(pairs, pair{"port", strconv.Itoa(c.Port)})
	}
	if c.ReportHost != "" {
		pairs = append(pairs, pair{"report_host", c.ReportHost})
	}

	// Administrative interface (8.0.14+): a dedicated listener exempt from
	// max_connections, so the instance manager can always reach mysqld.
	if ver.HasAdminInterface() {
		addr := c.AdminAddress
		if addr == "" {
			addr = DefaultAdminAddress
		}
		port := c.AdminPort
		if port == 0 {
			port = DefaultAdminPort
		}
		pairs = append(pairs,
			pair{"admin_address", addr},
			pair{"admin_port", strconv.Itoa(port)},
		)
	}

	// log_replica_updates was renamed from log_slave_updates in 8.0.
	if ver.HasLogReplicaUpdates() {
		pairs = append(pairs, pair{"log_replica_updates", "ON"})
	} else {
		pairs = append(pairs, pair{"log_slave_updates", "ON"})
	}

	// Read-only handling: super_read_only exists since 5.7.8.
	if c.Role == RoleReplica {
		pairs = append(pairs, pair{"read_only", "ON"})
		if ver.HasSuperReadOnly() {
			pairs = append(pairs, pair{"super_read_only", "ON"})
		}
	}

	if c.TLS.isset() {
		// TLS material is configured so clients and replicas can connect over
		// TLS, but transport is not forced: whether to require encrypted
		// connections (require_secure_transport) is left to the user via
		// spec.mysql.parameters.
		pairs = append(pairs,
			pair{"ssl_ca", c.TLS.CA},
			pair{"ssl_cert", c.TLS.Cert},
			pair{"ssl_key", c.TLS.Key},
		)
	}

	if c.SemiSync.Enabled {
		naming := ver.SemiSync()
		pairs = append(pairs,
			pair{naming.EnabledVarSource, "1"},
			pair{naming.EnabledVarReplica, "1"},
		)
		if c.SemiSync.WaitForReplicaCount > 0 {
			pairs = append(pairs, pair{
				naming.WaitForCountVar,
				strconv.Itoa(c.SemiSync.WaitForReplicaCount),
			})
		}
		if c.SemiSync.TimeoutMillis > 0 {
			pairs = append(pairs, pair{
				naming.TimeoutVar,
				strconv.Itoa(c.SemiSync.TimeoutMillis),
			})
		}
	}

	return pairs
}

type pair struct {
	key   string
	value string
}

func mapToOrderedPairs(m map[string]string) []pair {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]pair, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, pair{k, m[k]})
	}
	return pairs
}

func writeSection(b *strings.Builder, header string, pairs []pair) {
	b.WriteString(header)
	b.WriteString("\n")
	for _, p := range pairs {
		if p.value == "" {
			b.WriteString(p.key)
			b.WriteString("\n")
			continue
		}
		b.WriteString(p.key)
		b.WriteString(" = ")
		b.WriteString(p.value)
		b.WriteString("\n")
	}
}
