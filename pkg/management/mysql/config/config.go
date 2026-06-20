/*
Copyright 2026 The CloudNative MySQL Authors.

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

// Package config renders the mysqld configuration (my.cnf) for a cloudnative-mysql
// instance. The operator owns a set of replication- and TLS-critical keys; user
// supplied parameters may not override them. Rendering is version-aware to cope
// with keyword differences between MySQL 5.7 and 8.0+.
package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

// DefaultAdminPort is the default MySQL administrative interface port.
const DefaultAdminPort = 33062

// DefaultAdminAddress is the default bind address for the administrative
// interface. Loopback keeps it reachable only from inside the pod (the instance
// manager), never from the network.
const DefaultAdminAddress = "127.0.0.1"

// DefaultGroupReplicationPort is the port the Group Replication communication
// (XCom) layer listens on, used for group_replication_local_address and the
// group seeds. Operator-owned; not user-tunable in v1.
const DefaultGroupReplicationPort = 33061

// TopologyMode selects the replication topology an instance is rendered for. It
// mirrors spec.replication.mode; an empty value renders async (the default).
type TopologyMode string

const (
	// TopologyAsync renders asynchronous / semi-synchronous GTID replication.
	TopologyAsync TopologyMode = "async"
	// TopologyGroupReplication renders single-primary MySQL Group Replication.
	TopologyGroupReplication TopologyMode = "groupReplication"
)

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
	// Archiving configures continuous binary-log archiving durability/RPO
	// settings. Rendered only when enabled.
	Archiving Archiving
	// TopologyMode selects async (default) vs Group Replication rendering. When
	// TopologyGroupReplication the GroupReplication block below is rendered.
	TopologyMode TopologyMode
	// GroupReplication holds the Group Replication settings. Rendered only when
	// TopologyMode is TopologyGroupReplication.
	GroupReplication GroupReplication
	// UserParameters are operator-validated user my.cnf settings.
	UserParameters map[string]string
}

// GroupReplication is the fully-resolved input to rendering the
// group_replication_* settings. Addresses, seeds and the group name are
// operator-computed; the tunables mirror spec.replication.groupReplication.
type GroupReplication struct {
	// GroupName is the pinned group_replication_group_name (a UUID), stable for
	// the life of the group and identical on every member.
	GroupName string
	// LocalAddress is this member's XCom address, "<pod-fqdn>:<port>".
	LocalAddress string
	// GroupSeeds is the comma-separated list of member XCom addresses used to
	// reach an existing group when (re)joining.
	GroupSeeds string
	// IPAllowlist scopes group_replication_ip_allowlist to the cluster's network
	// (e.g. the Pod CIDR or service domain). Empty leaves the server default.
	IPAllowlist string
	// Consistency, ExitStateAction and AutoRejoinTries mirror the spec tunables.
	Consistency     string
	ExitStateAction string
	AutoRejoinTries int
	// RecoverySSL, when set, renders the distributed-recovery SSL channel paths,
	// reusing the cluster's TLS material.
	RecoverySSL TLSPaths
}

// Archiving configures the binary-log settings continuous archiving relies on.
// log_bin, gtid_mode, enforce_gtid_consistency and log_replica_updates are
// always rendered (they are replication requirements); these settings add the
// durability and RPO knobs archiving needs on top.
type Archiving struct {
	// Enabled renders the archiving-specific binary-log settings.
	Enabled bool
	// MaxBinlogSizeMB caps the active binary log before mysqld rotates it,
	// bounding the size-based RPO. 0 leaves the server default.
	MaxBinlogSizeMB int
	// BinlogExpireSeconds is the conservative backstop after which mysqld may
	// expire a binary log, applied under the active purge gate. 0 leaves the
	// server default.
	BinlogExpireSeconds int
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
	// Continuous-archiving durability/RPO knobs. Owned by the operator so a user
	// override cannot undermine the archive's durability guarantees.
	"sync_binlog":                {},
	"max_binlog_size":            {},
	"binlog_expire_logs_seconds": {},
	"expire_logs_days":           {},
}

// deniedKeys are [mysqld] keys that the operator does not itself set but which
// would break the managed topology, leak the administrative interface, subvert
// TLS, or relocate on-disk paths the operator relies on. They are rejected on
// top of managedKeys so a user override cannot destabilise an instance.
var deniedKeys = map[string]struct{}{
	"basedir":                {},
	"pid_file":               {},
	"port":                   {},
	"tmpdir":                 {},
	"plugin_dir":             {},
	"secure_file_priv":       {},
	"log_error":              {},
	"log_bin_basename":       {},
	"relay_log_basename":     {},
	"general_log_file":       {},
	"slow_query_log_file":    {},
	"server_uuid":            {},
	"skip_slave_start":       {},
	"skip_replica_start":     {},
	"report_port":            {},
	"auto_generate_certs":    {},
	"admin_ssl_ca":           {},
	"admin_ssl_cert":         {},
	"admin_ssl_key":          {},
	"admin_tls_ciphersuites": {},
	"tls_ciphersuites":       {},
}

// deprecatedKeys maps [mysqld] keys that are accepted but discouraged to a
// human-readable replacement. They produce a warning rather than a rejection.
var deprecatedKeys = map[string]string{
	"master_info_repository":    "removed on 8.0.23+; replication metadata is stored in tables",
	"relay_log_info_repository": "removed on 8.0.23+; replication metadata is stored in tables",
	"slave_parallel_workers":    "renamed to replica_parallel_workers on 8.0+",
	"slave_parallel_type":       "renamed to replica_parallel_type on 8.0+",
}

// normalizeKey lowercases and converts dashes to underscores so that the
// dash/underscore variants of a key compare equal.
func normalizeKey(key string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "-", "_")
}

// groupReplicationKeyPrefix matches the entire group_replication_* namespace
// plus plugin_load_add, which the operator owns under Group Replication. Treated
// as managed so a user parameter can never destabilise the group.
const groupReplicationKeyPrefix = "group_replication_"

// isGroupReplicationManagedKey reports whether the (normalized) key falls in the
// operator-owned Group Replication namespace.
func isGroupReplicationManagedKey(normalized string) bool {
	return strings.HasPrefix(normalized, groupReplicationKeyPrefix) || normalized == "plugin_load_add"
}

// StripGroupReplication removes every operator-owned Group Replication line
// (the group_replication_* namespace and plugin_load_add) from a rendered
// my.cnf, leaving comments, the section header and all other settings intact.
//
// It exists for `mysqld --initialize`: that mode deliberately ignores
// plugin_load_add (see MySQL warning MY-013501), so group_replication.so is
// never loaded and every group_replication_* setting becomes an "unknown
// variable" that aborts initialization after the data directory has been
// partially written. The GR block is meaningless during --initialize anyway, so
// the initializer feeds mysqld a stripped copy of the runtime config.
func StripGroupReplication(content string) string {
	lines := strings.Split(content, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") && !strings.HasPrefix(trimmed, "[") {
			key := trimmed
			if before, _, ok := strings.Cut(trimmed, "="); ok {
				key = before
			}
			if isGroupReplicationManagedKey(normalizeKey(key)) {
				continue
			}
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// IsManagedKey reports whether the given my.cnf key is owned by the operator.
func IsManagedKey(key string) bool {
	k := normalizeKey(key)
	if isGroupReplicationManagedKey(k) {
		return true
	}
	_, ok := managedKeys[k]
	return ok
}

// IsDeniedKey reports whether the given my.cnf key is forbidden to users,
// whether because the operator manages it or because overriding it would
// destabilise the instance.
func IsDeniedKey(key string) bool {
	k := normalizeKey(key)
	if isGroupReplicationManagedKey(k) {
		return true
	}
	if _, ok := managedKeys[k]; ok {
		return true
	}
	_, ok := deniedKeys[k]
	return ok
}

// ValidateUserParameters returns an error listing any user parameters that
// collide with operator-managed keys or are otherwise denied.
func ValidateUserParameters(params map[string]string) error {
	var conflicts []string
	for key := range params {
		if IsDeniedKey(key) {
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

// DeprecatedUserParameters returns human-readable warnings for any user
// parameters that are accepted but discouraged, sorted by key.
func DeprecatedUserParameters(params map[string]string) []string {
	var warnings []string
	for key := range params {
		if hint, ok := deprecatedKeys[normalizeKey(key)]; ok {
			warnings = append(warnings, fmt.Sprintf("%s: %s", key, hint))
		}
	}
	sort.Strings(warnings)
	return warnings
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
	b.WriteString("# Generated by cloudnative-mysql instance manager. Do not edit.\n")
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

	// Read-only handling: super_read_only exists since 5.7.8. Only replicas are
	// rendered read-only here. The run container additionally boots read-only via
	// a mysqld flag (see instance.Run) so a (re)starting instance cannot accept
	// writes before its role is reconciled; that flag is not applied to the
	// initdb/join temporary servers, which must be writable to bootstrap.
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
			pair{"loose-" + naming.EnabledVarSource, "1"},
			pair{"loose-" + naming.EnabledVarReplica, "1"},
		)
		if c.SemiSync.WaitForReplicaCount > 0 {
			pairs = append(pairs, pair{
				"loose-" + naming.WaitForCountVar,
				strconv.Itoa(c.SemiSync.WaitForReplicaCount),
			})
		}
		if c.SemiSync.TimeoutMillis > 0 {
			pairs = append(pairs, pair{
				"loose-" + naming.TimeoutVar,
				strconv.Itoa(c.SemiSync.TimeoutMillis),
			})
		}
	}

	if c.Archiving.Enabled {
		// sync_binlog=1 makes every committed transaction durable in the binary
		// log before the archiver can ship it; without it a crash could lose
		// acknowledged transactions that were never written to disk.
		pairs = append(pairs, pair{"sync_binlog", "1"})
		if c.Archiving.MaxBinlogSizeMB > 0 {
			pairs = append(pairs, pair{
				"max_binlog_size",
				strconv.Itoa(c.Archiving.MaxBinlogSizeMB * 1024 * 1024),
			})
		}
		if c.Archiving.BinlogExpireSeconds > 0 {
			key, value := binlogExpire(ver, c.Archiving.BinlogExpireSeconds)
			pairs = append(pairs, pair{key, value})
		}
	}

	if c.TopologyMode == TopologyGroupReplication {
		pairs = append(pairs, c.groupReplicationSettings(ver)...)
	}

	return pairs
}

// groupReplicationSettings renders the operator-owned group_replication_*
// namespace. The operator controls start and bootstrap: start_on_boot=OFF so a
// restarting member never rejoins unsupervised, and bootstrap_group is never
// rendered ON (a config-file ON would re-bootstrap on every boot, splitting the
// group). Bootstrap is a runtime SET GLOBAL, never a file default.
func (c *ServerConfig) groupReplicationSettings(ver version.Version) []pair {
	gr := c.GroupReplication
	pairs := []pair{
		{"plugin_load_add", "group_replication.so"},
		{"group_replication_group_name", gr.GroupName},
		{"group_replication_local_address", gr.LocalAddress},
		{"group_replication_group_seeds", gr.GroupSeeds},
		{"group_replication_start_on_boot", "OFF"},
		{"group_replication_bootstrap_group", "OFF"},
		{"group_replication_single_primary_mode", "ON"},
		{"group_replication_enforce_update_everywhere_checks", "OFF"},
	}

	if gr.Consistency != "" {
		pairs = append(pairs, pair{"group_replication_consistency", gr.Consistency})
	}
	if gr.ExitStateAction != "" {
		pairs = append(pairs, pair{"group_replication_exit_state_action", gr.ExitStateAction})
	}
	pairs = append(pairs, pair{"group_replication_autorejoin_tries", strconv.Itoa(gr.AutoRejoinTries)})

	// Distributed recovery and XCom run over TLS, reusing the cluster TLS
	// material so a joining member authenticates the donor.
	pairs = append(pairs,
		pair{"group_replication_ssl_mode", "REQUIRED"},
		pair{"group_replication_recovery_use_ssl", "ON"},
	)
	if gr.RecoverySSL.isset() {
		pairs = append(pairs,
			pair{"group_replication_recovery_ssl_ca", gr.RecoverySSL.CA},
			pair{"group_replication_recovery_ssl_cert", gr.RecoverySSL.Cert},
			pair{"group_replication_recovery_ssl_key", gr.RecoverySSL.Key},
		)
	}
	if gr.IPAllowlist != "" {
		pairs = append(pairs, pair{"group_replication_ip_allowlist", gr.IPAllowlist})
	}

	// Before 8.0.21 Group Replication rejects a non-NONE binlog checksum; newer
	// servers tolerate the default CRC32, so only force NONE where required.
	if ver.GroupReplicationRequiresNoBinlogChecksum() {
		pairs = append(pairs, pair{"binlog_checksum", "NONE"})
	}

	return pairs
}

// binlogExpire returns the version-appropriate binlog expiry variable and
// value. binlog_expire_logs_seconds was added in 8.0; older servers use the
// coarser expire_logs_days, so the seconds value is rounded up to whole days.
func binlogExpire(ver version.Version, seconds int) (string, string) {
	if ver.AtLeast(8, 0, 0) {
		return "binlog_expire_logs_seconds", strconv.Itoa(seconds)
	}
	days := max((seconds+86399)/86400, 1)
	return "expire_logs_days", strconv.Itoa(days)
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
