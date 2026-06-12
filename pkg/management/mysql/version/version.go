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

// Package version parses and compares MySQL server versions. It is shared by
// the config renderer and the replication manager, both of which make
// keyword/feature decisions based on the running server version.
package version

import (
	"fmt"
	"strconv"
	"strings"
)

// Version is a parsed MySQL major.minor.patch version.
type Version struct {
	Major int
	Minor int
	Patch int
}

// Parse extracts the version from a MySQL version string such as "8.0.36",
// "5.7.44-48" or "8.4". A leading "v" is tolerated and any vendor suffix after
// a dash is dropped.
func Parse(v string) (Version, error) {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "v")
	if v == "" {
		return Version{}, fmt.Errorf("empty MySQL version")
	}

	if idx := strings.IndexByte(v, '-'); idx != -1 {
		v = v[:idx]
	}

	parts := strings.Split(v, ".")
	out := Version{}
	var err error

	out.Major, err = strconv.Atoi(parts[0])
	if err != nil {
		return Version{}, fmt.Errorf("invalid MySQL version %q: %w", v, err)
	}
	if len(parts) > 1 {
		if out.Minor, err = strconv.Atoi(parts[1]); err != nil {
			return Version{}, fmt.Errorf("invalid MySQL version %q: %w", v, err)
		}
	}
	if len(parts) > 2 {
		if out.Patch, err = strconv.Atoi(parts[2]); err != nil {
			return Version{}, fmt.Errorf("invalid MySQL version %q: %w", v, err)
		}
	}

	return out, nil
}

// AtLeast reports whether the version is greater than or equal to
// major.minor.patch.
func (v Version) AtLeast(major, minor, patch int) bool {
	switch {
	case v.Major != major:
		return v.Major > major
	case v.Minor != minor:
		return v.Minor > minor
	default:
		return v.Patch >= patch
	}
}

// UsesReplicaTerminology reports whether the server uses the modern
// SOURCE/REPLICA replication syntax (CHANGE REPLICATION SOURCE TO, START
// REPLICA, SHOW REPLICA STATUS), introduced in MySQL 8.0.23. Older servers use
// the MASTER/SLAVE syntax.
func (v Version) UsesReplicaTerminology() bool {
	return v.AtLeast(8, 0, 23)
}

// HasSuperReadOnly reports whether super_read_only is available (MySQL 5.7.8+).
func (v Version) HasSuperReadOnly() bool {
	return v.AtLeast(5, 7, 8)
}

// HasGetSourcePublicKey reports whether CHANGE MASTER/REPLICATION SOURCE accepts
// the GET_{MASTER,SOURCE}_PUBLIC_KEY clause, which fetches the
// caching_sha2_password public key over a non-TLS link. Added in MySQL 5.7.23 /
// 8.0.4; older servers reject it as a syntax error.
func (v Version) HasGetSourcePublicKey() bool {
	return v.AtLeast(5, 7, 23)
}

// HasLogReplicaUpdates reports whether the log_replica_updates spelling is used
// instead of log_slave_updates (MySQL 8.0+).
func (v Version) HasLogReplicaUpdates() bool {
	return v.AtLeast(8, 0, 0)
}

// HasAdminInterface reports whether the server supports the administrative
// network interface (admin_address/admin_port), introduced in MySQL 8.0.14.
// Connections on that interface are not governed by max_connections, so the
// instance manager can always reach mysqld even when client connections are
// exhausted. On older servers the manager must rely on the reserved
// SUPER/CONNECTION_ADMIN connection slot instead.
func (v Version) HasAdminInterface() bool {
	return v.AtLeast(8, 0, 14)
}

// UsesResetBinaryLogsAndGtids reports whether the server uses
// "RESET BINARY LOGS AND GTIDS" (MySQL 8.4.0+) instead of the now-deprecated
// "RESET MASTER".
func (v Version) UsesResetBinaryLogsAndGtids() bool {
	return v.AtLeast(8, 4, 0)
}

// usesSourceSemiSyncNaming reports whether the server uses the source/replica
// semi-sync plugin and variable naming (MySQL 8.0.26+) instead of master/slave.
func (v Version) usesSourceSemiSyncNaming() bool {
	return v.AtLeast(8, 0, 26)
}

// SemiSyncNaming holds the version-appropriate identifiers for semi-synchronous
// replication: the system variable names, the plugin names and the shared
// library file names.
type SemiSyncNaming struct {
	// EnabledVarSource / EnabledVarReplica are the *_enabled system variables.
	EnabledVarSource  string
	EnabledVarReplica string
	// WaitForCountVar bounds how many acknowledgements the source waits for.
	WaitForCountVar string
	// TimeoutVar bounds the acknowledgement wait, in milliseconds.
	TimeoutVar string
	// PluginSource / PluginReplica are the INSTALL PLUGIN names.
	PluginSource  string
	PluginReplica string
	// LibSource / LibReplica are the plugin shared-library file names.
	LibSource  string
	LibReplica string
}

// SemiSync returns the semi-sync naming appropriate for the server version.
func (v Version) SemiSync() SemiSyncNaming {
	if v.usesSourceSemiSyncNaming() {
		return SemiSyncNaming{
			EnabledVarSource:  "rpl_semi_sync_source_enabled",
			EnabledVarReplica: "rpl_semi_sync_replica_enabled",
			WaitForCountVar:   "rpl_semi_sync_source_wait_for_replica_count",
			TimeoutVar:        "rpl_semi_sync_source_timeout",
			PluginSource:      "rpl_semi_sync_source",
			PluginReplica:     "rpl_semi_sync_replica",
			LibSource:         "semisync_source.so",
			LibReplica:        "semisync_replica.so",
		}
	}
	return SemiSyncNaming{
		EnabledVarSource:  "rpl_semi_sync_master_enabled",
		EnabledVarReplica: "rpl_semi_sync_slave_enabled",
		WaitForCountVar:   "rpl_semi_sync_master_wait_for_slave_count",
		TimeoutVar:        "rpl_semi_sync_master_timeout",
		PluginSource:      "rpl_semi_sync_master",
		PluginReplica:     "rpl_semi_sync_slave",
		LibSource:         "semisync_master.so",
		LibReplica:        "semisync_slave.so",
	}
}
