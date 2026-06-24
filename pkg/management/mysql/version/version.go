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

// Series returns the major.minor release series of the version, with the patch
// component zeroed. MySQL upgrades are reasoned about per series (8.0, 8.4,
// 9.0), not per patch.
func (v Version) Series() Version {
	return Version{Major: v.Major, Minor: v.Minor}
}

// UpgradeSeriesChain is the ordered set of MySQL series this operator supports
// upgrading across. Each adjacent pair is exactly one supported hop: an upgrade
// may move at most one entry forward, may not skip an entry (e.g. 8.0 -> 9.0
// must pass through 8.4), and may not move backward (in-place downgrade is
// unsupported). Extend this slice as further series are qualified.
var UpgradeSeriesChain = []Version{
	{Major: 8, Minor: 0},
	{Major: 8, Minor: 4},
	{Major: 9, Minor: 0},
}

// seriesIndex returns the position of v's series in UpgradeSeriesChain, or -1
// when the series is not a known upgrade hop.
func seriesIndex(v Version) int {
	s := v.Series()
	for i, entry := range UpgradeSeriesChain {
		if entry == s {
			return i
		}
	}
	return -1
}

// CheckUpgrade validates a server-version transition from one version to
// another along UpgradeSeriesChain. It returns nil when the transition is a
// no-op (same series, e.g. a patch bump) or a single supported hop forward, and
// a descriptive error otherwise: an in-place downgrade, a skipped series, or a
// series outside the supported chain. Both the admission webhook and the
// instance manager call this so the rule is enforced identically in both
// places.
func CheckUpgrade(from, to Version) error {
	if from.Series() == to.Series() {
		return nil
	}
	fromIdx := seriesIndex(from)
	toIdx := seriesIndex(to)
	if fromIdx == -1 {
		return fmt.Errorf("unsupported source MySQL series %d.%d", from.Major, from.Minor)
	}
	if toIdx == -1 {
		return fmt.Errorf("unsupported target MySQL series %d.%d", to.Major, to.Minor)
	}
	if toIdx < fromIdx {
		return fmt.Errorf("downgrade from MySQL %d.%d to %d.%d is not supported",
			from.Major, from.Minor, to.Major, to.Minor)
	}
	if toIdx > fromIdx+1 {
		next := UpgradeSeriesChain[fromIdx+1]
		return fmt.Errorf("cannot upgrade from MySQL %d.%d directly to %d.%d: upgrade to %d.%d first",
			from.Major, from.Minor, to.Major, to.Minor, next.Major, next.Minor)
	}
	return nil
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

// SupportsGroupReplication reports whether the server may run MySQL Group
// Replication under this operator. Group Replication exists from 8.0.0, but the
// operator requires a floor of 8.0.22 for stable single-primary auto-rejoin and
// the consistency levels it relies on. 5.6/5.7 GR is unsupported.
func (v Version) SupportsGroupReplication() bool {
	return v.AtLeast(8, 0, 22)
}

// HasGroupReplicationClone reports whether distributed recovery may use the
// Clone plugin to provision a joining member (MySQL 8.0.17+). Below this a join
// can only recover by replaying binlogs from a donor, which fails when the
// donor's logs have been purged past the joiner's position.
func (v Version) HasGroupReplicationClone() bool {
	return v.AtLeast(8, 0, 17)
}

// GroupReplicationRequiresNoBinlogChecksum reports whether Group Replication
// requires binlog_checksum=NONE. Versions before 8.0.21 reject a non-NONE
// checksum when starting the group; 8.0.21+ tolerate the default CRC32, so the
// operator only forces NONE on the older servers.
func (v Version) GroupReplicationRequiresNoBinlogChecksum() bool {
	return !v.AtLeast(8, 0, 21)
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
