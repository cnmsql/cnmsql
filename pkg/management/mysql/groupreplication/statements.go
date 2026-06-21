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

package groupreplication

import (
	"fmt"
	"strings"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

// RecoveryChannelName is the dedicated replication channel Group Replication uses
// for distributed recovery (binlog catch-up and Clone-plugin snapshots).
const RecoveryChannelName = "group_replication_recovery"

// Member states reported by performance_schema.replication_group_members.
const (
	MemberStateOnline      = "ONLINE"
	MemberStateRecovering  = "RECOVERING"
	MemberStateOffline     = "OFFLINE"
	MemberStateError       = "ERROR"
	MemberStateUnreachable = "UNREACHABLE"
)

// Member roles reported by performance_schema.replication_group_members.
const (
	MemberRolePrimary   = "PRIMARY"
	MemberRoleSecondary = "SECONDARY"
)

// StartGroupReplicationStatement starts the local member. On every member except
// the very first bootstrap it joins an existing group via distributed recovery;
// the bootstrap flag is never set here (see BootstrapGroupStatements).
func StartGroupReplicationStatement() string {
	return "START GROUP_REPLICATION"
}

// StopGroupReplicationStatement stops the local member, making it leave the
// group. This is the GR-native fencing primitive.
func StopGroupReplicationStatement() string {
	return "STOP GROUP_REPLICATION"
}

// BootstrapGroupStatements are the exactly-once group-creation sequence, run by
// the single designated bootstrap member only. The bootstrap flag is turned ON,
// the member starts (creating the group), then the flag is immediately turned
// OFF so no subsequent START re-bootstraps and forks a second group. The flag is
// never persisted to the config file for the same reason.
func BootstrapGroupStatements() []string {
	return []string{
		"SET GLOBAL group_replication_bootstrap_group = ON",
		"START GROUP_REPLICATION",
		"SET GLOBAL group_replication_bootstrap_group = OFF",
	}
}

// ConfigureRecoveryChannelStatement sets the credentials a joining member uses
// to authenticate to a donor on the distributed-recovery channel, before
// START GROUP_REPLICATION. The channel's TLS material comes from the
// group_replication_recovery_ssl_* server settings, so only the account is set
// here; an X509-authenticated account needs no password. The verb follows the
// server's replica/source terminology (CHANGE REPLICATION SOURCE TO on 8.0.23+,
// CHANGE MASTER TO below).
func ConfigureRecoveryChannelStatement(v version.Version, user, password string) string {
	verb, prefix := "CHANGE MASTER TO", "MASTER_"
	if v.UsesReplicaTerminology() {
		verb, prefix = "CHANGE REPLICATION SOURCE TO", "SOURCE_"
	}
	clauses := []string{prefix + "USER=" + quote(user)}
	if password != "" {
		clauses = append(clauses, prefix+"PASSWORD="+quote(password))
	}
	return fmt.Sprintf("%s %s FOR CHANNEL %s", verb, strings.Join(clauses, ", "), quote(RecoveryChannelName))
}

// ForceCloneStatement lowers group_replication_clone_threshold to 1 so the next
// START GROUP_REPLICATION provisions the member by cloning a donor wholesale,
// rather than replaying the donor's binlogs onto the joiner. A freshly
// initialised joiner already holds the cluster's accounts, so binlog recovery
// would conflict (duplicate CREATE USER); a full clone replaces the joiner's data
// cleanly. The runtime value is reset to the configured default by the implicit
// restart the clone performs.
func ForceCloneStatement() string {
	return "SET GLOBAL group_replication_clone_threshold = 1"
}

// SetAsPrimaryStatement returns the UDF call that performs a planned switchover
// to the member with the given server_uuid, the GR way to change the primary
// without an election. memberUUID must be a server_uuid already in the group.
func SetAsPrimaryStatement(memberUUID string) string {
	return fmt.Sprintf("SELECT group_replication_set_as_primary(%s)", quote(memberUUID))
}

// ForceMembersStatement returns the statement that forcibly resets the group
// membership to the given XCom addresses. This is the dangerous minority-recovery
// primitive (group_replication_force_members): it must only ever be run on a
// single surviving member after the operator has confirmed the rest are truly
// gone, since misuse splits the group. Passing no addresses returns the clear
// form used to reset the variable afterwards.
func ForceMembersStatement(addresses []string) string {
	return fmt.Sprintf("SET GLOBAL group_replication_force_members = %s", quote(strings.Join(addresses, ",")))
}

// SetGroupSeedsStatement updates group_replication_group_seeds at runtime, e.g.
// after a scale change, without restarting the member.
func SetGroupSeedsStatement(seeds []string) string {
	return fmt.Sprintf("SET GLOBAL group_replication_group_seeds = %s", quote(strings.Join(seeds, ",")))
}

// quote renders a single-quoted SQL string literal, escaping embedded quotes and
// backslashes. The inputs here (UUIDs, host:port addresses) are operator-computed
// and never user free-text, but quoting keeps the builders safe by construction.
func quote(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + r.Replace(s) + "'"
}
