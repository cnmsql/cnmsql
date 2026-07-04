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
	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

// ReplDialect is the engine's replication SQL dialect: the verbs, column names
// and syntax each flavor uses for CHANGE MASTER/SOURCE, START/STOP
// SLAVE/REPLICA, SHOW SLAVE/REPLICA STATUS and RESET MASTER/BINARY LOGS.
type ReplDialect interface {
	ChangeSource(v version.Version, opts replication.SourceOptions) string
	StartReplica(v version.Version) string
	StopReplica(v version.Version) string
	ResetReplica(v version.Version, all bool) string
	ShowReplicaStatus(v version.Version) string
	ResetBinaryLogs(v version.Version) string
}

// --- MySQL replication dialect ---

type mysqlReplDialect struct{}

func (mysqlReplDialect) ChangeSource(v version.Version, opts replication.SourceOptions) string {
	return replication.ChangeSourceStatement(v, opts)
}

func (mysqlReplDialect) StartReplica(v version.Version) string {
	return replication.StartReplicaStatement(v)
}

func (mysqlReplDialect) StopReplica(v version.Version) string {
	return replication.StopReplicaStatement(v)
}

func (mysqlReplDialect) ResetReplica(v version.Version, all bool) string {
	return replication.ResetReplicaStatement(v, all)
}

func (mysqlReplDialect) ShowReplicaStatus(v version.Version) string {
	return replication.ShowReplicaStatusStatement(v)
}

func (mysqlReplDialect) ResetBinaryLogs(v version.Version) string {
	return replication.ResetBinaryLogsStatement(v)
}

// --- MariaDB replication dialect ---

// MariaDB never adopted the SOURCE/REPLICA terminology; the canonical verbs
// remain CHANGE MASTER TO / START SLAVE / STOP SLAVE / SHOW SLAVE STATUS /
// RESET MASTER regardless of server version.

type mariadbReplDialect struct{}

func (mariadbReplDialect) ChangeSource(version.Version, replication.SourceOptions) string {
	// NOT IMPLEMENTED. MariaDB uses CHANGE MASTER TO ... MASTER_USE_GTID=slave_pos,
	// which the MySQL-oriented builder in the replication package cannot produce
	// (for high version numbers it would emit CHANGE REPLICATION SOURCE TO with
	// MASTER_AUTO_POSITION — invalid on MariaDB). This lands in M-MDB.4.
	panic("engine: MariaDB ReplDialect.ChangeSource not implemented (TODO M-MDB.4)")
}

func (mariadbReplDialect) StartReplica(version.Version) string {
	return "START SLAVE"
}

func (mariadbReplDialect) StopReplica(version.Version) string {
	return "STOP SLAVE"
}

func (mariadbReplDialect) ResetReplica(_ version.Version, all bool) string {
	if all {
		return "RESET SLAVE ALL"
	}
	return "RESET SLAVE"
}

func (mariadbReplDialect) ShowReplicaStatus(version.Version) string {
	return "SHOW SLAVE STATUS"
}

func (mariadbReplDialect) ResetBinaryLogs(version.Version) string {
	return "RESET MASTER"
}
