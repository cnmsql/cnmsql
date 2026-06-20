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
	"context"
	"database/sql"
	"fmt"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

// Manager executes Group Replication statements against a mysqld connection and
// reads the group view. Like the asynchronous replication.Manager it is a thin,
// ordered executor: the statement text is produced by the builders in this
// package and the policy that decides when to bootstrap, switch over or force
// membership lives in its callers.
type Manager struct {
	conn    pool.Connection
	version version.Version
}

// NewManager builds a Manager bound to a connection and server version.
func NewManager(conn pool.Connection, v version.Version) *Manager {
	return &Manager{conn: conn, version: v}
}

func (m *Manager) exec(ctx context.Context, stmt string) error {
	if _, err := m.conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("executing %q: %w", stmt, err)
	}
	return nil
}

// Start starts the local member, joining an existing group via distributed
// recovery. It never bootstraps; see Bootstrap.
func (m *Manager) Start(ctx context.Context) error {
	return m.exec(ctx, StartGroupReplicationStatement())
}

// Stop stops the local member, making it leave the group (the GR fencing
// primitive).
func (m *Manager) Stop(ctx context.Context) error {
	return m.exec(ctx, StopGroupReplicationStatement())
}

// Bootstrap runs the exactly-once group-creation sequence. It must only be
// called on the single designated bootstrap member when the group has never been
// bootstrapped; the caller is responsible for that gate. The bootstrap flag is
// turned off again before returning even if the start fails, so a later Start
// cannot accidentally re-bootstrap.
func (m *Manager) Bootstrap(ctx context.Context) (err error) {
	if execErr := m.exec(ctx, "SET GLOBAL group_replication_bootstrap_group = ON"); execErr != nil {
		return execErr
	}
	defer func() {
		if offErr := m.exec(ctx, "SET GLOBAL group_replication_bootstrap_group = OFF"); offErr != nil && err == nil {
			err = offErr
		}
	}()
	return m.exec(ctx, StartGroupReplicationStatement())
}

// SetAsPrimary performs a planned switchover to the member with the given
// server_uuid via group_replication_set_as_primary.
func (m *Manager) SetAsPrimary(ctx context.Context, memberUUID string) error {
	return m.exec(ctx, SetAsPrimaryStatement(memberUUID))
}

// GroupView is one member's parsed view of the group, read from
// performance_schema.replication_group_members.
type GroupView struct {
	// Configured is false when the plugin reports no rows (the member has never
	// joined a group).
	Configured bool
	// Members is the set of members the local member currently sees.
	Members []ViewMember
}

// ViewMember is one row of replication_group_members.
type ViewMember struct {
	MemberID string
	Host     string
	Port     int
	State    string
	Role     string
}

// Primary returns the member the group considers PRIMARY, if any.
func (g GroupView) Primary() (ViewMember, bool) {
	for _, member := range g.Members {
		if member.Role == MemberRolePrimary {
			return member, true
		}
	}
	return ViewMember{}, false
}

// readGroupMembersQuery is the projection the operator and in-Pod reader use to
// observe the group. member_role and member_host/port exist from 8.0.2, well
// below the operator's 8.0.22 GR floor.
const readGroupMembersQuery = `SELECT member_id, member_host, member_port, member_state, member_role
FROM performance_schema.replication_group_members`

// ReadGroupView reads the local member's view of the group. An empty result is
// reported as an unconfigured view rather than an error.
func (m *Manager) ReadGroupView(ctx context.Context) (GroupView, error) {
	rows, err := m.conn.QueryContext(ctx, readGroupMembersQuery)
	if err != nil {
		return GroupView{}, fmt.Errorf("querying replication_group_members: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var view GroupView
	for rows.Next() {
		var (
			member ViewMember
			port   sql.NullInt64
			host   sql.NullString
		)
		if err := rows.Scan(&member.MemberID, &host, &port, &member.State, &member.Role); err != nil {
			return GroupView{}, fmt.Errorf("scanning replication_group_members row: %w", err)
		}
		// When the plugin is loaded but GR has not been started (start_on_boot=OFF
		// before the operator bootstraps or joins), the table holds a single
		// placeholder row for the local server with an empty member_id and an
		// OFFLINE state. It is not a real group member: counting it would make the
		// view falsely Configured (stalling bootstrap) and skew quorum math, so
		// skip it. A started member always reports its own server_uuid here.
		if member.MemberID == "" {
			continue
		}
		member.Host = host.String
		member.Port = int(port.Int64)
		view.Members = append(view.Members, member)
	}
	if err := rows.Err(); err != nil {
		return GroupView{}, fmt.Errorf("iterating replication_group_members: %w", err)
	}
	view.Configured = len(view.Members) > 0
	return view, nil
}
