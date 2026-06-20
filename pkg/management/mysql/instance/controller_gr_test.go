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

package instance

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
)

// expectGroupViewQuery registers the replication_group_members read with the
// given single member.
func expectGroupViewQuery(mock sqlmock.Sqlmock, uuid, host string, port int, state, role string) {
	rows := sqlmock.NewRows([]string{"member_id", "member_host", "member_port", "member_state", "member_role"})
	if uuid != "" {
		rows.AddRow(uuid, host, port, state, role)
	}
	mock.ExpectQuery("replication_group_members").WillReturnRows(rows)
}

func TestGroupReplicationStatusReportsOnlinePrimary(t *testing.T) {
	t.Parallel()
	c, mock := newController(t, nil)
	c.EnableGroupReplication()

	expectGroupViewQuery(mock, "uuid-1", "gr-1.default.svc", 3306,
		groupreplication.MemberStateOnline, groupreplication.MemberRolePrimary)
	mock.ExpectQuery("SELECT @@global.server_uuid").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("uuid-1"))
	mock.ExpectQuery("SELECT @@global.group_replication_group_name").
		WillReturnRows(sqlmock.NewRows([]string{"v"}).AddRow("group-uuid"))

	gr := c.groupReplicationStatus(context.Background())
	if gr == nil {
		t.Fatal("expected a GR status block for an ONLINE member")
	}
	if gr.MemberID != "uuid-1" || gr.State != groupreplication.MemberStateOnline {
		t.Fatalf("local member state = %+v, want ONLINE uuid-1", gr)
	}
	if gr.Role != groupreplication.MemberRolePrimary {
		t.Fatalf("role = %q, want PRIMARY", gr.Role)
	}
	if gr.PrimaryMemberID != "uuid-1" {
		t.Fatalf("primaryMemberID = %q, want uuid-1", gr.PrimaryMemberID)
	}
	if gr.GroupName != "group-uuid" {
		t.Fatalf("groupName = %q, want group-uuid", gr.GroupName)
	}
	if len(gr.Members) != 1 {
		t.Fatalf("members = %d, want 1", len(gr.Members))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGroupReplicationStatusNilWhenNotConfigured(t *testing.T) {
	t.Parallel()
	c, mock := newController(t, nil)
	c.EnableGroupReplication()

	// No rows: the member has not joined a group.
	expectGroupViewQuery(mock, "", "", 0, "", "")

	if gr := c.groupReplicationStatus(context.Background()); gr != nil {
		t.Fatalf("expected nil GR status for an unconfigured member, got %+v", gr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStatusOmitsGroupReplicationForAsync(t *testing.T) {
	t.Parallel()
	// GR not enabled (async cluster): Status must not issue any GR query and must
	// leave the GroupReplication block nil.
	c, mock := newController(t, nil)
	expectStatusQueries(mock, false, false, false)
	expectBestEffortQueries(mock)

	status, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.GroupReplication != nil {
		t.Fatal("async Status must not populate the GroupReplication block")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
