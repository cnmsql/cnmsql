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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

func memberColumns() []string {
	return []string{"member_id", "member_host", "member_port", "member_state", "member_role"}
}

// When the plugin is loaded but GR has not been started, the table holds a lone
// placeholder row with an empty member_id and OFFLINE state. It must not make
// the view Configured, otherwise the role reconciler treats the member as a
// started-but-not-yet-online member and never bootstraps the group.
func TestReadGroupViewSkipsUnstartedPlaceholderRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows(memberColumns()).
		AddRow("", "", nil, MemberStateOffline, "")
	mock.ExpectQuery("FROM performance_schema.replication_group_members").WillReturnRows(rows)

	m := NewManager(db, version.Version{})
	view, err := m.ReadGroupView(context.Background())
	if err != nil {
		t.Fatalf("ReadGroupView: %v", err)
	}
	if view.Configured {
		t.Errorf("expected view to be unconfigured for the unstarted placeholder row")
	}
	if len(view.Members) != 0 {
		t.Errorf("expected no members, got %d", len(view.Members))
	}
}

// A started member reports its own server_uuid and is a real group member.
func TestReadGroupViewReportsStartedMember(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("opening sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows := sqlmock.NewRows(memberColumns()).
		AddRow("uuid-1", "gr-single-1", 3306, MemberStateOnline, MemberRolePrimary)
	mock.ExpectQuery("FROM performance_schema.replication_group_members").WillReturnRows(rows)

	m := NewManager(db, version.Version{})
	view, err := m.ReadGroupView(context.Background())
	if err != nil {
		t.Fatalf("ReadGroupView: %v", err)
	}
	if !view.Configured {
		t.Fatalf("expected a configured view")
	}
	if len(view.Members) != 1 {
		t.Fatalf("expected one member, got %d", len(view.Members))
	}
	got := view.Members[0]
	if got.MemberID != "uuid-1" || got.Host != "gr-single-1" || got.Port != 3306 ||
		got.State != MemberStateOnline || got.Role != MemberRolePrimary {
		t.Errorf("unexpected member: %+v", got)
	}
}
