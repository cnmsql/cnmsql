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
	"testing"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

func mustVersion(t *testing.T, s string) version.Version {
	t.Helper()
	v, err := version.Parse(s)
	if err != nil {
		t.Fatalf("parsing version %q: %v", s, err)
	}
	return v
}

func TestConfigureRecoveryChannelStatementX509OmitsPassword(t *testing.T) {
	// An X509 account authenticates with its client cert; no password clause, and
	// the channel name is always group_replication_recovery.
	got := ConfigureRecoveryChannelStatement(mustVersion(t, "8.0.36"), "repl", "")
	want := "CHANGE REPLICATION SOURCE TO SOURCE_USER='repl' FOR CHANNEL 'group_replication_recovery'"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestConfigureRecoveryChannelStatementIncludesPassword(t *testing.T) {
	got := ConfigureRecoveryChannelStatement(mustVersion(t, "8.0.36"), "repl", "s3cret")
	want := "CHANGE REPLICATION SOURCE TO SOURCE_USER='repl', SOURCE_PASSWORD='s3cret'" +
		" FOR CHANNEL 'group_replication_recovery'"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestConfigureRecoveryChannelStatementLegacyTerminology(t *testing.T) {
	// Below 8.0.23 the server only understands CHANGE MASTER TO / MASTER_USER.
	got := ConfigureRecoveryChannelStatement(mustVersion(t, "8.0.22"), "repl", "")
	want := "CHANGE MASTER TO MASTER_USER='repl' FOR CHANNEL 'group_replication_recovery'"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBootstrapGroupStatementsTurnFlagOffAfterStart(t *testing.T) {
	stmts := BootstrapGroupStatements()
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d: %v", len(stmts), stmts)
	}
	if stmts[0] != "SET GLOBAL group_replication_bootstrap_group = ON" {
		t.Errorf("first statement should arm bootstrap, got %q", stmts[0])
	}
	if stmts[1] != "START GROUP_REPLICATION" {
		t.Errorf("second statement should start, got %q", stmts[1])
	}
	if stmts[2] != "SET GLOBAL group_replication_bootstrap_group = OFF" {
		t.Errorf("third statement must disarm bootstrap so no later START re-bootstraps, got %q", stmts[2])
	}
}

func TestSetAsPrimaryStatementQuotesUUID(t *testing.T) {
	got := SetAsPrimaryStatement("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	want := "SELECT group_replication_set_as_primary('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestForceMembersStatementJoinsAddresses(t *testing.T) {
	got := ForceMembersStatement([]string{"a:33061", "b:33061"})
	want := "SET GLOBAL group_replication_force_members = 'a:33061,b:33061'"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestForceMembersStatementEmptyClears(t *testing.T) {
	got := ForceMembersStatement(nil)
	want := "SET GLOBAL group_replication_force_members = ''"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestQuoteEscapesEmbeddedQuotes(t *testing.T) {
	if got := quote("a'b"); got != `'a\'b'` {
		t.Errorf("quote did not escape embedded quote, got %q", got)
	}
}

func TestGroupViewPrimary(t *testing.T) {
	view := GroupView{Members: []ViewMember{
		{MemberID: "1", Role: MemberRoleSecondary, State: MemberStateOnline},
		{MemberID: "2", Role: MemberRolePrimary, State: MemberStateOnline},
	}}
	primary, ok := view.Primary()
	if !ok || primary.MemberID != "2" {
		t.Errorf("expected member 2 as primary, got %+v ok=%v", primary, ok)
	}

	none := GroupView{Members: []ViewMember{{MemberID: "1", Role: MemberRoleSecondary}}}
	if _, ok := none.Primary(); ok {
		t.Error("expected no primary when none has the PRIMARY role")
	}
}
