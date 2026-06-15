/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package replication

import (
	"strings"
	"testing"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

func mustParse(t *testing.T, v string) version.Version {
	t.Helper()
	parsed, err := version.Parse(v)
	if err != nil {
		t.Fatalf("version.Parse(%q): %v", v, err)
	}
	return parsed
}

func TestChangeSourceModernSyntax(t *testing.T) {
	v := mustParse(t, "8.0.36")
	stmt := ChangeSourceStatement(v, SourceOptions{
		Host:         "primary.svc",
		Port:         3306,
		User:         "repl",
		Password:     "secret",
		AutoPosition: true,
	})

	for _, want := range []string{
		"CHANGE REPLICATION SOURCE TO",
		"SOURCE_HOST='primary.svc'",
		"SOURCE_PORT=3306",
		"SOURCE_USER='repl'",
		"SOURCE_PASSWORD='secret'",
		"SOURCE_AUTO_POSITION=1",
	} {
		if !strings.Contains(stmt, want) {
			t.Errorf("expected %q in:\n%s", want, stmt)
		}
	}
	if strings.Contains(stmt, "MASTER_") {
		t.Errorf("modern syntax should not use MASTER_:\n%s", stmt)
	}
}

func TestChangeSourceLegacySyntax(t *testing.T) {
	v := mustParse(t, "5.7.44")
	stmt := ChangeSourceStatement(v, SourceOptions{
		Host:         "primary.svc",
		Port:         3306,
		User:         "repl",
		AutoPosition: true,
	})

	for _, want := range []string{
		"CHANGE MASTER TO",
		"MASTER_HOST='primary.svc'",
		"MASTER_AUTO_POSITION=1",
	} {
		if !strings.Contains(stmt, want) {
			t.Errorf("expected %q in:\n%s", want, stmt)
		}
	}
	if strings.Contains(stmt, "SOURCE_") {
		t.Errorf("legacy syntax should not use SOURCE_:\n%s", stmt)
	}
}

func TestChangeSourceGetPublicKey(t *testing.T) {
	modern := ChangeSourceStatement(mustParse(t, "8.0.36"), SourceOptions{Host: "h", User: "u", GetPublicKey: true})
	if !strings.Contains(modern, "GET_SOURCE_PUBLIC_KEY=1") {
		t.Errorf("modern get public key missing:\n%s", modern)
	}
	legacy := ChangeSourceStatement(mustParse(t, "5.7.44"), SourceOptions{Host: "h", User: "u", GetPublicKey: true})
	if !strings.Contains(legacy, "GET_MASTER_PUBLIC_KEY=1") {
		t.Errorf("legacy get public key missing:\n%s", legacy)
	}
	// Older 5.7 rejects the clause; it must be omitted.
	old := ChangeSourceStatement(mustParse(t, "5.7.22"), SourceOptions{Host: "h", User: "u", GetPublicKey: true})
	if strings.Contains(old, "PUBLIC_KEY") {
		t.Errorf("old server should not emit a public-key clause:\n%s", old)
	}
}

func TestChangeSourceMTLS(t *testing.T) {
	v := mustParse(t, "8.0.36")
	stmt := ChangeSourceStatement(v, SourceOptions{
		Host:    "primary.svc",
		User:    "repl",
		SSLCA:   "/tls/ca.crt",
		SSLCert: "/tls/tls.crt",
		SSLKey:  "/tls/tls.key",
	})

	for _, want := range []string{
		"SOURCE_SSL=1",
		"SOURCE_SSL_CA='/tls/ca.crt'",
		"SOURCE_SSL_CERT='/tls/tls.crt'",
		"SOURCE_SSL_KEY='/tls/tls.key'",
	} {
		if !strings.Contains(stmt, want) {
			t.Errorf("expected %q in:\n%s", want, stmt)
		}
	}
}

func TestChangeSourceEscapesPassword(t *testing.T) {
	v := mustParse(t, "8.0.36")
	stmt := ChangeSourceStatement(v, SourceOptions{
		Host:     "h",
		User:     "u",
		Password: "a'b\\c",
	})
	if !strings.Contains(stmt, `SOURCE_PASSWORD='a\'b\\c'`) {
		t.Errorf("password not escaped correctly:\n%s", stmt)
	}
}

func TestStartStopResetShowVersionAware(t *testing.T) {
	modern := mustParse(t, "8.0.23")
	legacy := mustParse(t, "8.0.22")

	if got := StartReplicaStatement(modern); got != "START REPLICA" {
		t.Errorf("modern start = %q", got)
	}
	if got := StartReplicaStatement(legacy); got != "START SLAVE" {
		t.Errorf("legacy start = %q", got)
	}
	if got := StopReplicaStatement(modern); got != "STOP REPLICA" {
		t.Errorf("modern stop = %q", got)
	}
	if got := StopReplicaStatement(legacy); got != "STOP SLAVE" {
		t.Errorf("legacy stop = %q", got)
	}
	if got := ShowReplicaStatusStatement(modern); got != "SHOW REPLICA STATUS" {
		t.Errorf("modern show = %q", got)
	}
	if got := ShowReplicaStatusStatement(legacy); got != "SHOW SLAVE STATUS" {
		t.Errorf("legacy show = %q", got)
	}
	if got := ResetReplicaStatement(modern, false); got != "RESET REPLICA" {
		t.Errorf("modern reset = %q", got)
	}
	if got := ResetReplicaStatement(legacy, true); got != "RESET SLAVE ALL" {
		t.Errorf("legacy reset all = %q", got)
	}
}

func TestResetBinaryLogsStatement(t *testing.T) {
	const legacy = "RESET MASTER"
	if got := ResetBinaryLogsStatement(mustParse(t, "8.4.0")); got != "RESET BINARY LOGS AND GTIDS" {
		t.Errorf("modern reset = %q", got)
	}
	if got := ResetBinaryLogsStatement(mustParse(t, "8.0.36")); got != legacy {
		t.Errorf("8.0 reset = %q", got)
	}
	if got := ResetBinaryLogsStatement(mustParse(t, "5.7.44")); got != legacy {
		t.Errorf("5.7 reset = %q", got)
	}
}

func TestSetGTIDPurgedStatement(t *testing.T) {
	got := SetGTIDPurgedStatement("uuid:1-5,uuid2:1-3")
	if got != "SET GLOBAL gtid_purged = 'uuid:1-5,uuid2:1-3'" {
		t.Errorf("gtid_purged = %q", got)
	}
}

func TestReadOnlyStatements(t *testing.T) {
	if got := SetReadOnlyStatement(true); got != "SET GLOBAL read_only = ON" {
		t.Errorf("read_only on = %q", got)
	}
	if got := SetSuperReadOnlyStatement(false); got != "SET GLOBAL super_read_only = OFF" {
		t.Errorf("super_read_only off = %q", got)
	}
}

func TestSemiSyncInstallVersionAware(t *testing.T) {
	modern := mustParse(t, "8.0.26")
	legacy := mustParse(t, "8.0.25")

	wantSource := "INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so'"
	if got := InstallSemiSyncSourceStatement(modern); got != wantSource {
		t.Errorf("modern source plugin = %q", got)
	}
	wantReplica := "INSTALL PLUGIN rpl_semi_sync_slave SONAME 'semisync_slave.so'"
	if got := InstallSemiSyncReplicaStatement(legacy); got != wantReplica {
		t.Errorf("legacy replica plugin = %q", got)
	}
}
