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

package pool

import (
	"strings"
	"testing"
)

func TestDSNSocket(t *testing.T) {
	dsn, err := Config{
		Socket:   "/var/run/mysqld/mysqld.sock",
		User:     "root",
		Password: "secret",
		Database: "app",
	}.DSN()
	if err != nil {
		t.Fatalf("DSN() error: %v", err)
	}

	if !strings.HasPrefix(dsn, "root:secret@unix(/var/run/mysqld/mysqld.sock)/app?") {
		t.Errorf("unexpected DSN prefix: %s", dsn)
	}
	if !strings.Contains(dsn, "parseTime=true") {
		t.Errorf("default params missing: %s", dsn)
	}
}

func TestDSNTCPDefaultPort(t *testing.T) {
	dsn, err := Config{Host: "127.0.0.1", User: "root"}.DSN()
	if err != nil {
		t.Fatalf("DSN() error: %v", err)
	}
	if !strings.HasPrefix(dsn, "root@tcp(127.0.0.1:3306)/?") {
		t.Errorf("unexpected DSN: %s", dsn)
	}
}

func TestDSNEscapesCredentials(t *testing.T) {
	dsn, err := Config{
		Host:     "db",
		Port:     3307,
		User:     "user@x",
		Password: "p:@/word",
	}.DSN()
	if err != nil {
		t.Fatalf("DSN() error: %v", err)
	}
	if strings.Contains(dsn, "user@x:") {
		t.Errorf("user should be escaped: %s", dsn)
	}
	if !strings.Contains(dsn, "tcp(db:3307)") {
		t.Errorf("custom port missing: %s", dsn)
	}
}

func TestDSNParamsOverrideDefaults(t *testing.T) {
	dsn, err := Config{
		Socket: "/s.sock",
		User:   "root",
		Params: map[string]string{"parseTime": "false", "charset": "utf8mb4"},
	}.DSN()
	if err != nil {
		t.Fatalf("DSN() error: %v", err)
	}
	if !strings.Contains(dsn, "parseTime=false") {
		t.Errorf("override not applied: %s", dsn)
	}
	if !strings.Contains(dsn, "charset=utf8mb4") {
		t.Errorf("extra param missing: %s", dsn)
	}
}

func TestDSNParamsAreSortedDeterministically(t *testing.T) {
	cfg := Config{Socket: "/s.sock", User: "root"}
	first, _ := cfg.DSN()
	for range 5 {
		got, _ := cfg.DSN()
		if got != first {
			t.Fatalf("DSN not deterministic:\n%s\n%s", first, got)
		}
	}
}

func TestDSNValidation(t *testing.T) {
	if _, err := (Config{Socket: "/s.sock"}).DSN(); err == nil {
		t.Error("expected error when user is empty")
	}
	if _, err := (Config{User: "root"}).DSN(); err == nil {
		t.Error("expected error when neither socket nor host is set")
	}
}
