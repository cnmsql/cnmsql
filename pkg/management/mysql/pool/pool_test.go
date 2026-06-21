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
