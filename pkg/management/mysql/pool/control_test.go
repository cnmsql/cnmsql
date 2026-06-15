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
	"testing"

	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
)

func mustVersion(t *testing.T, v string) version.Version {
	t.Helper()
	parsed, err := version.Parse(v)
	if err != nil {
		t.Fatalf("version.Parse(%q): %v", v, err)
	}
	return parsed
}

func TestControlConfigUsesAdminInterfaceWhenAvailable(t *testing.T) {
	cfg := ControlConfig(mustVersion(t, "8.0.36"), ControlParams{
		User:   "manager",
		Socket: "/var/run/mysqld/mysqld.sock",
	})

	if cfg.Host != "127.0.0.1" || cfg.Port != 33062 {
		t.Errorf("expected admin interface loopback:33062, got %s:%d", cfg.Host, cfg.Port)
	}
	if cfg.Socket != "" {
		t.Errorf("admin interface config should not use the socket, got %q", cfg.Socket)
	}
	if cfg.MaxOpenConns != 1 {
		t.Errorf("control pool must be capped at 1 connection, got %d", cfg.MaxOpenConns)
	}
}

func TestControlConfigCustomAdminEndpoint(t *testing.T) {
	cfg := ControlConfig(mustVersion(t, "8.4.0"), ControlParams{
		User:         "manager",
		AdminAddress: "127.0.0.5",
		AdminPort:    40000,
	})
	if cfg.Host != "127.0.0.5" || cfg.Port != 40000 {
		t.Errorf("custom admin endpoint not honoured: %s:%d", cfg.Host, cfg.Port)
	}
}

func TestControlConfigFallsBackToSocketOnLegacy(t *testing.T) {
	cfg := ControlConfig(mustVersion(t, "5.7.44"), ControlParams{
		User:   "manager",
		Socket: "/var/run/mysqld/mysqld.sock",
	})

	if cfg.Socket != "/var/run/mysqld/mysqld.sock" {
		t.Errorf("legacy config should use the socket, got %q", cfg.Socket)
	}
	if cfg.Host != "" || cfg.Port != 0 {
		t.Errorf("legacy config should not use the admin interface, got %s:%d", cfg.Host, cfg.Port)
	}
	if cfg.MaxOpenConns != 1 {
		t.Errorf("control pool must be capped at 1 connection, got %d", cfg.MaxOpenConns)
	}
}

func TestControlConfigDSNIsValid(t *testing.T) {
	cfg := ControlConfig(mustVersion(t, "8.0.36"), ControlParams{User: "manager"})
	if _, err := cfg.DSN(); err != nil {
		t.Errorf("control config produced invalid DSN: %v", err)
	}
}
