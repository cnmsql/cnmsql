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
	"testing"
)

func TestControlConfigUsesAdminInterfaceWhenAvailable(t *testing.T) {
	cfg := ControlConfig(true, ControlParams{
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
	cfg := ControlConfig(true, ControlParams{
		User:         "manager",
		AdminAddress: "127.0.0.5",
		AdminPort:    40000,
	})
	if cfg.Host != "127.0.0.5" || cfg.Port != 40000 {
		t.Errorf("custom admin endpoint not honoured: %s:%d", cfg.Host, cfg.Port)
	}
}

func TestControlConfigFallsBackToSocketOnLegacy(t *testing.T) {
	cfg := ControlConfig(false, ControlParams{
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
	cfg := ControlConfig(true, ControlParams{User: "manager"})
	if _, err := cfg.DSN(); err != nil {
		t.Errorf("control config produced invalid DSN: %v", err)
	}
}
