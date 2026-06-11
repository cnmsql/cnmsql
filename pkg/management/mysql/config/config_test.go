/*
Copyright 2026 The CNMySQL Authors.

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

package config

import (
	"strings"
	"testing"
)

func baseConfig() *ServerConfig {
	return &ServerConfig{
		ServerID:     1,
		Version:      "8.0.36",
		Role:         RolePrimary,
		DataDir:      "/var/lib/mysql",
		Socket:       "/var/run/mysqld/mysqld.sock",
		BinlogFormat: "ROW",
	}
}

func mustRender(t *testing.T, c *ServerConfig) string {
	t.Helper()
	out, err := c.Render()
	if err != nil {
		t.Fatalf("Render() unexpected error: %v", err)
	}
	return out
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected rendered config to contain %q\n---\n%s", needle, haystack)
	}
}

func assertNotContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if strings.Contains(haystack, needle) {
		t.Errorf("expected rendered config NOT to contain %q\n---\n%s", needle, haystack)
	}
}

func TestRenderPrimaryBaseline(t *testing.T) {
	out := mustRender(t, baseConfig())

	assertContains(t, out, "[mysqld]")
	assertContains(t, out, "server-id = 1")
	assertContains(t, out, "gtid_mode = ON")
	assertContains(t, out, "enforce_gtid_consistency = ON")
	assertContains(t, out, "binlog_format = ROW")
	assertContains(t, out, "log_bin = binlog")
	// A primary is not read-only.
	assertNotContains(t, out, "read_only")
	assertNotContains(t, out, "super_read_only")
}

func TestRenderReplicaIsReadOnly(t *testing.T) {
	c := baseConfig()
	c.Role = RoleReplica
	out := mustRender(t, c)

	assertContains(t, out, "read_only = ON")
	assertContains(t, out, "super_read_only = ON")
}

func TestRenderVersionAwareLogUpdates(t *testing.T) {
	c := baseConfig()
	c.Version = "8.0.36"
	assertContains(t, mustRender(t, c), "log_replica_updates = ON")
	assertNotContains(t, mustRender(t, c), "log_slave_updates")

	c.Version = "5.7.44"
	assertContains(t, mustRender(t, c), "log_slave_updates = ON")
	assertNotContains(t, mustRender(t, c), "log_replica_updates")
}

func TestRenderReplica56HasNoSuperReadOnly(t *testing.T) {
	c := baseConfig()
	c.Version = "5.6.51"
	c.Role = RoleReplica
	out := mustRender(t, c)

	assertContains(t, out, "read_only = ON")
	assertNotContains(t, out, "super_read_only")
}

func TestRenderTLSConfiguresMaterialWithoutForcingSecureTransport(t *testing.T) {
	c := baseConfig()
	c.TLS = TLSPaths{CA: "/tls/ca.crt", Cert: "/tls/tls.crt", Key: "/tls/tls.key"}
	out := mustRender(t, c)

	assertContains(t, out, "ssl_ca = /tls/ca.crt")
	assertContains(t, out, "ssl_cert = /tls/tls.crt")
	assertContains(t, out, "ssl_key = /tls/tls.key")
	// require_secure_transport is the user's choice, not forced by the operator.
	assertNotContains(t, out, "require_secure_transport")
}

func TestUserCanRequireSecureTransport(t *testing.T) {
	c := baseConfig()
	c.UserParameters = map[string]string{"require_secure_transport": "ON"}
	if err := ValidateUserParameters(c.UserParameters); err != nil {
		t.Fatalf("require_secure_transport should be user-settable: %v", err)
	}
	out := mustRender(t, c)
	assertContains(t, out, "require_secure_transport = ON")
}

func TestRenderSemiSync(t *testing.T) {
	c := baseConfig()
	c.SemiSync = SemiSync{Enabled: true, WaitForReplicaCount: 1, TimeoutMillis: 5000}
	out := mustRender(t, c)

	assertContains(t, out, "rpl_semi_sync_source_enabled = 1")
	assertContains(t, out, "rpl_semi_sync_replica_enabled = 1")
	assertContains(t, out, "rpl_semi_sync_source_wait_for_replica_count = 1")
	assertContains(t, out, "rpl_semi_sync_source_timeout = 5000")
}

func TestRenderAdminInterfaceModern(t *testing.T) {
	c := baseConfig()
	c.Version = "8.0.36"
	out := mustRender(t, c)

	assertContains(t, out, "admin_address = 127.0.0.1")
	assertContains(t, out, "admin_port = 33062")
}

func TestRenderAdminInterfaceCustom(t *testing.T) {
	c := baseConfig()
	c.Version = "8.4.0"
	c.AdminAddress = "127.0.0.2"
	c.AdminPort = 40000
	out := mustRender(t, c)

	assertContains(t, out, "admin_address = 127.0.0.2")
	assertContains(t, out, "admin_port = 40000")
}

func TestRenderAdminInterfaceUnsupportedVersions(t *testing.T) {
	for _, ver := range []string{"5.6.51", "5.7.44", "8.0.13"} {
		c := baseConfig()
		c.Version = ver
		out := mustRender(t, c)
		assertNotContains(t, out, "admin_address")
		assertNotContains(t, out, "admin_port")
	}
}

func TestRenderRejectsAdminInterfaceUserParameters(t *testing.T) {
	for _, key := range []string{"admin_address", "admin-port"} {
		c := baseConfig()
		c.UserParameters = map[string]string{key: "x"}
		if _, err := c.Render(); err == nil {
			t.Errorf("expected Render() to reject managed parameter %q", key)
		}
	}
}

func TestRenderUserParametersAppended(t *testing.T) {
	c := baseConfig()
	c.UserParameters = map[string]string{
		"max_connections":         "500",
		"innodb_buffer_pool_size": "2G",
	}
	out := mustRender(t, c)

	assertContains(t, out, "max_connections = 500")
	assertContains(t, out, "innodb_buffer_pool_size = 2G")
	assertContains(t, out, "# --- user-provided ---")
}

func TestRenderRejectsManagedUserParameters(t *testing.T) {
	managedAttempts := []string{
		"server-id", "server_id", "gtid_mode", "gtid-mode",
		"log_bin", "super_read_only", "binlog_format", "SSL_CA",
	}
	for _, key := range managedAttempts {
		c := baseConfig()
		c.UserParameters = map[string]string{key: "whatever"}
		if _, err := c.Render(); err == nil {
			t.Errorf("expected Render() to reject managed parameter %q", key)
		}
	}
}

func TestRenderRejectsInvalidServerID(t *testing.T) {
	c := baseConfig()
	c.ServerID = 0
	if _, err := c.Render(); err == nil {
		t.Error("expected Render() to reject serverID 0")
	}
}

func TestRenderRejectsInvalidRoleAndVersion(t *testing.T) {
	c := baseConfig()
	c.Role = "leader"
	if _, err := c.Render(); err == nil {
		t.Error("expected Render() to reject invalid role")
	}

	c = baseConfig()
	c.Version = "not-a-version"
	if _, err := c.Render(); err == nil {
		t.Error("expected Render() to reject invalid version")
	}
}

func TestValidateUserParametersListsAllConflicts(t *testing.T) {
	err := ValidateUserParameters(map[string]string{
		"server-id":       "5",
		"gtid_mode":       "OFF",
		"max_connections": "100",
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "server-id") || !strings.Contains(err.Error(), "gtid_mode") {
		t.Errorf("error should list both conflicts, got: %v", err)
	}
	if strings.Contains(err.Error(), "max_connections") {
		t.Errorf("error should not list the valid parameter, got: %v", err)
	}
}
