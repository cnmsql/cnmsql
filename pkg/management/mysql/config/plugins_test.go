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

package config

import (
	"slices"
	"strings"
	"testing"
)

func TestPluginLibrariesResolvesAndSorts(t *testing.T) {
	got, err := PluginLibraries([]string{"mysql_no_login", "connection_control"}, false)
	if err != nil {
		t.Fatalf("PluginLibraries() unexpected error: %v", err)
	}
	want := []string{"connection_control.so", "mysql_no_login.so"}
	if !slices.Equal(got, want) {
		t.Errorf("PluginLibraries() = %v, want %v", got, want)
	}
}

func TestPluginLibrariesEmpty(t *testing.T) {
	got, err := PluginLibraries(nil, false)
	if err != nil || got != nil {
		t.Errorf("PluginLibraries(nil) = %v, %v, want nil, nil", got, err)
	}
}

func TestPluginLibrariesIsPerFlavor(t *testing.T) {
	if _, err := PluginLibraries([]string{"connection_control"}, true); err == nil {
		t.Error("expected a MySQL plugin to be refused on MariaDB")
	}
	if _, err := PluginLibraries([]string{"auth_ed25519"}, false); err == nil {
		t.Error("expected a MariaDB plugin to be refused on MySQL")
	}
	if _, err := PluginLibraries([]string{"auth_ed25519", "query_response_time"}, true); err != nil {
		t.Errorf("unexpected error for MariaDB plugins: %v", err)
	}
}

// A name is only ever looked up, never turned into a path, so nothing a user
// writes reaches plugin_load_add.
func TestPluginLibrariesRefusesAnythingNotListed(t *testing.T) {
	for _, name := range []string{
		"", "audit_log", "connection_control.so", "CONNECTION_CONTROL",
		"../../tmp/evil", "x=/tmp/evil.so", "connection_control;evil.so",
		"group_replication", "semisync_source", "validate_password",
	} {
		if _, err := PluginLibraries([]string{name}, false); err == nil {
			t.Errorf("PluginLibraries(%q) accepted an unsupported plugin", name)
		}
	}
}

func TestPluginLibrariesErrorNamesEveryProblem(t *testing.T) {
	_, err := PluginLibraries([]string{"mysql_no_login", "nope", "mysql_no_login", "audit_log"}, false)
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{
		`"audit_log"`, `"nope"`, "supported: connection_control, mysql_no_login", "more than once: mysql_no_login",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestSupportedPluginsAreSortedAndDistinct(t *testing.T) {
	for _, mariadb := range []bool{false, true} {
		plugins := SupportedPlugins(mariadb)
		if !slices.IsSortedFunc(plugins, func(a, b Plugin) int { return strings.Compare(a.Name, b.Name) }) {
			t.Errorf("SupportedPlugins(%v) is not sorted by name", mariadb)
		}
		seen := map[string]bool{}
		for _, p := range plugins {
			if seen[p.Name] || seen[p.Library] {
				t.Errorf("SupportedPlugins(%v) repeats %s", mariadb, p.Name)
			}
			seen[p.Name], seen[p.Library] = true, true
			if p.Library != p.Name+".so" && !strings.HasSuffix(p.Library, ".so") {
				t.Errorf("plugin %s has library %q", p.Name, p.Library)
			}
			if strings.ContainsAny(p.Library, "/ =;") {
				t.Errorf("plugin %s has an unsafe library name %q", p.Name, p.Library)
			}
		}
	}
}

func TestRenderPluginLibraries(t *testing.T) {
	c := baseConfig()
	c.PluginLibraries = []string{"connection_control.so", "mysql_no_login.so"}
	c.UserParameters = map[string]string{"connection_control_failed_connections_threshold": "5"}
	out := mustRender(t, c)
	assertContains(t, out, "plugin_load_add = connection_control.so\n")
	assertContains(t, out, "plugin_load_add = mysql_no_login.so\n")
	if strings.Index(out, "plugin_load_add") > strings.Index(out, "# --- user-provided ---") {
		t.Errorf("plugins must be rendered in the operator-managed section\n---\n%s", out)
	}
}

func TestRenderWithoutPluginsIsUnchanged(t *testing.T) {
	assertNotContains(t, mustRender(t, baseConfig()), "plugin_load_add")
}

// The user still cannot load a library through parameters.
func TestPluginLoadingStaysDeniedInParameters(t *testing.T) {
	for _, key := range []string{
		"plugin_load", "plugin_load_add", "plugin-load-add", "loose-plugin_load_add", "plugin_dir",
	} {
		if err := ValidateUserParameters(map[string]string{key: "connection_control.so"}); err == nil {
			t.Errorf("ValidateUserParameters accepted %s", key)
		}
	}
}

func TestStripForInitializeDropsPluginsAndTheirVariables(t *testing.T) {
	c := baseConfig()
	c.PluginLibraries = []string{"connection_control.so"}
	c.UserParameters = map[string]string{
		"connection_control_failed_connections_threshold": "5",
		"loose-connection-control-min-connection-delay":   "2000",
		"max_connections": "200",
	}
	out := StripForInitialize(mustRender(t, c))
	assertNotContains(t, out, "plugin_load_add")
	assertNotContains(t, out, "connection_control")
	assertNotContains(t, out, "connection-control")
	assertContains(t, out, "max_connections = 200\n")
	assertContains(t, out, "[mysqld]\n")
	assertContains(t, out, "server-id = 1\n")
}

// A variable is only dropped when its plugin is loaded: without the plugin it
// is the user's mistake, and hiding it would move the failure to the first
// real start.
func TestStripForInitializeKeepsVariablesOfPluginsNotLoaded(t *testing.T) {
	c := baseConfig()
	c.UserParameters = map[string]string{"connection_control_failed_connections_threshold": "5"}
	out := StripForInitialize(mustRender(t, c))
	assertContains(t, out, "connection_control_failed_connections_threshold = 5\n")
}

func TestStripForInitializeStillStripsGroupReplication(t *testing.T) {
	in := "[mysqld]\nserver-id = 1\nplugin_load_add = group_replication.so\ngroup_replication_group_name = x\n"
	out := StripForInitialize(in)
	assertNotContains(t, out, "group_replication")
	assertContains(t, out, "server-id = 1\n")
}

func TestSetExtraPluginsExtendsBothFlavors(t *testing.T) {
	t.Cleanup(func() { _ = SetExtraPlugins(nil) })
	if err := SetExtraPlugins([]string{"audit_log", "password_check=simple_password_check.so"}); err != nil {
		t.Fatalf("SetExtraPlugins() unexpected error: %v", err)
	}
	got, err := PluginLibraries([]string{"audit_log", "connection_control"}, false)
	if err != nil {
		t.Fatalf("PluginLibraries() unexpected error: %v", err)
	}
	if want := []string{"audit_log.so", "connection_control.so"}; !slices.Equal(got, want) {
		t.Errorf("PluginLibraries() = %v, want %v", got, want)
	}
	got, err = PluginLibraries([]string{"password_check"}, true)
	if err != nil || !slices.Equal(got, []string{"simple_password_check.so"}) {
		t.Errorf("PluginLibraries() = %v, %v", got, err)
	}
	plugins := SupportedPlugins(false)
	if !slices.IsSortedFunc(plugins, func(a, b Plugin) int { return strings.Compare(a.Name, b.Name) }) {
		t.Errorf("SupportedPlugins() with extras is not sorted: %v", plugins)
	}
	if len(mysqlPlugins) != 2 {
		t.Errorf("SetExtraPlugins changed the built-in set: %v", mysqlPlugins)
	}

	if err := SetExtraPlugins(nil); err != nil {
		t.Fatalf("SetExtraPlugins(nil) unexpected error: %v", err)
	}
	if _, err := PluginLibraries([]string{"audit_log"}, false); err == nil {
		t.Error("extras survived being cleared")
	}
}

func TestSetExtraPluginsRefusesUnsafeEntries(t *testing.T) {
	t.Cleanup(func() { _ = SetExtraPlugins(nil) })
	for _, entry := range []string{
		"", "Audit_Log", "audit-log", "audit_log=", "audit_log=/tmp/evil.so", "audit_log=../evil.so",
		"audit_log=evil", "audit_log=evil.so;other.so", "audit_log=a=b.so",
		"connection_control", "auth_ed25519=other.so",
		"group_replication", "clone=mysql_clone.so", "semi=semisync_source.so",
	} {
		if err := SetExtraPlugins([]string{entry}); err == nil {
			t.Errorf("SetExtraPlugins(%q) accepted an unsafe entry", entry)
		}
	}
	if err := SetExtraPlugins([]string{"audit_log", "audit_log=other.so"}); err == nil {
		t.Error("SetExtraPlugins accepted the same name twice")
	}
	if len(extraPlugins) != 0 {
		t.Errorf("a refused list left extras behind: %v", extraPlugins)
	}
}
