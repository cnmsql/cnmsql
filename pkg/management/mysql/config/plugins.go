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
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Plugin is a server plugin a cluster may load through spec.mysql.plugins. The
// user names it; the operator chooses the library, so a Cluster author never
// controls a path the server loads code from.
type Plugin struct {
	// Name is what spec.mysql.plugins lists.
	Name string
	// Library is the shared object rendered as plugin_load_add.
	Library string
	// VariablePrefixes are the prefixes of the system variables the library
	// defines. They exist only while it is loaded, so the data directory
	// initialization, which loads no plugin, drops them (see
	// StripForInitialize).
	VariablePrefixes []string
}

// The supported plugins are those that only add checks, authentication methods
// or read-only tables. Left out on purpose: anything that writes to a path it
// is given (audit_log, server_audit, sql_errlog), holds key material (keyring,
// file_key_management), adds a storage engine or a listener (ha_*,
// handlersocket), authenticates against the Pod's own files (auth_pam,
// auth_socket), or that the operator loads itself (semi-sync, clone, group
// replication). Password validation plugins are left out too: they would
// reject the passwords the operator generates for its own accounts.
var (
	mysqlPlugins = []Plugin{
		{Name: "connection_control", Library: "connection_control.so", VariablePrefixes: []string{"connection_control_"}},
		{Name: "mysql_no_login", Library: "mysql_no_login.so"},
	}
	mariadbPlugins = []Plugin{
		{Name: "auth_ed25519", Library: "auth_ed25519.so"},
		{Name: "auth_mysql_sha2", Library: "auth_mysql_sha2.so", VariablePrefixes: []string{"caching_sha2_password_"}},
		{Name: "auth_parsec", Library: "auth_parsec.so"},
		{Name: "disks", Library: "disks.so"},
		{Name: "locales", Library: "locales.so"},
		{Name: "metadata_lock_info", Library: "metadata_lock_info.so"},
		{Name: "query_cache_info", Library: "query_cache_info.so"},
		{Name: "query_response_time", Library: "query_response_time.so", VariablePrefixes: []string{"query_response_time_"}},
		{Name: "type_mysql_json", Library: "type_mysql_json.so"},
	}
)

// extraPlugins are the plugins the operator's administrator added to the
// supported set with --extra-plugins. They apply to both flavors.
var extraPlugins []Plugin

var (
	extraPluginName    = regexp.MustCompile(`^[a-z0-9_]+$`)
	extraPluginLibrary = regexp.MustCompile(`^[A-Za-z0-9_]+\.so$`)
)

// operatorLoadedLibraries are the libraries the operator loads and configures
// itself. Loading one through spec.mysql.plugins would let a cluster turn on
// replication machinery the operator believes is off.
var operatorLoadedLibraries = []string{"group_replication", "mysql_clone", "semisync_"}

// SetExtraPlugins replaces the administrator-supplied additions to the
// supported plugins. Each entry is "name" (the library is then name.so) or
// "name=library.so". The library is a bare file name in the server's plugin
// directory, never a path. It is meant to be called once, at operator startup,
// before anything validates a Cluster.
func SetExtraPlugins(entries []string) error {
	extras := make([]Plugin, 0, len(entries))
	seen := map[string]struct{}{}
	for _, set := range [][]Plugin{mysqlPlugins, mariadbPlugins} {
		for _, p := range set {
			seen[p.Name] = struct{}{}
		}
	}
	for _, entry := range entries {
		name, library, hasLibrary := strings.Cut(strings.TrimSpace(entry), "=")
		if !hasLibrary {
			library = name + ".so"
		}
		if !extraPluginName.MatchString(name) {
			return fmt.Errorf("extra plugin %q: the name must be lower-case letters, digits and _", entry)
		}
		if !extraPluginLibrary.MatchString(library) {
			return fmt.Errorf("extra plugin %q: the library must be a file name ending in .so, without a path", entry)
		}
		if slices.ContainsFunc(operatorLoadedLibraries, func(prefix string) bool {
			return strings.HasPrefix(library, prefix)
		}) {
			return fmt.Errorf("extra plugin %q: %s is loaded by the operator", entry, library)
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("extra plugin %q: %s is already a supported plugin", entry, name)
		}
		seen[name] = struct{}{}
		extras = append(extras, Plugin{Name: name, Library: library})
	}
	extraPlugins = extras
	return nil
}

// SupportedPlugins returns the plugins spec.mysql.plugins accepts for the
// flavor, the built-in ones and the administrator's additions, sorted by name.
func SupportedPlugins(mariadb bool) []Plugin {
	builtin := mysqlPlugins
	if mariadb {
		builtin = mariadbPlugins
	}
	if len(extraPlugins) == 0 {
		return builtin
	}
	all := slices.Concat(builtin, extraPlugins)
	slices.SortFunc(all, func(a, b Plugin) int { return strings.Compare(a.Name, b.Name) })
	return all
}

// PluginLibraries validates the plugin names against the flavor's supported
// set and returns their libraries, sorted so that reordering the list does not
// change the rendered configuration.
func PluginLibraries(names []string, mariadb bool) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	supported := SupportedPlugins(mariadb)
	byName := make(map[string]string, len(supported))
	known := make([]string, 0, len(supported))
	for _, p := range supported {
		byName[p.Name] = p.Library
		known = append(known, p.Name)
	}
	seen := make(map[string]struct{}, len(names))
	libraries := make([]string, 0, len(names))
	var unknown, repeated []string
	for _, name := range names {
		library, ok := byName[name]
		if !ok {
			unknown = append(unknown, fmt.Sprintf("%q", name))
			continue
		}
		if _, dup := seen[name]; dup {
			repeated = append(repeated, name)
			continue
		}
		seen[name] = struct{}{}
		libraries = append(libraries, library)
	}
	var msgs []string
	if len(unknown) > 0 {
		sort.Strings(unknown)
		msgs = append(msgs, fmt.Sprintf("unsupported plugins: %s (supported: %s)",
			strings.Join(unknown, ", "), strings.Join(known, ", ")))
	}
	if len(repeated) > 0 {
		sort.Strings(repeated)
		msgs = append(msgs, "plugins listed more than once: "+strings.Join(repeated, ", "))
	}
	if len(msgs) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(msgs, "; "))
	}
	sort.Strings(libraries)
	return libraries, nil
}

// pluginVariablePrefixes returns the variable prefixes of every supported
// plugin, of either flavor, whose library is in libraries.
func pluginVariablePrefixes(libraries map[string]struct{}) []string {
	var prefixes []string
	for _, set := range [][]Plugin{mysqlPlugins, mariadbPlugins} {
		for _, p := range set {
			if _, ok := libraries[p.Library]; ok {
				prefixes = append(prefixes, p.VariablePrefixes...)
			}
		}
	}
	return prefixes
}
