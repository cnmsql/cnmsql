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

package version

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// Server is what a server binary reports about itself.
type Server struct {
	// Version is the major.minor.patch release, e.g. "8.4.11".
	Version string
	// MariaDB is true for a MariaDB server, false for MySQL / Percona Server.
	MariaDB bool
	// Banner is the first line of `--version`, kept for diagnostics.
	Banner string
}

var versionBanner = regexp.MustCompile(`\bVer\s+(\d+\.\d+\.\d+)\S*`)

// ParseBanner extracts the server release from `mysqld --version` output:
//
//	/usr/sbin/mysqld  Ver 8.4.11-11 for Linux on x86_64 (Percona Server (GPL), Release '11', ...)
//	mariadbd  Ver 11.4.13-MariaDB-deb12 for debian-linux-gnu on x86_64 (mariadb.org binary distribution)
func ParseBanner(out string) (Server, error) {
	banner, _, _ := strings.Cut(strings.TrimSpace(out), "\n")
	m := versionBanner.FindStringSubmatch(banner)
	if m == nil {
		return Server{}, fmt.Errorf("no server version in %q", banner)
	}
	return Server{
		Version: m[1],
		MariaDB: strings.Contains(banner, "MariaDB"),
		Banner:  banner,
	}, nil
}

// Detect runs `<mysqld> --version` and returns what the binary reports. The
// binary is the authority on its own version: image tags and catalog entries
// only name it.
//
// Callers that do not know the flavor yet pass mysqld, either the bare name or
// a path such as /usr/sbin/mysqld. When the image also has mariadbd there, Detect
// runs that instead: MariaDB 11.x keeps mysqld only as a compat name and prints
// a deprecation warning ahead of the banner.
func Detect(ctx context.Context, mysqld string) (Server, error) {
	mysqld = preferMariadbd(mysqld)
	out, err := exec.CommandContext(ctx, mysqld, "--version").CombinedOutput()
	if err != nil {
		return Server{}, fmt.Errorf("running %s --version: %w: %s", mysqld, err, bytes.TrimSpace(out))
	}
	return ParseBanner(string(out))
}

// preferMariadbd returns the mariadbd that sits where mysqld would be found (on
// PATH for the bare name, in the same directory for a path), or mysqld itself
// when there is none or the binary is not named mysqld.
func preferMariadbd(mysqld string) string {
	if filepath.Base(mysqld) != "mysqld" {
		return mysqld
	}
	candidate := "mariadbd"
	if mysqld != "mysqld" {
		candidate = filepath.Join(filepath.Dir(mysqld), "mariadbd")
	}
	if mariadbd, err := exec.LookPath(candidate); err == nil {
		return mariadbd
	}
	return mysqld
}

// Resolve returns override when it is set, else the version the mysqld binary
// reports. The override exists for tests and for Pods created before the
// operator stopped passing --server-version.
func Resolve(ctx context.Context, override, mysqld string) (string, error) {
	if override != "" {
		return override, nil
	}
	server, err := Detect(ctx, mysqld)
	if err != nil {
		return "", fmt.Errorf("detecting the server version: %w", err)
	}
	return server.Version, nil
}
