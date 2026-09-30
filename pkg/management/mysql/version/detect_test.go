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
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseBanner(t *testing.T) {
	cases := []struct {
		name    string
		out     string
		version string
		mariadb bool
	}{
		{
			"percona 8.4",
			"/usr/sbin/mysqld  Ver 8.4.11-11 for Linux on x86_64 (Percona Server (GPL), Release '11')\n",
			"8.4.11", false,
		},
		{
			"percona 8.0",
			"/usr/sbin/mysqld  Ver 8.0.46-37 for Linux on aarch64 (Percona Server (GPL), Release '37')",
			"8.0.46", false,
		},
		{
			"percona 9.7",
			"/usr/sbin/mysqld  Ver 9.7.2-2 for Linux on x86_64 (Percona Server (GPL), Release '2')",
			"9.7.2", false,
		},
		{
			"oracle mysql",
			"/usr/sbin/mysqld  Ver 8.4.6 for Linux on x86_64 (MySQL Community Server - GPL)",
			"8.4.6", false,
		},
		{
			"mariadb",
			"mariadbd  Ver 11.4.13-MariaDB-deb12 for debian-linux-gnu on x86_64 (mariadb.org binary distribution)",
			"11.4.13", true,
		},
		{
			"mariadb via mysqld compat name",
			"mysqld  Ver 10.11.19-MariaDB-deb12 for debian-linux-gnu on x86_64 (mariadb.org binary distribution)",
			"10.11.19", true,
		},
		{
			"warning before banner is ignored",
			"/usr/sbin/mysqld  Ver 8.4.11-11 for Linux on x86_64 (Percona Server (GPL))\n2026-09-29T00:00:00Z 0 [Warning] x",
			"8.4.11", false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseBanner(tc.out)
			if err != nil {
				t.Fatalf("ParseBanner: %v", err)
			}
			if got.Version != tc.version || got.MariaDB != tc.mariadb {
				t.Errorf("ParseBanner = %+v, want version %s mariadb %t", got, tc.version, tc.mariadb)
			}
			if _, err := Parse(got.Version); err != nil {
				t.Errorf("Parse(%q): %v", got.Version, err)
			}
		})
	}

	for _, out := range []string{"", "mysqld: unknown option", "mysqld  Ver unknown"} {
		if _, err := ParseBanner(out); err == nil {
			t.Errorf("ParseBanner(%q): expected an error", out)
		}
	}
}

func TestResolve(t *testing.T) {
	ctx := context.Background()

	if got, err := Resolve(ctx, "8.0.36", "/does/not/exist"); err != nil || got != "8.0.36" {
		t.Errorf("Resolve with override = %q, %v; want 8.0.36 without running the binary", got, err)
	}

	fake := filepath.Join(t.TempDir(), "mysqld")
	script := "#!/bin/sh\necho \"$0  Ver 8.4.11-11 for Linux on x86_64 (Percona Server (GPL))\"\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if got, err := Resolve(ctx, "", fake); err != nil || got != "8.4.11" {
		t.Errorf("Resolve from binary = %q, %v; want 8.4.11", got, err)
	}

	if _, err := Resolve(ctx, "", "/does/not/exist"); err == nil {
		t.Error("Resolve with a missing binary: expected an error")
	}
}

// writeServer puts a fake server binary named name in dir that prints out.
func writeServer(t *testing.T, dir, name, out string) {
	t.Helper()
	script := "#!/bin/sh\nprintf '%s\\n' \"" + out + "\"\n"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

// MariaDB 11.x warns on the mysqld compat name, ahead of the banner. Detect runs
// mariadbd itself when the image has it.
func TestDetectPrefersMariadbd(t *testing.T) {
	dir := t.TempDir()
	writeServer(t, dir, "mysqld", "mysqld: Deprecated program name. It will be removed in a future release")
	writeServer(t, dir, "mariadbd", "mariadbd  Ver 11.4.13-MariaDB-deb12 for debian-linux-gnu on x86_64")
	t.Setenv("PATH", dir)

	got, err := Detect(context.Background(), "mysqld")
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if got.Version != "11.4.13" || !got.MariaDB {
		t.Errorf("Detect = %+v, want MariaDB 11.4.13", got)
	}
}

// Percona images have no mariadbd, and an explicit path is always run as given.
func TestDetectKeepsMysqld(t *testing.T) {
	dir := t.TempDir()
	writeServer(t, dir, "mysqld", "mysqld  Ver 8.4.11-11 for Linux on x86_64 (Percona Server (GPL))")
	t.Setenv("PATH", dir)

	if got, err := Detect(context.Background(), "mysqld"); err != nil || got.Version != "8.4.11" || got.MariaDB {
		t.Errorf("Detect without mariadbd = %+v, %v; want Percona 8.4.11", got, err)
	}

	writeServer(t, dir, "mariadbd", "mariadbd  Ver 11.4.13-MariaDB-deb12 for debian-linux-gnu on x86_64")
	if got, err := Detect(context.Background(), filepath.Join(dir, "mysqld")); err != nil || got.Version != "8.4.11" {
		t.Errorf("Detect with an explicit path = %+v, %v; want the binary at that path", got, err)
	}
}
