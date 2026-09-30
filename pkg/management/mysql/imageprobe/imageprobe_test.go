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

package imageprobe

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func fakeServer(t *testing.T, banner string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mysqld")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho '"+banner+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunAndDecode(t *testing.T) {
	cases := []struct {
		banner, flavor, version string
	}{
		{"/usr/sbin/mysqld  Ver 8.4.11-11 for Linux on x86_64 (Percona Server (GPL))", FlavorMySQL, "8.4.11"},
		{"mysqld  Ver 11.4.13-MariaDB-deb12 for debian-linux-gnu on x86_64", FlavorMariaDB, "11.4.13"},
	}
	for _, tc := range cases {
		got, err := Run(context.Background(), fakeServer(t, tc.banner))
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		data, err := Encode(got)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(string(data))
		if err != nil {
			t.Fatalf("Decode(%s): %v", data, err)
		}
		if decoded.Flavor != tc.flavor || decoded.ServerVersion != tc.version || decoded.Banner != tc.banner {
			t.Errorf("round trip = %+v, want %s %s", decoded, tc.flavor, tc.version)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, msg := range []string{
		"",
		"exec: mysqld: not found",
		`{"flavor":"postgres","serverVersion":"16.4"}`,
		`{"flavor":"mysql","serverVersion":"latest"}`,
	} {
		if _, err := Decode(msg); err == nil {
			t.Errorf("Decode(%q): expected an error", msg)
		}
	}
}
