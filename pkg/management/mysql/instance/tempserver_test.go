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

package instance

import (
	"slices"
	"testing"
)

func TestTemporaryServerArgs(t *testing.T) {
	t.Parallel()
	got := temporaryServerArgs("/etc/mysql/my.cnf", "/var/lib/mysql", "/tmp/s.sock")
	want := []string{
		"--defaults-file=/etc/mysql/my.cnf", "--datadir=/var/lib/mysql",
		"--socket=/tmp/s.sock", "--skip-networking", "--slow-query-log=OFF",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("args = %q, want %q", got, want)
	}
	if got := temporaryServerArgs("", "/d", "/s"); got[0] != "--datadir=/d" {
		t.Fatalf("args without a config file = %q", got)
	}
}
