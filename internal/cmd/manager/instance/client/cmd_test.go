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

package client

import (
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/rootclient"
)

const helperEnv = "CNMSQL_TEST_CLIENT_HELPER"

// TestHelperClient is the child process of TestClientExecsWithThePassword: it
// runs the command with the arguments after "--", as the pod would.
func TestHelperClient(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("helper process")
	}
	cmd := NewCommand()
	cmd.SetArgs(os.Args[slices.Index(os.Args, "--")+1:])
	if err := cmd.Execute(); err != nil {
		os.Exit(2)
	}
	os.Exit(3) // unreachable: the client replaced this process
}

// TestClientExecsWithThePassword stands a shell in for the database client: it
// must receive the password in MYSQL_PWD (replacing any inherited one), its
// arguments verbatim, and the caller's input after the password line.
func TestClientExecsWithThePassword(t *testing.T) {
	client := `printf 'pw=%s args=%s|%s\n' "$MYSQL_PWD" "$1" "$2"; cat`
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHelperClient$", "--",
		"sh", "-c", client, "fake-client", "it's; $(rm -rf /)")
	cmd.Env = append(os.Environ(), helperEnv+"=1", "MYSQL_PWD=inherited")
	cmd.Stdin = strings.NewReader("p'a ss$word\nSELECT 1;\n")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("running the command: %v (output %q)", err, out)
	}
	want := rootclient.PasswordReady + "pw=p'a ss$word args=it's; $(rm -rf /)|\nSELECT 1;\n"
	if string(out) != want {
		t.Errorf("output = %q, want %q", out, want)
	}
}
