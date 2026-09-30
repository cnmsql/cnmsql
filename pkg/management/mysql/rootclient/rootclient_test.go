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

package rootclient

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestReadPasswordLeavesTheRestForTheClient(t *testing.T) {
	t.Parallel()
	in := strings.NewReader("p'a ss$word\r\nSELECT 1;\n")
	var out bytes.Buffer
	got, err := ReadPassword(in, &out)
	if err != nil {
		t.Fatalf("ReadPassword() error = %v", err)
	}
	if got != "p'a ss$word" {
		t.Errorf("password = %q", got)
	}
	if out.String() != PasswordReady {
		t.Errorf("output = %q, want only the ready marker", out.String())
	}
	if rest, _ := io.ReadAll(in); string(rest) != "SELECT 1;\n" {
		t.Errorf("left for the client = %q, want the input after the password line", rest)
	}
}

func TestReadPasswordFailsWithoutALine(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"", "no newline"} {
		if _, err := ReadPassword(strings.NewReader(in), io.Discard); err == nil {
			t.Errorf("ReadPassword(%q) succeeded, want an error", in)
		}
	}
}
