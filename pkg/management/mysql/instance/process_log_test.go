/*
Copyright 2026 The cloudnative-mysql Authors.

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
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
)

func TestProcessLogWriterLogsCompleteLines(t *testing.T) {
	t.Parallel()
	var lines []string
	logger := funcr.NewJSON(func(obj string) {
		lines = append(lines, obj)
	}, funcr.Options{})
	w := newProcessLogWriter(logger, "stderr")

	if n, err := w.Write([]byte("first line\nsecond")); err != nil || n != len("first line\nsecond") {
		t.Fatalf("Write() = %d, %v", n, err)
	}
	if n, err := w.Write([]byte(" line\r\n")); err != nil || n != len(" line\r\n") {
		t.Fatalf("Write() = %d, %v", n, err)
	}

	if len(lines) != 2 {
		t.Fatalf("logged lines = %d, want 2: %#v", len(lines), lines)
	}
	for _, want := range []string{
		`"line":"first line"`,
		`"line":"second line"`,
		`"stream":"stderr"`,
		`"msg":"Process output"`,
	} {
		if !strings.Contains(strings.Join(lines, "\n"), want) {
			t.Fatalf("logs %q do not contain %s", lines, want)
		}
	}
}
