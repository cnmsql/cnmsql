/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
