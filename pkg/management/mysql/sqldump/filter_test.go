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

package sqldump

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

// The fixtures are real dumps, taken with the operator's dump arguments, of
// four databases: billing (a routine and a trigger), shop (a view, and a value
// holding a marker line), we`ird (a backtick in its name) and ünï (a view).
var fixtures = []string{"percona80.sql", "percona84.sql", "mariadb114.sql"}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func filter(t *testing.T, dump []byte, selected ...string) (string, []string) {
	t.Helper()
	var out bytes.Buffer
	kept, err := FilterDatabases(&out, bytes.NewReader(dump), selected)
	if err != nil {
		t.Fatalf("FilterDatabases(%q): %v", selected, err)
	}
	return out.String(), kept
}

// useLines returns the USE statements in a dump, one per section.
func useLines(dump string) []string {
	var uses []string
	for line := range strings.Lines(dump) {
		if strings.HasPrefix(line, "USE ") {
			uses = append(uses, strings.TrimSpace(line))
		}
	}
	return uses
}

func TestFilterDatabasesEmptySelectionCopiesEverything(t *testing.T) {
	for _, name := range fixtures {
		dump := readFixture(t, name)
		out, kept := filter(t, dump)
		if out != string(dump) || kept != nil {
			t.Errorf("%s: an empty selection must copy the dump unchanged", name)
		}
	}
}

func TestFilterDatabasesKeepsSelectedSections(t *testing.T) {
	for _, name := range fixtures {
		t.Run(name, func(t *testing.T) {
			dump := readFixture(t, name)
			header := string(dump[:bytes.Index(dump, []byte(markerPrefix))])

			for _, tc := range []struct {
				selected []string
				wantKept []string
				wantUses []string
			}{
				{
					// shop comes back twice: its tables, then its view in the
					// second pass.
					selected: []string{"shop"},
					wantKept: []string{"shop"},
					wantUses: []string{"USE `shop`;", "USE `shop`;"},
				},
				{
					selected: []string{"we`ird", "billing"},
					wantKept: []string{"billing", "we`ird"},
					wantUses: []string{"USE `billing`;", "USE `we``ird`;", "USE `billing`;", "USE `we``ird`;"},
				},
				{
					selected: []string{"ünï"},
					wantKept: []string{"ünï"},
					wantUses: []string{"USE `ünï`;", "USE `ünï`;"},
				},
				{
					selected: []string{"absent"},
					wantKept: nil,
					wantUses: nil,
				},
			} {
				out, kept := filter(t, dump, tc.selected...)
				if !slices.Equal(kept, tc.wantKept) {
					t.Errorf("%q: kept %q, want %q", tc.selected, kept, tc.wantKept)
				}
				if uses := useLines(out); !slices.Equal(uses, tc.wantUses) {
					t.Errorf("%q: USE statements %q, want %q", tc.selected, uses, tc.wantUses)
				}
				if !strings.HasPrefix(out, header) {
					t.Errorf("%q: the header must always be kept", tc.selected)
				}
			}
		})
	}
}

// A value holding a marker line stays in its section: the client escapes the
// newline, so the marker text never starts a line.
func TestFilterDatabasesIgnoresMarkerTextInsideValues(t *testing.T) {
	for _, name := range fixtures {
		dump := readFixture(t, name)
		out, _ := filter(t, dump, "shop")
		if !strings.Contains(out, `'line one\n-- Current Database: `+"`billing`"+`\nline three'`) {
			t.Errorf("%s: the shop row holding a marker was not kept", name)
		}
		if strings.Contains(out, "INSERT INTO `invoices`") {
			t.Errorf("%s: billing rows leaked into a shop-only load", name)
		}
	}
}

// Lines longer than the read buffer are streamed in pieces. Marker text that
// lands at the start of a piece is still inside a line, so it is not a marker.
func TestFilterDatabasesLongLines(t *testing.T) {
	// A full buffer holds the line's first readBufferBytes bytes, so the
	// second piece of this line starts exactly with the marker text.
	long := strings.Repeat("x", readBufferBytes-len("INSERT INTO t VALUES ('"))
	dump := "-- header\n" +
		"-- Current Database: `keep`\n" +
		"INSERT INTO t VALUES ('" + long + "-- Current Database: `drop`\n" +
		"after long line\n" +
		"-- Current Database: `drop`\n" +
		"INSERT INTO t VALUES ('" + long + "');\n" +
		"-- Current Database: `keep`\n" +
		"view pass"
	// HalfReader makes every read short, so the filter's buffering is
	// exercised the hard way too.
	var out bytes.Buffer
	kept, err := FilterDatabases(&out, iotest.HalfReader(strings.NewReader(dump)), []string{"keep"})
	if err != nil {
		t.Fatal(err)
	}
	want := "-- header\n" +
		"-- Current Database: `keep`\n" +
		"INSERT INTO t VALUES ('" + long + "-- Current Database: `drop`\n" +
		"after long line\n" +
		"-- Current Database: `keep`\n" +
		"view pass"
	if out.String() != want {
		t.Errorf("long-line filter output differs (got %d bytes, want %d)", out.Len(), len(want))
	}
	if !slices.Equal(kept, []string{"keep"}) {
		t.Errorf("kept %q, want [keep]", kept)
	}
}

func TestFilterDatabasesMalformedMarker(t *testing.T) {
	for _, dump := range []string{
		"-- Current Database: `unterminated\n",
		"-- Current Database: `a`b`\n",
		"-- Current Database: ``\n",
		"-- Current Database: `" + strings.Repeat("a", maxMarkerLine) + "`\n",
	} {
		_, err := FilterDatabases(&bytes.Buffer{}, strings.NewReader(dump), []string{"a"})
		if err == nil {
			t.Errorf("%.40q: want an error", dump)
		}
	}
}

func TestFilterDatabasesMarkerAtEOF(t *testing.T) {
	var out bytes.Buffer
	kept, err := FilterDatabases(&out, strings.NewReader("h\n-- Current Database: `a`"), []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "h\n-- Current Database: `a`" || !slices.Equal(kept, []string{"a"}) {
		t.Errorf("got %q, kept %q", out.String(), kept)
	}
}

func TestFilterDatabasesWriteError(t *testing.T) {
	_, err := FilterDatabases(errWriter{}, strings.NewReader("header\n"), []string{"a"})
	if err == nil {
		t.Fatal("want the writer's error")
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, os.ErrClosed }
