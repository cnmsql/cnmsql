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

package engine

import (
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/version"
)

func TestForFlavor(t *testing.T) {
	tests := []struct {
		name          string
		flavor        Flavor
		wantFlavor    Flavor
		wantErr       bool
		superReadOnly bool
		supportsGR    bool
	}{
		{name: "mysql", flavor: FlavorMySQL, wantFlavor: FlavorMySQL, superReadOnly: true, supportsGR: true},
		{name: "mariadb", flavor: FlavorMariaDB, wantFlavor: FlavorMariaDB, superReadOnly: false, supportsGR: false},
		{name: "empty defaults to mysql", flavor: "", wantFlavor: FlavorMySQL, superReadOnly: true, supportsGR: true},
		{name: "unknown errors", flavor: "postgres", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, err := ForFlavor(tc.flavor)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ForFlavor(%q) = nil error, want error", tc.flavor)
				}
				return
			}
			if err != nil {
				t.Fatalf("ForFlavor(%q) unexpected error: %v", tc.flavor, err)
			}
			if e.Flavor() != tc.wantFlavor {
				t.Errorf("Flavor() = %q, want %q", e.Flavor(), tc.wantFlavor)
			}
			if e.HasSuperReadOnly() != tc.superReadOnly {
				t.Errorf("HasSuperReadOnly() = %v, want %v", e.HasSuperReadOnly(), tc.superReadOnly)
			}
			if e.SupportsGroupReplication() != tc.supportsGR {
				t.Errorf("SupportsGroupReplication() = %v, want %v", e.SupportsGroupReplication(), tc.supportsGR)
			}
			if e.GTID() == nil {
				t.Error("GTID() = nil")
			}
		})
	}
}

func TestSeriesFlavor(t *testing.T) {
	tests := []struct {
		series     string
		wantFlavor Flavor
		wantOK     bool
	}{
		{series: "8.0", wantFlavor: FlavorMySQL, wantOK: true},
		{series: "8.4", wantFlavor: FlavorMySQL, wantOK: true},
		{series: "9.7", wantFlavor: FlavorMySQL, wantOK: true},
		// Innovation series outside the chain still belong to MySQL.
		{series: "9.6", wantFlavor: FlavorMySQL, wantOK: true},
		{series: "10.11", wantFlavor: FlavorMariaDB, wantOK: true},
		{series: "11.8", wantFlavor: FlavorMariaDB, wantOK: true},
		{series: "12.3", wantFlavor: FlavorMariaDB, wantOK: true},
		// MySQL calendar versions (YY.M) are unclaimed until a calendar series
		// joins the chain; they must not be read as MariaDB.
		{series: "26.7", wantOK: false},
		{series: "5.7", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.series, func(t *testing.T) {
			v, err := version.Parse(tc.series)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := SeriesFlavor(v.Series())
			if ok != tc.wantOK || got != tc.wantFlavor {
				t.Errorf("SeriesFlavor(%s) = (%q, %v), want (%q, %v)", tc.series, got, ok, tc.wantFlavor, tc.wantOK)
			}
		})
	}
}

func TestMustForFlavorPanicsOnUnknown(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustForFlavor did not panic on unknown flavor")
		}
	}()
	MustForFlavor("nope")
}

func TestOrderingString(t *testing.T) {
	for o, want := range map[Ordering]string{
		OrderingEqual:    "equal",
		OrderingAhead:    "ahead",
		OrderingBehind:   "behind",
		OrderingDiverged: "diverged",
		Ordering(99):     "unknown",
	} {
		if got := o.String(); got != want {
			t.Errorf("Ordering(%d).String() = %q, want %q", o, got, want)
		}
	}
}
