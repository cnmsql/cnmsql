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

import "testing"

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
