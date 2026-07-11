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

const (
	uuid1 = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuid2 = "8a6a1e2b-0000-0000-0000-000000000abc"
)

// TestGTIDCompare runs the same logical scenarios (equal / ahead / behind /
// diverged / empty) against both engines with flavor-appropriate strings. These
// are the four relationships failover candidate selection and diverged-instance
// detection depend on.
func TestGTIDCompare(t *testing.T) {
	tests := []struct {
		name  string
		model GTIDModel
		a, b  string
		want  Ordering
	}{
		// --- MySQL: UUID-keyed interval sets ---
		{"mysql equal", mysqlGTID{}, uuid1 + ":1-10", uuid1 + ":1-10", OrderingEqual},
		{"mysql equal both empty", mysqlGTID{}, "", "", OrderingEqual},
		{"mysql ahead by range", mysqlGTID{}, uuid1 + ":1-12", uuid1 + ":1-10", OrderingAhead},
		{"mysql behind by range", mysqlGTID{}, uuid1 + ":1-8", uuid1 + ":1-10", OrderingBehind},
		{"mysql ahead of empty", mysqlGTID{}, uuid1 + ":1-5", "", OrderingAhead},
		{"mysql behind empty", mysqlGTID{}, "", uuid1 + ":1-5", OrderingBehind},
		// Errant transaction: a has an interval on uuid2 the primary (b) lacks,
		// while b is otherwise ahead on uuid1 -> neither contains the other.
		{"mysql diverged errant", mysqlGTID{}, uuid1 + ":1-5," + uuid2 + ":1-2", uuid1 + ":1-10", OrderingDiverged},

		// --- MariaDB: domain-server-seq triples ---
		{"mariadb equal", mariadbGTID{}, "0-1-100", "0-1-100", OrderingEqual},
		{"mariadb equal both empty", mariadbGTID{}, "", "", OrderingEqual},
		// Same domain+seq, different server-id => same history (MariaDB semantics).
		{"mariadb equal diff server", mariadbGTID{}, "0-7-100", "0-1-100", OrderingEqual},
		{"mariadb ahead by seq", mariadbGTID{}, "0-1-120", "0-1-100", OrderingAhead},
		{"mariadb behind by seq", mariadbGTID{}, "0-1-80", "0-1-100", OrderingBehind},
		{"mariadb ahead extra domain", mariadbGTID{}, "0-1-100,1-1-5", "0-1-100", OrderingAhead},
		{"mariadb ahead of empty", mariadbGTID{}, "0-1-1", "", OrderingAhead},
		{"mariadb behind empty", mariadbGTID{}, "", "0-1-1", OrderingBehind},
		// Errant transaction: a has domain 1 the primary (b) lacks, b is ahead
		// on domain 0 -> neither contains the other.
		{"mariadb diverged errant", mariadbGTID{}, "0-1-50,1-2-3", "0-1-100", OrderingDiverged},
		// Both sides advanced a different domain the other lacks.
		{"mariadb diverged split domains", mariadbGTID{}, "0-1-100,1-1-5", "0-1-100,2-1-5", OrderingDiverged},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.model.Compare(tc.a, tc.b)
			if err != nil {
				t.Fatalf("Compare(%q, %q) error: %v", tc.a, tc.b, err)
			}
			if got != tc.want {
				t.Errorf("Compare(%q, %q) = %s, want %s", tc.a, tc.b, got, tc.want)
			}

			// Compare must be the mirror of itself: swapping ahead<->behind.
			rev, err := tc.model.Compare(tc.b, tc.a)
			if err != nil {
				t.Fatalf("Compare(%q, %q) error: %v", tc.b, tc.a, err)
			}
			if want := mirror(tc.want); rev != want {
				t.Errorf("Compare(%q, %q) = %s, want mirror %s", tc.b, tc.a, rev, want)
			}
		})
	}
}

func mirror(o Ordering) Ordering {
	switch o {
	case OrderingAhead:
		return OrderingBehind
	case OrderingBehind:
		return OrderingAhead
	default:
		return o
	}
}

func TestGTIDContainsAndEqual(t *testing.T) {
	tests := []struct {
		name         string
		model        GTIDModel
		super, sub   string
		wantContains bool
		wantEqual    bool
	}{
		{"mysql superset", mysqlGTID{}, uuid1 + ":1-10", uuid1 + ":1-5", true, false},
		{"mysql equal", mysqlGTID{}, uuid1 + ":1-10", uuid1 + ":1-10", true, true},
		{"mysql not superset", mysqlGTID{}, uuid1 + ":1-5", uuid1 + ":1-10", false, false},
		{"mysql multi-uuid superset", mysqlGTID{}, uuid1 + ":1-10," + uuid2 + ":1-3", uuid1 + ":1-5", true, false},
		{"mariadb superset", mariadbGTID{}, "0-1-100", "0-1-50", true, false},
		{"mariadb equal", mariadbGTID{}, "0-1-100", "0-1-100", true, true},
		{"mariadb not superset missing domain", mariadbGTID{}, "0-1-100", "0-1-100,1-1-1", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotContains, err := tc.model.Contains(tc.super, tc.sub)
			if err != nil {
				t.Fatalf("Contains error: %v", err)
			}
			if gotContains != tc.wantContains {
				t.Errorf("Contains(%q, %q) = %v, want %v", tc.super, tc.sub, gotContains, tc.wantContains)
			}
			gotEqual, err := tc.model.Equal(tc.super, tc.sub)
			if err != nil {
				t.Fatalf("Equal error: %v", err)
			}
			if gotEqual != tc.wantEqual {
				t.Errorf("Equal(%q, %q) = %v, want %v", tc.super, tc.sub, gotEqual, tc.wantEqual)
			}
		})
	}
}

func TestGTIDIsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name  string
		model GTIDModel
		raw   string
		want  bool
	}{
		{"mysql empty", mysqlGTID{}, "", true},
		{"mysql nonempty", mysqlGTID{}, uuid1 + ":1", false},
		{"mariadb empty", mariadbGTID{}, "", true},
		{"mariadb whitespace only", mariadbGTID{}, "  \n ", true},
		{"mariadb nonempty", mariadbGTID{}, "0-1-1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.model.IsEmpty(tc.raw)
			if err != nil {
				t.Fatalf("IsEmpty error: %v", err)
			}
			if got != tc.want {
				t.Errorf("IsEmpty(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestGTIDCanonical(t *testing.T) {
	for _, tc := range []struct {
		name  string
		model GTIDModel
		raw   string
		want  string
	}{
		// Domains rendered in ascending order regardless of input order.
		{"mariadb reorders domains", mariadbGTID{}, "2-1-5,0-1-100,1-3-7", "0-1-100,1-3-7,2-1-5"},
		{"mariadb trims whitespace", mariadbGTID{}, " 0-1-100 ", "0-1-100"},
		{"mariadb empty", mariadbGTID{}, "", ""},
		// MySQL canonicalization lower-cases and coalesces via the shared parser.
		{"mysql canonical", mysqlGTID{}, uuid1 + ":1-3:4-5", uuid1 + ":1-5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.model.Canonical(tc.raw)
			if err != nil {
				t.Fatalf("Canonical error: %v", err)
			}
			if got != tc.want {
				t.Errorf("Canonical(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestMariaDBGTIDParseErrors(t *testing.T) {
	m := mariadbGTID{}
	for _, raw := range []string{
		"0-1",      // too few components
		"0-1-2-3",  // too many components
		"a-1-100",  // non-numeric domain
		"0-b-100",  // non-numeric server
		"0-1-x",    // non-numeric sequence
		"-1-1-100", // negative domain
	} {
		if _, err := m.IsEmpty(raw); err == nil {
			t.Errorf("parse %q: expected error, got nil", raw)
		}
	}
}

// TestMariaDBDuplicateDomainKeepsHighestSeq documents the defensive rule that a
// domain appearing twice collapses to its highest sequence number.
func TestMariaDBDuplicateDomainKeepsHighestSeq(t *testing.T) {
	got, err := mariadbGTID{}.Canonical("0-1-50,0-2-90,0-1-70")
	if err != nil {
		t.Fatalf("Canonical error: %v", err)
	}
	if got != "0-2-90" {
		t.Errorf("Canonical = %q, want %q", got, "0-2-90")
	}
}

func TestGTIDMissingCount(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		model GTIDModel
		have  string
		want  string
		count int64
	}{
		{"mysql: contiguous gap", mysqlGTID{}, "uuid:1-40", "uuid:1-100", 60},
		{"mysql: nothing missing", mysqlGTID{}, "uuid:1-100", "uuid:1-40", 0},
		{"mysql: empty holds nothing", mysqlGTID{}, "", "uuid:1-5", 5},
		{"mysql: split intervals", mysqlGTID{}, "uuid:1-10:21-30", "uuid:1-30", 10},
		{"mysql: gap spans two sources", mysqlGTID{}, "a:1-5", "a:1-10,b:1-3", 8},
		{"mysql: source absent entirely", mysqlGTID{}, "a:1-5", "b:1-7", 7},
		{"mariadb: sequence gap", mariadbGTID{}, "0-1-40", "0-1-100", 60},
		{"mariadb: nothing missing", mariadbGTID{}, "0-1-100", "0-1-40", 0},
		{"mariadb: domain absent", mariadbGTID{}, "0-1-10", "0-1-10,1-2-7", 7},
		{"mariadb: server id is not a transaction", mariadbGTID{}, "0-1-50", "0-9-50", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.model.MissingCount(tc.have, tc.want)
			if err != nil {
				t.Fatalf("MissingCount error: %v", err)
			}
			if got != tc.count {
				t.Errorf("MissingCount(%q, %q) = %d, want %d", tc.have, tc.want, got, tc.count)
			}
		})
	}
}

func TestGTIDUnion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		model GTIDModel
		sets  []string
		want  string
	}{
		{"mysql: merges adjacent", mysqlGTID{}, []string{"uuid:1-40", "uuid:41-100"}, "uuid:1-100"},
		{"mysql: keeps both sources", mysqlGTID{}, []string{"a:1-5", "b:1-3"}, "a:1-5,b:1-3"},
		{"mysql: empty is identity", mysqlGTID{}, []string{"", "a:1-5"}, "a:1-5"},
		{"mariadb: highest sequence wins", mariadbGTID{}, []string{"0-1-40", "0-1-100"}, "0-1-100"},
		{"mariadb: keeps both domains", mariadbGTID{}, []string{"0-1-40", "1-2-7"}, "0-1-40,1-2-7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := tc.model.Union(tc.sets...)
			if err != nil {
				t.Fatalf("Union error: %v", err)
			}
			if got != tc.want {
				t.Errorf("Union(%q) = %q, want %q", tc.sets, got, tc.want)
			}
		})
	}
}
