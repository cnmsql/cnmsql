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

package replication

import "testing"

func TestGTIDSetHoles(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, set, want string
	}{
		{"empty", "", ""},
		{"one interval", diffA + ":1-10", ""},
		{"single transaction", diffA + ":7", ""},
		{"one hole", diffA + ":1-3:7-10", diffA + ":4-6"},
		{"several holes", diffA + ":1:3-4:7-18", diffA + ":2:5-6"},
		{"per uuid", diffA + ":1-3:5," + diffB + ":2-4:9", diffA + ":4," + diffB + ":5-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := HolesGTIDString(tc.set)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("Holes(%q) = %q, want %q", tc.set, got, tc.want)
			}
		})
	}
}

func TestGTIDSetIntersect(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, a, b, want string
	}{
		{"empty", "", diffA + ":1-5", ""},
		{"disjoint sources", diffA + ":1-5", diffB + ":1-5", ""},
		{"overlap", diffA + ":1-10", diffA + ":4-6:9-20", diffA + ":4-6:9-10"},
		{"contained", diffA + ":3-4", diffA + ":1-10", diffA + ":3-4"},
		{"two sources", diffA + ":1-5," + diffB + ":1-5", diffA + ":5," + diffB + ":2", diffA + ":5," + diffB + ":2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := IntersectGTIDStrings(tc.a, tc.b)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("%q ∩ %q = %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
