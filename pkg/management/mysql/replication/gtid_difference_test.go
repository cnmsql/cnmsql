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

const (
	diffA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	diffB = "7f2b1c90-0000-11e1-9e33-c80aa9429562"
)

func TestGTIDSetDifference(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, a, b, want string
	}{
		{"both empty", "", "", ""},
		{"empty minus set", "", diffA + ":1-5", ""},
		{"set minus empty", diffA + ":1-5", "", diffA + ":1-5"},
		{"disjoint sources", diffA + ":1-5", diffB + ":1-5", diffA + ":1-5"},
		{"fully contained", diffA + ":3-4", diffA + ":1-10", ""},
		{"equal", diffA + ":1-10", diffA + ":1-10", ""},
		{"hole in the middle", diffA + ":1-10", diffA + ":4-6", diffA + ":1-3:7-10"},
		{"trim first", diffA + ":1-10", diffA + ":1", diffA + ":2-10"},
		{"trim last", diffA + ":1-10", diffA + ":10", diffA + ":1-9"},
		{"several holes", diffA + ":1-20", diffA + ":2:5-6:19-30", diffA + ":1:3-4:7-18"},
		{"overlap both sides", diffA + ":5-10:20-30", diffA + ":8-25", diffA + ":5-7:26-30"},
		{"lagged promotion", diffA + ":1-219", diffA + ":1-218," + diffB + ":1-300", diffA + ":219"},
		{"max bound", diffA + ":1-9223372036854775807", diffA + ":2-9223372036854775806",
			diffA + ":1:9223372036854775807"},
		{"source drops out", diffA + ":1-5," + diffB + ":1-3", diffA + ":1-5", diffB + ":1-3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, err := ParseGTIDSet(tc.a)
			if err != nil {
				t.Fatal(err)
			}
			b, err := ParseGTIDSet(tc.b)
			if err != nil {
				t.Fatal(err)
			}
			before := a.String()
			got := a.Difference(b)
			checkNormalized(t, got, "difference")
			if got.String() != tc.want {
				t.Fatalf("%q \\ %q = %q, want %q", tc.a, tc.b, got.String(), tc.want)
			}
			if a.String() != before {
				t.Fatalf("Difference mutated its receiver: %q -> %q", before, a.String())
			}
			str, err := DifferenceGTIDStrings(tc.a, tc.b)
			if err != nil {
				t.Fatal(err)
			}
			if str != tc.want {
				t.Fatalf("DifferenceGTIDStrings(%q, %q) = %q, want %q", tc.a, tc.b, str, tc.want)
			}
		})
	}
}

func TestIntersectsGTIDStrings(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b string
		want bool
	}{
		{"", "", false},
		{diffA + ":1-5", "", false},
		{diffA + ":1-5", diffA + ":6-9", false},
		{diffA + ":1-5", diffB + ":1-5", false},
		{diffA + ":1-5", diffA + ":5-9", true},
		{diffA + ":219", diffA + ":1-218:219", true},
	}
	for _, tc := range cases {
		got, err := IntersectsGTIDStrings(tc.a, tc.b)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("IntersectsGTIDStrings(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
	if _, err := DifferenceGTIDStrings("uuid", ""); err == nil {
		t.Fatal("DifferenceGTIDStrings accepted a malformed set")
	}
	if _, err := IntersectsGTIDStrings("", ":1"); err == nil {
		t.Fatal("IntersectsGTIDStrings accepted a malformed set")
	}
}

// FuzzGTIDSetDifference checks the set-difference algebra: a \ b is normalized,
// is a subset of a, shares nothing with b, and together with b covers a.
func FuzzGTIDSetDifference(f *testing.F) {
	for i, seed := range gtidSeeds {
		f.Add(seed, gtidSeeds[(i+1)%len(gtidSeeds)])
		f.Add(seed, seed)
	}
	f.Fuzz(func(t *testing.T, rawA, rawB string) {
		a, err := ParseGTIDSet(rawA)
		if err != nil {
			return
		}
		b, err := ParseGTIDSet(rawB)
		if err != nil {
			return
		}
		diff := a.Difference(b)
		checkNormalized(t, diff, "difference")
		if !a.Contains(diff) {
			t.Fatalf("%q \\ %q = %q is not a subset of %q", a.String(), b.String(), diff.String(), a.String())
		}
		// Nothing in diff is in b: b misses every transaction diff holds.
		if !diff.IsEmpty() && b.MissingCount(diff) != diffSize(diff) {
			t.Fatalf("%q \\ %q = %q overlaps %q", a.String(), b.String(), diff.String(), b.String())
		}
		cover := diff.Clone()
		cover.Union(b)
		if !cover.Contains(a) {
			t.Fatalf("(%q \\ %q) ∪ %q does not cover %q", a.String(), b.String(), b.String(), a.String())
		}
		if diff.IsEmpty() != b.Contains(a) {
			t.Fatalf("empty difference %v disagrees with Contains %v for %q, %q",
				diff.IsEmpty(), b.Contains(a), a.String(), b.String())
		}
	})
}

// diffSize counts the transactions in a set, saturating like MissingCount.
func diffSize(s GTIDSet) int64 {
	return GTIDSet{}.MissingCount(s)
}
