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

import (
	"strings"
	"testing"
)

// gtidSeeds are shared by the GTID fuzz targets. They cover the shapes mysqld
// and mariadbd actually emit plus the boundary values that stress the interval
// arithmetic.
var gtidSeeds = []string{
	"",
	"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:8-10",
	"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,7f2b1c90-0000-11e1-9e33-c80aa9429562:1-3",
	"3E11FA47-71CA-11E1-9E33-C80AA9429562:1",
	"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,\n3e11fa47-71ca-11e1-9e33-c80aa9429562:6-9",
	"3e11fa47-71ca-11e1-9e33-c80aa9429562:5-9:1-3",
	"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9223372036854775807",
	"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9223372036854775807:9223372036854775807",
	"3e11fa47-71ca-11e1-9e33-c80aa9429562:9223372036854775806-9223372036854775807",
	"0-1-2",
	"uuid:0",
	"uuid:5-1",
	"uuid",
	":1-5",
}

// checkNormalized asserts the representation invariant every GTIDSet returned
// by the package must hold: source UUIDs are lowercased, and each source's
// intervals are valid, sorted by start, and fully coalesced (no overlapping or
// merely adjacent ranges survive normalization).
func checkNormalized(t *testing.T, set GTIDSet, origin string) {
	t.Helper()
	for uuid, intervals := range set {
		if uuid != strings.ToLower(uuid) {
			t.Fatalf("%s: uuid %q not lowercased", origin, uuid)
		}
		for i, iv := range intervals {
			if iv.Start < 1 || iv.End < iv.Start {
				t.Fatalf("%s: source %q interval %d is invalid: %+v", origin, uuid, i, iv)
			}
			if i == 0 {
				continue
			}
			prev := intervals[i-1]
			// Both bounds are >= 1 here, so this subtraction cannot
			// overflow the way a prev.End+1 comparison would.
			if iv.Start <= prev.End || iv.Start-prev.End <= 1 {
				t.Fatalf("%s: source %q intervals %d,%d are unsorted or uncoalesced: %+v then %+v",
					origin, uuid, i-1, i, prev, iv)
			}
		}
	}
}

// FuzzParseGTIDSet checks that any accepted GTID set string yields a
// normalized set that survives a String/ParseGTIDSet round trip unchanged, and
// that the reflexive containment identities hold.
func FuzzParseGTIDSet(f *testing.F) {
	for _, seed := range gtidSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		set, err := ParseGTIDSet(raw)
		if err != nil {
			if set != nil {
				t.Fatalf("ParseGTIDSet(%q) returned a set alongside error %v", raw, err)
			}
			return
		}
		checkNormalized(t, set, "parsed")

		// String must render a form the parser accepts back as the same set,
		// and canonical rendering must be a fixed point: scan.go stores the
		// rendered form in backup manifests and re-parses it later.
		rendered := set.String()
		again, err := ParseGTIDSet(rendered)
		if err != nil {
			t.Fatalf("re-parsing rendered form %q of %q failed: %v", rendered, raw, err)
		}
		checkNormalized(t, again, "re-parsed")
		if !again.Equal(set) {
			t.Fatalf("round trip changed the set: %q -> %q -> %q", raw, rendered, again.String())
		}
		if got := again.String(); got != rendered {
			t.Fatalf("rendering is not a fixed point: %q -> %q -> %q", raw, rendered, got)
		}

		// A set always contains itself and is missing nothing from itself.
		if !set.Contains(set) {
			t.Fatalf("%q does not contain itself", rendered)
		}
		if n := set.MissingCount(set); n != 0 {
			t.Fatalf("%q reports %d transactions missing from itself", rendered, n)
		}
		if clone := set.Clone(); !clone.Equal(set) || clone.String() != rendered {
			t.Fatalf("Clone of %q differs: %q", rendered, clone.String())
		}
		if set.IsEmpty() != (rendered == "") {
			t.Fatalf("IsEmpty()=%v disagrees with rendering %q", set.IsEmpty(), rendered)
		}
	})
}

// FuzzGTIDSetUnion checks the union algebra across two independently parsed
// sets: the union must be normalized, must contain both operands, and must
// report a non-negative, self-consistent missing count.
func FuzzGTIDSetUnion(f *testing.F) {
	for i, seed := range gtidSeeds {
		f.Add(seed, gtidSeeds[(i+1)%len(gtidSeeds)])
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

		// MissingCount is a set-difference size: it can never be negative, and
		// it must be zero exactly when the other set adds nothing.
		missing := a.MissingCount(b)
		if missing < 0 {
			t.Fatalf("MissingCount(%q, %q) = %d, want >= 0", a.String(), b.String(), missing)
		}
		if (missing == 0) != a.Contains(b) {
			t.Fatalf("MissingCount(%q, %q) = %d disagrees with Contains = %v",
				a.String(), b.String(), missing, a.Contains(b))
		}

		union := a.Clone()
		union.Union(b)
		checkNormalized(t, union, "union")
		if !union.Contains(a) || !union.Contains(b) {
			t.Fatalf("union %q does not contain both %q and %q", union.String(), a.String(), b.String())
		}
		if n := union.MissingCount(a); n != 0 {
			t.Fatalf("union %q reports %d transactions missing from operand %q", union.String(), n, a.String())
		}
		if n := union.MissingCount(b); n != 0 {
			t.Fatalf("union %q reports %d transactions missing from operand %q", union.String(), n, b.String())
		}

		// Union is idempotent and commutative on the transactions it holds.
		twice := union.Clone()
		twice.Union(b)
		if !twice.Equal(union) || twice.String() != union.String() {
			t.Fatalf("union is not idempotent: %q then %q", union.String(), twice.String())
		}
		other := b.Clone()
		other.Union(a)
		if !other.Equal(union) || other.String() != union.String() {
			t.Fatalf("union is not commutative: %q vs %q", union.String(), other.String())
		}

		// UnionGTIDStrings is the string-level wrapper and must agree.
		merged, err := UnionGTIDStrings(rawA, rawB)
		if err != nil {
			t.Fatalf("UnionGTIDStrings(%q, %q) failed after both parsed: %v", rawA, rawB, err)
		}
		if merged != union.String() {
			t.Fatalf("UnionGTIDStrings(%q, %q) = %q, want %q", rawA, rawB, merged, union.String())
		}
	})
}
