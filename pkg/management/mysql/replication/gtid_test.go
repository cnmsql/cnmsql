/*
Copyright 2026 The CNMySQL Authors.

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

func TestParseGTIDSetNormalizesIntervals(t *testing.T) {
	t.Parallel()
	set, err := ParseGTIDSet("UUID:8-10:1-5:6-7 , uuid2:1-3\n:5")
	if err != nil {
		t.Fatal(err)
	}
	got := set["uuid"]
	if len(got) != 1 || got[0] != (GTIDInterval{Start: 1, End: 10}) {
		t.Fatalf("uuid intervals = %#v, want [{1 10}]", got)
	}
	got2 := set["uuid2"]
	if len(got2) != 2 || got2[0] != (GTIDInterval{1, 3}) || got2[1] != (GTIDInterval{5, 5}) {
		t.Fatalf("uuid2 intervals = %#v", got2)
	}
}

func TestParseGTIDSetEmpty(t *testing.T) {
	t.Parallel()
	set, err := ParseGTIDSet("   \n ")
	if err != nil {
		t.Fatal(err)
	}
	if !set.IsEmpty() {
		t.Fatalf("expected empty set, got %#v", set)
	}
}

func TestParseGTIDSetRejectsBadInput(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"uuid", "uuid:0", "uuid:5-1", "uuid:abc", ":1-2"} {
		if _, err := ParseGTIDSet(raw); err == nil {
			t.Fatalf("expected error for %q", raw)
		}
	}
}

func TestGTIDSetContains(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		super, sub  string
		wantContain bool
	}{
		{"equal", "uuid:1-10", "uuid:1-10", true},
		{"superset", "uuid:1-10", "uuid:1-7", true},
		{"behind", "uuid:1-7", "uuid:1-10", false},
		{"empty subset", "uuid:1-10", "", true},
		{"missing uuid", "uuid:1-10", "other:1-2", false},
		{"multi source", "a:1-5,b:1-9", "a:1-5,b:1-3", true},
		{"multi source short", "a:1-5,b:1-2", "a:1-5,b:1-3", false},
		{"gap covered", "uuid:1-3:5-9", "uuid:6-8", true},
		{"gap not covered", "uuid:1-3:5-9", "uuid:3-6", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := GTIDContains(tt.super, tt.sub)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.wantContain {
				t.Fatalf("GTIDContains(%q,%q) = %v, want %v", tt.super, tt.sub, got, tt.wantContain)
			}
		})
	}
}

func TestGTIDSetString(t *testing.T) {
	t.Parallel()
	set, err := ParseGTIDSet("UUID2:1-3,UUID1:8-10:1-5:6-7")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := set.String(), "uuid1:1-10,uuid2:1-3"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	empty := GTIDSet{}
	if got := empty.String(); got != "" {
		t.Fatalf("empty String() = %q, want \"\"", got)
	}
}

func TestGTIDSetUnionAndClone(t *testing.T) {
	t.Parallel()
	a, _ := ParseGTIDSet("uuid:1-5")
	clone := a.Clone()
	b, _ := ParseGTIDSet("uuid:6-8,other:1-2")
	a.Union(b)
	if got, want := a.String(), "other:1-2,uuid:1-8"; got != want {
		t.Fatalf("union = %q, want %q", got, want)
	}
	if got, want := clone.String(), "uuid:1-5"; got != want {
		t.Fatalf("clone mutated: %q, want %q", got, want)
	}
}

func TestUnionGTIDStrings(t *testing.T) {
	t.Parallel()
	got, err := UnionGTIDStrings("uuid:1-5", "uuid:4-9", "", "other:1-2")
	if err != nil {
		t.Fatal(err)
	}
	if want := "other:1-2,uuid:1-9"; got != want {
		t.Fatalf("UnionGTIDStrings = %q, want %q", got, want)
	}
	if _, err := UnionGTIDStrings("bad-input"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestGTIDSetEqual(t *testing.T) {
	t.Parallel()
	a, _ := ParseGTIDSet("uuid:1-5:6-10")
	b, _ := ParseGTIDSet("uuid:1-10")
	if !a.Equal(b) {
		t.Fatal("coalesced sets should be equal")
	}
	c, _ := ParseGTIDSet("uuid:1-9")
	if a.Equal(c) {
		t.Fatal("different sets should not be equal")
	}
}
