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
	"slices"
	"testing"
)

func mustGTID(t *testing.T, s string) MariaDBGTID {
	t.Helper()
	g, err := ParseMariaDBGTID(s)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestParseMariaDBGTID(t *testing.T) {
	t.Parallel()
	g := mustGTID(t, " 1-22-333 ")
	if g != (MariaDBGTID{Domain: 1, Server: 22, Seq: 333}) || g.String() != "1-22-333" {
		t.Fatalf("parsed %+v (%s)", g, g)
	}
	for _, bad := range []string{"", "1-2", "a-1-2", "1-2-3-4", "1-2-x"} {
		if _, err := ParseMariaDBGTID(bad); err == nil {
			t.Fatalf("ParseMariaDBGTID(%q) accepted a malformed gtid", bad)
		}
	}
	pos, err := ParseMariaDBPosition("2-5-9,0-1-7")
	if err != nil {
		t.Fatal(err)
	}
	if want := []MariaDBGTID{{0, 1, 7}, {2, 5, 9}}; !slices.Equal(pos, want) {
		t.Fatalf("position = %+v, want %+v", pos, want)
	}
	if pos, err := ParseMariaDBPosition(""); err != nil || len(pos) != 0 {
		t.Fatalf("empty position = %+v, %v", pos, err)
	}
}

// lagged is the motivating MariaDB fork: server 1 authored up to 0-1-219, the
// lagged successor (server 2) inherited 0-1-218 and reused 219 onwards.
var lagged = MariaDBTimeline{
	{ServerID: 1, Handoff: ""},
	{ServerID: 2, Handoff: "0-1-218"},
}

// failback is A -> S -> A where S crashed with 0-2-301 and A inherited 0-2-300.
var failback = MariaDBTimeline{
	{ServerID: 1, Handoff: ""},
	{ServerID: 2, Handoff: "0-1-218"},
	{ServerID: 1, Handoff: "0-2-300"},
}

func TestMariaDBTimelineVerdict(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		timeline MariaDBTimeline
		gtid     string
		on, know bool
	}{
		{"empty timeline has no verdict", nil, "0-1-5", false, false},
		{"genesis epoch judges from the first transaction", lagged, "0-1-1", true, true},
		{"canonical before the handoff", lagged, "0-1-218", true, true},
		{"dead transaction past the handoff", lagged, "0-1-219", false, true},
		{"successor's own transaction", lagged, "0-2-219", true, true},
		{"successor far ahead", lagged, "0-2-9000", true, true},
		{"failback: interim epoch canonical", failback, "0-2-300", true, true},
		{"failback: interim dead tail", failback, "0-2-301", false, true},
		{"failback: returning primary", failback, "0-1-301", true, true},
		{"failback: first epoch still judged", failback, "0-1-100", true, true},
		{"unknown domain judged from genesis", lagged, "5-2-3", true, true},
		{"floor: at the oldest handoff", MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}, "0-1-218", false, false},
		{"floor: below the oldest handoff", MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}, "0-1-3", false, false},
		{"floor: above the oldest handoff", MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}, "0-2-219", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			on, known := tc.timeline.Verdict(mustGTID(t, tc.gtid))
			if on != tc.on || known != tc.know {
				t.Fatalf("Verdict(%s) = (on %v, known %v), want (on %v, known %v)", tc.gtid, on, known, tc.on, tc.know)
			}
		})
	}
}

// A successor that died before the operator observed it leaves a stretch no
// epoch names: the next handoff points at a server that is not the previous
// epoch's author. Nothing in that stretch gets a verdict, except the handoff
// itself, which the surviving timeline inherited by definition.
func TestMariaDBTimelineUnknownStretch(t *testing.T) {
	t.Parallel()
	tl := MariaDBTimeline{
		{ServerID: 1, Handoff: ""},
		{ServerID: 3, Handoff: "0-2-305"}, // server 2 promoted and died unobserved
	}
	for _, g := range []string{"0-1-219", "0-2-219", "0-1-10", "0-2-304"} {
		if _, known := tl.Verdict(mustGTID(t, g)); known {
			t.Fatalf("%s in an unobserved primary's stretch must get no verdict", g)
		}
	}
	if on, known := tl.Verdict(mustGTID(t, "0-2-305")); !on || !known {
		t.Fatal("the handoff GTID itself is on the timeline")
	}
	if on, known := tl.Verdict(mustGTID(t, "0-1-305")); on || !known {
		t.Fatal("a different server at the handoff sequence is off the timeline")
	}
	if on, known := tl.Verdict(mustGTID(t, "0-3-306")); !on || !known {
		t.Fatal("the observed successor's own writes are on the timeline")
	}
}

// A replica re-cloned from the current primary keeps its config-assigned
// server_id; its position names whoever authored the last transaction, so it is
// on the timeline.
func TestMariaDBTimelineReclonedReplicaIsOn(t *testing.T) {
	t.Parallel()
	on, known, err := lagged.Judge("0-2-400")
	if err != nil || !on || !known {
		t.Fatalf("Judge = (%v, %v, %v), want on", on, known, err)
	}
}

func TestMariaDBTimelineNonMonotonicDomainHasNoVerdict(t *testing.T) {
	t.Parallel()
	tl := MariaDBTimeline{
		{ServerID: 1, Handoff: "0-9-50"},
		{ServerID: 2, Handoff: "0-1-40"},
	}
	if _, known := tl.Verdict(mustGTID(t, "0-2-60")); known {
		t.Fatal("a domain whose handoffs go backwards cannot be judged")
	}
}

func TestMariaDBTimelineUnparseableHandoffHasNoVerdict(t *testing.T) {
	t.Parallel()
	tl := MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "garbage"}}
	if _, known := tl.Verdict(mustGTID(t, "0-2-60")); known {
		t.Fatal("an unparseable handoff cannot be judged")
	}
}

func TestMariaDBTimelineJudge(t *testing.T) {
	t.Parallel()
	tl := MariaDBTimeline{
		{ServerID: 1, Handoff: ""},
		{ServerID: 2, Handoff: "0-1-218,1-1-50"},
	}
	cases := []struct {
		pos      string
		on, know bool
	}{
		{"", true, true},
		{"0-2-300,1-2-60", true, true},
		{"0-1-219,1-2-60", false, true}, // one domain off settles it
		{"0-2-300,1-1-51", false, true}, // off in the second domain
		{"0-2-300,7-2-3", true, true},   // domain new to the timeline: judged from genesis
		{"0-1-200,1-1-40", true, true},  // canonical prefix in both domains
		{"0-2-300", true, true},
	}
	for _, tc := range cases {
		on, known, err := tl.Judge(tc.pos)
		if err != nil {
			t.Fatal(err)
		}
		if on != tc.on || known != tc.know {
			t.Fatalf("Judge(%q) = (on %v, known %v), want (on %v, known %v)", tc.pos, on, known, tc.on, tc.know)
		}
	}
	floor := MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}
	if _, known, _ := floor.Judge("0-2-300,1-9-4"); !known {
		t.Fatal("a domain absent from every handoff is judged from genesis")
	}
	if _, known, _ := floor.Judge("0-1-100"); known {
		t.Fatal("a position below the floor gets no verdict")
	}
	if _, _, err := tl.Judge("bad"); err == nil {
		t.Fatal("Judge accepted a malformed position")
	}
}

func TestMariaDBTimelineDeadAfter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		timeline MariaDBTimeline
		gtid     string
		want     uint64
		ok       bool
	}{
		{"lagged promotion", lagged, "0-1-219", 218, true},
		{"lagged promotion, far dead tail", lagged, "0-1-250", 218, true},
		{"failback interim tail", failback, "0-2-301", 300, true},
		{"on the timeline", lagged, "0-2-300", 0, false},
		{"no verdict", MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}, "0-1-100", 0, false},
		// With the genesis epoch pruned, the author's own epoch is gone; the
		// floor is still a sound cut, since everything above it is attributed.
		{"author's epoch pruned", MariaDBTimeline{{ServerID: 2, Handoff: "0-1-218"}}, "0-1-219", 218, true},
		// A timeline known from genesis attributes every sequence: a server that
		// never authored any of them holds only disowned transactions.
		{"server never primary", lagged, "0-7-219", 0, true},
		// An unobserved primary's stretch: its successor inherited 0-7-305, so
		// 7's transactions up to there may be canonical and only those past it
		// are disowned.
		{"unobserved author", MariaDBTimeline{{ServerID: 1}, {ServerID: 3, Handoff: "0-7-305"}}, "0-7-400", 305, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := tc.timeline.DeadAfter(mustGTID(t, tc.gtid))
			if got != tc.want || ok != tc.ok {
				t.Fatalf("DeadAfter(%s) = (%d, %v), want (%d, %v)", tc.gtid, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestPruneMariaDBTimeline(t *testing.T) {
	t.Parallel()
	three := MariaDBTimeline{
		{ServerID: 1, Handoff: ""},
		{ServerID: 2, Handoff: "0-1-218"},
		{ServerID: 1, Handoff: "0-2-300"},
	}

	t.Run("a position at the next handoff keeps the oldest entry", func(t *testing.T) {
		t.Parallel()
		res := PruneMariaDBTimeline(three, map[string]string{"a": "0-1-218", "b": "0-1-900"}, MariaDBTimelineCeiling)
		if len(res.Timeline) != 3 || res.Dropped != 0 {
			t.Fatalf("kept %d, dropped %d; want all 3 kept", len(res.Timeline), res.Dropped)
		}
	})
	t.Run("positions past the next handoffs drop entries", func(t *testing.T) {
		t.Parallel()
		res := PruneMariaDBTimeline(three, map[string]string{"a": "0-2-250"}, MariaDBTimelineCeiling)
		if len(res.Timeline) != 2 || res.Timeline[0].Handoff != "0-1-218" {
			t.Fatalf("timeline = %+v, want the oldest entry dropped", res.Timeline)
		}
		res = PruneMariaDBTimeline(three, map[string]string{"a": "0-1-301"}, MariaDBTimelineCeiling)
		if len(res.Timeline) != 1 || res.Dropped != 2 || res.Truncated {
			t.Fatalf("timeline = %+v (dropped %d), want only the newest entry", res.Timeline, res.Dropped)
		}
	})
	t.Run("any domain pins", func(t *testing.T) {
		t.Parallel()
		tl := MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-10,1-1-20"}}
		res := PruneMariaDBTimeline(tl, map[string]string{"a": "0-2-99,1-1-20"}, MariaDBTimelineCeiling)
		if len(res.Timeline) != 2 {
			t.Fatal("a position at the handoff in a second domain must pin the entry")
		}
	})
	t.Run("a domain the reference lacks pins nothing", func(t *testing.T) {
		t.Parallel()
		tl := MariaDBTimeline{{ServerID: 1}, {ServerID: 2, Handoff: "0-1-10,1-1-20"}}
		res := PruneMariaDBTimeline(tl, map[string]string{"a": "0-2-99"}, MariaDBTimelineCeiling)
		if len(res.Timeline) != 1 {
			t.Fatalf("timeline = %+v, want pruned", res.Timeline)
		}
	})
	t.Run("no references keep only the newest entry", func(t *testing.T) {
		t.Parallel()
		res := PruneMariaDBTimeline(three, nil, MariaDBTimelineCeiling)
		if len(res.Timeline) != 1 || res.Timeline[0].Handoff != "0-2-300" {
			t.Fatalf("timeline = %+v", res.Timeline)
		}
	})
	t.Run("the ceiling drops pinned entries and names who pinned them", func(t *testing.T) {
		t.Parallel()
		res := PruneMariaDBTimeline(three, map[string]string{"z": "0-1-5", "a": "0-1-7", "c": "0-2-900"}, 2)
		if len(res.Timeline) != 2 || !res.Truncated || res.Dropped != 1 {
			t.Fatalf("timeline = %+v truncated=%v dropped=%d", res.Timeline, res.Truncated, res.Dropped)
		}
		if want := []string{"a", "z"}; !slices.Equal(res.PinnedBy, want) {
			t.Fatalf("PinnedBy = %v, want %v", res.PinnedBy, want)
		}
	})
	t.Run("the input is not modified", func(t *testing.T) {
		t.Parallel()
		in := slices.Clone(three)
		PruneMariaDBTimeline(in, nil, MariaDBTimelineCeiling)
		if !slices.Equal(in, three) {
			t.Fatal("PruneMariaDBTimeline mutated its input")
		}
	})
}
