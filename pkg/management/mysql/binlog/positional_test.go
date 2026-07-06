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

package binlog

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// TestScanMariaDBBoundaries checks that each GTID transaction's start offset is
// the end_log_pos of the preceding event, and that the Gtid_list header (which
// carries no transaction GTID) only advances the running position.
func TestScanMariaDBBoundaries(t *testing.T) {
	t.Parallel()
	got, err := scanMariaDBBoundaries(strings.NewReader(sampleMariaDBBinlog))
	if err != nil {
		t.Fatal(err)
	}
	want := []TxnBoundary{
		{Domain: 0, Seq: 10, StartPos: 299}, // starts where the Gtid_list event ended
		{Domain: 0, Seq: 11, StartPos: 341},
		{Domain: 1, Seq: 3, StartPos: 420},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("boundaries = %+v, want %+v", got, want)
	}
}

func TestSingleDomainMariaGTID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		domain  uint32
		seq     uint64
		ok      bool
		wantErr bool
	}{
		{name: "single", in: "0-2-57", domain: 0, seq: 57, ok: true},
		{name: "whitespace", in: " 0-2-57 \n", domain: 0, seq: 57, ok: true},
		{name: "non-zero domain", in: "3-9-1000", domain: 3, seq: 1000, ok: true},
		{name: "empty", in: "", ok: false},
		{name: "multi-domain", in: "0-1-14,1-5-3", wantErr: true},
		{name: "malformed", in: "0-1", wantErr: true},
		{name: "bad seq", in: "0-1-x", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			domain, seq, ok, err := SingleDomainMariaGTID(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.ok || domain != tc.domain || seq != tc.seq {
				t.Fatalf("got (%d,%d,%v), want (%d,%d,%v)", domain, seq, ok, tc.domain, tc.seq, tc.ok)
			}
		})
	}
}

func TestMariaSeqForDomain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pos    string
		domain uint32
		want   uint64
	}{
		{pos: "0-1-14", domain: 0, want: 14},
		{pos: "0-1-14,1-5-3", domain: 1, want: 3},
		{pos: "0-1-14", domain: 1, want: 0},
		{pos: "", domain: 0, want: 0},
		{pos: "garbage", domain: 0, want: 0},
	}
	for _, tc := range tests {
		if got := MariaSeqForDomain(tc.pos, tc.domain); got != tc.want {
			t.Errorf("MariaSeqForDomain(%q, %d) = %d, want %d", tc.pos, tc.domain, got, tc.want)
		}
	}
}

func TestAnchorSeqFromBoundaries(t *testing.T) {
	t.Parallel()
	// {Domain, Seq, StartPos}
	bounds := []TxnBoundary{{0, 1, 325}, {0, 2, 500}, {0, 3, 831}, {1, 9, 900}}
	tests := []struct {
		name   string
		domain uint32
		pos    int64
		want   uint64
	}{
		{name: "position after seq 2 excludes seq 3 at boundary", domain: 0, pos: 831, want: 2},
		{name: "position past everything", domain: 0, pos: 2000, want: 3},
		{name: "position at genesis before any txn", domain: 0, pos: 4, want: 0},
		{name: "position mid-stream", domain: 0, pos: 600, want: 2},
		{name: "other domain isolated", domain: 1, pos: 2000, want: 9},
		{name: "domain absent", domain: 5, pos: 2000, want: 0},
	}
	for _, tc := range tests {
		if got := AnchorSeqFromBoundaries(bounds, tc.domain, tc.pos); got != tc.want {
			t.Errorf("%s: AnchorSeqFromBoundaries(_, %d, %d) = %d, want %d",
				tc.name, tc.domain, tc.pos, got, tc.want)
		}
	}
}

func TestPlanMariadbPositional(t *testing.T) {
	t.Parallel()

	twoFiles := [][]TxnBoundary{
		{{0, 10, 100}, {0, 11, 200}, {0, 12, 300}}, // f1
		{{0, 13, 100}, {0, 14, 200}, {0, 15, 300}}, // f2
	}
	oneFile := [][]TxnBoundary{
		{{0, 10, 100}, {0, 11, 200}, {0, 12, 300}},
	}

	//nolint:prealloc // crossServer is appended below
	tests := []struct {
		name       string
		files      []string
		boundaries [][]TxnBoundary
		anchor     uint64
		target     uint64
		want       []ReplayChunk
		wantErr    error
	}{
		{
			name:       "target in second file",
			files:      []string{"f1", "f2"},
			boundaries: twoFiles,
			anchor:     11,
			target:     14,
			want: []ReplayChunk{
				{Files: []string{"f1"}, StartPosition: 300},
				{Files: []string{"f2"}, StopPosition: 300},
			},
		},
		{
			name:       "target is last transaction, no stop bound",
			files:      []string{"f1", "f2"},
			boundaries: twoFiles,
			anchor:     11,
			target:     15,
			want: []ReplayChunk{
				{Files: []string{"f1", "f2"}, StartPosition: 300},
			},
		},
		{
			name:       "anchor and target in same file",
			files:      []string{"f1"},
			boundaries: oneFile,
			anchor:     10,
			target:     11,
			want: []ReplayChunk{
				{Files: []string{"f1"}, StartPosition: 200, StopPosition: 300},
			},
		},
		{
			name:       "anchor at genesis replays from first transaction",
			files:      []string{"f1"},
			boundaries: oneFile,
			anchor:     0,
			target:     11,
			want: []ReplayChunk{
				{Files: []string{"f1"}, StartPosition: 100, StopPosition: 300},
			},
		},
		{
			name:       "nothing to replay past the anchor",
			files:      []string{"f1"},
			boundaries: oneFile,
			anchor:     12,
			target:     12,
			want:       nil,
		},
		{
			name:       "target beyond archive coverage",
			files:      []string{"f1"},
			boundaries: oneFile,
			anchor:     0,
			target:     99,
			wantErr:    ErrTargetBeyondArchive,
		},
		{
			// Real failover archive (mdb-stress-pitr): server 1 archives 0-1-1..14
			// and 0-1-15..26; server 2's binlog re-logs 0-1-15..26 (log_slave_updates)
			// then continues 0-2-27..57, plus a trailing file past the target. The
			// overlap must not be replayed twice, and the trailing file not at all.
			name: "failover with re-logged overlap",
			files: []string{
				"1_binlog.000001", "1_binlog.000002",
				"2_binlog.000001", "2_binlog.000002", "2_binlog.000003",
			},
			boundaries: [][]TxnBoundary{
				// 1/binlog.000001: 0-1-1..14 (all at/below the anchor)
				{{0, 1, 4}, {0, 14, 2800}},
				// 1/binlog.000002: 0-1-15..26
				{{0, 15, 339}, {0, 26, 76000}},
				// 2/binlog.000001: rotation only, no transactions
				{},
				// 2/binlog.000002: re-log 0-1-15..26 then 0-2-27..57
				{{0, 15, 339}, {0, 26, 60000}, {0, 27, 61000}, {0, 57, 284000}},
				// 2/binlog.000003: 0-2-58..62 (past the target)
				{{0, 58, 339}, {0, 62, 34000}},
			},
			anchor: 14,
			target: 57,
			want: []ReplayChunk{
				{Files: []string{"1_binlog.000002"}, StartPosition: 339},
				{Files: []string{"2_binlog.000002"}, StartPosition: 61000},
			},
		},
	}

	crossServer := []struct {
		name       string
		files      []string
		boundaries [][]TxnBoundary
		anchor     uint64
		target     uint64
		want       []ReplayChunk
		wantErr    error
	}{
		{
			// Union recovery: no single server spans 1..30, but A has 1-10 & 21-30 and
			// M has 11-20. Files arrive grouped by segment (A's then M's), i.e. NOT in
			// sequence order; the planner must sort them and stitch a monotonic replay.
			name: "cross-server union stitches gap-free",
			files: []string{
				"A_binlog.000001", "A_binlog.000002", "M_binlog.000001",
			},
			boundaries: [][]TxnBoundary{
				{{0, 1, 100}, {0, 10, 1000}},  // A 1-10
				{{0, 21, 100}, {0, 30, 1000}}, // A 21-30
				{{0, 11, 100}, {0, 20, 1000}}, // M 11-20
			},
			anchor: 0,
			target: 30,
			want: []ReplayChunk{
				{Files: []string{"A_binlog.000001", "M_binlog.000001", "A_binlog.000002"}, StartPosition: 100},
			},
		},
		{
			// A genuine hole (11-20 missing entirely) must fail closed, not replay
			// 21-30 straight after 10 (which mariadb-binlog rejects as out of order).
			name: "true gap fails closed",
			files: []string{
				"A_binlog.000001", "A_binlog.000002",
			},
			boundaries: [][]TxnBoundary{
				{{0, 1, 100}, {0, 10, 1000}},  // A 1-10
				{{0, 21, 100}, {0, 30, 1000}}, // A 21-30 (11-20 missing)
			},
			anchor:  0,
			target:  30,
			wantErr: ErrForkedTimeline,
		},
	}
	tests = append(tests, crossServer...)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PlanMariadbPositional(tc.files, tc.boundaries, 0, tc.anchor, tc.target)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("chunks = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestSelectMariadbSegments(t *testing.T) {
	t.Parallel()

	seg := func(uuid, start, end string, files ...string) objectstore.ArchiveSegment {
		return objectstore.ArchiveSegment{
			ServerUUID:   uuid,
			StartGTIDSet: start,
			GTIDSet:      end,
			Binlogs:      files,
		}
	}

	tests := []struct {
		name     string
		segments []objectstore.ArchiveSegment
		anchor   uint64
		target   uint64
		wantUUID []string
		wantErr  error
	}{
		{
			// One incarnation spanning the whole range: only it is downloaded.
			name:     "single segment covers everything",
			segments: []objectstore.ArchiveSegment{seg("A", "0-1-1", "0-1-30", "b1")},
			anchor:   0, target: 20,
			wantUUID: []string{"A"},
		},
		{
			// Two failover segments; the target is in the first, so the second
			// (past the target) is pruned from the download.
			name: "prunes segment past the target",
			segments: []objectstore.ArchiveSegment{
				seg("A", "0-1-1", "0-1-20", "a1"),
				seg("B", "0-1-15", "0-2-40", "b1"),
			},
			anchor: 0, target: 18,
			wantUUID: []string{"A"},
		},
		{
			// Union recovery: A has 1-10 & 21-30, M fills 11-20. All three needed.
			name: "union across three segments",
			segments: []objectstore.ArchiveSegment{
				seg("A1", "0-1-1", "0-1-10", "a1"),
				seg("A2", "0-1-21", "0-1-30", "a2"),
				seg("M", "0-1-11", "0-1-20", "m1"),
			},
			anchor: 0, target: 30,
			wantUUID: []string{"A1", "M", "A2"},
		},
		{
			// A real hole (11-20 missing) the union cannot bridge: fail closed.
			name: "gap fails closed",
			segments: []objectstore.ArchiveSegment{
				seg("A1", "0-1-1", "0-1-10", "a1"),
				seg("A2", "0-1-21", "0-1-30", "a2"),
			},
			anchor: 0, target: 30,
			wantErr: ErrForkedTimeline,
		},
		{
			// Target beyond everything archived in the domain.
			name:     "target beyond archive",
			segments: []objectstore.ArchiveSegment{seg("A", "0-1-1", "0-1-10", "a1")},
			anchor:   0, target: 99,
			wantErr: ErrTargetBeyondArchive,
		},
		{
			// Anchor already at/after the target: nothing to download.
			name:     "nothing past anchor",
			segments: []objectstore.ArchiveSegment{seg("A", "0-1-1", "0-1-30", "a1")},
			anchor:   30, target: 30,
			wantUUID: nil,
		},
		{
			// Anchor mid-range picks up only the segment carrying the remainder.
			name: "anchor mid-range skips covered segment",
			segments: []objectstore.ArchiveSegment{
				seg("A", "0-1-1", "0-1-10", "a1"),
				seg("B", "0-1-11", "0-1-20", "b1"),
			},
			anchor: 10, target: 20,
			wantUUID: []string{"B"},
		},
		{
			// Pre-range archive (no StartGTIDSet): start defaults to genesis so the
			// segment is always contiguity-eligible — reproduces old inclusive behavior.
			name:     "missing start gtid set treated as genesis",
			segments: []objectstore.ArchiveSegment{seg("A", "", "0-1-30", "a1")},
			anchor:   0, target: 20,
			wantUUID: []string{"A"},
		},
		{
			// Segments in a different domain contribute nothing to this domain's cover.
			name: "other-domain segment ignored",
			segments: []objectstore.ArchiveSegment{
				seg("A", "0-1-1", "0-1-30", "a1"),
				seg("X", "1-1-1", "1-1-50", "x1"),
			},
			anchor: 0, target: 25,
			wantUUID: []string{"A"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SelectMariadbSegments(tc.segments, 0, tc.anchor, tc.target)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var gotUUID []string
			for _, s := range got {
				gotUUID = append(gotUUID, s.ServerUUID)
			}
			if !reflect.DeepEqual(gotUUID, tc.wantUUID) {
				t.Fatalf("selected = %v, want %v", gotUUID, tc.wantUUID)
			}
		})
	}
}
