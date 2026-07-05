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

func TestPlanMariadbPositional(t *testing.T) {
	t.Parallel()

	twoFiles := [][]TxnBoundary{
		{{0, 10, 100}, {0, 11, 200}, {0, 12, 300}}, // f1
		{{0, 13, 100}, {0, 14, 200}, {0, 15, 300}}, // f2
	}
	oneFile := [][]TxnBoundary{
		{{0, 10, 100}, {0, 11, 200}, {0, 12, 300}},
	}

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
	}

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
