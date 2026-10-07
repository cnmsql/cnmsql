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

package objectstore

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func forkedIndex() *ArchiveIndex {
	detected := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	return &ArchiveIndex{
		ClusterName: "demo",
		Segments: []ArchiveSegment{
			{
				ServerUUID: "old",
				Binlogs:    []string{"binlog.000001", "binlog.000002"},
				GTIDSet:    "0-1-219",
				Fork: &ArchiveFork{
					GTIDSet:          "u1:219",
					AfterSeq:         map[uint32]uint64{0: 218},
					AuthorityGTIDSet: "0-2-300",
					DetectedAt:       detected,
					DetectedBy:       "demo-2/new",
				},
			},
			{ServerUUID: "new", Binlogs: []string{"binlog.000001"}, GTIDSet: "0-2-300"},
		},
		ForkCheck: &ArchiveForkCheck{CheckedAt: detected, CheckedBy: "demo-2/new"},
	}
}

func TestArchiveIndexForkRoundTrip(t *testing.T) {
	t.Parallel()
	in := forkedIndex()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out ArchiveIndex
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out.Segments[0].Fork, in.Segments[0].Fork) {
		t.Fatalf("fork = %+v, want %+v", out.Segments[0].Fork, in.Segments[0].Fork)
	}
	if !reflect.DeepEqual(out.ForkCheck, in.ForkCheck) {
		t.Fatalf("forkCheck = %+v, want %+v", out.ForkCheck, in.ForkCheck)
	}
	if out.Segments[1].Fork != nil {
		t.Fatal("an unforked segment must decode without a fork record")
	}
}

// An index written before fork records existed decodes with neither field, and
// the retired handoffGTID key is ignored.
func TestArchiveIndexLegacyDecodes(t *testing.T) {
	t.Parallel()
	var idx ArchiveIndex
	legacy := `{"clusterName":"demo","segments":[{"serverUUID":"a","handoffGTID":"a:1-5","gtidSet":"a:1-5"}]}`
	if err := json.Unmarshal([]byte(legacy), &idx); err != nil {
		t.Fatal(err)
	}
	if idx.ForkCheck != nil || idx.Segments[0].Fork != nil {
		t.Fatalf("legacy index decoded fork fields: %+v", idx)
	}
}

func TestRetentionKeepsForkRecords(t *testing.T) {
	t.Parallel()
	idx := forkedIndex()

	partial := rewriteIndex(idx, map[string]map[string]struct{}{"old": {"binlog.000001": {}}})
	if !reflect.DeepEqual(partial.ForkCheck, idx.ForkCheck) {
		t.Fatalf("partial retention lost forkCheck: %+v", partial.ForkCheck)
	}
	seg, ok := partial.Segment("old")
	if !ok || !reflect.DeepEqual(seg.Fork, idx.Segments[0].Fork) {
		t.Fatalf("partial retention lost the fork record: %+v", seg)
	}

	full := rewriteIndex(idx, map[string]map[string]struct{}{
		"old": {"binlog.000001": {}, "binlog.000002": {}},
	})
	if _, ok := full.Segment("old"); ok {
		t.Fatal("a segment whose files are all gone must leave the index with its fork record")
	}
	if !reflect.DeepEqual(full.ForkCheck, idx.ForkCheck) {
		t.Fatal("dropping the forked segment must keep the index's forkCheck")
	}
}
