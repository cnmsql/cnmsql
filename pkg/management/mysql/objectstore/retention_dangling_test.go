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
	"context"
	"slices"
	"testing"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
)

// danglingFake is the archive index plus the set of manifest keys that exist.
type danglingFake struct {
	memVersioned
	manifests map[string]bool
}

func (d *danglingFake) Exists(_ context.Context, _, key string) (bool, error) {
	return d.manifests[key], nil
}

func danglingFixture(
	t *testing.T, listed []string, existing ...string,
) (*danglingFake, mysqlv1alpha1.S3ObjectStore, []BinlogEntry) {
	t.Helper()
	store := mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cnmsql"}
	fake := &danglingFake{manifests: map[string]bool{}}
	idx := ArchiveIndex{ClusterName: "demo", Segments: []ArchiveSegment{{
		ServerUUID: "uuid-a", Binlogs: listed, GTIDSet: "uuid-a:1-50",
	}}}
	if err := fake.PutJSONIf(context.Background(), "", "", &idx, ""); err != nil {
		t.Fatal(err)
	}
	entries := make([]BinlogEntry, 0, len(existing))
	for _, name := range existing {
		keys, err := BuildBinlogKeys(store, "demo", "uuid-a", name)
		if err != nil {
			t.Fatal(err)
		}
		fake.manifests[keys.ManifestKey] = true
		entries = append(entries, BinlogEntry{Keys: keys, Meta: BinlogMetadata{ServerUUID: "uuid-a", BinlogName: name}})
	}
	return fake, store, entries
}

func danglingIndex(t *testing.T, fake *danglingFake) ArchiveIndex {
	t.Helper()
	var idx ArchiveIndex
	if _, _, err := fake.GetJSONVersion(context.Background(), "", "", &idx); err != nil {
		t.Fatal(err)
	}
	return idx
}

// An index write that started from a copy older than a retention pass brings
// back the entries that pass removed. Their files are gone, so a recovery
// that planned them would fail on the download: the next pass drops them.
func TestDropDanglingIndexEntriesRemovesEntriesWhoseFilesAreGone(t *testing.T) {
	t.Parallel()
	fake, store, entries := danglingFixture(t,
		[]string{"binlog.000001", "binlog.000002", "binlog.000003"}, "binlog.000003")
	dropped, err := DropDanglingIndexEntries(context.Background(), fake, store, "demo", entries)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 2 {
		t.Errorf("dropped = %d, want 2", dropped)
	}
	seg := danglingIndex(t, fake).Segments[0]
	if !slices.Equal(seg.Binlogs, []string{"binlog.000003"}) {
		t.Errorf("index binlogs = %v, want only binlog.000003", seg.Binlogs)
	}
}

// A file archived after the manifests were listed is in the index and not in
// the listing. Its manifest exists, so it stays.
func TestDropDanglingIndexEntriesKeepsAFileArchivedSinceTheListing(t *testing.T) {
	t.Parallel()
	fake, store, entries := danglingFixture(t,
		[]string{"binlog.000001", "binlog.000002"}, "binlog.000001", "binlog.000002")
	dropped, err := DropDanglingIndexEntries(context.Background(), fake, store, "demo", entries[:1])
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 0 || len(danglingIndex(t, fake).Segments[0].Binlogs) != 2 {
		t.Errorf("dropped = %d, index = %v: a file with a manifest was dropped",
			dropped, danglingIndex(t, fake).Segments[0].Binlogs)
	}
}

// With nothing dangling the index is not written.
func TestDropDanglingIndexEntriesLeavesACleanIndexAlone(t *testing.T) {
	t.Parallel()
	fake, store, entries := danglingFixture(t, []string{"binlog.000001"}, "binlog.000001")
	before := fake.puts
	if _, err := DropDanglingIndexEntries(context.Background(), fake, store, "demo", entries); err != nil {
		t.Fatal(err)
	}
	if fake.puts != before {
		t.Errorf("index written %d time(s) with nothing dangling", fake.puts-before)
	}
}
