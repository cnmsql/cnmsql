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
	"context"
	"slices"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// A failed index write after the status write leaves a file archived but
// unindexed. With the file still on disk, the next pass must fold it in, on a
// fresh process and on the same one.
func TestLostIndexWriteIsRepaired(t *testing.T) {
	t.Parallel()
	for _, sameProcess := range []bool{false, true} {
		dir := t.TempDir()
		writeBinlog(t, dir, "binlog.000001", "one")
		writeBinlog(t, dir, "binlog.000002", "two")
		writeBinlog(t, dir, "binlog.000003", "active")

		mem := newMemStore()
		store := &crashStore{memStore: mem, failPutSub: objectstore.ArchiveIndexName}
		scan := staticScan(map[string]string{
			"binlog.000001": testUUID + ":1-3",
			"binlog.000002": testUUID + ":4-6",
		})
		logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"}})

		a := newTestArchiver(t, store, dir, scan)
		if _, err := a.ArchivePending(context.Background(), logs); err == nil {
			t.Fatal("expected the simulated index-write failure to surface")
		}
		store.failPutSub = ""
		resume := a
		if !sameProcess {
			resume = newTestArchiver(t, store, dir, scan)
		}
		if _, err := resume.ArchivePending(context.Background(), logs); err != nil {
			t.Fatal(err)
		}
		seg := readSegment(t, mem, resume)
		for _, want := range []string{"binlog.000001", "binlog.000002"} {
			if !slices.Contains(seg.Binlogs, want) {
				t.Errorf("sameProcess=%v: index binlogs = %v, missing %s", sameProcess, seg.Binlogs, want)
			}
		}
		if seg.GTIDSet != testUUID+":1-6" {
			t.Errorf("sameProcess=%v: segment set = %q, want %s:1-6", sameProcess, seg.GTIDSet, testUUID)
		}
	}
}

// A steady pass over files already indexed writes nothing.
func TestIndexedFilesDoNotRewriteTheIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "active")
	mem := newMemStore()
	scan := staticScan(map[string]string{"binlog.000001": testUUID + ":1-3"})
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}})
	a := newTestArchiver(t, mem, dir, scan)
	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	key := "backups/" + objectstore.ArchiveIndexKey(a.objectStore, "demo")
	before := mem.versions[key]
	fresh := newTestArchiver(t, mem, dir, scan)
	for range 2 {
		if _, err := fresh.ArchivePending(context.Background(), logs); err != nil {
			t.Fatal(err)
		}
	}
	if after := mem.versions[key]; after != before {
		t.Fatalf("index rewritten %d times on steady passes", after-before)
	}
}
