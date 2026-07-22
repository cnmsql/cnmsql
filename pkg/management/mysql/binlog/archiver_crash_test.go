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
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// crashStore wraps memStore and fails one chosen write, simulating a process
// crash at that exact commit point.
type crashStore struct {
	*memStore
	failPutSub    string
	failUploadSub string
	tripped       bool
}

func (c *crashStore) PutJSON(ctx context.Context, bucket, key string, v any) error {
	if !c.tripped && c.failPutSub != "" && strings.Contains(key, c.failPutSub) {
		c.tripped = true
		return errors.New("simulated crash before write")
	}
	return c.memStore.PutJSON(ctx, bucket, key, v)
}

func (c *crashStore) Upload(ctx context.Context, bucket, key string, r io.Reader, size int64, ct string) error {
	if !c.tripped && c.failUploadSub != "" && strings.Contains(key, c.failUploadSub) {
		// The bytes land in the store, then the process dies before the manifest.
		c.tripped = true
		if err := c.memStore.Upload(ctx, bucket, key, r, size, ct); err != nil {
			return err
		}
		return errors.New("simulated crash after bytes, before manifest")
	}
	return c.memStore.Upload(ctx, bucket, key, r, size, ct)
}

func readSegment(t *testing.T, mem *memStore, a *Archiver) objectstore.ArchiveSegment {
	t.Helper()
	var idx objectstore.ArchiveIndex
	if err := mem.GetJSON(context.Background(), "backups",
		objectstore.ArchiveIndexKey(a.objectStore, "demo"), &idx); err != nil {
		t.Fatal(err)
	}
	if len(idx.Segments) != 1 {
		t.Fatalf("segments = %+v, want exactly one", idx.Segments)
	}
	return idx.Segments[0]
}

// TestResumeAfterManifestCrashReArchives covers the benign half of issue #89: a
// crash after a file's bytes upload but before its manifest, with the file still
// present locally on restart. The next pass must re-upload and finish it; nothing
// is lost.
func TestResumeAfterManifestCrashReArchives(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "two")
	writeBinlog(t, dir, "binlog.000003", "active")

	mem := newMemStore()
	store := &crashStore{memStore: mem, failUploadSub: "binlog.000002"}
	scan := staticScan(map[string]string{
		"binlog.000001": testUUID + ":1-3",
		"binlog.000002": testUUID + ":4-6",
	})
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"}})

	a := newTestArchiver(t, store, dir, scan)
	if _, err := a.ArchivePending(context.Background(), logs); err == nil {
		t.Fatal("expected the simulated crash to surface as an error")
	}

	// Resume with a healthy store; the file is still on disk (SHOW BINARY LOGS
	// still lists it), so the archiver reconciles it.
	a2 := newTestArchiver(t, store, dir, scan)
	if _, err := a2.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	seg := readSegment(t, mem, a2)
	for _, want := range []string{"binlog.000001", "binlog.000002"} {
		if !slices.Contains(seg.Binlogs, want) {
			t.Errorf("index binlogs = %v, missing %s after resume", seg.Binlogs, want)
		}
	}
	if seg.GTIDSet != testUUID+":1-6" {
		t.Fatalf("seg covered = %q, want %s:1-6", seg.GTIDSet, testUUID)
	}
}

// TestIndexCoverageNeverOutrunsFileList is the core regression for issue #89. A
// crash lands file 1's manifest and status but drops its index write; file 1 then
// leaves the local listing (mysqld's own expiry) before the archiver resumes.
//
// The segment's covered GTID set must never claim file 1's range (1-3) while its
// file list cannot name file 1: recovery reads seg.Binlogs, so a covered range
// with no backing file is a silent replay gap. After the fix, the index advances
// the file name and its coverage together, so the segment reports only what it can
// actually replay (file 2, 4-6) rather than a phantom 1-6.
func TestIndexCoverageNeverOutrunsFileList(t *testing.T) {
	t.Parallel()
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
		t.Fatal("expected the simulated index-write crash to surface as an error")
	}

	// binlog.000001 no longer appears in SHOW BINARY LOGS on restart, though its
	// manifest is committed in the store.
	store.failPutSub = ""
	resumeLogs := MarkActive([]BinaryLog{{Name: "binlog.000002"}, {Name: "binlog.000003"}})
	a2 := newTestArchiver(t, store, dir, scan)
	if _, err := a2.ArchivePending(context.Background(), resumeLogs); err != nil {
		t.Fatal(err)
	}

	seg := readSegment(t, mem, a2)
	// The segment must not claim file 1's GTIDs while its list cannot name file 1.
	if slices.Contains(seg.Binlogs, "binlog.000001") {
		t.Fatalf("binlog.000001 unexpectedly in seg.Binlogs = %v", seg.Binlogs)
	}
	if seg.GTIDSet != testUUID+":4-6" {
		t.Fatalf("seg covered = %q, want %s:4-6 (must not include unlisted file 1's 1-3)",
			seg.GTIDSet, testUUID)
	}
}
