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
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// staleStore is an eventually consistent store without conditional writes
// (OpenStack Swift behind an S3 API): every index write is unconditional, and
// a read can be made to return the index as it was a number of writes ago.
type staleStore struct {
	*memStore
	history [][]byte
	// staleReads is how many index writes back the next index read goes; it
	// applies to one read.
	staleReads int
}

func (s *staleStore) PutJSONIf(ctx context.Context, bucket, key string, v any, _ string) error {
	if err := s.PutJSON(ctx, bucket, key, v); err != nil {
		return err
	}
	if strings.HasSuffix(key, objectstore.ArchiveIndexName) {
		s.history = append(s.history, s.objects[bucket+"/"+key])
	}
	return nil
}

func (s *staleStore) GetJSONVersion(ctx context.Context, bucket, key string, v any) (string, bool, error) {
	back := s.staleReads
	if back == 0 || !strings.HasSuffix(key, objectstore.ArchiveIndexName) {
		return s.memStore.GetJSONVersion(ctx, bucket, key, v)
	}
	s.staleReads = 0
	at := len(s.history) - 1 - back
	if at < 0 {
		return "", false, nil
	}
	return "stale", true, json.Unmarshal(s.history[at], v)
}

func threeFileArchive(t *testing.T) (dir string, scan Scanner, logs func(rotated int) []BinaryLog) {
	t.Helper()
	dir = t.TempDir()
	for _, name := range []string{"binlog.000001", "binlog.000002", "binlog.000003", "binlog.000004", "binlog.000005"} {
		writeBinlog(t, dir, name, name)
	}
	scan = staticScan(map[string]string{
		"binlog.000001": testUUID + ":1-3",
		"binlog.000002": testUUID + ":4-6",
		"binlog.000003": testUUID + ":7-9",
		"binlog.000004": testUUID + ":10-12",
	})
	logs = func(rotated int) []BinaryLog {
		var out []BinaryLog
		for i := 1; i <= rotated+1; i++ {
			out = append(out, BinaryLog{Name: "binlog.00000" + string(rune('0'+i))})
		}
		return MarkActive(out)
	}
	return dir, scan, logs
}

// An index write that starts from a stale copy of the index must not drop the
// file a previous write added: the bytes and the manifest are in the store,
// and recovery only reads the index.
func TestStaleIndexReadDoesNotDropAFile(t *testing.T) {
	t.Parallel()
	dir, scan, logs := threeFileArchive(t)
	store := &staleStore{memStore: newMemStore()}
	a := newTestArchiver(t, store, dir, scan)
	if _, err := a.ArchivePending(context.Background(), logs(2)); err != nil {
		t.Fatal(err)
	}

	// The write for binlog.000003 reads the index as it was before
	// binlog.000002 was added.
	store.staleReads = 1
	if _, err := a.ArchivePending(context.Background(), logs(3)); err != nil {
		t.Fatal(err)
	}

	// In sequence order: recovery replays a segment's files as listed.
	seg := readSegment(t, store.memStore, a)
	if want := []string{"binlog.000001", "binlog.000002", "binlog.000003"}; !slices.Equal(seg.Binlogs, want) {
		t.Errorf("index binlogs = %v, want %v", seg.Binlogs, want)
	}
	if want := testUUID + ":1-9"; seg.GTIDSet != want {
		t.Errorf("segment set = %q, want %q", seg.GTIDSet, want)
	}
}

// A stale read can go back further than one write.
func TestStaleIndexReadRestoresEveryDroppedFile(t *testing.T) {
	t.Parallel()
	dir, scan, logs := threeFileArchive(t)
	store := &staleStore{memStore: newMemStore()}
	a := newTestArchiver(t, store, dir, scan)
	if _, err := a.ArchivePending(context.Background(), logs(3)); err != nil {
		t.Fatal(err)
	}

	store.staleReads = 2
	if _, err := a.ArchivePending(context.Background(), logs(4)); err != nil {
		t.Fatal(err)
	}

	seg := readSegment(t, store.memStore, a)
	if len(seg.Binlogs) != 4 || seg.GTIDSet != testUUID+":1-12" {
		t.Errorf("index binlogs = %v set = %q, want four files covering %s:1-12", seg.Binlogs, seg.GTIDSet, testUUID)
	}
}

// A stale read can predate the instance's segment altogether. The files it
// drops are then older than the one being added, like expired ones, but their
// manifests are still in the store.
func TestStaleIndexReadRestoresADroppedSegment(t *testing.T) {
	t.Parallel()
	dir, scan, logs := threeFileArchive(t)
	store := &staleStore{memStore: newMemStore()}
	a := newTestArchiver(t, store, dir, scan)
	if _, err := a.ArchivePending(context.Background(), logs(3)); err != nil {
		t.Fatal(err)
	}

	store.staleReads = 3
	if _, err := a.ArchivePending(context.Background(), logs(4)); err != nil {
		t.Fatal(err)
	}

	seg := readSegment(t, store.memStore, a)
	want := []string{"binlog.000001", "binlog.000002", "binlog.000003", "binlog.000004"}
	if !slices.Equal(seg.Binlogs, want) || seg.GTIDSet != testUUID+":1-12" {
		t.Errorf("index binlogs = %v set = %q, want %v covering %s:1-12", seg.Binlogs, seg.GTIDSet, want, testUUID)
	}
}

// Retention removes a file from the store and then from the index. The
// archiver must not put it back.
func TestExpiredFilesAreNotRestoredToTheIndex(t *testing.T) {
	t.Parallel()
	dir, scan, logs := threeFileArchive(t)
	store := &staleStore{memStore: newMemStore()}
	a := newTestArchiver(t, store, dir, scan)
	ctx := context.Background()
	if _, err := a.ArchivePending(ctx, logs(3)); err != nil {
		t.Fatal(err)
	}

	keys, err := objectstore.BuildBinlogKeys(a.objectStore, "demo", testUUID, "binlog.000001")
	if err != nil {
		t.Fatal(err)
	}
	delete(store.objects, "backups/"+keys.BinlogKey)
	delete(store.objects, "backups/"+keys.ManifestKey)
	indexKey := objectstore.ArchiveIndexKey(a.objectStore, "demo")
	var idx objectstore.ArchiveIndex
	if err := store.GetJSON(ctx, "backups", indexKey, &idx); err != nil {
		t.Fatal(err)
	}
	idx.Segments[0].Binlogs = []string{"binlog.000002", "binlog.000003"}
	if err := store.PutJSON(ctx, "backups", indexKey, idx); err != nil {
		t.Fatal(err)
	}

	if _, err := a.ArchivePending(ctx, logs(4)); err != nil {
		t.Fatal(err)
	}
	seg := readSegment(t, store.memStore, a)
	if want := []string{"binlog.000002", "binlog.000003", "binlog.000004"}; !slices.Equal(seg.Binlogs, want) {
		t.Errorf("index binlogs = %v, want %v", seg.Binlogs, want)
	}
}

// The check that follows every archive pass reads the index too. When that
// read is the stale one, the write-back must not drop the file just added, and
// a check that finds a file missing puts it back without waiting for the next
// rotation.
func TestArchiveCheckRestoresADroppedFile(t *testing.T) {
	t.Parallel()
	dir, scan, logs := threeFileArchive(t)
	store := &staleStore{memStore: newMemStore()}
	src := &executedSource{executed: testUUID + ":1-9"}
	a := newForkArchiver(t, store, dir, scan, src.source)
	ctx := context.Background()
	if _, err := a.ArchivePending(ctx, logs(2)); err != nil {
		t.Fatal(err)
	}

	store.staleReads = 1
	if _, err := a.CheckArchive(ctx, time.Unix(1700000100, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	if seg := readSegment(t, store.memStore, a); len(seg.Binlogs) != 2 || seg.GTIDSet != testUUID+":1-6" {
		t.Fatalf("after a stale check: binlogs = %v set = %q, want both files", seg.Binlogs, seg.GTIDSet)
	}

	// Another writer's stale write drops binlog.000002 behind the archiver's
	// back; nothing is due, but the check sees the hole.
	store.objects["backups/"+objectstore.ArchiveIndexKey(a.objectStore, "demo")] = store.history[0]
	if _, err := a.CheckArchive(ctx, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if seg := readSegment(t, store.memStore, a); len(seg.Binlogs) != 2 || seg.GTIDSet != testUUID+":1-6" {
		t.Fatalf("after a foreign stale write: binlogs = %v set = %q, want both files", seg.Binlogs, seg.GTIDSet)
	}
}
