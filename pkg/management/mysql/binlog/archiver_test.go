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

package binlog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/objectstore"
)

// memStore is an in-memory Store for archiver tests.
type memStore struct {
	objects map[string][]byte
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) Upload(_ context.Context, bucket, key string, r io.Reader, _ int64, _ string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.objects[bucket+"/"+key] = data
	return nil
}

func (m *memStore) PutJSON(_ context.Context, bucket, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	m.objects[bucket+"/"+key] = data
	return nil
}

func (m *memStore) GetJSON(_ context.Context, bucket, key string, v any) error {
	data, ok := m.objects[bucket+"/"+key]
	if !ok {
		return errors.New("not found")
	}
	return json.Unmarshal(data, v)
}

func (m *memStore) Exists(_ context.Context, bucket, key string) (bool, error) {
	_, ok := m.objects[bucket+"/"+key]
	return ok, nil
}

const testUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func writeBinlog(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func newTestArchiver(t *testing.T, store Store, dir string, scan Scanner) *Archiver {
	t.Helper()
	a, err := NewArchiver(ArchiverOptions{
		Store:        store,
		ObjectStore:  mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cnmysql"},
		ClusterName:  "demo",
		InstanceName: "demo-1",
		ServerUUID:   testUUID,
		BinlogDir:    dir,
		Scan:         scan,
		Now:          func() time.Time { return time.Unix(1700000000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// staticScan returns canned GTID ranges keyed by file basename.
func staticScan(ranges map[string]string) Scanner {
	return func(_ context.Context, path string) (ScanResult, error) {
		name := filepath.Base(path)
		set := ranges[name]
		return ScanResult{GTIDSet: set, LastGTID: lastOf(set)}, nil
	}
}

func lastOf(set string) string {
	if set == "" {
		return ""
	}
	return set
}

func TestArchivePendingShipsRotatedFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "two")
	writeBinlog(t, dir, "binlog.000003", "active-tail")

	store := newMemStore()
	scan := staticScan(map[string]string{
		"binlog.000001": testUUID + ":1-3",
		"binlog.000002": testUUID + ":4-6",
	})
	a := newTestArchiver(t, store, dir, scan)

	logs := MarkActive([]BinaryLog{
		{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"},
	})
	res, err := a.ArchivePending(context.Background(), logs)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Archived) != 2 {
		t.Fatalf("archived %v, want 2 files", res.Archived)
	}
	if res.LastArchivedBinlog != "binlog.000002" {
		t.Fatalf("last archived = %q", res.LastArchivedBinlog)
	}
	if res.CoveredGTIDSet != testUUID+":1-6" {
		t.Fatalf("covered = %q", res.CoveredGTIDSet)
	}
	// The active tail must never be uploaded.
	if _, ok := store.objects["backups/cnmysql/demo/binlogs/"+testUUID+"/binlog.000003"]; ok {
		t.Fatal("active log was uploaded")
	}
	// Body + manifest landed for the rotated files.
	if got := store.objects["backups/cnmysql/demo/binlogs/"+testUUID+"/binlog.000001"]; !bytes.Equal(got, []byte("one")) {
		t.Fatalf("binlog body = %q", got)
	}
	if _, ok := store.objects["backups/cnmysql/demo/binlogs/"+testUUID+"/binlog.000001.json"]; !ok {
		t.Fatal("manifest missing")
	}

	// Cluster index records the segment and cumulative coverage.
	var idx objectstore.ArchiveIndex
	indexKey := objectstore.ArchiveIndexKey(a.objectStore, "demo")
	if err := store.GetJSON(context.Background(), "backups", indexKey, &idx); err != nil {
		t.Fatal(err)
	}
	if len(idx.Segments) != 1 || idx.Segments[0].ServerUUID != testUUID {
		t.Fatalf("index segments = %+v", idx.Segments)
	}
	if idx.CoveredGTIDSet != testUUID+":1-6" {
		t.Fatalf("index covered = %q", idx.CoveredGTIDSet)
	}
}

func TestArchivePendingIsIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "active")

	store := newMemStore()
	scan := staticScan(map[string]string{"binlog.000001": testUUID + ":1-3"})
	a := newTestArchiver(t, store, dir, scan)
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}})

	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	// Second pass archives nothing new but still converges the frontier.
	res, err := a.ArchivePending(context.Background(), logs)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Archived) != 0 {
		t.Fatalf("second pass archived %v, want none", res.Archived)
	}
	if res.CoveredGTIDSet != testUUID+":1-3" {
		t.Fatalf("covered = %q", res.CoveredGTIDSet)
	}
}

func TestArchivePendingDetectsCollision(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "original")
	writeBinlog(t, dir, "binlog.000002", "active")

	store := newMemStore()
	scan := staticScan(map[string]string{"binlog.000001": testUUID + ":1-3"})
	a := newTestArchiver(t, store, dir, scan)
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}})
	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}

	// Simulate a RESET MASTER reusing binlog.000001 under the same UUID with
	// different bytes: rewrite the local file, drop the cached body so a re-pass
	// must reconcile against the stored manifest.
	writeBinlog(t, dir, "binlog.000001", "DIFFERENT CONTENT")
	_, err := a.ArchivePending(context.Background(), logs)
	if !errors.Is(err, ErrCollision) {
		t.Fatalf("expected ErrCollision, got %v", err)
	}
}

func TestArchivePendingStopsOnUploadError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "two")
	writeBinlog(t, dir, "binlog.000003", "active")

	store := &failingStore{memStore: newMemStore(), failOnKey: "cnmysql/demo/binlogs/" + testUUID + "/binlog.000002"}
	scan := staticScan(map[string]string{
		"binlog.000001": testUUID + ":1-3",
		"binlog.000002": testUUID + ":4-6",
	})
	a := newTestArchiver(t, store, dir, scan)
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"}})

	res, err := a.ArchivePending(context.Background(), logs)
	if err == nil {
		t.Fatal("expected upload error")
	}
	// Frontier advanced only to the file that succeeded.
	if res.LastArchivedBinlog != "binlog.000001" {
		t.Fatalf("frontier advanced past failure: %q", res.LastArchivedBinlog)
	}
}

type failingStore struct {
	*memStore
	failOnKey string
}

func (f *failingStore) Upload(ctx context.Context, bucket, key string, r io.Reader, size int64, ct string) error {
	if key == f.failOnKey {
		return errors.New("simulated upload failure")
	}
	return f.memStore.Upload(ctx, bucket, key, r, size, ct)
}
