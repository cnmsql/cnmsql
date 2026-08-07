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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
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
		ObjectStore:  mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cnmsql"},
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
	if _, ok := store.objects["backups/cnmsql/demo/binlogs/"+testUUID+"/binlog.000003"]; ok {
		t.Fatal("active log was uploaded")
	}
	// Body + manifest landed for the rotated files.
	key := "backups/cnmsql/demo/binlogs/" + testUUID + "/binlog.000001"
	if got := store.objects[key]; !bytes.Equal(got, []byte("one")) {
		t.Fatalf("binlog body = %q", got)
	}
	if _, ok := store.objects["backups/cnmsql/demo/binlogs/"+testUUID+"/binlog.000001.json"]; !ok {
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

// firstGTIDScan is a scanner that reports a per-file FirstGTID alongside its set,
// so the index's StartGTIDSet plumbing can be exercised.
func firstGTIDScan(first, sets map[string]string) Scanner {
	return func(_ context.Context, path string) (ScanResult, error) {
		name := filepath.Base(path)
		return ScanResult{
			FirstGTID: first[name],
			GTIDSet:   sets[name],
			LastGTID:  sets[name],
		}, nil
	}
}

// TestArchivePendingSetsStartGTIDOnce checks that the segment's StartGTIDSet is
// fixed at the first archived file's FirstGTID and never advances as later files
// extend the segment — it is the segment's per-domain range start for recovery.
func TestArchivePendingSetsStartGTIDOnce(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "active")

	first1 := testUUID + ":1"
	store := newMemStore()
	scan := firstGTIDScan(
		map[string]string{"binlog.000001": first1},
		map[string]string{"binlog.000001": testUUID + ":1-10"},
	)
	a := newTestArchiver(t, store, dir, scan)
	indexKey := objectstore.ArchiveIndexKey(a.objectStore, "demo")

	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}})
	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	var idx objectstore.ArchiveIndex
	if err := store.GetJSON(context.Background(), "backups", indexKey, &idx); err != nil {
		t.Fatal(err)
	}
	if got := idx.Segments[0].StartGTIDSet; got != first1 {
		t.Fatalf("StartGTIDSet = %q, want %q", got, first1)
	}

	// binlog.000002 rotates and archives with a later FirstGTID; the segment's
	// StartGTIDSet must stay pinned to the first file while GTIDSet advances.
	writeBinlog(t, dir, "binlog.000003", "active")
	scan2 := firstGTIDScan(
		map[string]string{"binlog.000001": first1, "binlog.000002": testUUID + ":11"},
		map[string]string{"binlog.000001": testUUID + ":1-10", "binlog.000002": testUUID + ":11-20"},
	)
	a2 := newTestArchiver(t, store, dir, scan2)
	logs2 := MarkActive([]BinaryLog{
		{Name: "binlog.000001"}, {Name: "binlog.000002"}, {Name: "binlog.000003"},
	})
	if _, err := a2.ArchivePending(context.Background(), logs2); err != nil {
		t.Fatal(err)
	}
	if err := store.GetJSON(context.Background(), "backups", indexKey, &idx); err != nil {
		t.Fatal(err)
	}
	if got := idx.Segments[0].StartGTIDSet; got != first1 {
		t.Fatalf("StartGTIDSet after 2nd pass = %q, want %q (set once)", got, first1)
	}
	if got := idx.Segments[0].GTIDSet; got != testUUID+":1-20" {
		t.Fatalf("GTIDSet after 2nd pass = %q, want %s:1-20", got, testUUID)
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

	failKey := "cnmsql/demo/binlogs/" + testUUID + "/binlog.000002"
	store := &failingStore{memStore: newMemStore(), failOnKey: failKey}
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

// countingStore wraps a memStore to count the calls that cost a round trip, so
// tests can assert that a steady-state pass is quiet.
type countingStore struct {
	*memStore
	uploads, puts, gets, exists int
}

func newCountingStore() *countingStore { return &countingStore{memStore: newMemStore()} }

func (c *countingStore) Upload(ctx context.Context, bucket, key string, r io.Reader, n int64, ct string) error {
	c.uploads++
	return c.memStore.Upload(ctx, bucket, key, r, n, ct)
}

func (c *countingStore) PutJSON(ctx context.Context, bucket, key string, v any) error {
	c.puts++
	return c.memStore.PutJSON(ctx, bucket, key, v)
}

func (c *countingStore) GetJSON(ctx context.Context, bucket, key string, v any) error {
	c.gets++
	return c.memStore.GetJSON(ctx, bucket, key, v)
}

func (c *countingStore) Exists(ctx context.Context, bucket, key string) (bool, error) {
	c.exists++
	return c.memStore.Exists(ctx, bucket, key)
}

// countingScan wraps a Scanner to record how many files it decoded.
func countingScan(inner Scanner, n *int) Scanner {
	return func(ctx context.Context, path string) (ScanResult, error) {
		*n++
		return inner(ctx, path)
	}
}

// A rotated binlog is immutable, so once a pass has proven a file archived, no
// later pass should read it again. This is the regression that made an idle
// primary decode and hash its entire retained binlog set every poll interval.
func TestArchivePendingRereadsNothingOnceVerified(t *testing.T) {
	t.Parallel()
	// Two rotated logs that a first pass ships, plus the log mysqld still holds
	// open, which is never archivable.
	const (
		firstRotated  = shippedLog
		secondRotated = strandedLog
		openLog       = activeLog3
	)
	dir := t.TempDir()
	writeBinlog(t, dir, firstRotated, "one")
	writeBinlog(t, dir, secondRotated, "two")
	writeBinlog(t, dir, openLog, "active")

	store := newCountingStore()
	scans := 0
	scan := countingScan(staticScan(map[string]string{
		firstRotated:  testUUID + ":1-3",
		secondRotated: testUUID + ":4-6",
	}), &scans)
	a := newTestArchiver(t, store, dir, scan)
	logs := MarkActive([]BinaryLog{
		{Name: firstRotated}, {Name: secondRotated}, {Name: openLog},
	})

	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}
	if scans != 2 {
		t.Fatalf("first pass scanned %d files, want 2", scans)
	}

	scans = 0
	store.uploads, store.puts, store.gets, store.exists = 0, 0, 0, 0
	res, err := a.ArchivePending(context.Background(), logs)
	if err != nil {
		t.Fatal(err)
	}
	if scans != 0 {
		t.Fatalf("steady-state pass scanned %d files, want 0", scans)
	}
	if store.uploads != 0 || store.puts != 0 || store.gets != 1 || store.exists != 1 {
		t.Fatalf("steady-state pass hit the store: uploads=%d puts=%d gets=%d exists=%d; "+
			"want only the single status read",
			store.uploads, store.puts, store.gets, store.exists)
	}
	// The frontier must still be reported correctly from the cache.
	if res.LastArchivedBinlog != secondRotated {
		t.Fatalf("frontier = %q, want %s", res.LastArchivedBinlog, secondRotated)
	}
	if res.CoveredGTIDSet != testUUID+":1-6" {
		t.Fatalf("covered = %q, want %s:1-6", res.CoveredGTIDSet, testUUID)
	}
}

// The verified cache must not weaken the collision check: a file recreated under
// a reused name has to be re-verified even when the replacement is the same
// length, which a size-only stamp would miss.
func TestArchivePendingDetectsCollisionOnSameSizeRewrite(t *testing.T) {
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

	// RESET MASTER reusing the name: unlink and recreate with a payload of the
	// exact same length, so only the file's identity distinguishes it.
	if err := os.Remove(filepath.Join(dir, "binlog.000001")); err != nil {
		t.Fatal(err)
	}
	writeBinlog(t, dir, "binlog.000001", "REPLACED") // same 8 bytes as "original"

	_, err := a.ArchivePending(context.Background(), logs)
	if !errors.Is(err, ErrCollision) {
		t.Fatalf("expected ErrCollision after same-size rewrite, got %v", err)
	}
}

// A crash between the manifest write and the status write leaves the archive
// holding a file whose coverage the status does not record. Suppressing
// redundant status writes must not suppress that repair.
func TestArchivePendingRepairsStatusMissingCoverage(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, "binlog.000001", "one")
	writeBinlog(t, dir, "binlog.000002", "active")

	store := newMemStore()
	scan := staticScan(map[string]string{"binlog.000001": testUUID + ":1-3"})
	logs := MarkActive([]BinaryLog{{Name: "binlog.000001"}, {Name: "binlog.000002"}})

	a := newTestArchiver(t, store, dir, scan)
	if _, err := a.ArchivePending(context.Background(), logs); err != nil {
		t.Fatal(err)
	}

	// Simulate the crash: drop the status object, keeping the manifest. Use a
	// fresh archiver so no in-memory cache masks the repair, as a restart would.
	statusKey := "backups/" + objectstore.ArchiveStatusKey(
		mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cnmsql"}, "demo", testUUID)
	if _, ok := store.objects[statusKey]; !ok {
		t.Fatalf("status key %q absent; test needs updating", statusKey)
	}
	delete(store.objects, statusKey)

	b := newTestArchiver(t, store, dir, scan)
	res, err := b.ArchivePending(context.Background(), logs)
	if err != nil {
		t.Fatal(err)
	}
	if res.CoveredGTIDSet != testUUID+":1-3" {
		t.Fatalf("covered = %q, want %s:1-3", res.CoveredGTIDSet, testUUID)
	}
	if _, ok := store.objects[statusKey]; !ok {
		t.Fatal("status was not rewritten after coverage went missing")
	}
}

// The drain path (a demoted primary shipping its stranded tail) runs the same
// ArchivePending over a list whose earlier files are already cached. The
// stranded file must still ship and still be reported in Archived, which is
// what the "Drained binary logs stranded by a demotion" log line keys on.
func TestArchivePendingShipsStrandedTailPastVerifiedFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeBinlog(t, dir, shippedLog, "one")
	writeBinlog(t, dir, strandedLog, "active")

	store := newMemStore()
	sets := map[string]string{
		shippedLog:  testUUID + ":1-3",
		strandedLog: testUUID + ":4-6",
	}
	a := newTestArchiver(t, store, dir, staticScan(sets))

	// While primary: strandedLog is the active log and is not archivable.
	if _, err := a.ArchivePending(context.Background(),
		MarkActive([]BinaryLog{{Name: shippedLog}, {Name: strandedLog}})); err != nil {
		t.Fatal(err)
	}

	// Demotion rotates: strandedLog is now closed, activeLog3 is the open log.
	writeBinlog(t, dir, activeLog3, "active")
	res, err := a.ArchivePending(context.Background(), MarkActive([]BinaryLog{
		{Name: shippedLog}, {Name: strandedLog}, {Name: activeLog3},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Archived) != 1 || res.Archived[0] != strandedLog {
		t.Fatalf("drain archived %v, want [%s]", res.Archived, strandedLog)
	}
	if res.CoveredGTIDSet != testUUID+":1-6" {
		t.Fatalf("covered = %q, want %s:1-6", res.CoveredGTIDSet, testUUID)
	}
}
