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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore/objectstoretest"
)

// archiveFake is one server's archive: file i (1-based) was uploaded at
// uploaded(i) and its last event is at lastEvent(i). It counts manifest reads.
type archiveFake struct {
	manifests map[BinlogKeys]BinlogMetadata
	read      map[string]int
	failOn    string
}

func newArchiveFake() *archiveFake {
	return &archiveFake{manifests: map[BinlogKeys]BinlogMetadata{}, read: map[string]int{}}
}

// add archives one file and returns its listing entry.
func (a *archiveFake) add(uuid string, seq int, uploaded, lastEvent time.Time) ArchivedBinlog {
	name := fmt.Sprintf("binlog.%06d", seq)
	keys := BinlogKeys{BinlogKey: uuid + "/" + name, ManifestKey: uuid + "/" + name + ".json"}
	a.manifests[keys] = BinlogMetadata{ServerUUID: uuid, BinlogName: name, LastEventTime: lastEvent}
	return ArchivedBinlog{Keys: keys, ServerUUID: uuid, Name: name, UploadedAt: uploaded}
}

func (a *archiveFake) reader(_ context.Context, keys BinlogKeys) (BinlogMetadata, error) {
	a.read[keys.ManifestKey]++
	if keys.ManifestKey == a.failOn {
		return BinlogMetadata{}, errors.New("store unavailable")
	}
	return a.manifests[keys], nil
}

func (a *archiveFake) reads() int {
	n := 0
	for _, c := range a.read {
		n += c
	}
	return n
}

func expiredNames(t *testing.T, a *archiveFake, files []ArchivedBinlog, horizon time.Time) []string {
	t.Helper()
	expired, err := ExpiredBinlogs(context.Background(), a.reader, files, horizon)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(expired))
	for _, f := range expired {
		names = append(names, f.ServerUUID+"/"+f.Name)
	}
	return names
}

var expiryEpoch = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// hourly returns an archive of n files, one an hour: file i holds the events
// of hour i and was uploaded a minute after its last one.
func hourly(a *archiveFake, uuid string, n int) []ArchivedBinlog {
	files := make([]ArchivedBinlog, 0, n)
	for i := 1; i <= n; i++ {
		last := expiryEpoch.Add(time.Duration(i) * time.Hour)
		files = append(files, a.add(uuid, i, last.Add(time.Minute), last))
	}
	return files
}

func TestExpiredBinlogsFindsTheBoundaryWithoutReadingEveryManifest(t *testing.T) {
	a := newArchiveFake()
	files := hourly(a, "u", 3000)
	// Files 1..2000 end before the horizon.
	horizon := expiryEpoch.Add(2000*time.Hour + 30*time.Minute)

	got := expiredNames(t, a, files, horizon)

	if len(got) != 2000 || got[0] != "u/binlog.000001" || got[1999] != "u/binlog.002000" {
		t.Fatalf("expired %d files (%v ... %v), want binlog.000001 to binlog.002000",
			len(got), got[:1], got[len(got)-1:])
	}
	if a.reads() > 15 {
		t.Fatalf("read %d manifests of a 3000-file segment, want a binary search", a.reads())
	}
}

// A file uploaded after the horizon is never expired, whatever its manifest
// says: an event timestamp is whatever the session set it to.
func TestExpiredBinlogsKeepsAFileUploadedAfterTheHorizonWhateverItsManifestSays(t *testing.T) {
	a := newArchiveFake()
	files := hourly(a, "u", 100)
	horizon := expiryEpoch.Add(30*time.Hour + 30*time.Minute)
	// File 60 ends on a timestamp from long before the archive.
	bogus := a.manifests[files[59].Keys]
	bogus.LastEventTime = expiryEpoch.Add(-24 * time.Hour)
	a.manifests[files[59].Keys] = bogus

	got := expiredNames(t, a, files, horizon)

	if len(got) != 30 || got[29] != "u/binlog.000030" {
		t.Fatalf("expired %v, want binlog.000001 to binlog.000030", got)
	}
	for key := range a.read {
		if key > "u/binlog.000030.json" {
			t.Fatalf("read %s, a file uploaded after the horizon", key)
		}
	}
}

// The upload time is the store's clock. A store running behind makes recent
// files look old; the events they hold still say they are not.
func TestExpiredBinlogsKeepsRecentEventsAStoreClockRunningBehindCallsOld(t *testing.T) {
	a := newArchiveFake()
	var files []ArchivedBinlog
	for i := 1; i <= 100; i++ {
		last := expiryEpoch.Add(time.Duration(i) * time.Hour)
		files = append(files, a.add("u", i, last.Add(-48*time.Hour), last))
	}
	horizon := expiryEpoch.Add(30*time.Hour + 30*time.Minute)

	got := expiredNames(t, a, files, horizon)

	if len(got) != 30 || got[29] != "u/binlog.000030" {
		t.Fatalf("expired %v, want binlog.000001 to binlog.000030", got)
	}
}

func TestExpiredBinlogsKeepsAFileUploadedJustBeforeTheHorizon(t *testing.T) {
	a := newArchiveFake()
	horizon := expiryEpoch.Add(10 * time.Hour)
	files := []ArchivedBinlog{
		a.add("u", 1, horizon.Add(-2*time.Hour), horizon.Add(-3*time.Hour)),
		a.add("u", 2, horizon.Add(-time.Minute), horizon.Add(-2*time.Minute)),
	}

	got := expiredNames(t, a, files, horizon)

	if len(got) != 1 || got[0] != "u/binlog.000001" {
		t.Fatalf("expired %v, want only binlog.000001: binlog.000002 is within the clock margin", got)
	}
}

// A store that lists no modification time gives nothing to search on, so each
// such file is judged by its own manifest.
func TestExpiredBinlogsJudgesAFileWithNoUploadTimeByItsManifest(t *testing.T) {
	a := newArchiveFake()
	horizon := expiryEpoch.Add(10 * time.Hour)
	files := []ArchivedBinlog{
		a.add("u", 1, time.Time{}, horizon.Add(-3*time.Hour)),
		a.add("u", 2, time.Time{}, horizon.Add(time.Hour)),
		a.add("u", 3, time.Time{}, horizon.Add(-2*time.Hour)),
	}

	got := expiredNames(t, a, files, horizon)

	if len(got) != 2 || got[0] != "u/binlog.000001" || got[1] != "u/binlog.000003" {
		t.Fatalf("expired %v, want binlog.000001 and binlog.000003", got)
	}
}

func TestExpiredBinlogsKeepsAFileWhoseLastEventTimeIsUnknown(t *testing.T) {
	a := newArchiveFake()
	horizon := expiryEpoch.Add(10 * time.Hour)
	files := []ArchivedBinlog{
		a.add("u", 1, horizon.Add(-5*time.Hour), time.Time{}),
		a.add("v", 1, time.Time{}, time.Time{}),
	}

	if got := expiredNames(t, a, files, horizon); len(got) != 0 {
		t.Fatalf("expired %v, want nothing: no file says when its last event was", got)
	}
}

func TestExpiredBinlogsSearchesEachServerOnItsOwn(t *testing.T) {
	a := newArchiveFake()
	// "old" stopped archiving long before the horizon, "new" straddles it.
	files := append(hourly(a, "old", 10), hourly(a, "new", 40)...)
	horizon := expiryEpoch.Add(20*time.Hour + 30*time.Minute)

	got := expiredNames(t, a, files, horizon)

	if len(got) != 30 {
		t.Fatalf("expired %d files %v, want all 10 of old and the first 20 of new", len(got), got)
	}
	if got[0] != "new/binlog.000001" || got[19] != "new/binlog.000020" || got[29] != "old/binlog.000010" {
		t.Fatalf("expired %v, want new/binlog.000001-20 then old/binlog.000001-10", got)
	}
}

func TestExpiredBinlogsReportsAManifestItCannotRead(t *testing.T) {
	a := newArchiveFake()
	files := hourly(a, "u", 1)
	a.failOn = files[0].Keys.ManifestKey

	_, err := ExpiredBinlogs(context.Background(), a.reader, files, expiryEpoch.Add(100*time.Hour))
	if err == nil {
		t.Fatal("no error for a manifest that could not be read")
	}
}

func TestListArchivedBinlogsNamesEachFileAndWhenItWasUploaded(t *testing.T) {
	const uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	srv, _ := objectstoretest.NewServer(t, "binlogs", map[string][]byte{
		"archive/demo/binlogs/" + uuid + "/binlog.000001":         []byte("b"),
		"archive/demo/binlogs/" + uuid + "/binlog.000001.json":    []byte("{}"),
		"archive/demo/binlogs/" + uuid + "/_archive_status.json":  []byte("{}"),
		"archive/demo/binlogs/_index.json":                        []byte("{}"),
		"archive/demo/binlogs/" + uuid + "/nested/binlog.01.json": []byte("{}"),
	})
	store := mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"}

	got, err := ListArchivedBinlogs(context.Background(), testClient(t, srv.URL), store, "demo")
	if err != nil {
		t.Fatal(err)
	}

	want := []ArchivedBinlog{{
		Keys: BinlogKeys{
			BinlogKey:   "archive/demo/binlogs/" + uuid + "/binlog.000001",
			ManifestKey: "archive/demo/binlogs/" + uuid + "/binlog.000001.json",
		},
		ServerUUID: uuid,
		Name:       "binlog.000001",
		UploadedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}}
	if len(got) != 1 || got[0].Keys != want[0].Keys || got[0].ServerUUID != uuid ||
		got[0].Name != "binlog.000001" || !got[0].UploadedAt.Equal(want[0].UploadedAt) {
		t.Fatalf("listed %+v, want %+v", got, want)
	}
}

// Deleting a day of binlogs one request at a time holds the pass for as long
// as the store takes to answer each. A file's manifest still goes after the
// file: the listing finds files by their manifest, so a manifest deleted
// first would leave a file no later pass sees.
func TestApplyBinlogExpiryDeletesFilesConcurrentlyAndEachManifestLast(t *testing.T) {
	var mu sync.Mutex
	var deleted []string
	inFlight, peak := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		mu.Lock()
		inFlight++
		peak = max(peak, inFlight)
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		deleted = append(deleted, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	var plan RetentionPlan
	for i := 1; i <= 16; i++ {
		name := fmt.Sprintf("archive/demo/binlogs/u/binlog.%06d", i)
		plan.DeleteBinlogKeys = append(plan.DeleteBinlogKeys, name, name+".json")
	}
	store := mysqlv1alpha1.S3ObjectStore{Bucket: "binlogs", Path: "archive"}
	if err := ApplyBinlogExpiry(context.Background(), testClient(t, srv.URL), store, "demo", plan); err != nil {
		t.Fatal(err)
	}

	if len(deleted) != 32 {
		t.Fatalf("deleted %d objects, want 32: %v", len(deleted), deleted)
	}
	if peak < 2 {
		t.Fatal("deletes ran one at a time")
	}
	for i := 1; i <= 16; i++ {
		file := fmt.Sprintf("/binlogs/archive/demo/binlogs/u/binlog.%06d", i)
		if slices.Index(deleted, file) > slices.Index(deleted, file+".json") {
			t.Fatalf("manifest of %s was deleted before the file: %v", file, deleted)
		}
	}
}
