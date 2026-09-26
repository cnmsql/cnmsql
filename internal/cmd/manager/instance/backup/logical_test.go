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

package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

type memStore struct {
	mu         sync.Mutex
	objects    map[string][]byte
	json       map[string]any
	removed    []string
	uploadErr  error
	putJSONErr error
}

func newMemStore() *memStore {
	return &memStore{objects: map[string][]byte{}, json: map[string]any{}}
}

func (m *memStore) Upload(_ context.Context, _, key string, r io.Reader, _ int64, _ string) error {
	if m.uploadErr != nil {
		return m.uploadErr
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = b
	return nil
}

func (m *memStore) PutJSON(_ context.Context, _, key string, v any) error {
	if m.putJSONErr != nil {
		return m.putJSONErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.json[key] = v
	return nil
}

func (m *memStore) Remove(_ context.Context, _, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	m.removed = append(m.removed, key)
	return nil
}

const sampleDump = "-- MySQL dump 10.13\n-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000001', MASTER_LOG_POS=4;\n" +
	"INSERT INTO t VALUES (1);\n-- Dump completed on 2026-09-25 16:29:19\n"

// sourceServer answers POST /cluster/dump like an instance manager would.
type sourceServer struct {
	// refusals are served, in order, before the dump succeeds.
	refusals []int
	// abort cuts the stream short right after the response head.
	abort    bool
	body     string
	trailer  map[string]string
	requests []webserver.DumpRequest
}

func (s *sourceServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req webserver.DumpRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	s.requests = append(s.requests, req)
	if len(s.refusals) > 0 {
		status := s.refusals[0]
		s.refusals = s.refusals[1:]
		if status == http.StatusNotFound {
			http.NotFound(w, r)
			return
		}
		reasons := map[int]string{
			http.StatusNotImplemented:      webserver.DumpReasonToolUnavailable,
			http.StatusServiceUnavailable:  webserver.DumpReasonAccountMissing,
			http.StatusConflict:            webserver.DumpReasonInProgress,
			http.StatusUnprocessableEntity: webserver.DumpReasonInvalidRequest,
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(webserver.ReasonErrorBody{Reason: reasons[status], Error: "refused " + reasons[status]})
		return
	}
	w.Header().Set(webserver.DumpToolHeader, "mysqldump")
	w.Header().Set(webserver.DumpFlavorHeader, "mysql")
	w.Header().Set(webserver.DumpServerVersionHeader, "8.4.11-11")
	w.Header().Set(webserver.DumpDatabasesHeader, webserver.EncodeDumpDatabases([]string{"billing", "shop"}))
	w.Header().Set("Trailer", webserver.DumpSnapshotGTIDTrailer+", "+webserver.DumpSnapshotBinlogTrailer+", "+webserver.DumpErrorTrailer)
	w.WriteHeader(http.StatusOK)
	if s.abort {
		// Flush the head and a first chunk so the client gets the 200 and then
		// finds the stream cut short, instead of failing the request itself.
		_, _ = io.WriteString(w, "-- MySQL dump 10.13\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}
	_, _ = io.WriteString(w, s.body)
	for k, v := range s.trailer {
		w.Header().Set(k, v)
	}
}

func logicalOpts(url string) uploadOptions {
	return uploadOptions{
		Method:           methodLogical,
		SourceManagerURL: url,
		Bucket:           "backups",
		ArchiveKey:       "prod/shop/nightly/id/dump.sql.zst",
		MetadataKey:      "prod/shop/nightly/id/logical.json",
		BackupID:         "id",
		BackupName:       "nightly",
		ClusterName:      "shop",
		InstanceName:     "shop-2",
		Databases:        []string{"billing", "shop"},
		DumpArgs:         []string{"--max-allowed-packet=1G"},
	}
}

func fastRetries(t *testing.T, timeout time.Duration) {
	t.Helper()
	interval, total := dumpRetryInterval, dumpRetryTimeout
	dumpRetryInterval, dumpRetryTimeout = 5*time.Millisecond, timeout
	t.Cleanup(func() { dumpRetryInterval, dumpRetryTimeout = interval, total })
}

func runWithStore(t *testing.T, store *memStore, src *sourceServer) error {
	t.Helper()
	srv := httptest.NewServer(src)
	t.Cleanup(srv.Close)
	return runLogicalUpload(context.Background(), logicalOpts(srv.URL), store, srv.Client(), "s3cret")
}

func runAgainst(t *testing.T, src *sourceServer) (*memStore, error) {
	t.Helper()
	store := newMemStore()
	return store, runWithStore(t, store, src)
}

func decompress(t *testing.T, b []byte) string {
	t.Helper()
	r, err := objectstore.NewZstdReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestLogicalUploadWritesDumpAndManifest(t *testing.T) {
	src := &sourceServer{body: sampleDump, trailer: map[string]string{
		webserver.DumpSnapshotBinlogTrailer: "mysql-bin.000001:4",
		webserver.DumpSnapshotGTIDTrailer:   "0-1-13",
	}}
	store, err := runAgainst(t, src)
	if err != nil {
		t.Fatal(err)
	}

	req := src.requests[0]
	if req.Password != "s3cret" || !slices.Equal(req.Databases, []string{"billing", "shop"}) ||
		!slices.Equal(req.ExtraArgs, []string{"--max-allowed-packet=1G"}) {
		t.Fatalf("request = %+v", req)
	}

	archive := store.objects["prod/shop/nightly/id/dump.sql.zst"]
	if got := decompress(t, archive); got != sampleDump {
		t.Fatalf("archive = %q", got)
	}
	meta, ok := store.json["prod/shop/nightly/id/logical.json"].(objectstore.LogicalBackupMetadata)
	if !ok {
		t.Fatalf("manifest = %#v", store.json)
	}
	sum := sha256.Sum256(archive)
	if meta.SHA256 != hex.EncodeToString(sum[:]) || meta.SizeBytes != int64(len(archive)) ||
		meta.UncompressedBytes != int64(len(sampleDump)) {
		t.Fatalf("manifest sizes/checksum = %+v", meta)
	}
	if meta.FormatVersion != 1 || meta.Method != "logical" || meta.Compression != "zstd" ||
		meta.Tool != "mysqldump" || meta.Flavor != "mysql" || meta.ServerVersion != "8.4.11-11" ||
		meta.SnapshotBinlog != "mysql-bin.000001:4" || meta.SnapshotGTID != "0-1-13" ||
		!slices.Equal(meta.Databases, []string{"billing", "shop"}) || meta.InstanceName != "shop-2" ||
		meta.ArchiveKey != "prod/shop/nightly/id/dump.sql.zst" {
		t.Fatalf("manifest = %+v", meta)
	}
	if meta.CompletedAt.Before(meta.StartedAt) {
		t.Fatalf("times = %v / %v", meta.StartedAt, meta.CompletedAt)
	}
}

func TestLogicalUploadRetriesRefusalsThatClear(t *testing.T) {
	fastRetries(t, time.Minute)
	src := &sourceServer{
		refusals: []int{http.StatusServiceUnavailable, http.StatusConflict},
		body:     sampleDump,
	}
	if _, err := runAgainst(t, src); err != nil {
		t.Fatal(err)
	}
	if len(src.requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(src.requests))
	}
}

func TestLogicalUploadRefusalReasons(t *testing.T) {
	fastRetries(t, 20*time.Millisecond)
	many := func(status int) []int { return slices.Repeat([]int{status}, 1000) }
	for _, tc := range []struct {
		refusals []int
		reason   string
	}{
		{[]int{http.StatusNotFound}, backupworker.ReasonInstanceManagerOutdated},
		{[]int{http.StatusNotImplemented}, webserver.DumpReasonToolUnavailable},
		{[]int{http.StatusUnprocessableEntity}, webserver.DumpReasonInvalidRequest},
		{many(http.StatusServiceUnavailable), webserver.DumpReasonAccountMissing},
		{many(http.StatusConflict), webserver.DumpReasonInProgress},
		{[]int{http.StatusInternalServerError}, backupworker.ReasonDumpFailed},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			store, err := runAgainst(t, &sourceServer{refusals: tc.refusals, body: sampleDump})
			var f *backupworker.Failure
			if !errors.As(err, &f) || f.Reason != tc.reason {
				t.Fatalf("err = %v, want reason %s", err, tc.reason)
			}
			if !strings.Contains(err.Error(), "shop-2") {
				t.Fatalf("error does not name the instance: %v", err)
			}
			if len(store.objects) != 0 || len(store.json) != 0 {
				t.Fatalf("a refused dump uploaded something: %v %v", store.objects, store.json)
			}
		})
	}
}

func TestLogicalUploadRejectsIncompleteDumps(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  *sourceServer
	}{
		{"error trailer", &sourceServer{body: sampleDump, trailer: map[string]string{
			webserver.DumpErrorTrailer: "mysqldump failed: exit status 3",
		}}},
		{"missing footer", &sourceServer{body: "-- MySQL dump\nINSERT INTO t VALUES (1);\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := runAgainst(t, tc.src)
			var f *backupworker.Failure
			if !errors.As(err, &f) || f.Reason != backupworker.ReasonDumpFailed {
				t.Fatalf("err = %v, want DumpFailed", err)
			}
			if len(store.objects) != 0 || !slices.Contains(store.removed, "prod/shop/nightly/id/dump.sql.zst") {
				t.Fatalf("incomplete archive left behind: objects=%v removed=%v", store.objects, store.removed)
			}
			if len(store.json) != 0 {
				t.Fatalf("manifest written for an incomplete dump: %v", store.json)
			}
		})
	}
}

func TestLogicalUploadStoreFailureIsNotADumpFailure(t *testing.T) {
	store := newMemStore()
	store.uploadErr = errors.New("s3 is down")
	err := runWithStore(t, store, &sourceServer{body: sampleDump})
	var f *backupworker.Failure
	if !errors.As(err, &f) || f.Reason != "" {
		t.Fatalf("err = %v, want an upload failure without a reason", err)
	}
	if strings.Contains(err.Error(), "upload finished") {
		t.Fatalf("upload failure message leaked the copy teardown: %v", err)
	}
	if !strings.Contains(err.Error(), "s3 is down") {
		t.Fatalf("error does not name the store failure: %v", err)
	}
	if len(store.objects) != 0 || !slices.Contains(store.removed, "prod/shop/nightly/id/dump.sql.zst") {
		t.Fatalf("archive left behind: objects=%v removed=%v", store.objects, store.removed)
	}
	if len(store.json) != 0 {
		t.Fatalf("manifest written after a failed upload: %v", store.json)
	}
}

func TestLogicalUploadManifestFailureRemovesArchive(t *testing.T) {
	store := newMemStore()
	store.putJSONErr = errors.New("manifest upload failed")
	err := runWithStore(t, store, &sourceServer{body: sampleDump})
	var f *backupworker.Failure
	if !errors.As(err, &f) || f.Reason != "" {
		t.Fatalf("err = %v, want a manifest failure without a reason", err)
	}
	if len(store.objects) != 0 || !slices.Contains(store.removed, "prod/shop/nightly/id/dump.sql.zst") {
		t.Fatalf("archive left behind: objects=%v removed=%v", store.objects, store.removed)
	}
	if len(store.json) != 0 {
		t.Fatalf("manifest written for a failed upload: %v", store.json)
	}
}

func TestLogicalUploadLargeDumpKeepsFooter(t *testing.T) {
	body := strings.Repeat("INSERT INTO t VALUES (1,'x');\n", 100) + "-- Dump completed on 2026-09-25 16:29:19\n"
	store, err := runAgainst(t, &sourceServer{body: body})
	if err != nil {
		t.Fatal(err)
	}
	archive := store.objects["prod/shop/nightly/id/dump.sql.zst"]
	if got := decompress(t, archive); got != body {
		t.Fatalf("archive = %q", got)
	}
	meta, ok := store.json["prod/shop/nightly/id/logical.json"].(objectstore.LogicalBackupMetadata)
	if !ok || meta.UncompressedBytes != int64(len(body)) {
		t.Fatalf("manifest = %#v", store.json)
	}
}

func TestLogicalUploadCutStreamIsDumpFailed(t *testing.T) {
	store, err := runAgainst(t, &sourceServer{body: sampleDump, abort: true})
	var f *backupworker.Failure
	if !errors.As(err, &f) || f.Reason != backupworker.ReasonDumpFailed {
		t.Fatalf("err = %v, want DumpFailed", err)
	}
	if len(store.objects) != 0 || !slices.Contains(store.removed, "prod/shop/nightly/id/dump.sql.zst") {
		t.Fatalf("archive left behind: objects=%v removed=%v", store.objects, store.removed)
	}
}

func TestReportFailureWritesTerminationMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "termination-log")
	prev := terminationLogPath
	terminationLogPath = path
	t.Cleanup(func() { terminationLogPath = prev })

	reportFailure(errors.New("no reason"))
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("an error without a reason must not write a termination message")
	}

	reportFailure(&backupworker.Failure{Reason: webserver.DumpReasonToolUnavailable, Err: errors.New(strings.Repeat("x", 10000))})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > backupworker.MaxTerminationMessageBytes {
		t.Fatalf("termination message is %d bytes", len(raw))
	}
	var msg backupworker.TerminationMessage
	if err := json.Unmarshal(raw, &msg); err != nil || msg.Reason != webserver.DumpReasonToolUnavailable || msg.Message == "" {
		t.Fatalf("message = %+v, %v", msg, err)
	}
}

func TestUploadOptionsRejectUnknownMethod(t *testing.T) {
	opts := logicalOpts("https://x")
	opts.TLSCert, opts.TLSKey, opts.TLSCA = "c", "k", "ca"
	if err := opts.validate(); err != nil {
		t.Fatal(err)
	}
	opts.Method = "mydumper"
	if err := opts.validate(); err == nil {
		t.Fatal("expected an unknown method to be rejected")
	}
}

func TestUploadOptionsRejectCompressForALogicalBackup(t *testing.T) {
	opts := logicalOpts("https://x")
	opts.TLSCert, opts.TLSKey, opts.TLSCA = "c", "k", "ca"
	opts.Compress = true
	if err := opts.validate(); err == nil || !strings.Contains(err.Error(), "--compress") {
		t.Errorf("err = %v, want --compress rejected", err)
	}
}
