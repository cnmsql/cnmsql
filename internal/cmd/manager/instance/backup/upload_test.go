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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/backupworker"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// A source that stops sending mid-upload must fail the backup as
// SourceStalled instead of hanging until the Job's active deadline.
func TestPhysicalUploadSourceStallIsSourceStalled(t *testing.T) {
	fastStall(t, 50*time.Millisecond)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "xtrabackup archive chunk\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Hang until the worker gives up on the stream (its watchdog cancels
		// the upload context) or the test server is torn down.
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)

	store := newMemStore()
	opts := logicalOpts(srv.URL)
	opts.Method = methodXtrabackup
	err := runPhysicalUpload(context.Background(), opts, store, srv.Client())
	var f *backupworker.Failure
	if !errors.As(err, &f) || f.Reason != backupworker.ReasonSourceStalled {
		t.Fatalf("err = %v, want SourceStalled", err)
	}
	if !strings.Contains(err.Error(), "no bytes moved") {
		t.Fatalf("error does not describe the stall: %v", err)
	}
	if len(store.json) != 0 {
		t.Fatalf("manifest written for a stalled upload: %v", store.json)
	}
}

// An object store that stops accepting bytes fails the backup as
// ObjectStoreStalled.
func TestPhysicalUploadStoreStallIsObjectStoreStalled(t *testing.T) {
	fastStall(t, 50*time.Millisecond)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strings.Repeat("xtrabackup archive chunk\n", 50000))
	}))
	t.Cleanup(srv.Close)

	store := newMemStore()
	store.hang = true
	opts := logicalOpts(srv.URL)
	opts.Method = methodXtrabackup
	err := runPhysicalUpload(context.Background(), opts, store, srv.Client())
	var f *backupworker.Failure
	if !errors.As(err, &f) || f.Reason != backupworker.ReasonObjectStoreStalled {
		t.Fatalf("err = %v, want ObjectStoreStalled", err)
	}
	if len(store.json) != 0 {
		t.Fatalf("manifest written for a stalled upload: %v", store.json)
	}
}

// An archive that outgrew its multipart part size fails as ArchiveTooLarge
// with no manifest, instead of recording a truncated backup as complete.
func TestPhysicalUploadTooLargeIsArchiveTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "xtrabackup archive chunk\n")
	}))
	t.Cleanup(srv.Close)

	store := newMemStore()
	store.uploadErr = fmt.Errorf("uploading s3://b/k: %w", objectstore.ErrUploadTooLarge)
	opts := logicalOpts(srv.URL)
	opts.Method = methodXtrabackup
	err := runPhysicalUpload(context.Background(), opts, store, srv.Client())
	var f *backupworker.Failure
	if !errors.As(err, &f) || f.Reason != backupworker.ReasonArchiveTooLarge {
		t.Fatalf("err = %v, want ArchiveTooLarge", err)
	}
	if len(store.json) != 0 {
		t.Fatalf("manifest written for a truncated upload: %v", store.json)
	}
}

// A healthy source and store upload end to end.
func TestPhysicalUploadEndToEnd(t *testing.T) {
	fastStall(t, time.Minute)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Trailer", "x-foo")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "xtrabackup archive chunk\n")
	}))
	t.Cleanup(srv.Close)

	store := newMemStore()
	opts := logicalOpts(srv.URL)
	opts.Method = methodXtrabackup
	if err := runPhysicalUpload(context.Background(), opts, store, srv.Client()); err != nil {
		t.Fatal(err)
	}
	if string(store.objects["prod/shop/nightly/id/dump.sql.zst"]) != "xtrabackup archive chunk\n" {
		t.Fatalf("archive = %q", store.objects["prod/shop/nightly/id/dump.sql.zst"])
	}
	meta, ok := store.json["prod/shop/nightly/id/logical.json"].(objectstore.BackupMetadata)
	if !ok || meta.SizeBytes != int64(len("xtrabackup archive chunk\n")) || meta.Method != methodXtrabackup {
		t.Fatalf("manifest = %#v", store.json["prod/shop/nightly/id/logical.json"])
	}
}
