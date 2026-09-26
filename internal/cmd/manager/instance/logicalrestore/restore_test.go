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

package logicalrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

const testStream = "-- MySQL dump 10.13\n" +
	"/*!40101 SET NAMES utf8mb4 */;\n" +
	"-- Current Database: `shop`\n" +
	"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `shop`;\n" +
	"-- Current Database: `billing`\n" +
	"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `billing`;\n" +
	"-- Dump completed on 2026-09-26 10:00:00\n"

// memStore is an object store holding one dump and its manifest.
type memStore struct {
	objects map[string][]byte
	// failDownload makes every dump download fail.
	failDownload bool
}

func (m *memStore) GetJSON(_ context.Context, _, key string, v any) error {
	b, ok := m.objects[key]
	if !ok {
		return fmt.Errorf("%s: not found", key)
	}
	return json.Unmarshal(b, v)
}

func (m *memStore) Download(_ context.Context, _, key string, w io.Writer) (int64, error) {
	if m.failDownload {
		return 0, errors.New("store: connection reset")
	}
	b, ok := m.objects[key]
	if !ok {
		return 0, fmt.Errorf("%s: not found", key)
	}
	n, err := w.Write(b)
	return int64(n), err
}

func compress(t *testing.T, s string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := objectstore.NewZstdWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(zw, s); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// newStore holds stream, compressed, with a manifest. mutate edits the
// manifest after its checksum is set.
func newStore(t *testing.T, stream string, mutate func(*objectstore.LogicalBackupMetadata)) *memStore {
	t.Helper()
	dump := compress(t, stream)
	sum := sha256.Sum256(dump)
	meta := objectstore.LogicalBackupMetadata{
		FormatVersion: objectstore.LogicalFormatVersion,
		BackupID:      "b-1",
		Flavor:        "mysql",
		Compression:   objectstore.LogicalCompressionZstd,
		Databases:     []string{"billing", "shop"},
		SizeBytes:     int64(len(dump)),
		SHA256:        hex.EncodeToString(sum[:]),
	}
	if mutate != nil {
		mutate(&meta)
	}
	manifest, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	return &memStore{objects: map[string][]byte{"d/dump.sql.zst": dump, "d/logical.json": manifest}}
}

// fakeTarget stands in for the instance's POST /cluster/load.
type fakeTarget struct {
	mu       sync.Mutex
	query    map[string][]string
	body     []byte
	bodyErr  error
	requests int
	// refuse answers with this status and reason without reading the body.
	refuseStatus int
	refuseReason string
}

func (f *fakeTarget) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	f.query = r.URL.Query()
	if f.refuseStatus != 0 {
		w.WriteHeader(f.refuseStatus)
		if f.refuseReason != "" {
			_ = json.NewEncoder(w).Encode(webserver.ReasonErrorBody{Reason: f.refuseReason, Error: "refused"})
		}
		return
	}
	f.body, f.bodyErr = io.ReadAll(r.Body)
	if f.bodyErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(webserver.ReasonErrorBody{Reason: webserver.LoadReasonFailed, Error: f.bodyErr.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(webserver.LoadResult{Databases: f.query[webserver.LoadDatabaseParam], Bytes: int64(len(f.body))})
}

func testOptions(url string) options {
	return options{
		TargetURL:    url + "/cluster/load",
		InstanceName: "shop-1",
		Bucket:       "backups",
		DumpKey:      "d/dump.sql.zst",
		ManifestKey:  "d/logical.json",
		Databases:    []string{"shop"},
		Policy:       webserver.LoadPolicyFailIfExists,
		Flavor:       "mysql",
	}
}

func runAgainst(t *testing.T, target *fakeTarget, st store, mutate func(*options)) error {
	t.Helper()
	srv := httptest.NewServer(target)
	t.Cleanup(srv.Close)
	opts := testOptions(srv.URL)
	if mutate != nil {
		mutate(&opts)
	}
	client := &http.Client{Transport: &http.Transport{ExpectContinueTimeout: time.Minute}}
	return run(context.Background(), opts, st, client)
}

func assertFailure(t *testing.T, err error, reason string, wantUnchanged bool) {
	t.Helper()
	var f *backupworker.Failure
	if !errors.As(err, &f) {
		t.Fatalf("err = %v, want a failure with reason %s", err, reason)
	}
	if f.Reason != reason || f.Unchanged != wantUnchanged {
		t.Fatalf("failure = %s (unchanged %v): %v; want %s (unchanged %v)", f.Reason, f.Unchanged, f.Err, reason, wantUnchanged)
	}
}

func TestRestoreStreamsTheSelectedDatabases(t *testing.T) {
	target := &fakeTarget{}
	if err := runAgainst(t, target, newStore(t, testStream, nil), nil); err != nil {
		t.Fatal(err)
	}
	body := string(target.body)
	if !strings.Contains(body, "`shop`;") || strings.Contains(body, "`billing`;") ||
		!strings.HasPrefix(body, "-- MySQL dump") {
		t.Errorf("target received:\n%s", body)
	}
	if !slices.Equal(target.query[webserver.LoadDatabaseParam], []string{"shop"}) ||
		target.query[webserver.LoadPolicyParam][0] != webserver.LoadPolicyFailIfExists {
		t.Errorf("query = %v", target.query)
	}
}

func TestRestoreChecksTheManifestBeforeCallingTheTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		st     *memStore
		opts   func(*options)
		reason string
	}{
		{"a database not in the dump", newStore(t, testStream, nil),
			func(o *options) { o.Databases = []string{"crm"} }, backupworker.ReasonIncompatible},
		{"another flavor", newStore(t, testStream, func(m *objectstore.LogicalBackupMetadata) { m.Flavor = "mariadb" }),
			nil, backupworker.ReasonIncompatible},
		{"no manifest", &memStore{objects: map[string][]byte{}}, nil, backupworker.ReasonDownloadFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := &fakeTarget{}
			err := runAgainst(t, target, tc.st, tc.opts)
			assertFailure(t, err, tc.reason, true)
			if target.requests != 0 {
				t.Error("the target was called")
			}
		})
	}
}

func TestRestoreReportsTheTargetsRefusal(t *testing.T) {
	for _, tc := range []struct {
		status        int
		reason        string
		wantReason    string
		wantUnchanged bool
	}{
		{http.StatusConflict, webserver.LoadReasonDatabaseNotEmpty, webserver.LoadReasonDatabaseNotEmpty, true},
		{http.StatusConflict, webserver.LoadReasonNotPrimary, webserver.LoadReasonNotPrimary, true},
		{http.StatusNotFound, "", backupworker.ReasonInstanceManagerOutdated, true},
		{http.StatusInternalServerError, webserver.LoadReasonFailed, webserver.LoadReasonFailed, false},
		{http.StatusBadGateway, "", webserver.LoadReasonFailed, false},
	} {
		t.Run(fmt.Sprintf("%d %s", tc.status, tc.reason), func(t *testing.T) {
			target := &fakeTarget{refuseStatus: tc.status, refuseReason: tc.reason}
			err := runAgainst(t, target, newStore(t, testStream, nil), nil)
			assertFailure(t, err, tc.wantReason, tc.wantUnchanged)
		})
	}
}

// A dump that fails its checks aborts the request instead of ending it, so the
// target never sees a clean end of input and never runs a cut statement.
func TestRestoreAbortsACorruptDump(t *testing.T) {
	for _, tc := range []struct {
		name   string
		st     *memStore
		reason string
	}{
		{"checksum mismatch", newStore(t, testStream, func(m *objectstore.LogicalBackupMetadata) { m.SHA256 = "00" }),
			backupworker.ReasonDumpCorrupt},
		{"no footer", newStore(t, strings.TrimSuffix(testStream, "-- Dump completed on 2026-09-26 10:00:00\n"), nil),
			backupworker.ReasonDumpCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := &fakeTarget{}
			err := runAgainst(t, target, tc.st, nil)
			assertFailure(t, err, tc.reason, false)
			target.mu.Lock()
			defer target.mu.Unlock()
			if target.bodyErr == nil {
				t.Error("the target read a clean end of input")
			}
		})
	}
}

func TestRestoreReportsAnUnreachableTargetAsUnchanged(t *testing.T) {
	opts := testOptions("http://127.0.0.1:1")
	client := &http.Client{Transport: &http.Transport{ExpectContinueTimeout: time.Minute}}
	err := run(context.Background(), opts, newStore(t, testStream, nil), client)
	assertFailure(t, err, backupworker.ReasonTargetUnreachable, true)
}

func TestReportFailureWritesUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "termination-log")
	prev := terminationLogPath
	terminationLogPath = path
	t.Cleanup(func() { terminationLogPath = prev })

	reportFailure(unchanged(webserver.LoadReasonDatabaseNotEmpty, errors.New("shop holds tables")))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var msg backupworker.TerminationMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Reason != webserver.LoadReasonDatabaseNotEmpty || !msg.Unchanged || msg.Message != "shop holds tables" {
		t.Errorf("message = %+v", msg)
	}
}

// A refusal decides the outcome even when the download failed meanwhile: the
// instance changed nothing.
func TestRestoreRefusalWinsOverAFailedDownload(t *testing.T) {
	st := newStore(t, testStream, nil)
	st.failDownload = true
	target := &fakeTarget{refuseStatus: http.StatusConflict, refuseReason: webserver.LoadReasonNotPrimary}
	err := runAgainst(t, target, st, nil)
	assertFailure(t, err, webserver.LoadReasonNotPrimary, true)
}

// A TLS handshake that fails (wrong CA, wrong server name) never reaches the
// instance's handler, so nothing was changed.
func TestRestoreReportsATLSFailureAsUnchanged(t *testing.T) {
	srv := httptest.NewTLSServer(&fakeTarget{})
	t.Cleanup(srv.Close)
	client := &http.Client{Transport: &http.Transport{ExpectContinueTimeout: time.Minute}}
	err := run(context.Background(), testOptions(srv.URL), newStore(t, testStream, nil), client)
	assertFailure(t, err, backupworker.ReasonTargetUnreachable, true)
}
