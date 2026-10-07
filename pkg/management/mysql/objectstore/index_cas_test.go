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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// condS3 is a one-object S3 that honours (or, when unsupported, rejects with
// 501) If-Match / If-None-Match on PUT, and records the condition headers.
type condS3 struct {
	mu          sync.Mutex
	body        []byte
	etag        int
	unsupported bool
	conditions  []string
}

func (s *condS3) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if s.body == nil {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, s.etag))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprint(len(s.body)))
		_, _ = w.Write(s.body)
	case http.MethodPut:
		ifMatch, ifNone := r.Header.Get("If-Match"), r.Header.Get("If-None-Match")
		s.conditions = append(s.conditions, "match="+ifMatch+" none="+ifNone)
		if s.unsupported && (ifMatch != "" || ifNone != "") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = io.WriteString(w, `<Error><Code>NotImplemented</Code><Message>no</Message></Error>`)
			return
		}
		current := fmt.Sprintf(`"v%d"`, s.etag)
		if (ifMatch != "" && (s.body == nil || ifMatch != current)) || (ifNone == "*" && s.body != nil) {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>changed</Message></Error>`)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		s.body = decodeAWSChunked(r, raw)
		s.etag++
		w.Header().Set("ETag", fmt.Sprintf(`"v%d"`, s.etag))
	}
}

// decodeAWSChunked strips the aws-chunked framing minio-go uses for signed
// streaming uploads over plain HTTP ("<hex size>;chunk-signature=...\r\n<data>\r\n").
func decodeAWSChunked(r *http.Request, raw []byte) []byte {
	if !strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return raw
	}
	var out []byte
	for len(raw) > 0 {
		line, rest, ok := strings.Cut(string(raw), "\r\n")
		if !ok {
			break
		}
		sizeHex, _, _ := strings.Cut(line, ";")
		var size int
		if _, err := fmt.Sscanf(sizeHex, "%x", &size); err != nil || size == 0 {
			break
		}
		out = append(out, rest[:size]...)
		raw = []byte(rest[size+2:])
	}
	return out
}

func condClient(t *testing.T, s *condS3) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(s.handler))
	t.Cleanup(server.Close)
	return testClient(t, server.URL)
}

func TestPutJSONIfCreatesOnlyWhenAbsentAndReplacesOnlyWhenUnchanged(t *testing.T) {
	t.Parallel()
	s := &condS3{}
	c := condClient(t, s)
	ctx := context.Background()

	if err := c.PutJSONIf(ctx, "bucket", "key", map[string]int{"a": 1}, ""); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.PutJSONIf(ctx, "bucket", "key", map[string]int{"a": 2}, ""); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("a second create must lose: %v", err)
	}
	var got map[string]int
	etag, found, err := c.GetJSONVersion(ctx, "bucket", "key", &got)
	if err != nil || !found || got["a"] != 1 || etag == "" {
		t.Fatalf("GetJSONVersion = %q %v %v %v", etag, found, got, err)
	}
	if err := c.PutJSONIf(ctx, "bucket", "key", map[string]int{"a": 3}, etag); err != nil {
		t.Fatalf("replace at the read version: %v", err)
	}
	if err := c.PutJSONIf(ctx, "bucket", "key", map[string]int{"a": 4}, etag); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("a replace at a stale version must lose: %v", err)
	}
	if !strings.Contains(s.conditions[0], "none=*") || !strings.Contains(s.conditions[2], `match="`+etag+`"`) {
		t.Fatalf("conditions sent = %v", s.conditions)
	}
}

func TestGetJSONVersionReportsAMissingObject(t *testing.T) {
	t.Parallel()
	c := condClient(t, &condS3{})
	var v map[string]int
	etag, found, err := c.GetJSONVersion(context.Background(), "bucket", "key", &v)
	if err != nil || found || etag != "" {
		t.Fatalf("GetJSONVersion = %q %v %v", etag, found, err)
	}
}

// A store that does not implement conditional PUTs gets today's unconditional
// write, and is not asked again.
func TestPutJSONIfFallsBackWithoutConditionalWrites(t *testing.T) {
	t.Parallel()
	s := &condS3{unsupported: true}
	c := condClient(t, s)
	ctx := context.Background()
	if err := c.PutJSONIf(ctx, "bucket", "key", map[string]int{"a": 1}, ""); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := c.PutJSONIf(ctx, "bucket", "key", map[string]int{"a": 2}, "stale"); err != nil {
		t.Fatalf("second write: %v", err)
	}
	want := []string{"match= none=*", "match= none=", "match= none="}
	if strings.Join(s.conditions, "|") != strings.Join(want, "|") {
		t.Fatalf("conditions = %v, want %v", s.conditions, want)
	}
}

// memVersioned is an in-memory VersionedStore whose beforePut hook lets a test
// play a concurrent writer between a read and the write that follows it.
type memVersioned struct {
	body      []byte
	version   int
	beforePut func()
	puts      int
}

func (m *memVersioned) GetJSONVersion(_ context.Context, _, _ string, v any) (string, bool, error) {
	if m.body == nil {
		return "", false, nil
	}
	return fmt.Sprint(m.version), true, json.Unmarshal(m.body, v)
}

func (m *memVersioned) PutJSONIf(_ context.Context, _, _ string, v any, etag string) error {
	if m.beforePut != nil {
		hook := m.beforePut
		m.beforePut = nil
		hook()
	}
	if (etag == "" && m.body != nil) || (etag != "" && etag != fmt.Sprint(m.version)) {
		return ErrPreconditionFailed
	}
	m.puts++
	m.version++
	m.body, _ = json.Marshal(v)
	return nil
}

func (m *memVersioned) set(idx ArchiveIndex) {
	m.body, _ = json.Marshal(idx)
	m.version++
}

func (m *memVersioned) index(t *testing.T) ArchiveIndex {
	t.Helper()
	var idx ArchiveIndex
	if err := json.Unmarshal(m.body, &idx); err != nil {
		t.Fatal(err)
	}
	return idx
}

func addSegment(uuid string) func(*ArchiveIndex, bool) (bool, error) {
	return func(idx *ArchiveIndex, _ bool) (bool, error) {
		idx.Segments = append(idx.Segments, ArchiveSegment{ServerUUID: uuid})
		return true, nil
	}
}

// A writer that loses the race re-reads the index and re-applies its change to
// what the winner wrote, so neither change is lost.
func TestUpdateArchiveIndexRebasesALostRace(t *testing.T) {
	t.Parallel()
	m := &memVersioned{}
	m.set(ArchiveIndex{Segments: []ArchiveSegment{{ServerUUID: "a"}}})
	m.beforePut = func() { m.set(ArchiveIndex{Segments: []ArchiveSegment{{ServerUUID: "a"}, {ServerUUID: "winner"}}}) }

	if err := UpdateArchiveIndex(context.Background(), m, "bucket", "key", addSegment("mine")); err != nil {
		t.Fatal(err)
	}
	idx := m.index(t)
	got := make([]string, 0, len(idx.Segments))
	for _, seg := range idx.Segments {
		got = append(got, seg.ServerUUID)
	}
	if strings.Join(got, ",") != "a,winner,mine" {
		t.Fatalf("segments = %v, want both writers' changes", got)
	}
}

func TestUpdateArchiveIndexCreatesAndSkips(t *testing.T) {
	t.Parallel()
	m := &memVersioned{}
	if err := UpdateArchiveIndex(context.Background(), m, "bucket", "key", addSegment("first")); err != nil {
		t.Fatal(err)
	}
	if len(m.index(t).Segments) != 1 {
		t.Fatal("an absent index must be created")
	}
	noop := func(*ArchiveIndex, bool) (bool, error) { return false, nil }
	before := m.puts
	if err := UpdateArchiveIndex(context.Background(), m, "bucket", "key", noop); err != nil || m.puts != before {
		t.Fatalf("a mutation that changes nothing must not write (%v, %d writes)", err, m.puts-before)
	}
	boom := errors.New("boom")
	failing := func(*ArchiveIndex, bool) (bool, error) { return false, boom }
	err := UpdateArchiveIndex(context.Background(), m, "bucket", "key", failing)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

// Endless contention gives up with an error instead of spinning.
func TestUpdateArchiveIndexGivesUpUnderContention(t *testing.T) {
	t.Parallel()
	m := &memVersioned{}
	m.set(ArchiveIndex{})
	var rival func()
	rival = func() { m.set(ArchiveIndex{}); m.beforePut = rival }
	m.beforePut = rival
	err := UpdateArchiveIndex(context.Background(), m, "bucket", "key", addSegment("mine"))
	if !errors.Is(err, ErrIndexContention) {
		t.Fatalf("err = %v, want ErrIndexContention", err)
	}
}

// The race the fork records cared about: a primary folding a file into an
// index it read before retention dropped a forked segment must not bring that
// segment back.
func TestApplyBinlogExpiryRebasesOnAConcurrentWrite(t *testing.T) {
	t.Parallel()
	m := &memVersioned{}
	m.set(ArchiveIndex{Segments: []ArchiveSegment{
		{ServerUUID: "old", Binlogs: []string{"binlog.000001"}, Fork: &ArchiveFork{GTIDSet: "u:219"}},
		{ServerUUID: "new", Binlogs: []string{"binlog.000001"}},
	}})
	plan := RetentionPlan{DeletedBinlogs: map[string]map[string]struct{}{"old": {"binlog.000001": {}}}}
	// The primary archives binlog.000002 while retention runs.
	m.beforePut = func() {
		idx := m.index(t)
		idx.Segments[1].Binlogs = append(idx.Segments[1].Binlogs, "binlog.000002")
		m.set(idx)
	}
	if err := rewriteArchiveIndex(context.Background(), m, "bucket", "key", plan); err != nil {
		t.Fatal(err)
	}
	idx := m.index(t)
	if len(idx.Segments) != 1 || idx.Segments[0].ServerUUID != "new" || len(idx.Segments[0].Binlogs) != 2 {
		t.Fatalf("index = %+v, want the forked segment gone and the primary's new file kept", idx)
	}
	// And the primary, rebasing on retention's write, cannot resurrect it.
	m.beforePut = func() {}
	if err := UpdateArchiveIndex(context.Background(), m, "bucket", "key", func(idx *ArchiveIndex, _ bool) (bool, error) {
		idx.Segments[0].Binlogs = append(idx.Segments[0].Binlogs, "binlog.000003")
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := m.index(t); len(got.Segments) != 1 {
		t.Fatalf("the dropped segment came back: %+v", got)
	}
}
