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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseEndpoint(t *testing.T) {
	t.Parallel()

	cases := []struct {
		endpoint   string
		wantHost   string
		wantSecure bool
		wantErr    bool
	}{
		{endpoint: "", wantHost: "s3.amazonaws.com", wantSecure: true},
		{endpoint: "minio.svc:9000", wantHost: "minio.svc:9000", wantSecure: true},
		{endpoint: "https://s3.example.com", wantHost: "s3.example.com", wantSecure: true},
		{endpoint: "http://minio.svc:9000", wantHost: "minio.svc:9000", wantSecure: false},
		{endpoint: "://broken", wantErr: true},
	}
	for _, tc := range cases {
		host, secure, err := parseEndpoint(tc.endpoint)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseEndpoint(%q) expected error", tc.endpoint)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseEndpoint(%q) error: %v", tc.endpoint, err)
		}
		if host != tc.wantHost || secure != tc.wantSecure {
			t.Fatalf("parseEndpoint(%q) = (%q, %t), want (%q, %t)", tc.endpoint, host, secure, tc.wantHost, tc.wantSecure)
		}
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv(EnvEndpoint, "http://minio.svc:9000")
	t.Setenv(EnvRegion, "us-east-1")
	t.Setenv(EnvSignatureVersion, "s3v2")
	t.Setenv(EnvForcePathStyle, "true")
	t.Setenv(EnvAccessKeyID, "key")
	t.Setenv(EnvSecretAccessKey, "secret")

	cfg := ConfigFromEnv()
	if cfg.Endpoint != "http://minio.svc:9000" || cfg.Region != "us-east-1" {
		t.Fatalf("endpoint/region = %q/%q", cfg.Endpoint, cfg.Region)
	}
	if !cfg.SignatureV2 {
		t.Fatal("expected signature v2")
	}
	if !cfg.ForcePathStyle {
		t.Fatal("expected force path style")
	}
	if cfg.AccessKeyID != "key" || cfg.SecretAccessKey != "secret" {
		t.Fatalf("credentials = %q/%q", cfg.AccessKeyID, cfg.SecretAccessKey)
	}
}

func TestNewClientFromConfig(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		Endpoint:        "http://minio.svc:9000",
		Region:          "us-east-1",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if client == nil || client.mc == nil {
		t.Fatal("expected initialised client")
	}
}

func TestSHA256Reader(t *testing.T) {
	t.Parallel()

	reader := NewSHA256Reader(strings.NewReader("hello world"))
	buf := make([]byte, 4)
	total := 0
	for {
		n, err := reader.Read(buf)
		total += n
		if err != nil {
			break
		}
	}
	if total != 11 {
		t.Fatalf("read %d bytes, want 11", total)
	}
	if reader.Count() != 11 {
		t.Fatalf("count = %d, want 11", reader.Count())
	}
	if got := reader.SumHex(); got != "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9" {
		t.Fatalf("sha256 = %q", got)
	}
}

// Streaming uploads of unknown length must not let the SDK size multipart
// parts from a 5TiB default: that allocates ~550MiB per part buffer in the
// backup workers. The default 64MiB part size keeps the buffer bounded; a
// client sized for a larger stream uses its own part size.
func TestPutOptionsSetsPartSize(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, "http://minio.svc:9000")
	if opts := client.putOptions("application/zstd"); opts.PartSize != 64<<20 {
		t.Fatalf("PartSize = %d, want %d", opts.PartSize, 64<<20)
	}
	client.SetUploadPartSize(256 << 20)
	if opts := client.putOptions("application/zstd"); opts.PartSize != 256<<20 {
		t.Fatalf("PartSize = %d, want %d", opts.PartSize, 256<<20)
	}
}

// The part size fits twice the expected stream in the 10000-part limit,
// within the S3 part-size bounds.
func TestUploadPartSizeFor(t *testing.T) {
	t.Parallel()

	const mib, gib, tib = int64(1 << 20), int64(1 << 30), int64(1 << 40)
	cases := []struct {
		expected int64
		want     uint64
	}{
		{expected: 0, want: 64 << 20},
		{expected: 100 * gib, want: 64 << 20},
		// 1TiB x 2 / 10000 = ~209.7MiB, rounded up to a whole MiB.
		{expected: tib, want: 210 << 20},
		{expected: 10 * tib, want: 2098 << 20},
		{expected: 100 * tib, want: 5 << 30},
	}
	for _, tc := range cases {
		got := UploadPartSizeFor(tc.expected)
		if got != tc.want {
			t.Errorf("UploadPartSizeFor(%d MiB) = %d MiB, want %d MiB", tc.expected/mib, got>>20, tc.want>>20)
		}
		if tc.expected > 0 && got < uint64(maxUploadPartSize) && got*maxUploadParts < uint64(2*tc.expected) {
			t.Errorf("UploadPartSizeFor(%d MiB) = %d MiB does not fit twice the stream", tc.expected/mib, got>>20)
		}
	}
}

// minio-go completes a streaming upload that reaches the part-count limit
// without noticing the reader still has data. The check after the upload must
// catch it, remove the short object and fail with ErrUploadTooLarge.
func TestCheckStreamEndedRemovesATruncatedObject(t *testing.T) {
	t.Parallel()

	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted = append(deleted, r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)

	err := client.checkStreamEnded(context.Background(), "backups", "demo/backup.xbstream", strings.NewReader("more"))
	if !errors.Is(err, ErrUploadTooLarge) {
		t.Fatalf("err = %v, want ErrUploadTooLarge", err)
	}
	if len(deleted) != 1 || deleted[0] != "/backups/demo/backup.xbstream" {
		t.Fatalf("deleted = %v, want the truncated object removed", deleted)
	}

	// A drained reader is a complete upload.
	deleted = nil
	if err := client.checkStreamEnded(context.Background(), "backups", "k", strings.NewReader("")); err != nil {
		t.Fatalf("drained reader: %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("a complete upload was removed: %v", deleted)
	}
}

// A store that stops sending mid-object fails the download with ErrStalled
// instead of holding it until the Job's deadline.
func TestDownloadFailsWhenTheStoreStalls(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", `"x"`)
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(bytes.Repeat([]byte("x"), 16))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	client := newTestClient(t, server.URL)
	client.stallTimeout = 200 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := client.Download(context.Background(), "backups", "dump.sql.zst", io.Discard)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrStalled) {
			t.Fatalf("err = %v, want ErrStalled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a stalled download must fail, not hang")
	}
}

func newTestClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	client, err := NewClient(Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestIsEmptyPrefix(t *testing.T) {
	t.Parallel()

	const nonEmptyBody = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>backups</Name><Prefix>demo/</Prefix><KeyCount>1</KeyCount>
  <MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>
  <Contents><Key>demo/backup-1/id/backup.xbstream</Key><Size>42</Size></Contents>
</ListBucketResult>`
	const emptyBody = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>backups</Name><Prefix>demo/</Prefix><KeyCount>0</KeyCount>
  <MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`

	cases := map[string]struct {
		body      string
		wantEmpty bool
	}{
		"non-empty": {body: nonEmptyBody, wantEmpty: false},
		"empty":     {body: emptyBody, wantEmpty: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client, err := NewClient(Config{
				Endpoint:        server.URL,
				Region:          "us-east-1",
				AccessKeyID:     "key",
				SecretAccessKey: "secret",
				ForcePathStyle:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			empty, err := client.IsEmptyPrefix(context.Background(), "backups", "demo/")
			if err != nil {
				t.Fatal(err)
			}
			if empty != tc.wantEmpty {
				t.Fatalf("IsEmptyPrefix = %t, want %t", empty, tc.wantEmpty)
			}
		})
	}
}

func TestBinlogEnvName(t *testing.T) {
	if got := BinlogEnvName(EnvBucket); got != "cnmsql_BINLOG_S3_BUCKET" {
		t.Fatalf("BinlogEnvName(%q) = %q", EnvBucket, got)
	}
	if got := BinlogEnvName(EnvSecretAccessKey); got != "cnmsql_BINLOG_S3_SECRET_ACCESS_KEY" {
		t.Fatalf("BinlogEnvName(%q) = %q", EnvSecretAccessKey, got)
	}
}

func TestBinlogConfigFromEnv(t *testing.T) {
	t.Setenv(EnvEndpoint, "http://base:9000")
	t.Setenv(EnvBucket, "base")
	if HasBinlogStoreEnv() {
		t.Fatal("no binlog env set, HasBinlogStoreEnv should be false")
	}
	t.Setenv(BinlogEnvName(EnvEndpoint), "http://archive:9000")
	t.Setenv(BinlogEnvName(EnvAccessKeyID), "archive-key")
	t.Setenv(BinlogEnvName(EnvForcePathStyle), "true")
	t.Setenv(BinlogEnvName(EnvBucket), "binlogs")
	t.Setenv(BinlogEnvName(EnvPath), "archive")

	if !HasBinlogStoreEnv() {
		t.Fatal("HasBinlogStoreEnv should be true once the binlog bucket is set")
	}
	cfg := BinlogConfigFromEnv()
	if cfg.Endpoint != "http://archive:9000" || cfg.AccessKeyID != "archive-key" || !cfg.ForcePathStyle {
		t.Fatalf("binlog config = %+v", cfg)
	}
	if store := BinlogStoreFromEnv(); store.Bucket != "binlogs" || store.Path != "archive" {
		t.Fatalf("binlog store = %+v", store)
	}
	if base := ConfigFromEnv(); base.Endpoint != "http://base:9000" {
		t.Fatalf("base config changed: %+v", base)
	}
}
