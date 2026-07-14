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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestIsNotFound(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		err  error
		want bool
	}{
		"nil":                {err: nil, want: false},
		"get missing key":    {err: minio.ErrorResponse{StatusCode: 404, Code: minio.NoSuchKey}, want: true},
		"head missing key":   {err: minio.ErrorResponse{StatusCode: 404, Code: "NotFound"}, want: true},
		"bodyless 404":       {err: minio.ErrorResponse{StatusCode: 404}, want: true},
		"missing bucket":     {err: minio.ErrorResponse{StatusCode: 404, Code: minio.NoSuchBucket}, want: true},
		"access denied":      {err: minio.ErrorResponse{StatusCode: 403, Code: "AccessDenied"}, want: false},
		"internal error":     {err: minio.ErrorResponse{StatusCode: 500, Code: "InternalError"}, want: false},
		"non-s3 error":       {err: fmt.Errorf("connection refused"), want: false},
		"slow down (retry!)": {err: minio.ErrorResponse{StatusCode: 503, Code: "SlowDown"}, want: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := isNotFound(tc.err); got != tc.want {
				t.Fatalf("isNotFound(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsUnsupportedListV2(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		err  error
		want bool
	}{
		"nil":              {err: nil, want: false},
		"gcs rejects v2":   {err: minio.ErrorResponse{StatusCode: 400, Code: "InvalidArgument"}, want: true},
		"not implemented":  {err: minio.ErrorResponse{StatusCode: 501, Code: "NotImplemented"}, want: true},
		"bad credentials":  {err: minio.ErrorResponse{StatusCode: 403, Code: "SignatureDoesNotMatch"}, want: false},
		"missing bucket":   {err: minio.ErrorResponse{StatusCode: 404, Code: minio.NoSuchBucket}, want: false},
		"transport error":  {err: fmt.Errorf("connection refused"), want: false},
		"server exploding": {err: minio.ErrorResponse{StatusCode: 500, Code: "InternalError"}, want: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := isUnsupportedListV2(tc.err); got != tc.want {
				t.Fatalf("isUnsupportedListV2(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// TestListObjectsFallsBackToV1 drives a store that answers ListObjectsV2 with a
// 400, as the GCS XML interop endpoint does, and asserts we retry the listing
// with the V1 API and latch onto it for subsequent calls.
func TestListObjectsFallsBackToV1(t *testing.T) {
	t.Parallel()

	const v1Body = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>backups</Name><Prefix>demo/</Prefix><IsTruncated>false</IsTruncated>
  <Contents><Key>demo/backup-1/metadata.json</Key><Size>42</Size></Contents>
</ListBucketResult>`
	const rejectV2 = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>InvalidArgument</Code><Message>Invalid argument: list-type</Message></Error>`

	var v2Calls, v1Calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("list-type") == "2" {
			v2Calls.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(rejectV2))
			return
		}
		v1Calls.Add(1)
		_, _ = w.Write([]byte(v1Body))
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint:        server.URL,
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatal(err)
	}

	for call := 1; call <= 2; call++ {
		objects, err := client.ListObjects(context.Background(), "backups", "demo/", true)
		if err != nil {
			t.Fatalf("call %d: %v", call, err)
		}
		if len(objects) != 1 || objects[0].Key != "demo/backup-1/metadata.json" {
			t.Fatalf("call %d: objects = %+v", call, objects)
		}
	}

	// The V2 rejection is paid once, not once per listing.
	if got := v2Calls.Load(); got != 1 {
		t.Fatalf("ListObjectsV2 attempts = %d, want 1 (the fallback must latch)", got)
	}
	if got := v1Calls.Load(); got != 2 {
		t.Fatalf("ListObjects V1 calls = %d, want 2", got)
	}
}

// TestListObjectsPropagatesRealErrors makes sure the V1 fallback does not paper
// over a credentials problem: a 403 must surface, not be retried as V1.
func TestListObjectsPropagatesRealErrors(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(
			`<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>denied</Message></Error>`))
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint:        server.URL,
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		ForcePathStyle:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListObjects(context.Background(), "backups", "demo/", true); err == nil {
		t.Fatal("expected the access-denied error to surface")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1 (a 403 must not be retried as a V1 listing)", got)
	}
}

// TestUploadAppliesWriteOptions asserts the SSE and storage-class settings reach
// the wire; they used to be accepted in the API and silently dropped.
func TestUploadAppliesWriteOptions(t *testing.T) {
	t.Parallel()

	var sse, keyID, storageClass string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sse = r.Header.Get("X-Amz-Server-Side-Encryption")
		keyID = r.Header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id")
		storageClass = r.Header.Get("X-Amz-Storage-Class")
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint:             server.URL,
		AccessKeyID:          "key",
		SecretAccessKey:      "secret",
		ForcePathStyle:       true,
		ServerSideEncryption: "aws:kms:my-key",
		StorageClass:         "STANDARD_IA",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = client.PutJSON(context.Background(), "backups", "demo/metadata.json", map[string]string{"a": "b"})
	if err != nil {
		t.Fatal(err)
	}
	if sse != "aws:kms" {
		t.Fatalf("sse header = %q, want aws:kms", sse)
	}
	if keyID != "my-key" {
		t.Fatalf("kms key header = %q, want my-key", keyID)
	}
	if storageClass != "STANDARD_IA" {
		t.Fatalf("storage class header = %q, want STANDARD_IA", storageClass)
	}
}

func TestParseServerSideEncryption(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		algorithm string
		wantNil   bool
		wantErr   bool
	}{
		"unset":       {algorithm: "", wantNil: true},
		"sse-s3":      {algorithm: "AES256"},
		"sse-s3 alt":  {algorithm: "aws:s3"},
		"kms default": {algorithm: "aws:kms"},
		"kms key":     {algorithm: "aws:kms:abc-123"},
		"garbage":     {algorithm: "rot13", wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sse, err := parseServerSideEncryption(tc.algorithm)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseServerSideEncryption(%q) expected an error", tc.algorithm)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (sse == nil) != tc.wantNil {
				t.Fatalf("parseServerSideEncryption(%q) = %v, wantNil=%t", tc.algorithm, sse, tc.wantNil)
			}
		})
	}
}

func TestSigningRegion(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		region   string
		endpoint string
		want     string
	}{
		"explicit wins":  {region: "eu-west-3", endpoint: "s3.amazonaws.com", want: "eu-west-3"},
		"r2 signs auto":  {endpoint: "abc123.r2.cloudflarestorage.com", want: "auto"},
		"r2 with port":   {endpoint: "abc123.r2.cloudflarestorage.com:443", want: "auto"},
		"minio default":  {endpoint: "minio.svc:9000", want: "us-east-1"},
		"not really r2":  {endpoint: "r2.cloudflarestorage.com.evil.tld", want: "us-east-1"},
		"empty endpoint": {endpoint: "s3.amazonaws.com", want: "us-east-1"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := signingRegion(tc.region, tc.endpoint); got != tc.want {
				t.Fatalf("signingRegion(%q, %q) = %q, want %q", tc.region, tc.endpoint, got, tc.want)
			}
		})
	}
}

func TestNewClientRejectsUnusableCABundle(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Config{
		Endpoint:        "https://s3.example.com",
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
		CABundle:        "not a pem",
	})
	if err == nil {
		t.Fatal("expected a CA bundle without a valid certificate to be rejected")
	}
}
