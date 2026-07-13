//go:build conformance

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

// Package s3 holds the object-store conformance suite: the operations cnmsql's
// backup, archiving, recovery and retention paths perform, run against a real
// endpoint so a provider can be qualified before anyone trusts their backups to
// it.
//
// It is opt-in — the suite is behind the `conformance` build tag and reads its
// destination from the same cnmsql_S3_* environment the backup workers get:
//
//	export cnmsql_S3_ENDPOINT=https://s3.example.com
//	export cnmsql_S3_BUCKET=cnmsql-conformance
//	export cnmsql_S3_ACCESS_KEY_ID=... cnmsql_S3_SECRET_ACCESS_KEY=...
//	make test-s3-conformance
//
// Every object it writes lives under a unique prefix that is removed on the way
// out, so it is safe to point at a real (empty) bucket. Each operation is its
// own subtest: a provider that fails only one tells you exactly which cnmsql
// feature it cannot support.
package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
)

// streamedSize is the payload used for the unknown-length upload. It is over the
// 5 MiB minimum part size, so the SDK is forced through a real multipart upload
// — the path a base backup takes, and the one providers most often get wrong.
const streamedSize = 6 << 20

// harness is a client bound to a throwaway prefix in the target bucket.
type harness struct {
	client *objectstore.Client
	bucket string
	prefix string
}

// newHarness builds the client from the cnmsql_S3_* environment, skipping the
// suite when no destination is configured.
func newHarness(t *testing.T) *harness {
	t.Helper()

	bucket := os.Getenv(objectstore.EnvBucket)
	if bucket == "" {
		t.Skipf("set %s (and the other cnmsql_S3_* variables) to run the object-store conformance suite",
			objectstore.EnvBucket)
	}

	client, err := objectstore.NewClient(objectstore.ConfigFromEnv())
	if err != nil {
		t.Fatalf("building the object-store client: %v", err)
	}

	prefix := fmt.Sprintf("%sconformance-%d/", keyPrefix(), time.Now().UnixNano())
	h := &harness{client: client, bucket: bucket, prefix: prefix}
	t.Cleanup(func() {
		if err := client.RemovePrefix(context.Background(), bucket, prefix); err != nil {
			t.Errorf("cleaning up s3://%s/%s: %v", bucket, prefix, err)
		}
	})
	return h
}

// keyPrefix returns the configured path with a trailing slash, or "".
func keyPrefix() string {
	path := os.Getenv(objectstore.EnvPath)
	if path == "" {
		return ""
	}
	return path + "/"
}

func (h *harness) key(name string) string { return h.prefix + name }

// TestConformance runs the operator's object-store operations against the
// configured endpoint. The subtests are ordered as a backup's life is: write,
// read back, list, then expire.
func TestConformance(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// The empty-archive guard: a fresh cluster refuses to adopt a destination
	// that already holds someone else's backups, so this must answer honestly on
	// a prefix nothing has been written to yet.
	t.Run("IsEmptyPrefix on a fresh prefix", func(t *testing.T) {
		empty, err := h.client.IsEmptyPrefix(ctx, h.bucket, h.prefix)
		if err != nil {
			t.Fatal(err)
		}
		if !empty {
			t.Fatal("a prefix nothing has been written to reports as non-empty")
		}
	})

	// Manifests (backup metadata, binlog manifests, the archive index) are
	// small JSON objects written with a known length.
	t.Run("PutJSON and GetJSON round-trip", func(t *testing.T) {
		want := map[string]string{"backupID": "abc-123"}
		if err := h.client.PutJSON(ctx, h.bucket, h.key("metadata.json"), want); err != nil {
			t.Fatal(err)
		}
		var got map[string]string
		if err := h.client.GetJSON(ctx, h.bucket, h.key("metadata.json"), &got); err != nil {
			t.Fatal(err)
		}
		if got["backupID"] != want["backupID"] {
			t.Fatalf("round-tripped %v, want %v", got, want)
		}
	})

	// The archive index is rewritten in place on every retention pass, so the
	// store must accept overwriting an existing key.
	t.Run("PutJSON overwrites an existing key", func(t *testing.T) {
		if err := h.client.PutJSON(ctx, h.bucket, h.key("metadata.json"),
			map[string]string{"backupID": "def-456"}); err != nil {
			t.Fatal(err)
		}
		var got map[string]string
		if err := h.client.GetJSON(ctx, h.bucket, h.key("metadata.json"), &got); err != nil {
			t.Fatal(err)
		}
		if got["backupID"] != "def-456" {
			t.Fatalf("overwrite did not take: %v", got)
		}
	})

	// A base backup is streamed straight out of xtrabackup, so its length is
	// unknown up front: the SDK uploads it as a multipart of unknown total size.
	payload := make([]byte, streamedSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	t.Run("Upload of unknown length (multipart)", func(t *testing.T) {
		if err := h.client.Upload(ctx, h.bucket, h.key("backup.xbstream"),
			bytes.NewReader(payload), -1, ""); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Download returns the bytes that were uploaded", func(t *testing.T) {
		var buf bytes.Buffer
		n, err := h.client.Download(ctx, h.bucket, h.key("backup.xbstream"), &buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(payload)) {
			t.Fatalf("downloaded %d bytes, want %d", n, len(payload))
		}
		if !bytes.Equal(buf.Bytes(), payload) {
			t.Fatal("downloaded bytes differ from what was uploaded")
		}
	})

	t.Run("Exists distinguishes present from absent", func(t *testing.T) {
		present, err := h.client.Exists(ctx, h.bucket, h.key("backup.xbstream"))
		if err != nil {
			t.Fatal(err)
		}
		if !present {
			t.Fatal("an object that was just uploaded reports as absent")
		}
		// A miss must be reported as (false, nil): recovery and retention both
		// probe for objects that may legitimately not be there, and a store that
		// turns this into an error stalls them.
		absent, err := h.client.Exists(ctx, h.bucket, h.key("does-not-exist.json"))
		if err != nil {
			t.Fatalf("probing a missing object must not error: %v", err)
		}
		if absent {
			t.Fatal("a missing object reports as present")
		}
	})

	// Retention walks the cluster prefix recursively to find manifests, and the
	// non-recursive (delimiter) walk is how a cluster's backup directories are
	// enumerated without reading every leaf.
	t.Run("ListObjects recursive and delimited", func(t *testing.T) {
		if err := h.client.PutJSON(ctx, h.bucket, h.key("nested/deep/manifest.json"),
			map[string]string{"a": "b"}); err != nil {
			t.Fatal(err)
		}
		recursive, err := h.client.ListObjects(ctx, h.bucket, h.prefix, true)
		if err != nil {
			t.Fatal(err)
		}
		if !containsKey(recursive, h.key("nested/deep/manifest.json")) {
			t.Fatalf("recursive listing missed the nested object: %v", keys(recursive))
		}
		delimited, err := h.client.ListObjects(ctx, h.bucket, h.prefix, false)
		if err != nil {
			t.Fatal(err)
		}
		if containsKey(delimited, h.key("nested/deep/manifest.json")) {
			t.Fatalf("delimited listing descended into the nested prefix: %v", keys(delimited))
		}
	})

	// Deleting an object that is already gone happens whenever a retention pass
	// is interrupted and re-runs; it has to be a no-op, not an error.
	t.Run("Remove is idempotent", func(t *testing.T) {
		if err := h.client.Remove(ctx, h.bucket, h.key("does-not-exist.json")); err != nil {
			t.Fatalf("removing an absent object must succeed: %v", err)
		}
	})

	// Expiring a base backup drops its whole directory. No provider supports
	// deleting a prefix in one call, so this expands to per-object deletes —
	// the cleanup path the issue that prompted this suite was about.
	t.Run("RemovePrefix empties the prefix", func(t *testing.T) {
		if err := h.client.RemovePrefix(ctx, h.bucket, h.prefix); err != nil {
			t.Fatal(err)
		}
		empty, err := h.client.IsEmptyPrefix(ctx, h.bucket, h.prefix)
		if err != nil {
			t.Fatal(err)
		}
		if !empty {
			remaining, _ := h.client.ListObjects(ctx, h.bucket, h.prefix, true)
			t.Fatalf("objects survived the prefix removal: %v", keys(remaining))
		}
	})
}

func containsKey(objects []objectstore.ObjectInfo, key string) bool {
	for _, object := range objects {
		if object.Key == key {
			return true
		}
	}
	return false
}

func keys(objects []objectstore.ObjectInfo) []string {
	out := make([]string, 0, len(objects))
	for _, object := range objects {
		out = append(out, object.Key)
	}
	return out
}
