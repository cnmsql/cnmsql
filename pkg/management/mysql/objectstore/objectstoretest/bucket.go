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

// Package objectstoretest serves an in-memory S3 bucket for tests.
package objectstoretest

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// modTime is every object's modification time.
var modTime = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// Bucket is one path-style S3 bucket. It answers ListObjectsV2, GetObject and
// HeadObject, which is what reading backups needs. Any other request gets a
// 404.
type Bucket struct {
	Name string

	mu      sync.Mutex
	objects map[string][]byte
}

// NewServer starts a server holding one bucket with the given objects and
// stops it when the test ends. Clients reach it at the returned server's URL
// with path-style addressing.
func NewServer(t testing.TB, bucket string, objects map[string][]byte) (*httptest.Server, *Bucket) {
	t.Helper()
	b := &Bucket{Name: bucket, objects: map[string][]byte{}}
	maps.Copy(b.objects, objects)
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	return srv, b
}

// Put stores an object, replacing any object under the same key.
func (b *Bucket) Put(key string, body []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = body
}

// ServeHTTP implements http.Handler.
func (b *Bucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/")
	if strings.TrimSuffix(path, "/") == b.Name && r.URL.Query().Get("list-type") == "2" {
		b.list(w, r.URL.Query().Get("prefix"))
		return
	}
	body, ok := b.objects[strings.TrimPrefix(path, b.Name+"/")]
	if !ok || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Last-Modified", modTime.Format(http.TimeFormat))
	w.Header().Set("ETag", `"x"`)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func (b *Bucket) list(w http.ResponseWriter, prefix string) {
	type content struct {
		Key          string
		Size         int
		LastModified string
		ETag         string
	}
	type result struct {
		XMLName  xml.Name `xml:"ListBucketResult"`
		Name     string
		Prefix   string
		KeyCount int
		MaxKeys  int
		Contents []content
	}
	out := result{Name: b.Name, Prefix: prefix, MaxKeys: 1000}
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		out.Contents = append(out.Contents, content{
			Key: k, Size: len(b.objects[k]), ETag: `"x"`, LastModified: modTime.Format(time.RFC3339),
		})
	}
	out.KeyCount = len(out.Contents)
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(out)
}
