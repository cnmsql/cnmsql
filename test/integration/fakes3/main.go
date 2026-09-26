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

// Command fakes3 serves one in-memory S3 bucket, for integration tests that
// need an object store inside a container. The objects come from a JSON file
// mapping each key to its content (base64, as encoding/json writes []byte).
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore/objectstoretest"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "address to listen on")
	bucket := flag.String("bucket", "backups", "bucket name")
	objectsFile := flag.String("objects", "", "JSON file mapping object keys to their content")
	flag.Parse()

	data, err := os.ReadFile(*objectsFile)
	if err != nil {
		log.Fatal(err)
	}
	var objects map[string][]byte
	if err := json.Unmarshal(data, &objects); err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           objectstoretest.NewBucket(*bucket, objects),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
