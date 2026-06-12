/*
Copyright 2026 The CNMySQL Authors.

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
	"testing"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

func TestBuildBinlogKeys(t *testing.T) {
	t.Parallel()

	store := mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "/cnmysql//prod/"}
	keys, err := BuildBinlogKeys(store, "demo", "3e11fa47-71ca-11e1-9e33-c80aa9429562", "binlog.000004")
	if err != nil {
		t.Fatal(err)
	}
	wantBinlog := "cnmysql/prod/demo/binlogs/3e11fa47-71ca-11e1-9e33-c80aa9429562/binlog.000004"
	if keys.BinlogKey != wantBinlog {
		t.Fatalf("binlog key = %q", keys.BinlogKey)
	}
	if keys.ManifestKey != wantBinlog+".json" {
		t.Fatalf("manifest key = %q", keys.ManifestKey)
	}
}

func TestBuildBinlogKeysPartitionsByUUID(t *testing.T) {
	t.Parallel()

	store := mysqlv1alpha1.S3ObjectStore{Bucket: "backups"}
	a, err := BuildBinlogKeys(store, "demo", "uuid-a", "binlog.000004")
	if err != nil {
		t.Fatal(err)
	}
	b, err := BuildBinlogKeys(store, "demo", "uuid-b", "binlog.000004")
	if err != nil {
		t.Fatal(err)
	}
	if a.BinlogKey == b.BinlogKey {
		t.Fatalf("like-named files from different UUIDs collided: %q", a.BinlogKey)
	}
}

func TestBuildBinlogKeysRequiresFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                              string
		bucket, cluster, uuid, binlogName string
	}{
		{"no bucket", "", "demo", "uuid", "binlog.000001"},
		{"no cluster", "backups", "", "uuid", "binlog.000001"},
		{"no uuid", "backups", "demo", "", "binlog.000001"},
		{"no binlog", "backups", "demo", "uuid", ""},
		{"uuid with slash", "backups", "demo", "uu/id", "binlog.000001"},
		{"binlog with slash", "backups", "demo", "uuid", "sub/binlog.000001"},
	}
	for _, tc := range cases {
		store := mysqlv1alpha1.S3ObjectStore{Bucket: tc.bucket}
		if _, err := BuildBinlogKeys(store, tc.cluster, tc.uuid, tc.binlogName); err == nil {
			t.Fatalf("%s: expected error", tc.name)
		}
	}
}

func TestArchiveKeys(t *testing.T) {
	t.Parallel()

	store := mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cnmysql"}
	if got, want := BinlogPrefix(store, "demo"), "cnmysql/demo/binlogs/"; got != want {
		t.Fatalf("BinlogPrefix = %q, want %q", got, want)
	}
	if got, want := ServerPrefix(store, "demo", "uuid"), "cnmysql/demo/binlogs/uuid/"; got != want {
		t.Fatalf("ServerPrefix = %q, want %q", got, want)
	}
	wantStatus := "cnmysql/demo/binlogs/uuid/_archive_status.json"
	if got := ArchiveStatusKey(store, "demo", "uuid"); got != wantStatus {
		t.Fatalf("ArchiveStatusKey = %q, want %q", got, wantStatus)
	}
	if got, want := ArchiveIndexKey(store, "demo"), "cnmysql/demo/binlogs/_index.json"; got != want {
		t.Fatalf("ArchiveIndexKey = %q, want %q", got, want)
	}
}

func TestArchiveIndexSegment(t *testing.T) {
	t.Parallel()

	idx := &ArchiveIndex{
		ClusterName: "demo",
		Segments: []ArchiveSegment{
			{ServerUUID: "uuid-a"},
			{ServerUUID: "uuid-b"},
		},
	}
	if seg, ok := idx.Segment("uuid-b"); !ok || seg.ServerUUID != "uuid-b" {
		t.Fatalf("Segment(uuid-b) = %+v, %v", seg, ok)
	}
	if _, ok := idx.Segment("missing"); ok {
		t.Fatal("Segment(missing) should not be found")
	}
}
