/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package objectstore

import (
	"testing"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func TestBuildBinlogKeys(t *testing.T) {
	t.Parallel()

	store := mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "/cloudnative-mysql//prod/"}
	keys, err := BuildBinlogKeys(store, "demo", "3e11fa47-71ca-11e1-9e33-c80aa9429562", "binlog.000004")
	if err != nil {
		t.Fatal(err)
	}
	wantBinlog := "cloudnative-mysql/prod/demo/binlogs/3e11fa47-71ca-11e1-9e33-c80aa9429562/binlog.000004"
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

	store := mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "cloudnative-mysql"}
	if got, want := BinlogPrefix(store, "demo"), "cloudnative-mysql/demo/binlogs/"; got != want {
		t.Fatalf("BinlogPrefix = %q, want %q", got, want)
	}
	if got, want := ServerPrefix(store, "demo", "uuid"), "cloudnative-mysql/demo/binlogs/uuid/"; got != want {
		t.Fatalf("ServerPrefix = %q, want %q", got, want)
	}
	wantStatus := "cloudnative-mysql/demo/binlogs/uuid/_archive_status.json"
	if got := ArchiveStatusKey(store, "demo", "uuid"); got != wantStatus {
		t.Fatalf("ArchiveStatusKey = %q, want %q", got, wantStatus)
	}
	if got, want := ArchiveIndexKey(store, "demo"), "cloudnative-mysql/demo/binlogs/_index.json"; got != want {
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
