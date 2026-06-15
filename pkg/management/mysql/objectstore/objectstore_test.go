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
	"bytes"
	"io"
	"testing"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func TestBuildBackupKeys(t *testing.T) {
	t.Parallel()

	keys, err := BuildBackupKeys(mysqlv1alpha1.S3ObjectStore{
		Bucket: "backups",
		Path:   "/cloudnative-mysql//prod/",
	}, "cluster-sample", "backup-sample", "backup-sample-123")
	if err != nil {
		t.Fatal(err)
	}
	if keys.ArchiveKey != "cloudnative-mysql/prod/cluster-sample/backup-sample/backup-sample-123/backup.xbstream" {
		t.Fatalf("archive key = %q", keys.ArchiveKey)
	}
	if keys.MetadataKey != "cloudnative-mysql/prod/cluster-sample/backup-sample/backup-sample-123/metadata.json" {
		t.Fatalf("metadata key = %q", keys.MetadataKey)
	}
	wantURI := "s3://backups/cloudnative-mysql/prod/cluster-sample/backup-sample/backup-sample-123/backup.xbstream"
	if keys.ArchiveURI != wantURI {
		t.Fatalf("archive URI = %q", keys.ArchiveURI)
	}
}

func TestClusterPrefix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		path    string
		cluster string
		want    string
	}{
		{path: "/cloudnative-mysql//prod/", cluster: "demo", want: "cloudnative-mysql/prod/demo/"},
		{path: "", cluster: "demo", want: "demo/"},
		{path: "backups", cluster: "demo", want: "backups/demo/"},
	}
	for _, tc := range cases {
		got := ClusterPrefix(mysqlv1alpha1.S3ObjectStore{Path: tc.path}, tc.cluster)
		if got != tc.want {
			t.Fatalf("ClusterPrefix(%q, %q) = %q, want %q", tc.path, tc.cluster, got, tc.want)
		}
	}
}

func TestBuildBackupKeysRequiresFields(t *testing.T) {
	t.Parallel()

	if _, err := BuildBackupKeys(mysqlv1alpha1.S3ObjectStore{}, "cluster", "backup", "id"); err == nil {
		t.Fatal("expected missing bucket error")
	}
	if _, err := BuildBackupKeys(mysqlv1alpha1.S3ObjectStore{Bucket: "backups"}, "", "backup", "id"); err == nil {
		t.Fatal("expected missing cluster error")
	}
}

func TestSHA256Writer(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	writer := NewSHA256Writer(&buf)
	if _, err := io.WriteString(writer, "hello "); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(writer, "world"); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "hello world" {
		t.Fatalf("written data = %q", got)
	}
	if got := writer.SumHex(); got != "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9" {
		t.Fatalf("sha256 = %q", got)
	}
}
