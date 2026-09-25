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

package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore/objectstoretest"
)

const downloadSQL = "-- Current Database: `shop`\nUSE `shop`;\n"

// downloadFixture serves one logical backup of cluster "prod" and returns a
// client holding the Backup, its Cluster and the store's Secret. The store's
// endpoint in the spec is unreachable unless storeEndpoint is set, so tests
// can check that --endpoint is what reaches the fake.
type downloadFixture struct {
	client   client.Reader
	backup   *mysqlv1alpha1.Backup
	endpoint string
	bucket   *objectstoretest.Bucket
	archive  []byte
}

func newDownloadFixture(t *testing.T, specEndpoint bool) *downloadFixture {
	t.Helper()
	var buf bytes.Buffer
	zw, err := objectstore.NewZstdWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = zw.Write([]byte(downloadSQL))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	manifest, err := json.Marshal(objectstore.LogicalBackupMetadata{
		FormatVersion: objectstore.LogicalFormatVersion,
		BackupID:      "b-1",
		Compression:   objectstore.LogicalCompressionZstd,
		SHA256:        hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, bucket := objectstoretest.NewServer(t, "backups", map[string][]byte{
		"clusters/prod/nightly/b-1/dump.sql.zst": buf.Bytes(),
		"clusters/prod/nightly/b-1/logical.json": manifest,
	})

	endpoint := "http://127.0.0.1:1"
	if specEndpoint {
		endpoint = srv.URL
	}
	cluster := &mysqlv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
		Spec: mysqlv1alpha1.ClusterSpec{Backup: &mysqlv1alpha1.BackupConfiguration{
			ObjectStore: &mysqlv1alpha1.S3ObjectStore{
				Bucket:   "backups",
				Path:     "clusters",
				Endpoint: endpoint,
				Credentials: mysqlv1alpha1.S3Credentials{
					AccessKeyID:     &mysqlv1alpha1.SecretKeySelector{Name: "s3", Key: "access"},
					SecretAccessKey: &mysqlv1alpha1.SecretKeySelector{Name: "s3", Key: "secret"},
				},
			},
		}},
	}
	backup := &mysqlv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec: mysqlv1alpha1.BackupSpec{
			Cluster: mysqlv1alpha1.LocalObjectReference{Name: "prod"},
			Method:  mysqlv1alpha1.BackupMethodLogical,
		},
		Status: mysqlv1alpha1.BackupStatus{Phase: mysqlv1alpha1.BackupPhaseCompleted, BackupID: "b-1"},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3", Namespace: "default"},
		Data:       map[string][]byte{"access": []byte("k"), "secret": []byte("s")},
	}
	return &downloadFixture{
		client:   clientfake.NewClientBuilder().WithScheme(plugin.Scheme).WithObjects(cluster, secret).Build(),
		backup:   backup,
		endpoint: srv.URL,
		bucket:   bucket,
		archive:  buf.Bytes(),
	}
}

func TestDownloadLogicalBackupToFile(t *testing.T) {
	f := newDownloadFixture(t, true)
	t.Chdir(t.TempDir())

	written, err := downloadLogicalBackup(context.Background(), f.client, f.backup, downloadOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if written != "nightly.sql.zst" {
		t.Errorf("written to %q, want nightly.sql.zst", written)
	}
	got, _ := os.ReadFile(written)
	if !bytes.Equal(got, f.archive) {
		t.Error("the file differs from the archive")
	}
	if _, err := os.Stat(written + ".partial"); !os.IsNotExist(err) {
		t.Error("the partial file was left behind")
	}

	// A second run refuses to overwrite it.
	if _, err := downloadLogicalBackup(context.Background(), f.client, f.backup, downloadOptions{}, nil); err == nil {
		t.Error("an existing file must not be overwritten")
	}
}

func TestDownloadLogicalBackupDecompressed(t *testing.T) {
	f := newDownloadFixture(t, true)
	dir := t.TempDir()
	t.Chdir(dir)

	written, err := downloadLogicalBackup(context.Background(), f.client, f.backup,
		downloadOptions{Decompress: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, written)); written != "nightly.sql" || string(got) != downloadSQL {
		t.Errorf("wrote %q to %s", got, written)
	}

	var out bytes.Buffer
	if _, err := downloadLogicalBackup(context.Background(), f.client, f.backup,
		downloadOptions{Decompress: true, Output: "-"}, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != downloadSQL {
		t.Errorf("stdout = %q", out.String())
	}
}

func TestDownloadLogicalBackupEndpointOverride(t *testing.T) {
	f := newDownloadFixture(t, false)
	var out bytes.Buffer
	if _, err := downloadLogicalBackup(context.Background(), f.client, f.backup,
		downloadOptions{Output: "-"}, &out); err == nil {
		t.Fatal("the spec's endpoint is unreachable; want an error without --endpoint")
	}
	out.Reset()
	if _, err := downloadLogicalBackup(context.Background(), f.client, f.backup,
		downloadOptions{Output: "-", Endpoint: f.endpoint}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), f.archive) {
		t.Error("stdout differs from the archive")
	}
}

func TestDownloadLogicalBackupChecksumMismatchLeavesNoFile(t *testing.T) {
	for _, decompress := range []bool{false, true} {
		f := newDownloadFixture(t, true)
		// Same bytes plus a trailing zstd skippable frame: still decodes, but the
		// checksum no longer matches.
		f.bucket.Put("clusters/prod/nightly/b-1/dump.sql.zst",
			append(append([]byte{}, f.archive...), 0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0))
		dir := t.TempDir()
		t.Chdir(dir)
		_, err := downloadLogicalBackup(context.Background(), f.client, f.backup,
			downloadOptions{Decompress: decompress}, nil)
		if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("decompress=%v: error = %v, want a checksum mismatch", decompress, err)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Errorf("decompress=%v: files left behind: %v", decompress, entries)
		}
	}
}

func TestDownloadLogicalBackupRejects(t *testing.T) {
	f := newDownloadFixture(t, true)
	physical := f.backup.DeepCopy()
	physical.Spec.Method = mysqlv1alpha1.BackupMethodXtrabackup
	running := f.backup.DeepCopy()
	running.Status.Phase = mysqlv1alpha1.BackupPhaseRunning
	for backup, want := range map[*mysqlv1alpha1.Backup]string{
		physical: "only logical backups can be downloaded",
		running:  "is not completed",
	} {
		_, err := downloadLogicalBackup(context.Background(), f.client, backup, downloadOptions{Output: "-"}, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to contain %q", err, want)
		}
	}
}

// The download subcommand must not shadow `backup CLUSTER`.
func TestBackupCommandStillTakesACluster(t *testing.T) {
	root := NewRootCommand()
	cmd, args, err := root.Find([]string{"backup", "cluster-sample"})
	if err != nil || cmd.Name() != "backup" || len(args) != 1 || args[0] != "cluster-sample" {
		t.Fatalf("backup CLUSTER resolved to %v %v %v", cmd.Name(), args, err)
	}
	cmd, _, err = root.Find([]string{"backup", "download", "nightly"})
	if err != nil || cmd.Name() != "download" {
		t.Fatalf("backup download resolved to %v %v", cmd.Name(), err)
	}
}
