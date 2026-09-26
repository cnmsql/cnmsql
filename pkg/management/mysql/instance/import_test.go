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

package instance

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

	"github.com/cnmsql/cnmsql/pkg/engine"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore/objectstoretest"
)

const importTestDump = "-- header\n" +
	"SET NAMES utf8mb4;\n" +
	"-- Current Database: `shop`\n" +
	"USE `shop`;\n" +
	"INSERT INTO items VALUES (1);\n" +
	"-- Current Database: `billing`\n" +
	"USE `billing`;\n" +
	"INSERT INTO invoices VALUES (1);\n"

func zstdBytes(t *testing.T, s string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := objectstore.NewZstdWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// importFixture serves a dump and its manifest from a fake bucket and returns
// options whose SQL client is a script that records its stdin, argv and
// credentials file into dir. The script's body can be replaced.
type importFixture struct {
	opts   ImportOptions
	meta   objectstore.LogicalBackupMetadata
	dir    string
	dump   []byte
	bucket *objectstoretest.Bucket
}

func newImportFixture(t *testing.T, clientBody string) *importFixture {
	t.Helper()
	f := &importFixture{dir: t.TempDir(), dump: zstdBytes(t, importTestDump)}
	f.meta = objectstore.LogicalBackupMetadata{
		FormatVersion: objectstore.LogicalFormatVersion,
		BackupID:      "20260925T120000",
		ClusterName:   "prod",
		Method:        "logical",
		Flavor:        "mysql",
		Compression:   objectstore.LogicalCompressionZstd,
		SHA256:        sha256Hex(f.dump),
		Databases:     []string{"billing", "shop"},
	}
	manifest, err := json.Marshal(f.meta)
	if err != nil {
		t.Fatal(err)
	}
	srv, bucket := objectstoretest.NewServer(t, "backups", map[string][]byte{
		"prod/nightly/1/dump.sql.zst": f.dump,
		"prod/nightly/1/logical.json": manifest,
	})
	f.bucket = bucket
	store, err := objectstore.NewClient(objectstore.Config{
		Endpoint: srv.URL, Region: "us-east-1", ForcePathStyle: true,
		AccessKeyID: "k", SecretAccessKey: "s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if clientBody == "" {
		clientBody = "cat > " + filepath.Join(f.dir, "stdin.sql") + "\n" +
			"echo \"$@\" > " + filepath.Join(f.dir, "argv") + "\n" +
			"cp \"${1#--defaults-extra-file=}\" " + filepath.Join(f.dir, "client.cnf") + "\n"
	}
	client := filepath.Join(f.dir, "mysql")
	if err := os.WriteFile(client, []byte("#!/bin/sh\n"+clientBody), 0o755); err != nil {
		t.Fatal(err)
	}
	f.opts = ImportOptions{
		Store:        store,
		Bucket:       "backups",
		DumpKey:      "prod/nightly/1/dump.sql.zst",
		ManifestKey:  "prod/nightly/1/logical.json",
		Engine:       engine.MustForFlavor(engine.FlavorMySQL),
		DataDir:      t.TempDir(),
		Socket:       "/run/mysqld/mysqld.sock",
		LoadPath:     client,
		WorkDir:      t.TempDir(),
		RootPassword: `p"ss\word`,
	}
	f.opts.applyDefaults()
	return f
}

func (f *importFixture) read(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestImportLoadStreamsTheFilteredDump(t *testing.T) {
	f := newImportFixture(t, "")
	f.opts.Databases = []string{"billing"}
	if err := f.opts.load(context.Background(), f.opts.LoadPath, f.meta); err != nil {
		t.Fatal(err)
	}
	want := "-- header\nSET NAMES utf8mb4;\n" +
		"-- Current Database: `billing`\nUSE `billing`;\nINSERT INTO invoices VALUES (1);\n"
	if got := f.read(t, "stdin.sql"); got != want {
		t.Errorf("client read:\n%s\nwant:\n%s", got, want)
	}
	argv := f.read(t, "argv")
	if strings.Contains(argv, "word") {
		t.Errorf("the root password leaked into argv: %s", argv)
	}
	if !strings.Contains(argv, "--max-allowed-packet=1073741824") {
		t.Errorf("argv lacks the engine's load args: %s", argv)
	}
	cnf := f.read(t, "client.cnf")
	for _, want := range []string{`user="root"`, `password="p\"ss\\word"`, `socket="/run/mysqld/mysqld.sock"`} {
		if !strings.Contains(cnf, want) {
			t.Errorf("credentials file lacks %s:\n%s", want, cnf)
		}
	}
	// The credentials file is removed once the load is done.
	if entries, _ := os.ReadDir(f.opts.WorkDir); len(entries) != 0 {
		t.Errorf("work dir not cleaned up: %v", entries)
	}
}

func TestImportLoadWholeDump(t *testing.T) {
	f := newImportFixture(t, "")
	if err := f.opts.load(context.Background(), f.opts.LoadPath, f.meta); err != nil {
		t.Fatal(err)
	}
	if got := f.read(t, "stdin.sql"); got != importTestDump {
		t.Errorf("client read %q, want the whole dump", got)
	}
}

func TestImportLoadFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		client  string
		mutate  func(*importFixture)
		wantErr string
	}{
		{
			name:    "the client rejects a statement",
			client:  "head -c 10 >/dev/null\necho 'ERROR 1064 (42000) at line 2: You have an error' >&2\nexit 1\n",
			wantErr: "ERROR 1064 (42000) at line 2",
		},
		{
			name:    "the client fails after reading everything",
			client:  "cat >/dev/null\necho 'ERROR 1146 (42S02) at line 8' >&2\nexit 1\n",
			wantErr: "ERROR 1146",
		},
		{
			name:    "checksum mismatch",
			mutate:  func(f *importFixture) { f.meta.SHA256 = strings.Repeat("0", 64) },
			wantErr: "checksum mismatch",
		},
		{
			name:    "missing dump object",
			mutate:  func(f *importFixture) { f.opts.DumpKey = "prod/nightly/1/absent.sql.zst" },
			wantErr: "downloading prod/nightly/1/absent.sql.zst",
		},
		{
			name: "corrupted dump",
			mutate: func(f *importFixture) {
				f.bucket.Put(f.opts.DumpKey, []byte("not zstd at all"))
			},
			wantErr: "reading prod/nightly/1/dump.sql.zst",
		},
		{
			name:    "a selected database with no section",
			mutate:  func(f *importFixture) { f.opts.Databases = []string{"crm"} },
			wantErr: `database "crm" but the dump has no section for it`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newImportFixture(t, tc.client)
			if tc.mutate != nil {
				tc.mutate(f)
			}
			err := f.opts.load(context.Background(), f.opts.LoadPath, f.meta)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// The checks that need no server run before one is started, so a bad import
// fails fast with a clear message.
func TestImportChecksBeforeStartingAServer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*ImportOptions)
		wantErr string
	}{
		{"another flavor", func(o *ImportOptions) { o.Engine = engine.MustForFlavor(engine.FlavorMariaDB) },
			"taken on a mysql server"},
		{"a database not in the dump", func(o *ImportOptions) { o.Databases = []string{"crm"} },
			"databases crm are not in the dump"},
		{"no SQL client", func(o *ImportOptions) { o.LoadPath = "/nonexistent/mysql" },
			"/nonexistent/mysql is not in this instance image"},
		{"a missing manifest", func(o *ImportOptions) { o.ManifestKey = "prod/nightly/1/absent.json" },
			"reading manifest prod/nightly/1/absent.json"},
		{"a root password with a newline", func(o *ImportOptions) { o.RootPassword = "a\nb" },
			"cannot contain a line break"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newImportFixture(t, "")
			// A server start would fail on this binary, so reaching it shows up
			// as a different error.
			f.opts.MysqldPath = "/nonexistent/mysqld"
			tc.mutate(&f.opts)
			err := Import(context.Background(), f.opts)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestImportSkipsWhenMarkerExists(t *testing.T) {
	f := newImportFixture(t, "")
	if err := os.WriteFile(filepath.Join(f.opts.DataDir, ImportMarkerName), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Nothing may run: not even the manifest read.
	f.opts.ManifestKey = "prod/nightly/1/absent.json"
	f.opts.MysqldPath = "/nonexistent/mysqld"
	if err := Import(context.Background(), f.opts); err != nil {
		t.Fatalf("a finished import must be a no-op, got %v", err)
	}
}
