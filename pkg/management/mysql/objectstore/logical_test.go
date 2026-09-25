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
	"encoding/json"
	"io"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/objectstore/objectstoretest"
)

func newFakeStore(t *testing.T, objects map[string]any) (*Client, mysqlv1alpha1.S3ObjectStore) {
	t.Helper()
	raw := map[string][]byte{}
	for k, v := range objects {
		switch b := v.(type) {
		case []byte:
			raw[k] = b
		default:
			payload, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			raw[k] = payload
		}
	}
	srv, _ := objectstoretest.NewServer(t, "backups", raw)
	client, err := NewClient(Config{
		Endpoint: srv.URL, Region: "us-east-1", ForcePathStyle: true,
		AccessKeyID: "k", SecretAccessKey: "s",
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "prod"}
}

func TestBuildLogicalBackupKeys(t *testing.T) {
	store := mysqlv1alpha1.S3ObjectStore{Bucket: "backups", Path: "/prod/"}
	keys, err := BuildLogicalBackupKeys(store, "shop", "nightly", "nightly-1")
	if err != nil {
		t.Fatal(err)
	}
	if keys.ArchiveKey != "prod/shop/nightly/nightly-1/dump.sql.zst" ||
		keys.MetadataKey != "prod/shop/nightly/nightly-1/logical.json" ||
		keys.ArchiveURI != "s3://backups/prod/shop/nightly/nightly-1/dump.sql.zst" {
		t.Fatalf("keys = %+v", keys)
	}
	physical, err := BuildBackupKeys(store, "shop", "nightly", "nightly-1")
	if err != nil {
		t.Fatal(err)
	}
	// Same directory, so reclaim and retention remove either kind the same way.
	if strings.TrimSuffix(physical.MetadataKey, BackupMetadataName) !=
		strings.TrimSuffix(keys.MetadataKey, LogicalMetadataName) {
		t.Fatalf("logical and physical keys live in different directories: %+v / %+v", keys, physical)
	}
	if _, err := BuildLogicalBackupKeys(mysqlv1alpha1.S3ObjectStore{}, "shop", "n", "id"); err == nil {
		t.Fatal("expected an error without a bucket")
	}
}

func TestListingKeepsPhysicalAndLogicalApart(t *testing.T) {
	completed := time.Date(2026, 9, 20, 3, 0, 0, 0, time.UTC)
	client, store := newFakeStore(t, map[string]any{
		"prod/shop/base/base-1/backup.xbstream": []byte("xb"),
		"prod/shop/base/base-1/metadata.json": BackupMetadata{
			BackupID: "base-1", Method: "xtrabackup", CompletedAt: completed,
		},
		"prod/shop/dump/dump-1/dump.sql.zst": []byte("zst"),
		"prod/shop/dump/dump-1/logical.json": LogicalBackupMetadata{
			FormatVersion: 1, BackupID: "dump-1", Method: "logical", Databases: []string{"billing"}, CompletedAt: completed,
		},
		// A sibling cluster whose name shares the prefix must not leak in.
		"prod/shop-staging/dump/dump-9/logical.json": LogicalBackupMetadata{BackupID: "dump-9"},
	})
	ctx := context.Background()

	base, err := ListBaseBackups(ctx, client, store, "shop")
	if err != nil {
		t.Fatal(err)
	}
	if len(base) != 1 || base[0].Meta.BackupID != "base-1" {
		t.Fatalf("ListBaseBackups = %+v, want only base-1", base)
	}

	logical, err := ListLogicalBackups(ctx, client, store, "shop")
	if err != nil {
		t.Fatal(err)
	}
	if len(logical) != 1 || logical[0].Meta.BackupID != "dump-1" || logical[0].Prefix != "prod/shop/dump/dump-1/" {
		t.Fatalf("ListLogicalBackups = %+v, want only dump-1", logical)
	}
	if !slices.Equal(logical[0].Meta.Databases, []string{"billing"}) {
		t.Fatalf("databases = %v", logical[0].Meta.Databases)
	}
}

func TestPlanLogicalRetention(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	cutoff := now.Add(-7 * day)
	entry := func(prefix string, age time.Duration) LogicalBackupEntry {
		return LogicalBackupEntry{Prefix: prefix, Meta: LogicalBackupMetadata{CompletedAt: now.Add(-age)}}
	}

	if got := PlanLogicalRetention(nil, cutoff); got != nil {
		t.Errorf("empty input: got %v", got)
	}
	if got := PlanLogicalRetention([]LogicalBackupEntry{entry("a/", 1*day), entry("b/", 2*day)}, cutoff); got != nil {
		t.Errorf("nothing expired: got %v", got)
	}

	got := PlanLogicalRetention([]LogicalBackupEntry{
		entry("old/", 30*day),
		entry("newest-expired/", 10*day),
		entry("older/", 20*day),
	}, cutoff)
	sort.Strings(got)
	if !slices.Equal(got, []string{"old/", "older/"}) {
		t.Errorf("all expired: got %v, want the newest kept", got)
	}

	got = PlanLogicalRetention([]LogicalBackupEntry{entry("fresh/", 1*day), entry("stale/", 9*day)}, cutoff)
	if !slices.Equal(got, []string{"stale/"}) {
		t.Errorf("mixed: got %v", got)
	}
}

func TestPlanLogicalRetentionBreaksEqualCompletedAtTiesDeterministically(t *testing.T) {
	completed := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entries := []LogicalBackupEntry{
		{Prefix: "prod/shop/dump/b/", Meta: LogicalBackupMetadata{CompletedAt: completed}},
		{Prefix: "prod/shop/dump/a/", Meta: LogicalBackupMetadata{CompletedAt: completed}},
		{Prefix: "prod/shop/dump/c/", Meta: LogicalBackupMetadata{CompletedAt: completed}},
	}
	cutoff := completed.Add(time.Hour)

	// The dumps share one completion time, so the tie must resolve the same
	// way on every run instead of depending on sort instability. Three equal
	// entries keep the check honest: Go's sort.Slice happens to be stable for
	// two elements (insertion sort), so a two-entry test would pass even
	// without the tie-break.
	first := PlanLogicalRetention(entries, cutoff)
	second := PlanLogicalRetention(entries, cutoff)
	if !slices.Equal(first, second) {
		t.Fatalf("plan is not deterministic: %v then %v", first, second)
	}
	if !slices.Equal(first, []string{"prod/shop/dump/a/", "prod/shop/dump/b/"}) {
		t.Errorf("got %v, want the same two oldest prefixes expired on every run", first)
	}
}

func TestPlanLogicalRetentionKeepsUndatedManifests(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	// Two manifests without a CompletedAt: the newest-kept rule alone would
	// only save one of them, so retention must keep both rather than guess a
	// completion date (fail-open GC of malformed manifests).
	entries := []LogicalBackupEntry{
		{Prefix: "prod/shop/dump/broken-1/", Meta: LogicalBackupMetadata{}},
		{Prefix: "prod/shop/dump/broken-2/", Meta: LogicalBackupMetadata{}},
		{Prefix: "prod/shop/dump/old/", Meta: LogicalBackupMetadata{CompletedAt: now.Add(-30 * 24 * time.Hour)}},
		{Prefix: "prod/shop/dump/fresh/", Meta: LogicalBackupMetadata{CompletedAt: now.Add(-2 * 24 * time.Hour)}},
	}
	got := PlanLogicalRetention(entries, now.Add(-7*24*time.Hour))
	if !slices.Equal(got, []string{"prod/shop/dump/old/"}) {
		t.Errorf("got %v, want only the dated expired dump", got)
	}

	if got := PlanLogicalRetention(entries[:2], now.Add(-7*24*time.Hour)); got != nil {
		t.Errorf("got %v, want nothing expired for undated manifests", got)
	}
}

func TestZstdRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("INSERT INTO `t` VALUES (1,'a');\n"), 10000)
	var compressed bytes.Buffer
	w, err := NewZstdWriter(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if compressed.Len() >= len(payload) {
		t.Fatalf("compressed %d bytes into %d", len(payload), compressed.Len())
	}
	r, err := NewZstdReader(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("round trip changed the payload")
	}
}

func TestLogicalBackupMetadataCheckImportable(t *testing.T) {
	meta := LogicalBackupMetadata{
		FormatVersion: LogicalFormatVersion,
		Compression:   LogicalCompressionZstd,
		Flavor:        "mysql",
		Databases:     []string{"billing", "shop"},
	}
	for _, tc := range []struct {
		name     string
		mutate   func(*LogicalBackupMetadata)
		flavor   string
		selected []string
		wantErr  string
	}{
		{name: "whole dump", flavor: "mysql"},
		{name: "a subset", flavor: "mysql", selected: []string{"shop"}},
		{name: "no recorded flavor", flavor: "mariadb",
			mutate: func(m *LogicalBackupMetadata) { m.Flavor = "" }},
		{name: "another flavor", flavor: "mariadb", wantErr: "taken on a mysql server"},
		{name: "a missing database", flavor: "mysql", selected: []string{"shop", "crm", "hr"},
			wantErr: "databases crm, hr are not in the dump, which holds billing, shop"},
		{name: "a newer format", flavor: "mysql", wantErr: "format version 2",
			mutate: func(m *LogicalBackupMetadata) { m.FormatVersion = 2 }},
		{name: "another compression", flavor: "mysql", wantErr: `compression "gzip"`,
			mutate: func(m *LogicalBackupMetadata) { m.Compression = "gzip" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := meta
			m.Databases = slices.Clone(meta.Databases)
			if tc.mutate != nil {
				tc.mutate(&m)
			}
			err := m.CheckImportable(tc.flavor, tc.selected)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestSelectAndFindLogicalBackups(t *testing.T) {
	if _, err := SelectLatestLogicalBackup(nil); err == nil {
		t.Error("an empty listing has no latest backup")
	}
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	entries := []LogicalBackupEntry{
		{Prefix: "a/", Meta: LogicalBackupMetadata{BackupID: "a", CompletedAt: now.Add(-2 * time.Hour)}},
		{Prefix: "b/", Meta: LogicalBackupMetadata{BackupID: "b", CompletedAt: now}},
		{Prefix: "c/", Meta: LogicalBackupMetadata{BackupID: "c", CompletedAt: now.Add(-time.Hour)}},
	}
	if latest, err := SelectLatestLogicalBackup(entries); err != nil || latest.Prefix != "b/" {
		t.Errorf("latest = %v, %v; want b/", latest.Prefix, err)
	}
	if found, err := FindLogicalBackupByID(entries, "c"); err != nil || found.Prefix != "c/" {
		t.Errorf("find c = %v, %v", found.Prefix, err)
	}
	if _, err := FindLogicalBackupByID(entries, "zz"); err == nil {
		t.Error("an unknown backupID must not be found")
	}
}
