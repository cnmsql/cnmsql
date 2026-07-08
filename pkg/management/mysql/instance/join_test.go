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
	"os"
	"path/filepath"
	"testing"

	"github.com/cnmsql/cnmsql/pkg/engine"
)

// TestReadBinlogInfoMissingFile verifies that a replica join tolerates a backup
// that carries no binlog-info file. mariabackup only writes
// mariadb_backup_binlog_info when the source has a non-empty binlog GTID
// position, so a primary whose data was authored out of the binlog produces a
// backup without one; the join must treat that as an empty start position, not
// fail.
func TestReadBinlogInfoMissingFile(t *testing.T) {
	bt := engine.MustForFlavor(engine.FlavorMariaDB).Backup()

	info, err := readBinlogInfoWithTool(t.TempDir(), bt)
	if err != nil {
		t.Fatalf("missing binlog-info must not error, got: %v", err)
	}
	if info.GTIDSet != "" || info.File != "" || info.Position != 0 {
		t.Fatalf("expected empty binlog info, got %+v", info)
	}
}

// TestPersistBinlogInfo verifies the backup's binlog-info file is copied into the
// data dir so a retried PITR can recover the anchor, and that a missing source
// file (empty-position backup) is a silent no-op.
func TestPersistBinlogInfo(t *testing.T) {
	bt := engine.MustForFlavor(engine.FlavorMariaDB).Backup()
	name := bt.BinlogInfoFileName()

	t.Run("copies into data dir", func(t *testing.T) {
		backupDir, dataDir := t.TempDir(), t.TempDir()
		want := "binlog.000004\t328\t0-1-42\n"
		if err := os.WriteFile(filepath.Join(backupDir, name), []byte(want), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := persistBinlogInfo(bt, backupDir, dataDir); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(dataDir, name))
		if err != nil {
			t.Fatalf("expected durable copy in data dir: %v", err)
		}
		if string(got) != want {
			t.Fatalf("copied content = %q, want %q", got, want)
		}
	})

	t.Run("missing source is a no-op", func(t *testing.T) {
		dataDir := t.TempDir()
		if err := persistBinlogInfo(bt, t.TempDir(), dataDir); err != nil {
			t.Fatalf("missing source must not error, got: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dataDir, name)); !os.IsNotExist(err) {
			t.Fatalf("expected no file written, got err=%v", err)
		}
	})
}

// TestReadBinlogInfoLegacyName verifies the reader accepts the legacy
// xtrabackup_binlog_info name that MariaBackup < 11.1 (e.g. 10.11) writes.
// Without this the anchor is silently empty and PITR replays from genesis.
func TestReadBinlogInfoLegacyName(t *testing.T) {
	bt := engine.MustForFlavor(engine.FlavorMariaDB).Backup()

	dir := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(dir, "xtrabackup_binlog_info"),
		[]byte("binlog.000002\t831\t0-1-2\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}

	info, err := readBinlogInfoWithTool(dir, bt)
	if err != nil {
		t.Fatal(err)
	}
	if info.GTIDSet != "0-1-2" || info.File != "binlog.000002" || info.Position != 831 {
		t.Fatalf("legacy-name anchor not read: %+v", info)
	}
}

// TestPersistBinlogInfoLegacyName verifies a legacy-named backup file is copied
// into the data dir preserving its name, so the durable retry path finds it.
func TestPersistBinlogInfoLegacyName(t *testing.T) {
	bt := engine.MustForFlavor(engine.FlavorMariaDB).Backup()
	backupDir, dataDir := t.TempDir(), t.TempDir()
	want := "binlog.000002\t831\t0-1-2\n"
	if err := os.WriteFile(filepath.Join(backupDir, "xtrabackup_binlog_info"), []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := persistBinlogInfo(bt, backupDir, dataDir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dataDir, "xtrabackup_binlog_info"))
	if err != nil {
		t.Fatalf("expected legacy-named durable copy: %v", err)
	}
	if string(got) != want {
		t.Fatalf("copied content = %q, want %q", got, want)
	}
}

func TestReadBinlogInfoPresent(t *testing.T) {
	bt := engine.MustForFlavor(engine.FlavorMariaDB).Backup()

	dir := t.TempDir()
	if err := os.WriteFile(
		filepath.Join(dir, bt.BinlogInfoFileName()),
		[]byte("binlog.000004\t328\t0-1-42\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}

	info, err := readBinlogInfoWithTool(dir, bt)
	if err != nil {
		t.Fatal(err)
	}
	if info.GTIDSet != "0-1-42" {
		t.Fatalf("GTIDSet = %q, want 0-1-42", info.GTIDSet)
	}
}
