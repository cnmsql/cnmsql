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
