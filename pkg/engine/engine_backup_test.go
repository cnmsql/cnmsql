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

package engine

import (
	"slices"
	"testing"
)

func TestMySQLBackupToolBinaries(t *testing.T) {
	bt := mysqlBackupTool{}
	if bt.BackupBinary() != "xtrabackup" {
		t.Errorf("BackupBinary = %q", bt.BackupBinary())
	}
	if bt.StreamBinary() != "xbstream" {
		t.Errorf("StreamBinary = %q", bt.StreamBinary())
	}
	if bt.BinlogClientBinary() != "mysqlbinlog" {
		t.Errorf("BinlogClientBinary = %q", bt.BinlogClientBinary())
	}
	if bt.SQLClientBinary() != "mysql" {
		t.Errorf("SQLClientBinary = %q", bt.SQLClientBinary())
	}
	if bt.BinlogInfoFileName() != "xtrabackup_binlog_info" {
		t.Errorf("BinlogInfoFileName = %q", bt.BinlogInfoFileName())
	}
}

func TestMariaDBBackupToolBinaries(t *testing.T) {
	bt := mariadbBackupTool{}
	if bt.BackupBinary() != "mariabackup" {
		t.Errorf("BackupBinary = %q", bt.BackupBinary())
	}
	if bt.StreamBinary() != "mbstream" {
		t.Errorf("StreamBinary = %q", bt.StreamBinary())
	}
	if bt.BinlogClientBinary() != "mariadb-binlog" {
		t.Errorf("BinlogClientBinary = %q", bt.BinlogClientBinary())
	}
	if bt.SQLClientBinary() != "mariadb" {
		t.Errorf("SQLClientBinary = %q", bt.SQLClientBinary())
	}
	if bt.BinlogInfoFileName() != "mariadb_backup_binlog_info" {
		t.Errorf("BinlogInfoFileName = %q", bt.BinlogInfoFileName())
	}
}

func TestMySQLBackupArgsByteIdentical(t *testing.T) {
	bt := mysqlBackupTool{}

	t.Run("stream+compress", func(t *testing.T) {
		args, err := bt.BackupArgs(BackupOpts{
			TargetDir: "/tmp/work",
			Socket:    "/run/mysqld.sock",
			User:      "cnmsql_backup",
			Stream:    true,
			Compress:  true,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"--stream=xbstream", "--compress"} {
			if !slices.Contains(args, want) {
				t.Errorf("missing %q in %v", want, args)
			}
		}
	})

	t.Run("socket", func(t *testing.T) {
		args, err := bt.BackupArgs(BackupOpts{
			TargetDir: "/backup",
			Socket:    "/run/mysqld.sock",
			User:      "root",
			Password:  "pw",
			Parallel:  4,
		})
		if err != nil {
			t.Fatal(err)
		}
		wantArgs := []string{
			"--backup", "--target-dir=/backup", "--user=root",
			"--password=pw", "--socket=/run/mysqld.sock", "--parallel=4",
		}
		for _, want := range wantArgs {
			if !slices.Contains(args, want) {
				t.Errorf("missing %q in %v", want, args)
			}
		}
	})

	t.Run("validation", func(t *testing.T) {
		if _, err := bt.BackupArgs(BackupOpts{User: "root"}); err == nil {
			t.Error("expected error without target dir")
		}
		if _, err := bt.BackupArgs(BackupOpts{TargetDir: "/b"}); err == nil {
			t.Error("expected error without user")
		}
	})
}

func TestMariaDBBackupArgs(t *testing.T) {
	bt := mariadbBackupTool{}

	args, err := bt.BackupArgs(BackupOpts{
		TargetDir: "/tmp/work",
		Socket:    "/run/mysqld.sock",
		User:      "cnmsql_backup",
		Stream:    true,
		Compress:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"--stream=xbstream", "--compress"} {
		if !slices.Contains(args, want) {
			t.Errorf("missing %q in %v", want, args)
		}
	}
}

func TestBackupToolArgsShared(t *testing.T) {
	mysqlBT := mysqlBackupTool{}
	mariadbBT := mariadbBackupTool{}

	t.Run("extract", func(t *testing.T) {
		mysqlArgs, _ := mysqlBT.ExtractArgs("/restore")
		mariadbArgs, _ := mariadbBT.ExtractArgs("/restore")
		want := []string{"-x", "--directory=/restore"}
		if !slices.Equal(mysqlArgs, want) {
			t.Errorf("MySQL ExtractArgs = %v, want %v", mysqlArgs, want)
		}
		if !slices.Equal(mariadbArgs, want) {
			t.Errorf("MariaDB ExtractArgs = %v, want %v", mariadbArgs, want)
		}
	})

	t.Run("prepare", func(t *testing.T) {
		mysqlArgs, _ := mysqlBT.PrepareArgs("/backup")
		mariadbArgs, _ := mariadbBT.PrepareArgs("/backup")
		want := []string{"--prepare", "--target-dir=/backup"}
		if !slices.Equal(mysqlArgs, want) {
			t.Errorf("MySQL PrepareArgs = %v, want %v", mysqlArgs, want)
		}
		if !slices.Equal(mariadbArgs, want) {
			t.Errorf("MariaDB PrepareArgs = %v, want %v", mariadbArgs, want)
		}
	})

	t.Run("copy-back", func(t *testing.T) {
		mysqlArgs, _ := mysqlBT.CopyBackArgs("/backup", "/var/lib/mysql")
		mariadbArgs, _ := mariadbBT.CopyBackArgs("/backup", "/var/lib/mysql")
		want := []string{"--copy-back", "--target-dir=/backup", "--datadir=/var/lib/mysql"}
		if !slices.Equal(mysqlArgs, want) {
			t.Errorf("MySQL CopyBackArgs = %v, want %v", mysqlArgs, want)
		}
		if !slices.Equal(mariadbArgs, want) {
			t.Errorf("MariaDB CopyBackArgs = %v, want %v", mariadbArgs, want)
		}
	})

	t.Run("decompress", func(t *testing.T) {
		mysqlArgs, _ := mysqlBT.DecompressArgs("/restore")
		mariadbArgs, _ := mariadbBT.DecompressArgs("/restore")
		want := []string{"--decompress", "--target-dir=/restore"}
		if !slices.Equal(mysqlArgs, want) {
			t.Errorf("MySQL DecompressArgs = %v, want %v", mysqlArgs, want)
		}
		if !slices.Equal(mariadbArgs, want) {
			t.Errorf("MariaDB DecompressArgs = %v, want %v", mariadbArgs, want)
		}
	})
}

func TestBackupToolParseBinlogInfo(t *testing.T) {
	mysqlBT := mysqlBackupTool{}
	mariadbBT := mariadbBackupTool{}

	t.Run("single GTID set", func(t *testing.T) {
		content := "binlog.000003\t157\t3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5\n"
		for name, bt := range map[string]BackupTool{"mysql": mysqlBT, "mariadb": mariadbBT} {
			t.Run(name, func(t *testing.T) {
				info, err := bt.ParseBinlogInfo(content)
				if err != nil {
					t.Fatal(err)
				}
				if info.File != "binlog.000003" || info.Position != 157 {
					t.Errorf("unexpected coords: %+v", info)
				}
				if info.GTIDSet != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5" {
					t.Errorf("gtid = %q", info.GTIDSet)
				}
			})
		}
	})

	t.Run("multi GTID set", func(t *testing.T) {
		content := "binlog.000003\t157\tuuid1:1-5,\nuuid2:1-3\n"
		for name, bt := range map[string]BackupTool{"mysql": mysqlBT, "mariadb": mariadbBT} {
			t.Run(name, func(t *testing.T) {
				info, err := bt.ParseBinlogInfo(content)
				if err != nil {
					t.Fatal(err)
				}
				if info.GTIDSet != "uuid1:1-5,uuid2:1-3" {
					t.Errorf("gtid = %q, want joined set", info.GTIDSet)
				}
			})
		}
	})

	t.Run("no GTID set", func(t *testing.T) {
		content := "binlog.000001\t4\n"
		for name, bt := range map[string]BackupTool{"mysql": mysqlBT, "mariadb": mariadbBT} {
			t.Run(name, func(t *testing.T) {
				info, err := bt.ParseBinlogInfo(content)
				if err != nil {
					t.Fatal(err)
				}
				if info.GTIDSet != "" || info.File != "binlog.000001" || info.Position != 4 {
					t.Errorf("unexpected: %+v", info)
				}
			})
		}
	})

	t.Run("empty content", func(t *testing.T) {
		for name, bt := range map[string]BackupTool{"mysql": mysqlBT, "mariadb": mariadbBT} {
			t.Run(name, func(t *testing.T) {
				if _, err := bt.ParseBinlogInfo(""); err == nil {
					t.Error("expected error for empty content")
				}
			})
		}
	})
}

func TestEngineBackup(t *testing.T) {
	t.Run("mysql backup tool", func(t *testing.T) {
		eng := MustForFlavor(FlavorMySQL)
		bt := eng.Backup()
		if bt == nil {
			t.Fatal("Backup() returned nil")
		}
		if bt.BackupBinary() != "xtrabackup" {
			t.Errorf("BackupBinary = %q", bt.BackupBinary())
		}
	})

	t.Run("mariadb backup tool", func(t *testing.T) {
		eng := MustForFlavor(FlavorMariaDB)
		bt := eng.Backup()
		if bt == nil {
			t.Fatal("Backup() returned nil")
		}
		if bt.BackupBinary() != "mariabackup" {
			t.Errorf("BackupBinary = %q", bt.BackupBinary())
		}
	})
}
