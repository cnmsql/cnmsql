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

package xtrabackup

import (
	"slices"
	"testing"
)

func TestBackupArgsSocket(t *testing.T) {
	args, err := BackupArgs(BackupOptions{
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
	if slices.Contains(args, "--host=") {
		t.Errorf("socket backup should not set host: %v", args)
	}
}

func TestBackupArgsTCP(t *testing.T) {
	args, err := BackupArgs(BackupOptions{
		TargetDir: "/backup",
		Host:      "primary",
		Port:      3306,
		User:      "repl",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"--host=primary", "--port=3306"} {
		if !slices.Contains(args, want) {
			t.Errorf("missing %q in %v", want, args)
		}
	}
}

func TestBackupArgsValidation(t *testing.T) {
	if _, err := BackupArgs(BackupOptions{User: "root"}); err == nil {
		t.Error("expected error without target dir")
	}
	if _, err := BackupArgs(BackupOptions{TargetDir: "/b"}); err == nil {
		t.Error("expected error without user")
	}
}

func TestBackupArgsStreamCompress(t *testing.T) {
	args, err := BackupArgs(BackupOptions{
		TargetDir: "/tmp/work",
		Socket:    "/run/mysqld.sock",
		User:      "cnmysql_backup",
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

func TestBackupArgsNoStreamByDefault(t *testing.T) {
	args, err := BackupArgs(BackupOptions{TargetDir: "/b", User: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(args, "--stream=xbstream") || slices.Contains(args, "--compress") {
		t.Errorf("non-stream backup should not set stream/compress: %v", args)
	}
}

func TestExtractAndDecompressArgs(t *testing.T) {
	extract, err := ExtractArgs("/restore")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"-x", "--directory=/restore"}; !slices.Equal(extract, want) {
		t.Errorf("ExtractArgs = %v, want %v", extract, want)
	}
	decompress, err := DecompressArgs("/restore")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"--decompress", "--target-dir=/restore"}; !slices.Equal(decompress, want) {
		t.Errorf("DecompressArgs = %v, want %v", decompress, want)
	}
	if _, err := ExtractArgs(""); err == nil {
		t.Error("expected error without target dir")
	}
	if _, err := DecompressArgs(""); err == nil {
		t.Error("expected error without target dir")
	}
}

func TestPrepareArgs(t *testing.T) {
	args, err := PrepareArgs("/backup")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--prepare", "--target-dir=/backup"}
	if !slices.Equal(args, want) {
		t.Errorf("PrepareArgs = %v, want %v", args, want)
	}
	if _, err := PrepareArgs(""); err == nil {
		t.Error("expected error without target dir")
	}
}

func TestCopyBackArgs(t *testing.T) {
	args, err := CopyBackArgs("/backup", "/var/lib/mysql")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--copy-back", "--target-dir=/backup", "--datadir=/var/lib/mysql"}
	if !slices.Equal(args, want) {
		t.Errorf("CopyBackArgs = %v, want %v", args, want)
	}
	if _, err := CopyBackArgs("/backup", ""); err == nil {
		t.Error("expected error without data dir")
	}
}

func TestParseBinlogInfoSingleGTID(t *testing.T) {
	info, err := ParseBinlogInfo("binlog.000003\t157\t3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5\n")
	if err != nil {
		t.Fatal(err)
	}
	if info.File != "binlog.000003" || info.Position != 157 {
		t.Errorf("unexpected coords: %+v", info)
	}
	if info.GTIDSet != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5" {
		t.Errorf("gtid = %q", info.GTIDSet)
	}
}

func TestParseBinlogInfoMultiGTID(t *testing.T) {
	// Multiple source UUIDs are comma-separated across newlines.
	content := "binlog.000003\t157\tuuid1:1-5,\nuuid2:1-3\n"
	info, err := ParseBinlogInfo(content)
	if err != nil {
		t.Fatal(err)
	}
	if info.GTIDSet != "uuid1:1-5,uuid2:1-3" {
		t.Errorf("gtid = %q, want joined set", info.GTIDSet)
	}
}

func TestParseBinlogInfoNoGTID(t *testing.T) {
	info, err := ParseBinlogInfo("binlog.000001\t4\n")
	if err != nil {
		t.Fatal(err)
	}
	if info.GTIDSet != "" || info.File != "binlog.000001" || info.Position != 4 {
		t.Errorf("unexpected: %+v", info)
	}
}

func TestParseBinlogInfoErrors(t *testing.T) {
	if _, err := ParseBinlogInfo(""); err == nil {
		t.Error("expected error for empty content")
	}
	if _, err := ParseBinlogInfo("binlog.000001\tnotanumber\tgtid"); err == nil {
		t.Error("expected error for bad position")
	}
}
