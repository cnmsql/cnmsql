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

package binlog

import (
	"reflect"
	"testing"
)

func TestParseSequence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want int64
		ok   bool
	}{
		{"binlog.000004", 4, true},
		{"/var/lib/mysql/binlog.000123", 123, true},
		{"mysql-bin.999999", 999999, true},
		{"binlog", 0, false},
		{"binlog.", 0, false},
		{"binlog.abc", 0, false},
	}
	for _, tc := range cases {
		got, err := ParseSequence(tc.name)
		if tc.ok && (err != nil || got != tc.want) {
			t.Fatalf("ParseSequence(%q) = %d, %v; want %d", tc.name, got, err, tc.want)
		}
		if !tc.ok && err == nil {
			t.Fatalf("ParseSequence(%q) expected error", tc.name)
		}
	}
}

func TestMarkActiveAndArchivable(t *testing.T) {
	t.Parallel()
	logs := []BinaryLog{
		{Name: "binlog.000002", SizeBytes: 100},
		{Name: "binlog.000004", SizeBytes: 50},
		{Name: "binlog.000003", SizeBytes: 200},
	}
	marked := MarkActive(logs)
	// Highest sequence (000004) is active regardless of list order.
	for _, l := range marked {
		if l.Name == "binlog.000004" && !l.Active {
			t.Fatal("binlog.000004 should be active")
		}
		if l.Name != "binlog.000004" && l.Active {
			t.Fatalf("%s should not be active", l.Name)
		}
	}
	archivable := Archivable(marked)
	names := make([]string, 0, len(archivable))
	for _, l := range archivable {
		names = append(names, l.Name)
	}
	if !reflect.DeepEqual(names, []string{"binlog.000002", "binlog.000003"}) {
		t.Fatalf("archivable = %v", names)
	}
}

func TestMarkActiveSingleLog(t *testing.T) {
	t.Parallel()
	marked := MarkActive([]BinaryLog{{Name: "binlog.000001"}})
	if !marked[0].Active {
		t.Fatal("the only log must be active")
	}
	if len(Archivable(marked)) != 0 {
		t.Fatal("a lone active log is not archivable")
	}
}

func TestReadArgs(t *testing.T) {
	t.Parallel()
	args, err := ReadArgs("/data/binlog.000004")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--base64-output=DECODE-ROWS", "/data/binlog.000004"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("ReadArgs = %v, want %v", args, want)
	}
	if _, err := ReadArgs(""); err == nil {
		t.Fatal("expected error on empty file")
	}
}

func TestReplayArgs(t *testing.T) {
	t.Parallel()
	args, err := ReplayArgs(ReplayOptions{
		Files:        []string{"binlog.000004", "binlog.000005"},
		StopDatetime: "2026-06-12 10:00:00",
		ExcludeGTIDs: "uuid:1-5",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"--disable-log-bin",
		"--stop-datetime=2026-06-12 10:00:00",
		"--exclude-gtids=uuid:1-5",
		"binlog.000004", "binlog.000005",
	}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("ReplayArgs = %v, want %v", args, want)
	}
}

func TestReplayArgsValidation(t *testing.T) {
	t.Parallel()
	if _, err := ReplayArgs(ReplayOptions{}); err == nil {
		t.Fatal("expected error with no files")
	}
	if _, err := ReplayArgs(ReplayOptions{
		Files:        []string{"a", "b"},
		StopPosition: 100,
	}); err == nil {
		t.Fatal("stop position with multiple files should error")
	}
	args, err := ReplayArgs(ReplayOptions{Files: []string{"a"}, StopPosition: 100})
	if err != nil {
		t.Fatal(err)
	}
	if args[1] != "--stop-position=100" {
		t.Fatalf("args = %v", args)
	}
}

func TestPurgeLogsStatement(t *testing.T) {
	t.Parallel()
	stmt, err := PurgeLogsStatement("binlog.000010")
	if err != nil {
		t.Fatal(err)
	}
	if stmt != "PURGE BINARY LOGS TO 'binlog.000010'" {
		t.Fatalf("stmt = %q", stmt)
	}
	if _, err := PurgeLogsStatement(""); err == nil {
		t.Fatal("expected error on empty target")
	}
	if _, err := PurgeLogsStatement("binlog'; DROP"); err == nil {
		t.Fatal("expected error on quote injection")
	}
}
