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
	"strings"
	"testing"
)

func TestParseMariabackupBinlogPos(t *testing.T) {
	tests := []struct {
		name     string
		stderr   string
		wantFile string
		wantPos  int64
		wantGTID string
		wantOK   bool
	}{
		{
			name:     "10.11 file+position only",
			stderr:   "mariabackup: MySQL binlog position: filename 'binlog.000004', position '831'\n",
			wantFile: "binlog.000004", wantPos: 831, wantGTID: "", wantOK: true,
		},
		{
			name:     "11.1 with GTID clause",
			stderr:   "mariabackup: MySQL binlog position: filename 'binlog.000004', position '831', GTID of the last change '0-1-2'\n",
			wantFile: "binlog.000004", wantPos: 831, wantGTID: "0-1-2", wantOK: true,
		},
		{
			name: "last match wins amid progress noise",
			stderr: "[00] copying...\n" +
				"mariabackup: MySQL binlog position: filename 'binlog.000004', position '111'\n" +
				"[00] more...\n" +
				"mariabackup: MySQL binlog position: filename 'binlog.000005', position '900'\n",
			wantFile: "binlog.000005", wantPos: 900, wantGTID: "", wantOK: true,
		},
		{
			name:   "no coordinate line",
			stderr: "[00] just some log output with no position line\n",
			wantOK: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			file, pos, gtid, ok := parseMariabackupBinlogPos(tc.stderr)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if file != tc.wantFile || pos != tc.wantPos || gtid != tc.wantGTID {
				t.Errorf("got (%q, %d, %q), want (%q, %d, %q)",
					file, pos, gtid, tc.wantFile, tc.wantPos, tc.wantGTID)
			}
		})
	}
}

func TestTailWriterKeepsLastBytes(t *testing.T) {
	tw := newTailWriter(10)
	// Write more than the cap across several writes; only the last 10 bytes survive.
	for _, chunk := range []string{"aaaa", "bbbb", "cccc", "dddd"} {
		if _, err := tw.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := tw.String(), "bbccccdddd"; got != want {
		t.Fatalf("tail = %q, want %q", got, want)
	}

	// The coordinate line printed at the very end must survive a flood of preceding
	// output when the cap is large enough to hold it.
	tw = newTailWriter(maxBinlogPosTailBytes)
	_, _ = tw.Write([]byte(strings.Repeat("noise\n", 100000)))
	line := "mariabackup: MySQL binlog position: filename 'binlog.000009', position '4242'\n"
	_, _ = tw.Write([]byte(line))
	if !strings.Contains(tw.String(), "binlog.000009") {
		t.Fatalf("tail dropped the trailing coordinate line")
	}
	if _, _, _, ok := parseMariabackupBinlogPos(tw.String()); !ok {
		t.Fatalf("coordinate line not parseable from retained tail")
	}
}
