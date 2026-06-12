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
	"strings"
	"testing"
)

// sampleBinlog mimics `mysqlbinlog --base64-output=DECODE-ROWS binlog.000004`
// output: a format-description event, a Previous-GTIDs event with its set on the
// following comment line, then two GTID transactions.
const sampleBinlog = `/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!40019 SET @@session.max_insert_delayed_threads=0*/;
# at 4
#260612 10:00:00 server id 1  end_log_pos 126 CRC32 0xdeadbeef 	Start: binlog v 4, server v 8.0.46
# at 126
#260612 10:00:00 server id 1  end_log_pos 197 CRC32 0x00000000 	Previous-GTIDs
# 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5
# at 197
#260612 10:00:05 server id 1  end_log_pos 274 CRC32 0x11111111 	GTID	last_committed=0	sequence_number=1
SET @@SESSION.GTID_NEXT= '3e11fa47-71ca-11e1-9e33-c80aa9429562:6'/*!*/;
# at 274
#260612 10:00:05 server id 1  end_log_pos 350 CRC32 0x22222222 	Query	thread_id=10	exec_time=0
BEGIN
/*!*/;
# at 350
### INSERT INTO demo.t
#260612 10:00:09 server id 1  end_log_pos 400 CRC32 0x33333333 	GTID	last_committed=1	sequence_number=2
SET @@SESSION.GTID_NEXT= '3e11fa47-71ca-11e1-9e33-c80aa9429562:7'/*!*/;
# at 400
#260612 10:00:09 server id 1  end_log_pos 480 CRC32 0x44444444 	Query	thread_id=10	exec_time=0
COMMIT
/*!*/;
`

func TestScan(t *testing.T) {
	t.Parallel()
	res, err := Scan(strings.NewReader(sampleBinlog))
	if err != nil {
		t.Fatal(err)
	}
	if res.PreviousGTIDs != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5" {
		t.Fatalf("PreviousGTIDs = %q", res.PreviousGTIDs)
	}
	if res.GTIDSet != "3e11fa47-71ca-11e1-9e33-c80aa9429562:6-7" {
		t.Fatalf("GTIDSet = %q", res.GTIDSet)
	}
	if res.FirstGTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:6" {
		t.Fatalf("FirstGTID = %q", res.FirstGTID)
	}
	if res.LastGTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:7" {
		t.Fatalf("LastGTID = %q", res.LastGTID)
	}
	if res.FirstEventTime.IsZero() || res.LastEventTime.IsZero() {
		t.Fatal("expected non-zero event times")
	}
	if !res.LastEventTime.After(res.FirstEventTime) {
		t.Fatalf("last (%v) should be after first (%v)", res.LastEventTime, res.FirstEventTime)
	}
	if got := res.FirstEventTime.Format("2006-01-02 15:04:05"); got != "2026-06-12 10:00:00" {
		t.Fatalf("FirstEventTime = %q", got)
	}
}

func TestScanEmptyContribution(t *testing.T) {
	t.Parallel()
	// A freshly-rotated file with only a Previous-GTIDs event and no
	// transactions contributes an empty GTID set.
	const onlyPrev = `# at 4
#260612 10:00:00 server id 1  end_log_pos 126 CRC32 0x0 	Start: binlog v 4
# at 126
#260612 10:00:00 server id 1  end_log_pos 197 CRC32 0x0 	Previous-GTIDs
# 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7
`
	res, err := Scan(strings.NewReader(onlyPrev))
	if err != nil {
		t.Fatal(err)
	}
	if res.PreviousGTIDs != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-7" {
		t.Fatalf("PreviousGTIDs = %q", res.PreviousGTIDs)
	}
	if res.GTIDSet != "" {
		t.Fatalf("GTIDSet = %q, want empty", res.GTIDSet)
	}
}

func TestScanEmptyPreviousGTIDs(t *testing.T) {
	t.Parallel()
	// The very first binlog has an empty Previous-GTIDs (no set line follows).
	const firstFile = `# at 126
#260612 10:00:00 server id 1  end_log_pos 197 CRC32 0x0 	Previous-GTIDs
# at 197
#260612 10:00:05 server id 1  end_log_pos 274 CRC32 0x0 	GTID
SET @@SESSION.GTID_NEXT= '3e11fa47-71ca-11e1-9e33-c80aa9429562:1'/*!*/;
`
	res, err := Scan(strings.NewReader(firstFile))
	if err != nil {
		t.Fatal(err)
	}
	if res.PreviousGTIDs != "" {
		t.Fatalf("PreviousGTIDs = %q, want empty", res.PreviousGTIDs)
	}
	if res.GTIDSet != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1" {
		t.Fatalf("GTIDSet = %q", res.GTIDSet)
	}
}
