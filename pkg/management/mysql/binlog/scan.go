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
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
)

// ScanResult is the GTID and timestamp summary extracted from one binary-log
// file's mysqlbinlog output. It is what the per-file manifest records so
// recovery can order and bound the stream without re-parsing the file.
type ScanResult struct {
	// PreviousGTIDs is the GTID set executed before this file (its head
	// Previous-GTIDs event). The cumulative archive frontier after a file equals
	// the next file's PreviousGTIDs.
	PreviousGTIDs string
	// GTIDSet is the canonical set of GTIDs this file contributes.
	GTIDSet string
	// FirstGTID and LastGTID are the first and last single GTIDs seen
	// ("uuid:n"), useful for quick inspection.
	FirstGTID string
	LastGTID  string
	// FirstEventTime and LastEventTime bound the file in wall-clock time, used
	// for targetTime recovery. They are parsed from mysqlbinlog event headers,
	// which print in the server's local time zone; treat them as UTC-normalized
	// monotonic bounds rather than absolute instants.
	FirstEventTime time.Time
	LastEventTime  time.Time
}

// gtidNextRe matches the per-transaction GTID assignment mysqlbinlog prints:
//
//	SET @@SESSION.GTID_NEXT= '3e11fa47-71ca-11e1-9e33-c80aa9429562:6'/*!*/;
var gtidNextRe = regexp.MustCompile(`(?i)SET\s+@@SESSION\.GTID_NEXT\s*=\s*'([0-9a-fA-F-]{36}):(\d+)'`)

// eventHeaderRe matches a mysqlbinlog event header line and captures its
// timestamp:
//
//	#240612 10:00:00 server id 1  end_log_pos 197 ...  GTID ...
var eventHeaderRe = regexp.MustCompile(`^#(\d{6})\s+(\d{1,2}:\d{2}:\d{2})\s+server id\b`)

// prevGTIDsLabelRe detects the Previous-GTIDs event header; the GTID set is on
// the following "# <set>" comment line.
var prevGTIDsLabelRe = regexp.MustCompile(`(?i)\bPrevious-?GTIDs\b`)

// gtidSetLineRe matches a comment line holding a GTID set, e.g.
// "# 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5".
var gtidSetLineRe = regexp.MustCompile(`^#\s*([0-9a-fA-F-]{36}:[0-9:,-]+)\s*$`)

const eventTimeLayout = "060102 15:04:05"

// Scan parses mysqlbinlog output (produced by ReadArgs) for a single file,
// extracting its GTID coverage and timestamp bounds.
func Scan(r io.Reader) (ScanResult, error) {
	var res ScanResult
	contributed := replication.GTIDSet{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	expectPrevGTIDs := false
	for scanner.Scan() {
		line := scanner.Text()

		if expectPrevGTIDs {
			expectPrevGTIDs = false
			if m := gtidSetLineRe.FindStringSubmatch(line); m != nil {
				res.PreviousGTIDs = canonicalGTIDSet(m[1])
				continue
			}
		}

		if m := eventHeaderRe.FindStringSubmatch(line); m != nil {
			if ts, err := time.Parse(eventTimeLayout, m[1]+" "+normalizeClock(m[2])); err == nil {
				if res.FirstEventTime.IsZero() {
					res.FirstEventTime = ts
				}
				res.LastEventTime = ts
			}
			if prevGTIDsLabelRe.MatchString(line) {
				expectPrevGTIDs = true
			}
			continue
		}

		if m := gtidNextRe.FindStringSubmatch(line); m != nil {
			uuid := strings.ToLower(m[1])
			n, ok := parsePositiveInt(m[2])
			if !ok {
				continue
			}
			gtid := uuid + ":" + m[2]
			if res.FirstGTID == "" {
				res.FirstGTID = gtid
			}
			res.LastGTID = gtid
			contributed.AddInterval(uuid, replication.GTIDInterval{Start: n, End: n})
		}
	}
	if err := scanner.Err(); err != nil {
		return ScanResult{}, fmt.Errorf("binlog: scanning mysqlbinlog output: %w", err)
	}

	res.GTIDSet = contributed.String()
	return res, nil
}

// canonicalGTIDSet re-parses and re-renders a GTID set so manifests store a
// normalized form; on parse failure it returns the input unchanged.
func canonicalGTIDSet(raw string) string {
	if set, err := replication.ParseGTIDSet(raw); err == nil {
		return set.String()
	}
	return raw
}

// normalizeClock left-pads a single-digit hour ("1:02:03" → "01:02:03") so it
// matches the fixed-width parse layout.
func normalizeClock(clock string) string {
	if len(clock) == 7 {
		return "0" + clock
	}
	return clock
}

func parsePositiveInt(s string) (int64, bool) {
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int64(r-'0')
	}
	return n, n > 0
}
