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

package binlog

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/replication"
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

// ScanOpts configures the scan for engine-specific binlog formats.
type ScanOpts struct {
	// MariaDB selects MariaDB GTID format parsing (domain-server-seq triples).
	MariaDB bool
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

// mariadbGTIDEventRe matches the per-transaction GTID token mariadb-binlog
// prints in the event-header line (not a SET statement; MariaDB has no
// GTID_NEXT). The header looks like:
//
//	#240612 10:00:00 server id 1  end_log_pos 384 CRC32 0x...  GTID 0-1-10 ddl
//
// The "GTID" keyword is upper-case here, which distinguishes it from the
// "Gtid list" event matched by mariadbGtidListRe below.
var mariadbGTIDEventRe = regexp.MustCompile(`\bGTID (\d+)-(\d+)-(\d+)\b`)

// mariadbGtidListRe matches the Gtid_list event at the head of a binlog, which
// carries the GTID set executed before this file (MariaDB's equivalent of
// mysqlbinlog's Previous-GTIDs). Rendered as:
//
//	#240612 10:00:00 server id 1  end_log_pos 256 ...  Gtid list [0-1-9,1-2-5]
//
// The bracketed body may be empty ("Gtid list []").
var mariadbGtidListRe = regexp.MustCompile(`Gtid list \[([0-9,\-]*)\]`)

const eventTimeLayout = "060102 15:04:05"

// Scan parses mysqlbinlog output (produced by ReadArgs) for a single file,
// extracting its GTID coverage and timestamp bounds. opts controls engine-specific
// parsing: MariaDB uses domain-server-seq triples; MySQL uses uuid:n format.
func Scan(r io.Reader, opts ...ScanOpts) (ScanResult, error) {
	var o ScanOpts
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.MariaDB {
		return scanMariaDB(r)
	}
	return scanMySQL(r)
}

func scanMySQL(r io.Reader) (ScanResult, error) {
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

// mariaGTID is the highest sequence seen for a domain, together with the server
// that produced it. Keeping the server per domain (rather than a single
// file-wide server) is what lets a binlog carrying transactions from several
// origin servers — e.g. a downstream with log_slave_updates — render each
// domain with its correct server component.
type mariaGTID struct {
	server uint32
	seq    uint64
}

type mariaScanAccum struct {
	entries map[uint32]mariaGTID
}

func (a *mariaScanAccum) add(domain uint32, server uint32, seq uint64) {
	if a.entries == nil {
		a.entries = make(map[uint32]mariaGTID)
	}
	if existing, ok := a.entries[domain]; !ok || seq >= existing.seq {
		a.entries[domain] = mariaGTID{server: server, seq: seq}
	}
}

func (a *mariaScanAccum) string() string {
	if len(a.entries) == 0 {
		return ""
	}
	domains := make([]uint32, 0, len(a.entries))
	for d := range a.entries {
		domains = append(domains, d)
	}
	slices.Sort(domains)
	var b strings.Builder
	for i, d := range domains {
		if i > 0 {
			b.WriteByte(',')
		}
		e := a.entries[d]
		b.WriteString(strconv.FormatUint(uint64(d), 10))
		b.WriteByte('-')
		b.WriteString(strconv.FormatUint(uint64(e.server), 10))
		b.WriteByte('-')
		b.WriteString(strconv.FormatUint(e.seq, 10))
	}
	return b.String()
}

func scanMariaDB(r io.Reader) (ScanResult, error) {
	var res ScanResult
	accum := mariaScanAccum{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		// Everything MariaDB carries — timestamps, the per-transaction GTID, and
		// the Gtid_list event — lives in the event-header comment line, unlike
		// MySQL where GTIDs land in a following SET @@SESSION.GTID_NEXT statement.
		m := eventHeaderRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		if ts, err := time.Parse(eventTimeLayout, m[1]+" "+normalizeClock(m[2])); err == nil {
			if res.FirstEventTime.IsZero() {
				res.FirstEventTime = ts
			}
			res.LastEventTime = ts
		}

		// The Gtid_list event carries the set executed before this file. Take the
		// first one (it heads the binlog) as PreviousGTIDs, then move on — its
		// bracketed body must not be mistaken for a transaction GTID.
		if gl := mariadbGtidListRe.FindStringSubmatch(line); gl != nil {
			if res.PreviousGTIDs == "" {
				res.PreviousGTIDs = gl[1]
			}
			continue
		}

		if g := mariadbGTIDEventRe.FindStringSubmatch(line); g != nil {
			domain, _ := strconv.ParseUint(g[1], 10, 32)
			server, _ := strconv.ParseUint(g[2], 10, 32)
			seq, _ := strconv.ParseUint(g[3], 10, 64)
			accum.add(uint32(domain), uint32(server), seq)
			gtid := g[1] + "-" + g[2] + "-" + g[3]
			if res.FirstGTID == "" {
				res.FirstGTID = gtid
			}
			res.LastGTID = gtid
		}
	}
	if err := scanner.Err(); err != nil {
		return ScanResult{}, fmt.Errorf("binlog: scanning mariadb-binlog output: %w", err)
	}

	res.GTIDSet = accum.string()
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
