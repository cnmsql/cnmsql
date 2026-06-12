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

// Package binlog drives MySQL binary-log continuous archiving from inside the
// instance manager: it enumerates the local binlog files, detects which are
// rotated (immutable, archivable) versus the active tail, extracts each file's
// GTID range and event timestamps, and builds the command lines used to read
// and replay them. It is the in-Pod foundation M7 builds the archiver on, the
// analog of the WAL machinery CNPG runs for PostgreSQL.
package binlog

import (
	"fmt"
	"path"
	"strconv"
	"strings"
)

// BinaryLog is one entry from SHOW BINARY LOGS: a binary-log file the server
// knows about, with its on-disk size.
type BinaryLog struct {
	// Name is the file basename, e.g. "binlog.000004".
	Name string
	// SizeBytes is the file size reported by the server.
	SizeBytes int64
	// Active reports whether this is the log mysqld is currently writing. Only
	// the active log is mutable; every other entry is immutable and archivable.
	Active bool
}

// ParseSequence extracts the numeric index from a binlog basename such as
// "binlog.000004" → 4. The basename is "<base>.<6+ digit sequence>".
func ParseSequence(name string) (int64, error) {
	name = path.Base(name)
	dot := strings.LastIndexByte(name, '.')
	if dot < 0 || dot == len(name)-1 {
		return 0, fmt.Errorf("binlog: %q has no sequence suffix", name)
	}
	seq, err := strconv.ParseInt(name[dot+1:], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("binlog: %q has a non-numeric sequence: %w", name, err)
	}
	return seq, nil
}

// MarkActive returns a copy of logs with the highest-sequence entry flagged
// Active. mysqld always writes the latest binlog, so the active log is simply
// the one with the greatest sequence; everything below it is rotated and
// archivable. The input order is preserved.
func MarkActive(logs []BinaryLog) []BinaryLog {
	out := make([]BinaryLog, len(logs))
	copy(out, logs)
	activeIdx := -1
	var activeSeq int64 = -1
	for i, l := range out {
		seq, err := ParseSequence(l.Name)
		if err != nil {
			continue
		}
		if seq > activeSeq {
			activeSeq = seq
			activeIdx = i
		}
	}
	if activeIdx >= 0 {
		out[activeIdx].Active = true
	}
	return out
}

// Archivable returns the rotated (non-active) logs in sequence order: the files
// eligible to ship because mysqld is no longer writing them. MarkActive must
// have been applied first.
func Archivable(logs []BinaryLog) []BinaryLog {
	var out []BinaryLog
	for _, l := range logs {
		if !l.Active {
			out = append(out, l)
		}
	}
	return out
}

// ReadArgs builds the `mysqlbinlog` argument list to scan a binary-log file for
// its GTID range and event timestamps. base64-output=DECODE-ROWS suppresses the
// bulky base64 row payloads (we only want headers, timestamps, Previous-GTIDs
// and the GTID_NEXT SET statements), keeping the scan cheap.
func ReadArgs(file string, extraArgs ...string) ([]string, error) {
	if file == "" {
		return nil, fmt.Errorf("binlog: file is required")
	}
	return append([]string{"--base64-output=DECODE-ROWS", file}, extraArgs...), nil
}

// ReplayArgs builds the `mysqlbinlog` argument list to replay a set of
// binary-log files (output is meant to be piped into a mysql client). The
// optional bounds restrict replay to the wanted GTID/time window:
//   - StopDatetime bounds a targetTime recovery ("YYYY-MM-DD HH:MM:SS").
//   - ExcludeGTIDs/IncludeGTIDs filter by GTID for targetGTID recovery and to
//     skip already-applied transactions.
type ReplayOptions struct {
	// Files are the binary-log files to replay, in order.
	Files []string
	// StopDatetime, when set, stops replay at the first event at or after it.
	StopDatetime string
	// StopPosition, when set with a single file, stops at that byte offset.
	StopPosition int64
	// IncludeGTIDs replays only these GTIDs (mysqlbinlog --include-gtids).
	IncludeGTIDs string
	// ExcludeGTIDs skips these GTIDs (mysqlbinlog --exclude-gtids); used to drop
	// transactions already present in the restored base backup.
	ExcludeGTIDs string
	// ExtraArgs are appended verbatim before the file list.
	ExtraArgs []string
}

// ReplayArgs builds the `mysqlbinlog` argument list for a bounded replay.
func ReplayArgs(o ReplayOptions) ([]string, error) {
	if len(o.Files) == 0 {
		return nil, fmt.Errorf("binlog: at least one file is required")
	}
	args := []string{"--disable-log-bin"}
	if o.StopDatetime != "" {
		args = append(args, "--stop-datetime="+o.StopDatetime)
	}
	if o.StopPosition > 0 {
		if len(o.Files) != 1 {
			return nil, fmt.Errorf("binlog: stop position requires exactly one file")
		}
		args = append(args, "--stop-position="+strconv.FormatInt(o.StopPosition, 10))
	}
	if o.IncludeGTIDs != "" {
		args = append(args, "--include-gtids="+o.IncludeGTIDs)
	}
	if o.ExcludeGTIDs != "" {
		args = append(args, "--exclude-gtids="+o.ExcludeGTIDs)
	}
	args = append(args, o.ExtraArgs...)
	return append(args, o.Files...), nil
}

// FlushLogsStatement returns the SQL that forces mysqld to rotate the active
// binary log, turning it into an immutable, archivable file. This is the RPO
// trigger: a periodic/sized flush bounds how long acknowledged data waits to be
// archived.
func FlushLogsStatement() string {
	return "FLUSH BINARY LOGS"
}

// PurgeLogsStatement returns the SQL that purges binary logs up to (but not
// including) the named file. The archiver only ever issues this for files it
// has already shipped, so mysqld never recycles an un-archived log.
func PurgeLogsStatement(upTo string) (string, error) {
	if upTo == "" {
		return "", fmt.Errorf("binlog: purge target file is required")
	}
	if strings.ContainsAny(upTo, "'\\") {
		return "", fmt.Errorf("binlog: invalid purge target %q", upTo)
	}
	return fmt.Sprintf("PURGE BINARY LOGS TO '%s'", upTo), nil
}
