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
	"regexp"
	"strconv"
	"strings"
)

// mariabackupBinlogPosRE matches the binlog-coordinate line mariabackup/xtrabackup
// prints near the end of a backup, e.g.
//
//	mariabackup: MySQL binlog position: filename 'binlog.000004', position '831'
//	mariabackup: MySQL binlog position: filename 'binlog.000004', position '831', GTID of the last change '0-1-2'
//
// The GTID clause is present on 11.1+ and absent on 10.11, which is exactly the
// gap this anchor resolution fills.
var mariabackupBinlogPosRE = regexp.MustCompile(
	`filename '([^']*)',\s*position '([^']*)'(?:,\s*GTID of the last change '([^']*)')?`)

// parseMariabackupBinlogPos extracts the base backup's binlog coordinates (and the
// GTID, when the tool already reported one) from mariabackup's stderr. When the
// output contains several matches only the last is used: the authoritative
// position line is printed once the backup's consistent point is fixed, at the end.
func parseMariabackupBinlogPos(stderr string) (file string, pos int64, gtid string, ok bool) {
	matches := mariabackupBinlogPosRE.FindAllStringSubmatch(stderr, -1)
	if len(matches) == 0 {
		return "", 0, "", false
	}
	m := matches[len(matches)-1]
	p, err := strconv.ParseInt(strings.TrimSpace(m[2]), 10, 64)
	if err != nil {
		return "", 0, "", false
	}
	return m[1], p, m[3], true
}

// tailWriter is an io.Writer that retains only the last max bytes written to it.
// mariabackup's stderr is dominated by progress lines and the coordinate line we
// need is printed at the very end, so a head-bounded buffer would miss it; keeping
// the tail bounds memory while preserving the line of interest.
type tailWriter struct {
	max int
	buf []byte
}

func newTailWriter(max int) *tailWriter { return &tailWriter{max: max} }

func (t *tailWriter) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = t.buf[len(t.buf)-t.max:]
	}
	return len(p), nil
}

func (t *tailWriter) String() string { return string(t.buf) }
