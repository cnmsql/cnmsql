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

package slowlog

import (
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/slowlog/internal/slowparse"
)

// MaxQueryBytes caps the statement text one record carries. A multi-megabyte
// INSERT would otherwise become a log line most log pipelines reject.
const MaxQueryBytes = 64 << 10

var (
	// timeFields and numberFields are the metrics promoted to typed record
	// fields, as {parser name, record key}.
	timeFields   = [][2]string{{"Query_time", "query_time"}, {"Lock_time", "lock_time"}}
	numberFields = [][2]string{
		{"Rows_sent", "rows_sent"}, {"Rows_examined", "rows_examined"},
		{"Rows_affected", "rows_affected"}, {"Bytes_sent", "bytes_sent"},
		{"Thread_id", "thread_id"},
	}
	// unparsedMetrics are values the parser cannot represent: log_slow_extra's
	// Start and End are timestamps, which it reads as integers and reports as 0.
	unparsedMetrics = map[string]bool{"Start": true, "End": true}
)

// keysAndValues renders one parsed entry as the key/value pairs of its record.
func keysAndValues(e *slowparse.Event) []any {
	kv := make([]any, 0, 28)
	if !e.Ts.IsZero() {
		kv = append(kv, "time", e.Ts.UTC().Format(time.RFC3339Nano))
	}
	for _, f := range [][2]string{{"user", e.User}, {"host", e.Host}, {"db", e.Db}} {
		if f[1] != "" {
			kv = append(kv, f[0], f[1])
		}
	}

	attributes := make(map[string]any)
	for name, v := range e.TimeMetrics {
		attributes[name] = v
	}
	for name, v := range e.NumberMetrics {
		if !unparsedMetrics[name] {
			attributes[name] = v
		}
	}
	for name, v := range e.BoolMetrics {
		attributes[name] = v
	}
	if e.RateType != "" {
		attributes["Log_slow_rate_type"] = e.RateType
		attributes["Log_slow_rate_limit"] = e.RateLimit
	}
	for _, f := range timeFields {
		if v, ok := e.TimeMetrics[f[0]]; ok {
			kv = append(kv, f[1], v)
			delete(attributes, f[0])
		}
	}
	for _, f := range numberFields {
		if v, ok := e.NumberMetrics[f[0]]; ok {
			kv = append(kv, f[1], v)
			delete(attributes, f[0])
		}
	}

	if e.Admin {
		kv = append(kv, "admin", true)
	}
	query, truncated := capQuery(redactSecrets(e.Query))
	kv = append(kv, "query", query)
	if truncated || e.QueryTruncated {
		kv = append(kv, "query_truncated", true)
	}
	if len(attributes) > 0 {
		kv = append(kv, "attributes", attributes)
	}
	return kv
}

// capQuery cuts q to at most MaxQueryBytes without splitting a UTF-8 sequence.
func capQuery(q string) (string, bool) {
	if len(q) <= MaxQueryBytes {
		return q, false
	}
	cut := MaxQueryBytes
	for cut > 0 && !utf8.RuneStart(q[cut]) {
		cut--
	}
	return q[:cut], true
}

// sqlLiteral matches a single- or double-quoted SQL string, with backslash and
// doubled-quote escapes.
const sqlLiteral = `('(?:[^'\\]|\\.|'')*'|"(?:[^"\\]|\\.|"")*")`

// secretPatterns match the prefix of a password literal. MySQL rewrites these
// statements before logging them; MariaDB logs them verbatim, including the
// CREATE USER, ALTER USER and CHANGE MASTER statements the operator sends with
// passwords from the cluster's Secrets. Redaction is best-effort: it covers the
// account and replication syntax, not passwords hidden in arbitrary SQL.
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(\bIDENTIFIED\s+(?:WITH\s+\S+\s+)?BY\s+(?:PASSWORD\s+)?)` + sqlLiteral),
	regexp.MustCompile(`(?i)(\bIDENTIFIED\s+(?:VIA|WITH)\s+\S+\s+(?:USING|AS)\s+(?:PASSWORD\s*\(\s*)?)` + sqlLiteral),
	regexp.MustCompile(`(?i)(\bSET\s+PASSWORD\b[^=]*=\s*(?:PASSWORD\s*\(\s*)?)` + sqlLiteral),
	regexp.MustCompile(`(?i)(\b(?:MASTER_|SOURCE_)?PASSWORD\s*=\s*)` + sqlLiteral),
}

// redactSecrets replaces password literals in q with <secret>, as MySQL does.
func redactSecrets(q string) string {
	for _, re := range secretPatterns {
		q = re.ReplaceAllString(q, "${1}<secret>")
	}
	return q
}
