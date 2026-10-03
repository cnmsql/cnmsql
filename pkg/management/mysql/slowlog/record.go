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
	"time"
	"unicode/utf8"

	mysqllog "github.com/percona/go-mysql/log"
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
func keysAndValues(e *mysqllog.Event) []any {
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
	query, truncated := capQuery(e.Query)
	kv = append(kv, "query", query)
	if truncated {
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
