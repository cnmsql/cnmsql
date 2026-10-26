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

// Package sqlredact replaces password literals in SQL text with <secret>, so
// statements copied into error messages, logs or other output never carry a
// password.
package sqlredact

import "regexp"

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
	// MariaDB chains authentication plugins: IDENTIFIED VIA a USING … OR b USING ….
	regexp.MustCompile(`(?i)(\bOR\s+\S+\s+(?:USING|AS)\s+(?:PASSWORD\s*\(\s*)?)` + sqlLiteral),
	regexp.MustCompile(`(?i)(\bSET\s+PASSWORD\b[^=]*=\s*(?:PASSWORD\s*\(\s*)?)` + sqlLiteral),
	regexp.MustCompile(`(?i)(\b(?:MASTER_|SOURCE_)?PASSWORD\s*=\s*)` + sqlLiteral),
}

// SQL returns q with password literals replaced by <secret>, as MySQL does
// before logging the same statements.
func SQL(q string) string {
	for _, re := range secretPatterns {
		q = re.ReplaceAllString(q, "${1}<secret>")
	}
	return q
}
