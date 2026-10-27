/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
See the License for the specific language governing permissions and
limitations under the License.
*/

package replication

import (
	"strings"
	"testing"
)

// quotingSeeds cover the inputs that matter for SQL literal escaping: the
// escape characters themselves, already-escaped sequences, statement
// terminators, comment introducers, and non-UTF-8 bytes. Names and passwords
// reaching quote() come from user-controlled CR fields.
var quotingSeeds = []string{
	"",
	"app",
	"o'brien",
	`back\slash`,
	`\`,
	`'`,
	`\'`,
	`''`,
	`\\`,
	`\\'`,
	`' OR '1'='1`,
	"'; DROP TABLE mysql.user; -- ",
	"a\x00b",
	"line\nbreak",
	"\xff\xfe",
	"emoji \U0001F600",
}

// unquoteLiteral reads quoted the way MySQL's lexer reads a single-quoted
// literal: a doubled quote is one quote and, unless noBackslashEscapes (the
// sql_mode flag), a backslash escapes the next byte. It fails if the literal
// closes before the last byte, since everything after it would be parsed as
// SQL. Operating on bytes rather than runes matches the byte-wise ReplaceAll
// in quote() and keeps invalid UTF-8 meaningful.
func unquoteLiteral(t *testing.T, quoted string, noBackslashEscapes bool) string {
	t.Helper()
	if len(quoted) < 2 || quoted[0] != '\'' {
		t.Fatalf("quoted literal %q does not start with a single quote", quoted)
	}
	var out []byte
	for i := 1; i < len(quoted); i++ {
		switch c := quoted[i]; {
		case c == '\\' && !noBackslashEscapes:
			i++
			if i >= len(quoted) {
				t.Fatalf("quoted literal %q ends with a dangling backslash", quoted)
			}
			out = append(out, quoted[i])
		case c == '\'' && i+1 < len(quoted) && quoted[i+1] == '\'':
			out = append(out, '\'')
			i++
		case c == '\'':
			if i != len(quoted)-1 {
				t.Fatalf("quoted literal %q closes at offset %d (noBackslashEscapes=%v)", quoted, i, noBackslashEscapes)
			}
			return string(out)
		default:
			out = append(out, c)
		}
	}
	t.Fatalf("quoted literal %q is never closed (noBackslashEscapes=%v)", quoted, noBackslashEscapes)
	return ""
}

// checkQuoted asserts quoted is exactly one literal holding s under the
// default sql_mode, and still exactly one literal (backslashes read verbatim)
// under NO_BACKSLASH_ESCAPES, which spec.mysql.parameters can set.
func checkQuoted(t *testing.T, s, quoted string) {
	t.Helper()
	if got := unquoteLiteral(t, quoted, false); got != s {
		t.Fatalf("%q unquotes to %q, want %q", quoted, got, s)
	}
	want := strings.ReplaceAll(s, `\`, `\\`)
	if got := unquoteLiteral(t, quoted, true); got != want {
		t.Fatalf("%q unquotes to %q under NO_BACKSLASH_ESCAPES, want %q", quoted, got, want)
	}
}

// FuzzQuote checks that quote() emits exactly one well-formed SQL string
// literal for any input: the escaping must be reversible, so no input can
// break out of the literal and inject statement text.
func FuzzQuote(f *testing.F) {
	for _, seed := range quotingSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		quoted := quote(s)
		checkQuoted(t, s, quoted)
		// Quote is the exported alias engine dialects use; it must not drift.
		if exported := Quote(s); exported != quoted {
			t.Fatalf("Quote(%q) = %q but quote(%q) = %q", s, exported, s, quoted)
		}
		// Escaping an already-escaped value must stay reversible rather than
		// collapsing back into an injectable form.
		checkQuoted(t, quoted, quote(quoted))
	})
}
