/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
See the License for the specific language governing permissions and
limitations under the License.
*/

package instance

import (
	"strings"
	"testing"
)

var quotingSeeds = []string{
	"", "app", "o'brien", `back\slash`, `\`, `'`, `\'`, "`", "``", "a`b",
	`' OR '1'='1`, "'; DROP TABLE mysql.user; -- ", "`; DROP DATABASE app; -- ",
	"a\x00b", "line\nbreak", "\xff\xfe", "emoji \U0001F600",
}

// unquoteIdent reverses quoteIdent(): it strips the outer backticks and undoes
// backtick doubling, failing if a lone backtick closes the identifier early.
func unquoteIdent(t *testing.T, quoted string) string {
	t.Helper()
	if len(quoted) < 2 || quoted[0] != '`' || quoted[len(quoted)-1] != '`' {
		t.Fatalf("quoted identifier %q is not wrapped in backticks", quoted)
	}
	body := quoted[1 : len(quoted)-1]
	var out []byte
	for i := 0; i < len(body); i++ {
		if body[i] != '`' {
			out = append(out, body[i])
			continue
		}
		if i+1 >= len(body) || body[i+1] != '`' {
			t.Fatalf("quoted identifier %q has an unescaped backtick at offset %d", quoted, i)
		}
		out = append(out, '`')
		i++
	}
	return string(out)
}

// unquoteLiteral reads quoted the way MySQL's lexer reads a single-quoted
// literal: a doubled quote is one quote and, unless noBackslashEscapes (the
// sql_mode flag), a backslash escapes the next byte. It fails if the literal
// closes before the last byte, since everything after it would be parsed as
// SQL.
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

// FuzzQuoteIdent checks that bootstrap identifier quoting is reversible, so no
// database name from a Cluster spec can break out of its backticks.
func FuzzQuoteIdent(f *testing.F) {
	for _, seed := range quotingSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		quoted := quoteIdent(s)
		if got := unquoteIdent(t, quoted); got != s {
			t.Fatalf("quoteIdent(%q) = %q, which unquotes to %q", s, quoted, got)
		}
	})
}

// FuzzQuoteString checks that bootstrap literal quoting is reversible, and
// that escapeName stays consistent with it: escapeName produces the same body
// without the surrounding quotes, which its callers supply themselves.
func FuzzQuoteString(f *testing.F) {
	for _, seed := range quotingSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		quoted := quoteString(s)
		if got := unquoteLiteral(t, quoted, false); got != s {
			t.Fatalf("quoteString(%q) = %q, which unquotes to %q", s, quoted, got)
		}
		// Under NO_BACKSLASH_ESCAPES (settable through spec.mysql.parameters)
		// backslashes read verbatim, but the value must stay one literal.
		want := strings.ReplaceAll(s, `\`, `\\`)
		if got := unquoteLiteral(t, quoted, true); got != want {
			t.Fatalf("quoteString(%q) = %q, which unquotes to %q under NO_BACKSLASH_ESCAPES", s, quoted, got)
		}
		// Callers wrap escapeName's output in quotes by hand, so the result
		// must be byte-identical to what quoteString would have produced.
		if wrapped := "'" + escapeName(s) + "'"; wrapped != quoted {
			t.Fatalf("escapeName(%q) wrapped = %q, but quoteString = %q", s, wrapped, quoted)
		}
	})
}
