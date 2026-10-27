/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
See the License for the specific language governing permissions and
limitations under the License.
*/

package user

import (
	"strings"
	"testing"
)

// quotingSeeds cover the escape characters themselves, already-escaped
// sequences, statement terminators, comment introducers, and non-UTF-8 bytes.
// Everything quoted in this package comes from DatabaseUser spec fields, so
// these are attacker-reachable shapes.
var quotingSeeds = []string{
	"",
	"app",
	"o'brien",
	`back\slash`,
	`\`,
	`'`,
	`\'`,
	"`",
	"``",
	"a`b",
	`' OR '1'='1`,
	"'; DROP TABLE mysql.user; -- ",
	"`; DROP TABLE mysql.user; -- ",
	"a\x00b",
	"line\nbreak",
	"\xff\xfe",
	"emoji \U0001F600",
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
		// A backtick in the body is only legal as half of a doubled pair;
		// a lone one would close the identifier and leak the rest as SQL.
		if i+1 >= len(body) || body[i+1] != '`' {
			t.Fatalf("quoted identifier %q has an unescaped backtick at offset %d", quoted, i)
		}
		out = append(out, '`')
		i++
	}
	return string(out)
}

// FuzzQuote checks that quote() emits exactly one well-formed SQL string
// literal for any DatabaseUser-supplied value.
func FuzzQuote(f *testing.F) {
	for _, seed := range quotingSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		quoted := quote(s)
		checkQuoted(t, s, quoted)
		checkQuoted(t, quoted, quote(quoted))
	})
}

// FuzzQuoteIdent checks that quoteIdent() emits exactly one well-formed SQL
// identifier: escaping must be reversible so no database or user name can
// break out of the backticks and inject statement text.
func FuzzQuoteIdent(f *testing.F) {
	for _, seed := range quotingSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		quoted := quoteIdent(s)
		if got := unquoteIdent(t, quoted); got != s {
			t.Fatalf("quoteIdent(%q) = %q, which unquotes to %q", s, quoted, got)
		}
		if got := unquoteIdent(t, quoteIdent(quoted)); got != quoted {
			t.Fatalf("double quoting identifier %q is not reversible: got %q", s, got)
		}
	})
}
