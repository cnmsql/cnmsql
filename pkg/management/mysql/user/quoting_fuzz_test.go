/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
See the License for the specific language governing permissions and
limitations under the License.
*/

package user

import "testing"

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

// unquoteLiteral reverses quote(): it strips the outer single quotes and undoes
// backslash escapes, failing if the interior terminates the literal early.
func unquoteLiteral(t *testing.T, quoted string) string {
	t.Helper()
	if len(quoted) < 2 || quoted[0] != '\'' || quoted[len(quoted)-1] != '\'' {
		t.Fatalf("quoted literal %q is not wrapped in single quotes", quoted)
	}
	body := quoted[1 : len(quoted)-1]
	var out []byte
	for i := 0; i < len(body); i++ {
		switch body[i] {
		case '\\':
			if i+1 >= len(body) {
				t.Fatalf("quoted literal %q ends with a dangling backslash", quoted)
			}
			i++
			out = append(out, body[i])
		case '\'':
			t.Fatalf("quoted literal %q contains an unescaped single quote at offset %d", quoted, i)
		default:
			out = append(out, body[i])
		}
	}
	return string(out)
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
		if got := unquoteLiteral(t, quoted); got != s {
			t.Fatalf("quote(%q) = %q, which unquotes to %q", s, quoted, got)
		}
		if got := unquoteLiteral(t, quote(quoted)); got != quoted {
			t.Fatalf("double quoting %q is not reversible: got %q", s, got)
		}
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
