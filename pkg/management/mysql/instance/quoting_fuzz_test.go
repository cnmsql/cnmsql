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

// unquoteLiteral reverses quoteString(): it strips the outer single quotes and
// undoes backslash escapes, failing if the literal terminates early.
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
		if got := unquoteLiteral(t, quoted); got != s {
			t.Fatalf("quoteString(%q) = %q, which unquotes to %q", s, quoted, got)
		}
		// Callers wrap escapeName's output in quotes by hand, so the result
		// must be byte-identical to what quoteString would have produced.
		if wrapped := "'" + escapeName(s) + "'"; wrapped != quoted {
			t.Fatalf("escapeName(%q) wrapped = %q, but quoteString = %q", s, wrapped, quoted)
		}
		if strings.Contains(escapeName(s), "'") && !strings.Contains(escapeName(s), `\'`) {
			t.Fatalf("escapeName(%q) leaves a bare quote: %q", s, escapeName(s))
		}
	})
}
