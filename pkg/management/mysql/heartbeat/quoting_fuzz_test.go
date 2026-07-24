/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
See the License for the specific language governing permissions and
limitations under the License.
*/

package heartbeat

import "testing"

var quotingSeeds = []string{
	"", "heartbeat", "o'brien", `back\slash`, `\`, `'`, "`", "``", "a`b",
	"`; DROP TABLE mysql.user; -- ", "a\x00b", "line\nbreak", "\xff\xfe",
	"emoji \U0001F600",
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

// FuzzQuoteIdent checks that heartbeat identifier quoting is reversible: the
// schema and table names it interpolates come from operator configuration.
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
