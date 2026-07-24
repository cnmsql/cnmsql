/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
See the License for the specific language governing permissions and
limitations under the License.
*/

package replication

import "testing"

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

// unquoteLiteral reverses the escaping quote() applies: it strips the outer
// single quotes and undoes backslash escapes, failing if the interior ever
// terminates the literal early. Operating on bytes rather than runes matches
// the byte-wise ReplaceAll in quote() and keeps invalid UTF-8 meaningful.
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
			// A trailing backslash would escape the closing quote and let the
			// literal run into the rest of the statement.
			if i+1 >= len(body) {
				t.Fatalf("quoted literal %q ends with a dangling backslash", quoted)
			}
			i++
			out = append(out, body[i])
		case '\'':
			// An unescaped quote inside the body closes the literal early:
			// everything after it would be parsed as SQL.
			t.Fatalf("quoted literal %q contains an unescaped single quote at offset %d", quoted, i)
		default:
			out = append(out, body[i])
		}
	}
	return string(out)
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
		if got := unquoteLiteral(t, quoted); got != s {
			t.Fatalf("quote(%q) = %q, which unquotes to %q", s, quoted, got)
		}
		// Quote is the exported alias engine dialects use; it must not drift.
		if exported := Quote(s); exported != quoted {
			t.Fatalf("Quote(%q) = %q but quote(%q) = %q", s, exported, s, quoted)
		}
		// Escaping an already-escaped value must stay reversible rather than
		// collapsing back into an injectable form.
		if got := unquoteLiteral(t, quote(quoted)); got != quoted {
			t.Fatalf("double quoting %q is not reversible: got %q", s, got)
		}
	})
}
