package plugin

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/logrusorgru/aurora/v4"
)

// captureOut redirects Out and forces colors on for the test.
func captureOut(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prevOut, prevColor := Out, aurora.DefaultColorizer
	Out = &buf
	aurora.DefaultColorizer = aurora.New(aurora.WithColors(true))
	t.Cleanup(func() { Out, aurora.DefaultColorizer = prevOut, prevColor })
	return &buf
}

func TestTableAlignsColoredCells(t *testing.T) {
	buf := captureOut(t)
	Table([]string{"NAME", "STATUS", "NODE"}, [][]string{
		{"demo-1", Green("OK").String(), "node-a"},
		{"demo-2", Red("Unreachable").String(), "node-b"},
	})
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d lines, want header, rule and 2 rows:\n%s", len(lines), buf.String())
	}
	// The NODE column starts at the same visible offset on every row.
	var col int
	for i, line := range lines {
		plain := ansiSequence.ReplaceAllString(line, "")
		idx := strings.Index(plain, "node-")
		if i == 0 {
			idx = strings.Index(plain, "NODE")
		}
		if i == 1 {
			continue // the dashed rule
		}
		if col == 0 {
			col = idx
		} else if idx != col {
			t.Errorf("line %d: NODE column at %d, want %d:\n%s", i, idx, col, buf.String())
		}
	}
	if !strings.HasPrefix(ansiSequence.ReplaceAllString(lines[1], ""), "----    ------       ----") {
		t.Errorf("rule = %q", lines[1])
	}
}

func TestFieldsAlignOnLongestKey(t *testing.T) {
	buf := captureOut(t)
	f := Fields{}
	f.Add("Name", "demo")
	f.Add("Primary instance", Green("demo-1"))
	f.Add("Empty", "")
	f.Print()
	plain := ansiSequence.ReplaceAllString(buf.String(), "")
	want := "Name:              demo\nPrimary instance:  demo-1\nEmpty:             -\n"
	if plain != want {
		t.Errorf("fields =\n%q\nwant\n%q", plain, want)
	}
}

func TestHumanFormatting(t *testing.T) {
	t.Parallel()
	for in, want := range map[int64]string{512: "512 B", 1536: "1.5 KiB", 3 << 30: "3.0 GiB"} {
		if got := HumanBytes(in); got != want {
			t.Errorf("HumanBytes(%d) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[time.Duration]string{
		250 * time.Millisecond: "250ms", 42 * time.Second: "42s", 7*time.Minute + 12*time.Second: "7m12s",
		7*time.Hour + 42*time.Minute: "7h42m", 100 * time.Hour: "4d4h",
	} {
		if got := HumanDuration(in); got != want {
			t.Errorf("HumanDuration(%s) = %q, want %q", in, got, want)
		}
	}
	if VisibleWidth(Red("abc").String()) != 3 {
		t.Error("VisibleWidth must ignore color sequences")
	}
}
