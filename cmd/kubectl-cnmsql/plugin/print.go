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

package plugin

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/logrusorgru/aurora/v4"
	"sigs.k8s.io/yaml"
)

// Out is where the human-readable helpers write. It is a variable so tests can
// capture output and --watch can render a frame off-screen before swapping it
// in.
var Out io.Writer = os.Stdout

// NoValue is the placeholder printed for an empty field, like kubectl cnpg.
const NoValue = "-"

// ansiSequence matches the SGR color sequences and OSC 8 hyperlinks aurora
// emits, so the renderers can measure the width a value actually occupies.
var ansiSequence = regexp.MustCompile(`\x1b\[[0-9;]*m|\x1b]8;[^\x1b]*\x1b\\`)

// VisibleWidth returns the number of terminal columns s occupies, ignoring
// color escape sequences.
func VisibleWidth(s string) int {
	return utf8.RuneCountInString(ansiSequence.ReplaceAllString(s, ""))
}

// Section prints a section title, preceded by a blank line.
func Section(title string) {
	_, _ = fmt.Fprintf(Out, "\n%s\n", aurora.Bold(aurora.Green(title)))
}

// Table renders rows as aligned columns under a bold header and a dashed rule,
// in the style of kubectl cnpg. Cells may carry color: alignment is computed on
// their visible width, which text/tabwriter cannot do.
func Table(header []string, rows [][]string) {
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = VisibleWidth(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				widths[i] = max(widths[i], VisibleWidth(cell))
			}
		}
	}

	rules := make([]string, len(header))
	bold := make([]string, len(header))
	for i, h := range header {
		rules[i] = strings.Repeat("-", VisibleWidth(h))
		bold[i] = aurora.Bold(h).String()
	}
	printRow(widths, bold)
	printRow(widths, rules)
	for _, row := range rows {
		printRow(widths, row)
	}
}

func printRow(widths []int, cols []string) {
	var b strings.Builder
	for i, c := range cols {
		if i >= len(widths) {
			break
		}
		b.WriteString(c)
		if i < len(cols)-1 && i < len(widths)-1 {
			b.WriteString(strings.Repeat(" ", widths[i]-VisibleWidth(c)+2))
		}
	}
	_, _ = fmt.Fprintln(Out, strings.TrimRight(b.String(), " "))
}

// Fields accumulates key/value lines for one section and prints them with the
// values aligned on the longest key.
type Fields struct {
	rows [][2]string
}

// Add appends a key/value line. The value is rendered with %v, so aurora
// values keep their color.
func (f *Fields) Add(key string, value any) {
	s := fmt.Sprint(value)
	if s == "" {
		s = NoValue
	}
	f.rows = append(f.rows, [2]string{key + ":", s})
}

// Print writes the accumulated lines.
func (f *Fields) Print() {
	width := 0
	for _, r := range f.rows {
		width = max(width, VisibleWidth(r[0]))
	}
	for _, r := range f.rows {
		_, _ = fmt.Fprintf(Out, "%s%s  %s\n", r[0], strings.Repeat(" ", width-VisibleWidth(r[0])), r[1])
	}
}

// KeyVal prints a single key/value line aligned on a fixed column. Sections
// with several lines should prefer Fields, which aligns on the longest key.
func KeyVal(key string, value any) {
	f := Fields{}
	f.Add(fmt.Sprintf("%-21s", key), value)
	f.Print()
}

// PrintObject marshals v as yaml or json.
func PrintObject(v any, format string) error {
	switch format {
	case "yaml":
		out, err := yaml.Marshal(v)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprint(Out, string(out))
	case "json":
		out, err := yaml.YAMLToJSON(mustYAML(v))
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintln(Out, string(out))
	default:
		return fmt.Errorf("unsupported output format %q (want json or yaml)", format)
	}
	return nil
}

func mustYAML(v any) []byte {
	out, _ := yaml.Marshal(v)
	return out
}

// Green, Red, Yellow, Bold and Faint wrap aurora so callers color through the
// colorizer configured by --color.
func Green(v any) aurora.Value  { return aurora.Green(v) }
func Red(v any) aurora.Value    { return aurora.Red(v) }
func Yellow(v any) aurora.Value { return aurora.Yellow(v) }
func Bold(v any) aurora.Value   { return aurora.Bold(v) }
func Faint(v any) aurora.Value  { return aurora.Faint(v) }

// Badge colors label green when ok, red when bad, and yellow otherwise.
func Badge(label string, ok, bad bool) aurora.Value {
	switch {
	case ok:
		return aurora.Green(label)
	case bad:
		return aurora.Red(label)
	default:
		return aurora.Yellow(label)
	}
}

// Or returns s, or the NoValue placeholder when s is empty.
func Or(s string) string {
	if s == "" {
		return NoValue
	}
	return s
}

// HumanBytes formats a byte count with binary units (1.5 GiB).
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// HumanDuration formats d compactly at a precision suited to its magnitude:
// 250ms, 42s, 7m12s, 7h42m, 12d4h.
func HumanDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// Timestamp formats t in local time followed by its age, e.g.
// "2026-09-24 11:11:06 CEST (7h42m ago)". A zero time renders as NoValue.
func Timestamp(t time.Time) string {
	if t.IsZero() {
		return NoValue
	}
	since := time.Since(t)
	age := HumanDuration(since) + " ago"
	if since < 0 {
		age = "in " + HumanDuration(since)
	}
	return fmt.Sprintf("%s %s", t.Local().Format("2006-01-02 15:04:05 MST"), Faint("("+age+")"))
}
