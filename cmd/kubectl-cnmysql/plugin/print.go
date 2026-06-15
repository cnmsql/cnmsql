/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package plugin

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"sigs.k8s.io/yaml"
)

// Section prints a bold-ish section header followed by a blank line.
func Section(title string) {
	fmt.Printf("\n%s\n", title)
}

// Table renders rows as an aligned table with the given header. Each row must
// have the same number of columns as the header.
func Table(header []string, rows [][]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	printRow(w, header)
	for _, row := range rows {
		printRow(w, row)
	}
	_ = w.Flush()
}

func printRow(w io.Writer, cols []string) {
	for i, c := range cols {
		if i > 0 {
			_, _ = fmt.Fprint(w, "\t")
		}
		_, _ = fmt.Fprint(w, c)
	}
	_, _ = fmt.Fprintln(w)
}

// KeyVal prints an indented "key: value" line, used in summary sections.
func KeyVal(key, value string) {
	fmt.Printf("  %-22s %s\n", key+":", value)
}

// PrintObject marshals v as JSON or YAML to stdout. format must be "json" or
// "yaml"; any other value returns an error.
func PrintObject(v any, format string) error {
	switch format {
	case "yaml":
		out, err := yaml.Marshal(v)
		if err != nil {
			return err
		}
		fmt.Print(string(out))
	case "json":
		out, err := yaml.YAMLToJSON(mustYAML(v))
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	default:
		return fmt.Errorf("unsupported output format %q (want json or yaml)", format)
	}
	return nil
}

func mustYAML(v any) []byte {
	out, _ := yaml.Marshal(v)
	return out
}
