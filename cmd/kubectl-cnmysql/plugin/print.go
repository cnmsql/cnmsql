/*
Copyright 2026 The CNMySQL Authors.

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
