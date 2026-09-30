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

// Package probe implements `manager instance probe`: report what an instance
// image contains.
package probe

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/imageprobe"
)

// NewCommand builds the `instance probe` command.
func NewCommand() *cobra.Command {
	var (
		mysqldPath string
		output     string
	)

	cmd := &cobra.Command{
		Use:   "probe",
		Short: "Report the flavor and server version of this image",
		Long: "Run the server binary with --version and print its flavor and server version as " +
			"JSON. The result is also written to --output, the container's termination message " +
			"by default, where the operator reads it from the probe Pod's status.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			result, err := imageprobe.Run(cmd.Context(), mysqldPath)
			if err != nil {
				return err
			}
			data, err := imageprobe.Encode(result)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), string(data)); err != nil {
				return err
			}
			if output == "" {
				return nil
			}
			return os.WriteFile(output, data, 0o644)
		},
	}

	// MariaDB images ship the server as mysqld too (natively on 10.x, through
	// mariadb-server-compat on 11.x and later). With the default name, the probe
	// runs mariadbd when the image has it (see version.Detect).
	cmd.Flags().StringVar(&mysqldPath, "mysqld", "mysqld", "Path to the server binary")
	cmd.Flags().StringVar(&output, "output", "/dev/termination-log", "Also write the result to this file (empty: stdout only)")

	return cmd
}
