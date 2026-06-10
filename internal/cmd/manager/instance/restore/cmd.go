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

// Package restore implements `manager instance restore`: restore from a backup.
package restore

import (
	"errors"

	"github.com/spf13/cobra"
)

// NewCommand builds the `instance restore` command.
func NewCommand() *cobra.Command {
	var dataDir string

	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore an instance from a physical backup",
		RunE: func(_ *cobra.Command, _ []string) error {
			return errors.New("instance restore: not implemented yet")
		},
	}

	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")

	return cmd
}
