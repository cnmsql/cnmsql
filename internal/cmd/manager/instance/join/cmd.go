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

// Package join implements `manager instance join`: provision a replica.
package join

import (
	"errors"

	"github.com/spf13/cobra"
)

// NewCommand builds the `instance join` command.
func NewCommand() *cobra.Command {
	var (
		dataDir string
		source  string
	)

	cmd := &cobra.Command{
		Use:   "join",
		Short: "Provision a replica from a source instance via XtraBackup",
		RunE: func(_ *cobra.Command, _ []string) error {
			return errors.New("instance join: not implemented yet")
		},
	}

	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")
	cmd.Flags().StringVar(&source, "source", "", "Host of the source instance to clone from")

	return cmd
}
