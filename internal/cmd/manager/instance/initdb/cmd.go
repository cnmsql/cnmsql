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

// Package initdb implements `manager instance initdb`: fresh data-dir bootstrap.
package initdb

import (
	"errors"

	"github.com/spf13/cobra"
)

// NewCommand builds the `instance initdb` command.
func NewCommand() *cobra.Command {
	var (
		dataDir  string
		database string
		owner    string
	)

	cmd := &cobra.Command{
		Use:   "initdb",
		Short: "Initialise a fresh MySQL data directory",
		RunE: func(_ *cobra.Command, _ []string) error {
			return errors.New("instance initdb: not implemented yet")
		},
	}

	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/lib/mysql", "MySQL data directory")
	cmd.Flags().StringVar(&database, "database", "", "Application database to create")
	cmd.Flags().StringVar(&owner, "owner", "", "Owner user of the application database")

	return cmd
}
