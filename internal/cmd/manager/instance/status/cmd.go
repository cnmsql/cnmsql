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

// Package status implements `manager instance status`: print local status.
package status

import (
	"errors"

	"github.com/spf13/cobra"
)

// NewCommand builds the `instance status` command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Print the local instance status as JSON",
		RunE: func(_ *cobra.Command, _ []string) error {
			return errors.New("instance status: not implemented yet")
		},
	}

	return cmd
}
