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
