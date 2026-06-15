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

// Package manager wires the cobra command tree for the in-pod instance manager.
package manager

import (
	"github.com/spf13/cobra"

	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance"
)

// NewRootCommand builds the root cobra command for the instance manager binary.
func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "manager",
		Short:         "cloudnative-mysql in-pod instance manager",
		Long:          "manager supervises mysqld and orchestrates bootstrap, replication and lifecycle for a cloudnative-mysql instance.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.AddCommand(instance.NewCommand())

	return cmd
}
