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

// Package instance provides the `manager instance` command tree.
package instance

import (
	"github.com/spf13/cobra"

	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance/backup"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance/initdb"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance/join"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance/restore"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance/run"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance/signal"
	"github.com/CloudNative-MySQL/cloudnative-mysql/internal/cmd/manager/instance/status"
)

// NewCommand builds the `instance` parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Manage the lifecycle of a single MySQL instance",
	}

	cmd.AddCommand(
		run.NewCommand(),
		backup.NewCommand(),
		initdb.NewCommand(),
		join.NewCommand(),
		restore.NewCommand(),
		status.NewCommand(),
		signal.NewCommand(),
	)

	return cmd
}
