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

// Package instance provides the `manager instance` command tree.
package instance

import (
	"github.com/spf13/cobra"

	"github.com/yyewolf/cnmysql/internal/cmd/manager/instance/initdb"
	"github.com/yyewolf/cnmysql/internal/cmd/manager/instance/join"
	"github.com/yyewolf/cnmysql/internal/cmd/manager/instance/restore"
	"github.com/yyewolf/cnmysql/internal/cmd/manager/instance/run"
	"github.com/yyewolf/cnmysql/internal/cmd/manager/instance/status"
)

// NewCommand builds the `instance` parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Manage the lifecycle of a single MySQL instance",
	}

	cmd.AddCommand(
		run.NewCommand(),
		initdb.NewCommand(),
		join.NewCommand(),
		restore.NewCommand(),
		status.NewCommand(),
	)

	return cmd
}
