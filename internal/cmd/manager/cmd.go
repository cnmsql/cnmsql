/*
Copyright 2026 The cloudnative-mysql Authors.

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
