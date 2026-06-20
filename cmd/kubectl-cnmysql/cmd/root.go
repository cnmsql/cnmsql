/*
Copyright 2026 The CloudNative MySQL Authors.

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

// Package cmd implements the kubectl-cnmysql command tree.
package cmd

import (
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"

	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
)

// configFlags carries the shared kubectl-style connection flags
// (--kubeconfig, --context, -n/--namespace, ...).
var configFlags = genericclioptions.NewConfigFlags(true)

// NewRootCommand builds the top-level `kubectl cnmysql` command.
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "cnmysql",
		Short: "Manage and inspect cloudnative-mysql clusters",
		Long: "kubectl cnmysql is a kubectl plugin for managing " +
			"cloudnative-mysql (Percona Server) clusters.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	configFlags.AddFlags(root.PersistentFlags())

	root.AddCommand(
		newVersionCommand(),
		newStatusCommand(),
		newGroupCommand(),
		newFenceCommand(),
		newPromoteCommand(),
		newRestartCommand(),
		newRestartInPlaceCommand(),
		newReinitCommand(),
		newReloadCommand(),
		newUserCommand(),
		newDatabaseCommand(),
		newMetricsCommand(),
		newLogsCommand(),
		newBackupCommand(),
		newMaintenanceCommand(),
		newDestroyCommand(),
	)
	return root
}

// newEnv resolves the shared client environment from the persistent flags.
func newEnv() (*plugin.Env, error) {
	return plugin.NewEnv(configFlags)
}

// firstArg returns the first positional argument, or "" if none was given. It
// lets commands treat a leading CLUSTER argument as optional (defaulting to the
// sole cluster in the namespace via plugin.ResolveCluster).
func firstArg(args []string) string {
	if len(args) > 0 {
		return args[0]
	}
	return ""
}

// deleteNow returns DeleteOptions that delete a Pod immediately (zero grace
// period) so a restart recreates it without waiting on the default grace.
func deleteNow() metav1.DeleteOptions {
	zero := int64(0)
	return metav1.DeleteOptions{GracePeriodSeconds: &zero}
}
