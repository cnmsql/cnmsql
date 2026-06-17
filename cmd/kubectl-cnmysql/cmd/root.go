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

// Package cmd implements the kubectl-cloudnative-mysql command tree.
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

// NewRootCommand builds the top-level `kubectl cloudnative-mysql` command.
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "cloudnative-mysql",
		Short: "Manage and inspect cloudnative-mysql clusters",
		Long: "kubectl cloudnative-mysql is a kubectl plugin for managing " +
			"cloudnative-mysql (Percona Server) clusters.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	configFlags.AddFlags(root.PersistentFlags())

	root.AddCommand(
		newVersionCommand(),
		newStatusCommand(),
		newFenceCommand(),
		newPromoteCommand(),
		newRestartCommand(),
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
