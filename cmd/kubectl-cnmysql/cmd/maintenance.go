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

package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
)

func newMaintenanceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "maintenance set|unset [CLUSTER]",
		Short: "Toggle the node maintenance window on a cluster",
		Long: "Set or clear spec.nodeMaintenanceWindow.inProgress. While set, the " +
			"operator tolerates node drains; with --reuse-pvc it reattaches the " +
			"existing PVCs to rescheduled Pods.",
	}
	cmd.AddCommand(newMaintenanceSetCommand(), newMaintenanceUnsetCommand())
	return cmd
}

func newMaintenanceSetCommand() *cobra.Command {
	var reusePVC bool
	cmd := &cobra.Command{
		Use:               "set [CLUSTER]",
		Short:             "Begin a node maintenance window",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMaintenance(cmd.Context(), firstArg(args), true, reusePVC)
		},
	}
	cmd.Flags().BoolVar(&reusePVC, "reuse-pvc", false, "reattach existing PVCs to rescheduled Pods")
	return cmd
}

func newMaintenanceUnsetCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "unset [CLUSTER]",
		Short:             "End a node maintenance window",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMaintenance(cmd.Context(), firstArg(args), false, false)
		},
	}
}

func runMaintenance(ctx context.Context, clusterName string, inProgress, reusePVC bool) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	before := cluster.DeepCopy()
	if inProgress {
		window := &mysqlv1alpha1.NodeMaintenanceWindow{InProgress: true}
		if reusePVC {
			reuse := true
			window.ReusePVC = &reuse
		}
		cluster.Spec.NodeMaintenanceWindow = window
	} else if cluster.Spec.NodeMaintenanceWindow != nil {
		cluster.Spec.NodeMaintenanceWindow.InProgress = false
	}
	if err := env.Client.Patch(ctx, cluster, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("updating maintenance window: %w", err)
	}

	if inProgress {
		fmt.Printf("maintenance window started for %q (reusePVC=%t)\n", cluster.Name, reusePVC)
	} else {
		fmt.Printf("maintenance window ended for %q\n", cluster.Name)
	}
	return nil
}
