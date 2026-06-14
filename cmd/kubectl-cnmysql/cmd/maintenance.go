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

package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
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
