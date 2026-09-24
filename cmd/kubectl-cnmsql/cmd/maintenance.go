/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

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

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newMaintenanceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "maintenance set|unset [CLUSTER]",
		Short: "Toggle the node maintenance window on a cluster",
		Long: "Set or clear spec.nodeMaintenanceWindow.inProgress. While set, the " +
			"operator tolerates node drains; with --reuse-pvc it reattaches the " +
			"existing PVCs to rescheduled Pods.\n\n" +
			"Use this before draining a node or performing Kubernetes node maintenance.",
		Example: `  # Begin a maintenance window
  kubectl cnmsql maintenance set cluster-sample

  # Begin a maintenance window without reusing PVCs (PDBs stay in force)
  kubectl cnmsql maintenance set cluster-sample --reuse-pvc=false

  # End the maintenance window
  kubectl cnmsql maintenance unset cluster-sample`,
	}
	cmd.AddCommand(newMaintenanceSetCommand(), newMaintenanceUnsetCommand())
	return cmd
}

func newMaintenanceSetCommand() *cobra.Command {
	var reusePVC, yes bool
	cmd := &cobra.Command{
		Use:   "set [CLUSTER]",
		Short: "Begin a node maintenance window",
		Long: `Set spec.nodeMaintenanceWindow.inProgress to true. While the window is
active and PVCs are reused (the default), the operator relaxes the cluster's
PodDisruptionBudgets so its nodes can be drained, and rescheduled Pods reattach
their existing PVCs. With --reuse-pvc=false the budgets stay in force, since
draining a node would otherwise discard that instance's data.`,
		Example: `  kubectl cnmsql maintenance set cluster-sample
  kubectl cnmsql maintenance set cluster-sample --reuse-pvc=false`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMaintenance(cmd.Context(), firstArg(args), true, reusePVC, yes)
		},
	}
	cmd.Flags().BoolVar(&reusePVC, "reuse-pvc", true,
		"reattach existing PVCs to rescheduled Pods (relaxes the PodDisruptionBudgets)")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip the confirmation prompt")
	return cmd
}

func newMaintenanceUnsetCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "unset [CLUSTER]",
		Short:             "End a node maintenance window",
		Long:              "Clear spec.nodeMaintenanceWindow.inProgress. The operator resumes normal disruption handling.",
		Example:           `  kubectl cnmsql maintenance unset cluster-sample`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMaintenance(cmd.Context(), firstArg(args), false, false, true)
		},
	}
}

func runMaintenance(ctx context.Context, clusterName string, inProgress, reusePVC, yes bool) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveClusterToModify(ctx, clusterName)
	if err != nil {
		return err
	}

	if inProgress {
		consequence := "Its PodDisruptionBudgets stay in force, so draining its nodes remains blocked."
		if reusePVC {
			consequence = "Its PodDisruptionBudgets will be relaxed, so draining a node takes that instance down."
		}
		if !plugin.Confirm(fmt.Sprintf("Start a node maintenance window on %q? %s", cluster.Name, consequence), yes) {
			fmt.Println("aborted")
			return nil
		}
	}

	before := cluster.DeepCopy()
	if inProgress {
		// ReusePVC is always written: left unset, the API defaults it to true.
		cluster.Spec.NodeMaintenanceWindow = &mysqlv1alpha1.NodeMaintenanceWindow{
			InProgress: true,
			ReusePVC:   &reusePVC,
		}
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
