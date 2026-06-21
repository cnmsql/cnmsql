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

	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newRestartInPlaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restart-inplace CLUSTER INSTANCE",
		Short: "Re-exec an instance manager in place without restarting mysqld",
		Long: "Trigger an in-place re-exec of the instance manager on INSTANCE via its " +
			"control API. The manager adopts the running mysqld instead of restarting " +
			"it, so the server stays up across the swap (the zero-restart " +
			"operator-upgrade path). Use this to verify mysqld survives a manager swap: " +
			"the Pod's RESTARTS count should stay flat and status.uptimeSeconds keep " +
			"climbing afterwards.",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeClusterInstanceArgs,
		Example: `  # Re-exec the instance manager on an instance without restarting mysqld
  kubectl cnmsql restart-inplace cluster-sample cluster-sample-2`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRestartInPlace(cmd.Context(), args[0], args[1])
		},
	}
	return cmd
}

func runRestartInPlace(ctx context.Context, clusterName, instance string) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}
	if !plugin.Contains(cluster.Status.InstanceNames, instance) {
		return fmt.Errorf("instance %q is not part of cluster %q", instance, cluster.Name)
	}

	cc, err := env.DialControl(ctx, cluster, instance)
	if err != nil {
		return err
	}
	defer cc.Close()

	if err := cc.Post(ctx, "/instance/manager/restart-inplace", nil, nil); err != nil {
		return fmt.Errorf("requesting in-place restart of %q: %w", instance, err)
	}
	fmt.Printf("requested in-place manager re-exec of %q (mysqld stays up)\n", instance)
	return nil
}
