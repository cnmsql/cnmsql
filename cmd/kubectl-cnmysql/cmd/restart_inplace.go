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

	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
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
