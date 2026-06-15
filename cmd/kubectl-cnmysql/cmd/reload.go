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
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
)

func newReloadCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "reload [CLUSTER]",
		Short: "Re-apply dynamic my.cnf parameters without restarting",
		Long: "Bump the reload annotation on the Cluster so the operator re-applies " +
			"dynamic configuration parameters to the running mysqld instances. " +
			"Parameters that require a restart are not applied by reload; use " +
			"'restart' for those.",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReload(cmd.Context(), firstArg(args))
		},
	}
}

func runReload(ctx context.Context, clusterName string) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	before := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[plugin.ReloadAnnotation] = time.Now().Format(time.RFC3339)
	if err := env.Client.Patch(ctx, cluster, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("requesting reload: %w", err)
	}
	fmt.Printf("requested configuration reload of %q\n", cluster.Name)
	return nil
}
