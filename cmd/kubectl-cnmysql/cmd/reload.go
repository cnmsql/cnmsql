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
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/yyewolf/cnmysql/cmd/kubectl-cnmysql/plugin"
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
