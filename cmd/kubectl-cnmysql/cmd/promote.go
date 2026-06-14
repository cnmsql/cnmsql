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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/yyewolf/cnmysql/cmd/kubectl-cnmysql/plugin"
)

func newPromoteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "promote CLUSTER INSTANCE",
		Short: "Promote a replica to primary (planned switchover)",
		Long: "Request a planned switchover by setting status.targetPrimary on the " +
			"Cluster. The operator coordinates demotion of the old primary, GTID " +
			"catch-up and routing.",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeClusterInstanceArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPromote(cmd.Context(), args[0], args[1])
		},
	}
}

func runPromote(ctx context.Context, clusterName, instance string) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.GetCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	if cluster.Status.CurrentPrimary == instance {
		return fmt.Errorf("%q is already the current primary", instance)
	}
	if !plugin.Contains(cluster.Status.InstanceNames, instance) {
		return fmt.Errorf("instance %q is not part of cluster %q", instance, clusterName)
	}
	if plugin.Contains(cluster.Status.DivergedInstances, instance) {
		return fmt.Errorf("refusing to promote %q: instance is diverged", instance)
	}
	if plugin.Contains(cluster.Status.FencedInstances, instance) {
		return fmt.Errorf("refusing to promote %q: instance is fenced", instance)
	}

	before := cluster.DeepCopy()
	cluster.Status.TargetPrimary = instance
	cluster.Status.TargetPrimaryTimestamp = metav1.Now().Format(time.RFC3339)
	if err := env.Client.Status().Patch(ctx, cluster, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patching target primary: %w", err)
	}

	fmt.Printf("requested switchover of %q to %q\n", clusterName, instance)
	return nil
}
