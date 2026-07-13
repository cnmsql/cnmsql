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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newPromoteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "promote CLUSTER INSTANCE",
		Short: "Promote a replica to primary (planned switchover)",
		Long: "Request a planned switchover by setting status.targetPrimary on the " +
			"Cluster. The operator validates the target, demotes the old primary, " +
			"waits for GTID catch-up, and promotes the new primary. Role Services " +
			"move after the database role is safe.\n\n" +
			"Under Group Replication, this invokes group_replication_set_as_primary " +
			"on the group instead of the stop/promote/demote dance. The operator " +
			"validates the target is an ONLINE SECONDARY, then observes the result.\n\n" +
			"Refuses diverged, fenced, or already-primary instances.",
		Example: `  # Promote a replica to primary
  kubectl cnmsql promote cluster-sample cluster-sample-2`,
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
	now := metav1.Now()
	cluster.Status.TargetPrimary = instance
	cluster.Status.TargetPrimaryTimestamp = &now
	if err := env.Client.Status().Patch(ctx, cluster, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patching target primary: %w", err)
	}

	fmt.Printf("requested switchover of %q to %q\n", clusterName, instance)
	return nil
}
