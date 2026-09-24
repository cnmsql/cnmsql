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
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/cnmsql/cnmsql/api/v1alpha1"
	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newRestartCommand() *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "restart [CLUSTER] [INSTANCE]",
		Short: "Restart all instances (rolling) or a single instance",
		Long: "Without INSTANCE, bump the restart annotation on the Cluster so the " +
			"operator performs a rolling restart. With INSTANCE, delete that Pod so " +
			"Kubernetes recreates it (the PVC is retained). CLUSTER defaults to the " +
			"sole cluster in the namespace; pass INSTANCE only together with CLUSTER.",
		Args:              cobra.MaximumNArgs(2),
		ValidArgsFunction: completeClusterInstanceArgs,
		Example: `  # Rolling restart all instances
  kubectl cnmsql restart cluster-sample

  # Restart a single instance (the primary prompts for confirmation)
  kubectl cnmsql restart cluster-sample cluster-sample-2

  # Skip confirmation prompts
  kubectl cnmsql restart cluster-sample --yes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			instance := ""
			if len(args) == 2 {
				instance = args[1]
			}
			return runRestart(cmd.Context(), firstArg(args), instance, yes)
		},
	}
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "skip confirmation prompts")
	return cmd
}

func runRestart(ctx context.Context, clusterName, instance string, yes bool) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.ResolveClusterToModify(ctx, clusterName)
	if err != nil {
		return err
	}

	if instance == "" {
		if !plugin.Confirm(fmt.Sprintf("Rolling-restart all instances of %q?", cluster.Name), yes) {
			fmt.Println("aborted")
			return nil
		}
		before := cluster.DeepCopy()
		if cluster.Annotations == nil {
			cluster.Annotations = map[string]string{}
		}
		cluster.Annotations[plugin.RestartAnnotation] = time.Now().Format(time.RFC3339)
		if err := env.Client.Patch(ctx, cluster, client.MergeFrom(before)); err != nil {
			return fmt.Errorf("requesting rolling restart: %w", err)
		}
		fmt.Printf("requested rolling restart of %q\n", cluster.Name)
		return nil
	}

	// Single-instance restart: delete the Pod and let the operator recreate it.
	// The delete is graceful so the preStop hook can hand a primary's role
	// over before mysqld stops, and mysqld itself shuts down cleanly.
	if !plugin.Contains(cluster.Status.InstanceNames, instance) {
		return fmt.Errorf("instance %q is not part of cluster %q", instance, cluster.Name)
	}
	if instance == plugin.PrimaryInstance(cluster) {
		if !plugin.Confirm(primaryRestartPrompt(cluster, instance), yes) {
			fmt.Println("aborted")
			return nil
		}
	}
	if err := env.Clientset.CoreV1().Pods(cluster.Namespace).Delete(ctx, instance, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("deleting pod %q: %w", instance, err)
	}
	fmt.Printf("restarting %q (pod deleted, will be recreated)\n", instance)
	return nil
}

// primaryRestartPrompt spells out what restarting the primary will do to the
// cluster's writes.
func primaryRestartPrompt(cluster *mysqlv1alpha1.Cluster, instance string) string {
	switch {
	case len(cluster.Status.InstanceNames) < 2:
		return fmt.Sprintf("%q is the only instance: the cluster is unavailable until it is back. Restart it?", instance)
	case cluster.IsSwitchoverOnDrainEnabled():
		return fmt.Sprintf("%q is the primary: the operator switches over to a replica "+
			"before it stops. Restart it?", instance)
	default:
		return fmt.Sprintf("%q is the primary and switchover on drain is disabled: "+
			"stopping it triggers a failover. Restart it?", instance)
	}
}
