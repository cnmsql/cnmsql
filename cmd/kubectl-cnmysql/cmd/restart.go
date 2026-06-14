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
	cluster, err := env.ResolveCluster(ctx, clusterName)
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
	if instance == plugin.PrimaryInstance(cluster) {
		if !plugin.Confirm(fmt.Sprintf("%q is the primary. Restart it?", instance), yes) {
			fmt.Println("aborted")
			return nil
		}
	}
	if err := env.Clientset.CoreV1().Pods(cluster.Namespace).Delete(ctx, instance, deleteNow()); err != nil {
		return fmt.Errorf("deleting pod %q: %w", instance, err)
	}
	fmt.Printf("restarting %q (pod deleted, will be recreated)\n", instance)
	return nil
}
