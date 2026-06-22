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
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newFenceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fence on|off CLUSTER INSTANCE",
		Short: "Fence (isolate) or unfence an instance from routing",
		Long: "Fence an instance by stamping the fencing annotation on its Pod, " +
			"which the operator picks up to remove it from routing and exclude it " +
			"from failover. Use '*' as INSTANCE to fence every instance.\n\n" +
			"Fencing the primary stops writes for the cluster because the rw Service " +
			"loses its endpoint. Under Group Replication, fencing runs STOP " +
			"GROUP_REPLICATION and is quorum-guarded: the operator refuses if fencing " +
			"would drop the group below majority.",
		Example: `  # Fence a specific instance
  kubectl cnmsql fence on cluster-sample cluster-sample-2

  # Unfence it
  kubectl cnmsql fence off cluster-sample cluster-sample-2

  # Fence every instance in the cluster
  kubectl cnmsql fence on cluster-sample '*'`,
		Args: cobra.ExactArgs(3),
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 0:
				return []string{"on", "off"}, cobra.ShellCompDirectiveNoFileComp
			case 1:
				return completeCluster(cmd.Context(), toComplete)
			case 2:
				return completeInstance(cmd.Context(), args[1], toComplete)
			default:
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			state := args[0]
			if state != "on" && state != "off" {
				return fmt.Errorf("first argument must be 'on' or 'off', got %q", state)
			}
			return runFence(cmd.Context(), state == "on", args[1], args[2])
		},
	}
	return cmd
}

func runFence(ctx context.Context, fence bool, clusterName, instance string) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	cluster, err := env.GetCluster(ctx, clusterName)
	if err != nil {
		return err
	}

	var targets []corev1.Pod
	pods, err := env.ListPods(ctx, cluster)
	if err != nil {
		return err
	}
	if instance == "*" {
		targets = pods
	} else {
		for i := range pods {
			if pods[i].Name == instance {
				targets = append(targets, pods[i])
			}
		}
		if len(targets) == 0 {
			return fmt.Errorf("instance %q not found in cluster %q", instance, clusterName)
		}
	}

	for i := range targets {
		pod := &targets[i]
		before := pod.DeepCopy()
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		if fence {
			pod.Annotations[plugin.FencingAnnotation] = plugin.FencingValue
		} else {
			delete(pod.Annotations, plugin.FencingAnnotation)
		}
		if err := env.Client.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
			return fmt.Errorf("updating fencing on %q: %w", pod.Name, err)
		}
		verb := "fenced"
		if !fence {
			verb = "unfenced"
		}
		fmt.Printf("%s %s\n", verb, pod.Name)
	}
	return nil
}
