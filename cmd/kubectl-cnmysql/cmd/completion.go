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
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/yyewolf/cnmysql/api/v1alpha1"
)

// completeCluster returns the names of clusters in the resolved namespace that
// match toComplete. It is used as the dynamic completion for CLUSTER arguments.
func completeCluster(ctx context.Context, toComplete string) ([]string, cobra.ShellCompDirective) {
	env, err := newEnv()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	list := &mysqlv1alpha1.ClusterList{}
	if err := env.Client.List(ctx, list, client.InNamespace(env.Namespace)); err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	var names []string
	for i := range list.Items {
		if strings.HasPrefix(list.Items[i].Name, toComplete) {
			names = append(names, list.Items[i].Name)
		}
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}

// completeInstance returns the instance (pod) names of clusterName that match
// toComplete, used for INSTANCE arguments.
func completeInstance(ctx context.Context, clusterName, toComplete string) ([]string, cobra.ShellCompDirective) {
	env, err := newEnv()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	cluster, err := env.GetCluster(ctx, clusterName)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	pods, err := env.ListPods(ctx, cluster)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	var names []string
	for i := range pods {
		if strings.HasPrefix(pods[i].Name, toComplete) {
			names = append(names, pods[i].Name)
		}
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}

// completeClusterArg completes a single CLUSTER positional argument.
func completeClusterArg(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return completeCluster(cmd.Context(), toComplete)
}

// completeClusterInstanceArgs completes a CLUSTER then INSTANCE positional pair.
func completeClusterInstanceArgs(
	cmd *cobra.Command, args []string, toComplete string,
) ([]string, cobra.ShellCompDirective) {
	switch len(args) {
	case 0:
		return completeCluster(cmd.Context(), toComplete)
	case 1:
		return completeInstance(cmd.Context(), args[0], toComplete)
	default:
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}
