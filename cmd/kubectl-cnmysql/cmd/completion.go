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
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
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
