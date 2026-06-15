/*
Copyright 2026 The cloudnative-mysql Authors.

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
	"bufio"
	"context"
	"fmt"
	"sync"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"

	mysqlv1alpha1 "github.com/CloudNative-MySQL/cloudnative-mysql/api/v1alpha1"
	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
)

func newLogsCommand() *cobra.Command {
	var (
		follow     bool
		timestamps bool
		tail       int64
	)
	cmd := &cobra.Command{
		Use:   "logs [CLUSTER] [INSTANCE]",
		Short: "Stream logs from a cluster's instances",
		Long: "Stream container logs from the cluster's Pods. Without INSTANCE, " +
			"logs from all instances are merged with a per-instance prefix.",
		Args:              cobra.MaximumNArgs(2),
		ValidArgsFunction: completeClusterInstanceArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			cluster, err := env.ResolveCluster(ctx, firstArg(args))
			if err != nil {
				return err
			}
			opts := &corev1.PodLogOptions{Follow: follow, Timestamps: timestamps}
			if tail >= 0 {
				opts.TailLines = &tail
			}
			if len(args) == 2 {
				return streamPodLogs(ctx, env, cluster.Namespace, args[1], opts, false)
			}
			return streamAllLogs(ctx, env, cluster, opts)
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "stream new logs as they arrive")
	cmd.Flags().BoolVarP(&timestamps, "timestamps", "t", false, "include timestamps")
	cmd.Flags().Int64Var(&tail, "tail", -1, "number of recent lines to show (-1 = all)")
	return cmd
}

func streamAllLogs(
	ctx context.Context, env *plugin.Env, cluster *mysqlv1alpha1.Cluster, opts *corev1.PodLogOptions,
) error {
	pods, err := env.ListPods(ctx, cluster)
	if err != nil {
		return err
	}
	if len(pods) == 0 {
		return fmt.Errorf("no instances found for cluster %q", cluster.Name)
	}
	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			if err := streamPodLogs(ctx, env, cluster.Namespace, name, opts, true); err != nil {
				fmt.Printf("[%s] error: %v\n", name, err)
			}
		}(pods[i].Name)
	}
	wg.Wait()
	return nil
}

func streamPodLogs(
	ctx context.Context, env *plugin.Env, namespace, name string, opts *corev1.PodLogOptions, prefix bool,
) error {
	req := env.Clientset.CoreV1().Pods(namespace).GetLogs(name, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Errorf("opening log stream for %q: %w", name, err)
	}
	defer func() { _ = stream.Close() }()

	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if prefix {
			fmt.Printf("[%s] %s\n", name, scanner.Text())
		} else {
			fmt.Println(scanner.Text())
		}
	}
	return scanner.Err()
}
