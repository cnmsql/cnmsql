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
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/yyewolf/cnmysql/cmd/kubectl-cnmysql/plugin"
)

func newMetricsCommand() *cobra.Command {
	var (
		filter   string
		watch    bool
		interval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "metrics [CLUSTER] [INSTANCE]",
		Short: "Scrape the Prometheus metrics of an instance",
		Long: "Port-forward to an instance's metrics endpoint and print the raw " +
			"Prometheus scrape. CLUSTER defaults to the sole cluster in the " +
			"namespace; without INSTANCE, the primary is used.",
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
			instance := plugin.PrimaryInstance(cluster)
			if len(args) == 2 {
				instance = args[1]
			}
			if instance == "" {
				return fmt.Errorf("cluster %q has no primary; specify an INSTANCE", cluster.Name)
			}
			// One port-forward is kept open for the whole invocation, including
			// across --watch frames.
			cc, err := env.DialMetrics(ctx, cluster, instance)
			if err != nil {
				return err
			}
			defer cc.Close()
			render := func(ctx context.Context) error {
				body, err := cc.GetText(ctx, "/metrics")
				if err != nil {
					return err
				}
				printMetrics(body, filter)
				return nil
			}
			return watchOrOnce(ctx, watch, "metrics "+instance, interval, render)
		},
	}
	cmd.Flags().StringVar(&filter, "filter", "", "only print lines containing this substring")
	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "continuously refresh until interrupted")
	cmd.Flags().DurationVar(&interval, "watch-interval", defaultWatchInterval, "refresh interval for --watch")
	return cmd
}

func printMetrics(body, filter string) {
	if filter == "" {
		fmt.Print(body)
		return
	}
	for line := range strings.SplitSeq(body, "\n") {
		if strings.Contains(line, filter) {
			fmt.Println(line)
		}
	}
}
