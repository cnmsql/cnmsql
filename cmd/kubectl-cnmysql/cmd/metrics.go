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
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
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
