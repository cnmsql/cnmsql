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
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newShellCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "shell [CLUSTER] [INSTANCE] [-- CLIENT_ARGS...]",
		Short: "Open a database client shell on an instance",
		Long: "Open a database client (mysql or mariadb, matching the cluster's " +
			"flavor) as root over the instance's local socket. INSTANCE defaults " +
			"to the primary; pass it only together with CLUSTER.\n\n" +
			"Arguments after -- are passed to the client. When stdin is not a " +
			"terminal no TTY is allocated, so SQL can be piped in.\n\n" +
			"The session runs through the API server with the plugin's own " +
			"connection flags (--context, --kubeconfig, ...). The root password is " +
			"sent over the exec stream and never appears on a command line.",
		Example: `  # Open a shell on the primary of the default cluster
  kubectl cnmsql shell

  # Open a shell on a replica
  kubectl cnmsql shell cluster-sample cluster-sample-2

  # Run a single statement
  kubectl cnmsql shell cluster-sample -- -e "SELECT @@hostname"

  # Pipe a script in
  kubectl cnmsql shell cluster-sample -- mydb < schema.sql`,
		ValidArgsFunction: completeClusterInstanceArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			positional, clientArgs := args, []string(nil)
			if dash := cmd.ArgsLenAtDash(); dash >= 0 {
				positional, clientArgs = args[:dash], args[dash:]
			}
			if len(positional) > 2 {
				return fmt.Errorf("accepts at most CLUSTER and INSTANCE before --, received %d arguments", len(positional))
			}

			ctx := cmd.Context()
			env, err := newEnv()
			if err != nil {
				return err
			}
			cluster, err := env.ResolveClusterToModify(ctx, firstArg(positional))
			if err != nil {
				return err
			}
			instance := plugin.PrimaryInstance(cluster)
			if len(positional) == 2 {
				instance = positional[1]
				if !plugin.Contains(cluster.Status.InstanceNames, instance) {
					return fmt.Errorf("instance %q is not part of cluster %q", instance, cluster.Name)
				}
			}
			if instance == "" {
				return fmt.Errorf("cluster %q has no primary yet", cluster.Name)
			}

			return rootClient(ctx, env, plugin.RootClientOptions{
				Cluster:  cluster,
				Instance: instance,
				Args:     clientArgs,
				Stdin:    os.Stdin,
				Stdout:   os.Stdout,
				Stderr:   os.Stderr,
				TTY:      term.IsTerminal(int(os.Stdin.Fd())), //nolint:gosec // file descriptors always fit in int
			})
		},
	}
}
