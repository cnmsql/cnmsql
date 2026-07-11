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
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cnmsql/cnmsql/cmd/kubectl-cnmsql/plugin"
)

func newShellCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "shell [CLUSTER]",
		Short: "Open a database client shell on the primary",
		Long: "Open an interactive database client shell (mysql or mariadb) on the " +
			"cluster's primary instance. The appropriate client binary is selected " +
			"automatically from the cluster's flavor.\n\n" +
			"The command delegates to kubectl exec and passes stdin/stdout/stderr " +
			"through to the interactive session.",
		Example: `  # Open a shell on the default cluster
  kubectl cnmsql shell

  # Open a shell on a named cluster
  kubectl cnmsql shell my-cluster`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
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
			primary := plugin.PrimaryInstance(cluster)
			if primary == "" {
				return fmt.Errorf("cluster %q has no primary yet", cluster.Name)
			}

			secretName := cluster.Name + "-root"
			if cluster.Spec.RootPasswordSecret != nil && cluster.Spec.RootPasswordSecret.Name != "" {
				secretName = cluster.Spec.RootPasswordSecret.Name
			}

			secret, err := env.Clientset.CoreV1().Secrets(cluster.Namespace).Get(ctx, secretName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("getting root password secret %q: %w", secretName, err)
			}

			password := string(secret.Data["password"])
			if password == "" {
				return fmt.Errorf("root password secret %q has empty password", secretName)
			}

			clientBin := "mysql"
			if cluster.ResolvedFlavor() == "mariadb" {
				clientBin = "mariadb"
			}

			shellCmd := fmt.Sprintf("MYSQL_PWD='%s' %s --socket=/var/run/mysqld/mysqld.sock --user=root",
				strings.ReplaceAll(password, "'", "'\\''"), clientBin)

			kubectlArgs := []string{
				"exec", "-it",
				"-n", cluster.Namespace,
				primary,
				"-c", "mysql",
				"--",
				"sh", "-c", shellCmd,
			}

			kubectll := exec.CommandContext(ctx, "kubectl", kubectlArgs...)
			kubectll.Stdin = os.Stdin
			kubectll.Stdout = os.Stdout
			kubectll.Stderr = os.Stderr
			return kubectll.Run()
		},
	}
}
