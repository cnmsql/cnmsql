/*
Copyright 2026 The CloudNative MySQL Authors.

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

	"github.com/spf13/cobra"

	"github.com/CloudNative-MySQL/cloudnative-mysql/cmd/kubectl-cnmysql/plugin"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
)

func newUserCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Manage MySQL users on a cluster",
		Long: "Create, alter, drop and list MySQL users via the instance manager control API on the primary.\n\n" +
			"Passwords are never accepted as command-line flags. Use --password-stdin to " +
			"pipe from a file or secret, or let the plugin prompt on the terminal with " +
			"echo disabled.\n\n" +
			"All subcommands require a reachable primary. Reserved operator accounts " +
			"(cloudnative-mysql_*) cannot be managed.",
		Example: `  # Create a user (prompts for password)
  kubectl cnmysql user create cluster-sample --name=app

  # Create a user with a password from a secret on stdin
  kubectl get secret app-creds -o jsonpath='{.data.password}' | base64 -d | \
    kubectl cnmysql user create cluster-sample --name=app --password-stdin

  # Create a superuser with X509 TLS requirement
  kubectl cnmysql user create cluster-sample --name=admin --superuser --require-tls=x509

  # Alter a user password (prompts for new password)
  kubectl cnmysql user alter cluster-sample --name=app

  # List users
  kubectl cnmysql user list cluster-sample

  # Drop a user
  kubectl cnmysql user drop cluster-sample --name=app`,
	}
	cmd.AddCommand(newUserCreateCommand(), newUserAlterCommand(), newUserDropCommand(), newUserListCommand())
	return cmd
}

// userTarget resolves the cluster and opens a control connection to its primary.
func userTarget(ctx context.Context, clusterName string) (*plugin.ControlClient, func(), error) {
	env, err := newEnv()
	if err != nil {
		return nil, nil, err
	}
	cluster, err := env.ResolveCluster(ctx, clusterName)
	if err != nil {
		return nil, nil, err
	}
	primary := plugin.PrimaryInstance(cluster)
	if primary == "" {
		return nil, nil, fmt.Errorf("cluster %q has no primary yet", cluster.Name)
	}
	cc, err := env.DialControl(ctx, cluster, primary)
	if err != nil {
		return nil, nil, err
	}
	return cc, cc.Close, nil
}

func newUserCreateCommand() *cobra.Command {
	var (
		host        string
		superuser   bool
		passwdStdin bool
		privileges  string
		on          string
		requireTLS  string
	)
	cmd := &cobra.Command{
		Use:               "create [CLUSTER] --name=USER",
		Short:             "Create a MySQL user",
		Long:              "Create a new MySQL user on the cluster's primary. Passwords are read from stdin (--password-stdin) or prompted on the terminal. Use --superuser for ALL PRIVILEGES, or --privileges for specific grants.",
		Example: `  # Create a user (prompts for password)
  kubectl cnmysql user create cluster-sample --name=app

  # Create a user with password from a file
  kubectl cnmysql user create cluster-sample --name=app --password-stdin < secret.txt

  # Create a user with specific privileges
  kubectl cnmysql user create cluster-sample --name=reader --privileges=SELECT --on=mydb.*

  # Create a superuser requiring X509 client certificate
  kubectl cnmysql user create cluster-sample --name=admin --superuser --require-tls=x509`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if user.IsReservedUser(name) {
				return fmt.Errorf("%q is a reserved operator account and cannot be managed", name)
			}
			password, err := plugin.ReadPassword(passwdStdin)
			if err != nil {
				return err
			}
			req := user.CreateUserRequest{
				Name:       name,
				Host:       host,
				Password:   password,
				Superuser:  superuser,
				RequireTLS: requireTLS,
			}
			if privileges != "" {
				req.Privileges = []user.Privilege{{
					Privileges: splitCSV(privileges),
					On:         on,
				}}
			}
			ctx := cmd.Context()
			cc, closeFn, err := userTarget(ctx, firstArg(args))
			if err != nil {
				return err
			}
			defer closeFn()
			if err := cc.Post(ctx, "/user/create", req, nil); err != nil {
				return err
			}
			fmt.Printf("created user %s@%s\n", name, defaultHost(host))
			return nil
		},
	}
	cmd.Flags().String("name", "", "user name (required)")
	cmd.Flags().StringVar(&host, "host", "%", "host pattern")
	cmd.Flags().BoolVar(&superuser, "superuser", false, "grant superuser (ALL PRIVILEGES)")
	cmd.Flags().BoolVar(&passwdStdin, "password-stdin", false, "read the password from stdin")
	cmd.Flags().StringVar(&privileges, "privileges", "", "comma-separated privileges (e.g. SELECT,INSERT)")
	cmd.Flags().StringVar(&on, "on", "*.*", "privilege target (e.g. mydb.*)")
	cmd.Flags().StringVar(&requireTLS, "require-tls", "", "TLS requirement: none|ssl|x509")
	return cmd
}

func newUserAlterCommand() *cobra.Command {
	var (
		host        string
		passwdStdin bool
		requireTLS  string
	)
	cmd := &cobra.Command{
		Use:               "alter [CLUSTER] --name=USER",
		Short:             "Alter a MySQL user's password or TLS requirement",
		Long:              "Alter a MySQL user's password, TLS requirement, or both. The new password is read from stdin (--password-stdin) or prompted on the terminal.",
		Example: `  # Change a user's password (prompts)
  kubectl cnmysql user alter cluster-sample --name=app

  # Change password from stdin
  kubectl cnmysql user alter cluster-sample --name=app --password-stdin < newpass.txt

  # Set TLS requirement to X509
  kubectl cnmysql user alter cluster-sample --name=app --require-tls=x509`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if user.IsReservedUser(name) {
				return fmt.Errorf("%q is a reserved operator account and cannot be managed", name)
			}
			req := user.AlterUserRequest{Name: name, Host: host}
			if passwdStdin {
				password, err := plugin.ReadPassword(true)
				if err != nil {
					return err
				}
				req.Password = &password
			}
			if cmd.Flags().Changed("require-tls") {
				req.RequireTLS = &requireTLS
			}
			ctx := cmd.Context()
			cc, closeFn, err := userTarget(ctx, firstArg(args))
			if err != nil {
				return err
			}
			defer closeFn()
			if err := cc.Post(ctx, "/user/alter", req, nil); err != nil {
				return err
			}
			fmt.Printf("altered user %s@%s\n", name, defaultHost(host))
			return nil
		},
	}
	cmd.Flags().String("name", "", "user name (required)")
	cmd.Flags().StringVar(&host, "host", "%", "host pattern")
	cmd.Flags().BoolVar(&passwdStdin, "password-stdin", false, "read the new password from stdin")
	cmd.Flags().StringVar(&requireTLS, "require-tls", "", "TLS requirement: none|ssl|x509")
	return cmd
}

func newUserDropCommand() *cobra.Command {
	var host string
	cmd := &cobra.Command{
		Use:               "drop [CLUSTER] --name=USER",
		Short:             "Drop a MySQL user",
		Long:              "Drop a MySQL user from the cluster's primary. Reserved operator accounts (cloudnative-mysql_*) cannot be dropped.",
		Example: `  # Drop a user
  kubectl cnmysql user drop cluster-sample --name=app`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if user.IsReservedUser(name) {
				return fmt.Errorf("%q is a reserved operator account and cannot be managed", name)
			}
			ctx := cmd.Context()
			cc, closeFn, err := userTarget(ctx, firstArg(args))
			if err != nil {
				return err
			}
			defer closeFn()
			if err := cc.Post(ctx, "/user/drop", user.DropUserRequest{Name: name, Host: host}, nil); err != nil {
				return err
			}
			fmt.Printf("dropped user %s@%s\n", name, defaultHost(host))
			return nil
		},
	}
	cmd.Flags().String("name", "", "user name (required)")
	cmd.Flags().StringVar(&host, "host", "%", "host pattern")
	return cmd
}

func newUserListCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "list [CLUSTER]",
		Short:             "List managed MySQL users",
		Long:              "List all managed MySQL users on the cluster's primary, with their host patterns, TLS requirements and grants.",
		Example: `  # List users
  kubectl cnmysql user list cluster-sample`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cc, closeFn, err := userTarget(ctx, firstArg(args))
			if err != nil {
				return err
			}
			defer closeFn()
			var resp user.ListUsersResponse
			if err := cc.Get(ctx, "/user/list", &resp); err != nil {
				return err
			}
			rows := make([][]string, 0, len(resp.Users))
			for _, u := range resp.Users {
				rows = append(rows, []string{u.Name, u.Host, u.RequireTLS, strings.Join(u.Grants, "; ")})
			}
			plugin.Table([]string{"NAME", "HOST", "TLS", "GRANTS"}, rows)
			return nil
		},
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func defaultHost(host string) string {
	if host == "" {
		return "%"
	}
	return host
}
