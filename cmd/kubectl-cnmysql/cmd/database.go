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
	"fmt"

	"github.com/spf13/cobra"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/user"
)

func newDatabaseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "database",
		Aliases: []string{"db"},
		Short:   "Manage MySQL databases on a cluster",
		Long:    "Create, drop and list MySQL schemas via the instance manager control API on the primary.",
	}
	cmd.AddCommand(newDatabaseCreateCommand(), newDatabaseDropCommand(), newDatabaseListCommand())
	return cmd
}

func newDatabaseCreateCommand() *cobra.Command {
	var charset, collation string
	cmd := &cobra.Command{
		Use:               "create [CLUSTER] --name=DB",
		Short:             "Create a MySQL database",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			ctx := cmd.Context()
			cc, closeFn, err := userTarget(ctx, firstArg(args))
			if err != nil {
				return err
			}
			defer closeFn()
			req := user.CreateDatabaseRequest{Name: name, CharacterSet: charset, Collation: collation}
			if err := cc.Post(ctx, "/database/create", req, nil); err != nil {
				return err
			}
			fmt.Printf("created database %q\n", name)
			return nil
		},
	}
	cmd.Flags().String("name", "", "database name (required)")
	cmd.Flags().StringVar(&charset, "charset", "", "character set (e.g. utf8mb4)")
	cmd.Flags().StringVar(&collation, "collation", "", "collation (e.g. utf8mb4_unicode_ci)")
	return cmd
}

func newDatabaseDropCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "drop [CLUSTER] --name=DB",
		Short:             "Drop a MySQL database",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if user.IsReservedDatabase(name) {
				return fmt.Errorf("%q is a MySQL system database and cannot be dropped", name)
			}
			ctx := cmd.Context()
			cc, closeFn, err := userTarget(ctx, firstArg(args))
			if err != nil {
				return err
			}
			defer closeFn()
			if err := cc.Post(ctx, "/database/drop", user.DropDatabaseRequest{Name: name}, nil); err != nil {
				return err
			}
			fmt.Printf("dropped database %q\n", name)
			return nil
		},
	}
	cmd.Flags().String("name", "", "database name (required)")
	return cmd
}

func newDatabaseListCommand() *cobra.Command {
	return &cobra.Command{
		Use:               "list [CLUSTER]",
		Short:             "List user-managed MySQL databases",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeClusterArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cc, closeFn, err := userTarget(ctx, firstArg(args))
			if err != nil {
				return err
			}
			defer closeFn()
			var resp user.ListDatabasesResponse
			if err := cc.Get(ctx, "/database/list", &resp); err != nil {
				return err
			}
			for _, db := range resp.Databases {
				fmt.Println(db)
			}
			return nil
		},
	}
}
