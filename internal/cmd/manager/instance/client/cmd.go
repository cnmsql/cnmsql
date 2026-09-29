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

// Package client implements `manager instance client`: run the database client
// as root, with the root password read from stdin.
package client

import (
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/rootclient"
)

// NewCommand builds the `instance client` command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "client -- <client> [args...]",
		Short: "Run a database client with the root password read from stdin",
		Long: "Print a marker, read the root password from the first line of stdin with terminal " +
			"echo off, then replace this process with the client, passing the password in " +
			"MYSQL_PWD. `kubectl cnmsql` runs this through `kubectl exec`, so the password never " +
			"appears in any process's arguments or on a terminal, and the image needs no shell.",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			password, err := rootclient.ReadPassword(os.Stdin, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			return execClient(args[0], args[1:], password)
		},
	}
	// Everything from the client name on belongs to the client.
	cmd.Flags().SetInterspersed(false)
	return cmd
}

// execClient replaces this process with client, so the client owns the exec's
// stdin, stdout and terminal directly.
func execClient(client string, args []string, password string) error {
	path, err := exec.LookPath(client)
	if err != nil {
		return fmt.Errorf("finding the database client: %w", err)
	}
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		return strings.HasPrefix(kv, "MYSQL_PWD=")
	})
	env = append(env, "MYSQL_PWD="+password)
	return syscall.Exec(path, append([]string{client}, args...), env)
}
