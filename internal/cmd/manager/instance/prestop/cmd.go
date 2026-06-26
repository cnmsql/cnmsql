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

// Package prestop implements `manager instance prestop`: a container preStop
// hook that holds a draining primary alive until the operator has switched the
// primary role away, so a node drain produces a clean switchover instead of an
// emergency failover.
package prestop

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/pool"
)

// NewCommand builds the `instance prestop` command. It runs as the mysql
// container's preStop hook: it blocks until this instance is no longer the
// writable primary (the operator demoted it as part of a switchover) or the
// timeout elapses. preStop runs before SIGTERM, so mysqld and the in-Pod role
// reconciler stay alive throughout, letting the handoff complete cleanly.
func NewCommand() *cobra.Command {
	var (
		socket   string
		user     string
		timeout  time.Duration
		interval time.Duration
	)
	cmd := &cobra.Command{
		Use:   "prestop",
		Short: "Block until this instance is no longer the writable primary (graceful switchover handoff)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := pool.Config{
				Socket:       socket,
				User:         user,
				Password:     os.Getenv("MYSQL_CONTROL_PASSWORD"),
				MaxOpenConns: 1,
			}
			db, err := pool.Open(cmd.Context(), cfg)
			if err != nil {
				// If mysqld is unreachable there is nothing to coordinate; let the
				// shutdown proceed rather than blocking the drain.
				fmt.Fprintf(os.Stderr, "prestop: cannot reach mysqld (%v); proceeding with shutdown\n", err)
				return nil
			}
			defer func() { _ = db.Close() }()
			return WaitUntilDemoted(cmd.Context(), db, timeout, interval)
		},
	}
	cmd.Flags().StringVar(&socket, "socket", "/var/run/mysqld/mysqld.sock", "Unix socket path")
	cmd.Flags().StringVar(&user, "control-user", "root", "Privileged user for the control connection")
	cmd.Flags().DurationVar(&timeout, "timeout", 25*time.Second,
		"Maximum time to wait for the switchover handoff before proceeding with shutdown")
	cmd.Flags().DurationVar(&interval, "poll-interval", time.Second,
		"How often to poll the local read_only state")
	return cmd
}

// WaitUntilDemoted blocks until the local mysqld reports read_only=ON — meaning
// the instance has been demoted to a replica, so any switchover has completed —
// or the timeout elapses. A replica is already read_only, so it returns at once.
//
// It always returns nil: a preStop hook must never fail the Pod's termination.
// On timeout or any error it simply lets the normal shutdown proceed, which
// degrades to the operator's reactive failover path.
func WaitUntilDemoted(ctx context.Context, db pool.Connection, timeout, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		readOnly, err := isReadOnly(ctx, db)
		if err == nil && readOnly {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

// isReadOnly reports whether the local server has global read_only enabled,
// which the operator sets when it demotes a former primary to a replica.
func isReadOnly(ctx context.Context, db pool.Connection) (bool, error) {
	var readOnly int64
	if err := db.QueryRowContext(ctx, "SELECT @@global.read_only").Scan(&readOnly); err != nil {
		return false, err
	}
	return readOnly == 1, nil
}
