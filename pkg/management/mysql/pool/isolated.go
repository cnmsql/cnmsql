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

package pool

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RunIsolated runs statements on a dedicated connection taken out of db, with
// the session's metadata-lock wait bounded to lockWait, and restores the
// session default before the connection returns to the pool.
//
// The instance manager's control pool is a single connection (ControlConfig).
// A statement on it that pends on a metadata lock — SET GLOBAL read_only
// waiting behind a queued DDL, say — starves the heartbeat, the status and
// every other control call for exactly as long as the server is willing to
// wait, which by default is about a year. Statements that can legitimately
// wait on a lock run here instead: the dedicated connection keeps the pool
// free, and the session lock_wait_timeout turns the unbounded server-side
// wait into a lock-wait-timeout failure the caller sees and can surface. The
// bound must stay below the pool's own readTimeout so the server always
// decides before the client gives up and the statement never outlives the
// call for longer than the bound.
//
// A lockWait below one second is clamped to one: zero would disable the wait
// entirely, failing the first statement that meets any lock.
func RunIsolated(ctx context.Context, db *sql.DB, lockWait time.Duration, statements ...string) (err error) {
	if len(statements) == 0 {
		return nil
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("pool: dedicated connection: %w", err)
	}
	defer func() {
		// The connection returns to the shared pool: restore the session
		// default so the bound cannot leak to the next borrower. A cancelled
		// caller context must not stop the restore, so it runs on its own.
		resetCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, resetErr := conn.ExecContext(resetCtx, "SET SESSION lock_wait_timeout = DEFAULT")
		if resetErr != nil && err == nil {
			err = fmt.Errorf("pool: restoring session lock_wait_timeout: %w", resetErr)
		}
		_ = conn.Close()
	}()
	seconds := max(int(lockWait.Seconds()), 1)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", seconds)); err != nil {
		return fmt.Errorf("pool: setting session lock_wait_timeout: %w", err)
	}
	for _, stmt := range statements {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("pool: executing %q: %w", stmt, err)
		}
	}
	return nil
}
