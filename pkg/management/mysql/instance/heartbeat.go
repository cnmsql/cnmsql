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

package instance

import (
	"context"
	"database/sql"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cnmsql/cnmsql/pkg/management/mysql/heartbeat"
	"github.com/cnmsql/cnmsql/pkg/management/mysql/webserver"
)

// HeartbeatConfig configures the in-Pod replication-lag heartbeat.
type HeartbeatConfig struct {
	// Enabled turns the heartbeat on. The loop runs in every Pod; only the
	// writable primary stamps the table.
	Enabled bool
	// Interval is the stamping period. Zero takes the package default.
	Interval time.Duration
}

// startHeartbeat runs the heartbeat loop against the control connection and
// returns it so its readings can be surfaced in status. Unlike the archiver the
// loop has no terminal error: a heartbeat that cannot be written or read is a
// lost lag reading, not a reason to take the instance down, so there is no error
// channel for the run loop to select on.
func startHeartbeat(ctx context.Context, cfg HeartbeatConfig, db *sql.DB) *heartbeat.Loop {
	log := logf.FromContext(ctx).WithName("heartbeat")
	loop := heartbeat.NewLoop(db, heartbeat.Config{Interval: cfg.Interval}, log)
	go loop.Run(ctx)
	log.Info("Started replication-lag heartbeat", "interval", cfg.Interval)
	return loop
}

// heartbeatStatusProvider adapts a heartbeat Loop's State to the webserver
// status shape.
func heartbeatStatusProvider(loop *heartbeat.Loop) func() *webserver.ReplicationLagStatus {
	return func() *webserver.ReplicationLagStatus {
		s := loop.State()
		out := &webserver.ReplicationLagStatus{
			Writer:    s.Writing,
			LastError: s.LastError,
		}
		if !s.SampledAt.IsZero() {
			out.SampledAt = s.SampledAt.UTC().Format(time.RFC3339)
		}
		if s.LagKnown {
			millis := s.Lag.Milliseconds()
			out.LagMillis = &millis
		}
		return out
	}
}
