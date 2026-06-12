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

// Package instance implements the InstanceController the control API serves: it
// builds the instance status and drives lifecycle actions by combining the
// connection pool and the replication manager.
package instance

import (
	"context"
	"errors"
	"fmt"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/version"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/webserver"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Supervisor abstracts the mysqld process lifecycle so the controller can
// trigger a restart or shutdown without depending on the supervisor
// implementation.
type Supervisor interface {
	Restart(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// Controller is the concrete webserver.InstanceController backed by a local
// mysqld connection.
type Controller struct {
	name       string
	conn       pool.Connection
	repl       *replication.Manager
	version    version.Version
	versionStr string
	expected   webserver.Role
	supervisor Supervisor
	backup     *BackupConfig
	// archiving, when set, supplies the continuous archiver's current state so it
	// surfaces in the instance status.
	archiving func() *webserver.ArchivingStatus
}

// NewController builds a Controller for the named instance. versionStr is the
// MySQL server version (e.g. "8.0.36"); supervisor may be nil if restart is not
// available in the current context.
func NewController(
	name string,
	conn pool.Connection,
	versionStr string,
	expected webserver.Role,
	supervisor Supervisor,
) (*Controller, error) {
	v, err := version.Parse(versionStr)
	if err != nil {
		return nil, err
	}
	if expected == "" {
		expected = webserver.RoleUnknown
	}
	return &Controller{
		name:       name,
		conn:       conn,
		repl:       replication.NewManager(conn, v),
		version:    v,
		versionStr: versionStr,
		expected:   expected,
		supervisor: supervisor,
	}, nil
}

// SetArchivingProvider registers a callback that supplies the continuous
// archiver's current state for inclusion in the instance status.
func (c *Controller) SetArchivingProvider(provider func() *webserver.ArchivingStatus) {
	c.archiving = provider
}

// Healthz reports liveness: the server answers a ping.
func (c *Controller) Healthz(ctx context.Context) error {
	return c.conn.PingContext(ctx)
}

// Readyz reports readiness: the server answers a ping and, if it is a replica,
// both replication threads are running.
func (c *Controller) Readyz(ctx context.Context) error {
	if err := c.conn.PingContext(ctx); err != nil {
		return err
	}
	state, err := c.repl.ReplicaState(ctx)
	if err != nil {
		return err
	}
	if c.expected == webserver.RoleReplica && !state.Configured {
		return errors.New("replication source is not configured")
	}
	if state.Configured && (!state.IORunning || !state.SQLRunning) {
		return fmt.Errorf("replication not healthy (io=%t sql=%t): %s",
			state.IORunning, state.SQLRunning, state.LastError)
	}
	return nil
}

// Status assembles the full instance status from the server.
func (c *Controller) Status(ctx context.Context) (*webserver.Status, error) {
	roState, err := c.repl.ReadOnly(ctx)
	if err != nil {
		return nil, err
	}
	replicaState, err := c.repl.ReplicaState(ctx)
	if err != nil {
		return nil, err
	}

	status := &webserver.Status{
		InstanceName:  c.name,
		Version:       c.versionStr,
		Role:          c.role(replicaState),
		ReadOnly:      roState.ReadOnly,
		SuperReadOnly: roState.SuperReadOnly,
		IsReady:       c.Readyz(ctx) == nil,
	}

	// Best-effort, non-critical fields.
	if gtid, err := c.repl.GTIDExecuted(ctx); err == nil {
		status.GTIDExecuted = gtid
	}
	if purged, err := c.repl.GTIDPurged(ctx); err == nil {
		status.GTIDPurged = purged
	}
	if semi, err := c.repl.SemiSyncStatus(ctx); err == nil {
		status.SemiSync = webserver.SemiSyncStatus{
			SourceEnabled:  semi.SourceEnabled,
			ReplicaEnabled: semi.ReplicaEnabled,
		}
	}
	if uptime, err := c.repl.Uptime(ctx); err == nil {
		status.UptimeSeconds = uptime
	}
	if c.archiving != nil {
		status.Archiving = c.archiving()
	}

	if replicaState.Configured {
		status.Replication = &webserver.ReplicationStatus{
			SourceHost:          replicaState.SourceHost,
			IORunning:           replicaState.IORunning,
			SQLRunning:          replicaState.SQLRunning,
			SecondsBehindSource: replicaState.SecondsBehindSource,
			LastError:           replicaState.LastError,
			RetrievedGTIDSet:    replicaState.RetrievedGTIDSet,
		}
	}

	return status, nil
}

// Promote transitions a replica to primary.
func (c *Controller) Promote(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("instance-controller").WithValues("instance", c.name)
	log.Info("Promoting instance")
	if err := c.repl.Promote(ctx); err != nil {
		return err
	}
	c.expected = webserver.RolePrimary
	log.Info("Promoted instance")
	return nil
}

// Demote makes a primary read-only.
func (c *Controller) Demote(ctx context.Context) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Demoting instance", "instance", c.name)
	return c.repl.Demote(ctx)
}

// EnsureReplicaStarted resumes replication when this instance is a configured
// replica whose threads did not auto-start with mysqld.
func (c *Controller) EnsureReplicaStarted(ctx context.Context) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Ensuring replication is started", "instance", c.name)
	return c.repl.EnsureReplicaStarted(ctx)
}

// EnsureReplicaConfigured restores missing replication source metadata and
// resumes stopped replication threads for an expected replica.
func (c *Controller) EnsureReplicaConfigured(ctx context.Context, opts replication.SourceOptions) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Ensuring replication source is configured",
		"instance", c.name,
		"sourceHost", opts.Host,
		"sourcePort", opts.Port)
	if err := c.repl.EnsureReplicaConfigured(ctx, opts); err != nil {
		return err
	}
	c.expected = webserver.RoleReplica
	logf.FromContext(ctx).WithName("instance-controller").Info("Configured replication source",
		"instance", c.name,
		"sourceHost", opts.Host)
	return nil
}

// Restart restarts mysqld via the supervisor.
func (c *Controller) Restart(ctx context.Context) error {
	if c.supervisor == nil {
		return errors.New("restart is not available: no supervisor configured")
	}
	logf.FromContext(ctx).WithName("instance-controller").Info("Restarting mysqld", "instance", c.name)
	return c.supervisor.Restart(ctx)
}

// Shutdown stops mysqld via the supervisor. The PID1 run loop then exits and the
// Pod (RestartPolicy: Always) restarts clean. Used as the fallback when a live
// demotion of a former primary fails.
func (c *Controller) Shutdown(ctx context.Context) error {
	if c.supervisor == nil {
		return errors.New("shutdown is not available: no supervisor configured")
	}
	logf.FromContext(ctx).WithName("instance-controller").Info("Shutting down mysqld", "instance", c.name)
	return c.supervisor.Shutdown(ctx)
}

// role derives the reported role from the replica state.
func (c *Controller) role(state *replication.ReplicaState) webserver.Role {
	if state != nil && state.Configured {
		return webserver.RoleReplica
	}
	if c.expected == webserver.RoleReplica {
		return webserver.RoleUnknown
	}
	return webserver.RolePrimary
}

// Ensure Controller satisfies the control API contract.
var _ webserver.InstanceController = (*Controller)(nil)
