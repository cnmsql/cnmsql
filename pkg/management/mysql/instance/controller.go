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
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yyewolf/cnmysql/pkg/management/mysql/pool"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/replication"
	"github.com/yyewolf/cnmysql/pkg/management/mysql/user"
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
	users      *user.Manager
	version    version.Version
	versionStr string
	expected   webserver.Role
	supervisor Supervisor
	backup     *BackupConfig
	// archiving, when set, supplies the continuous archiver's current state so it
	// surfaces in the instance status.
	archiving func() *webserver.ArchivingStatus
	// isolation, when set, fails the liveness probe once the instance has lost
	// contact with the Kubernetes API server for too long. nil disables the check.
	isolation *IsolationDetector
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
		users:      user.NewManager(conn),
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

// SetIsolationDetector wires the API-server isolation detector into the liveness
// probe. When set, Healthz fails if the instance can no longer reach the API
// server, so the kubelet restarts a partitioned container as a last resort.
func (c *Controller) SetIsolationDetector(detector *IsolationDetector) {
	c.isolation = detector
}

// Healthz reports liveness: the server answers a ping and the instance is not
// isolated from the Kubernetes API server.
func (c *Controller) Healthz(ctx context.Context) error {
	if err := c.conn.PingContext(ctx); err != nil {
		return err
	}
	return c.isolation.Check()
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

// SetSemiSyncWaitForReplicaCount adjusts, at runtime, how many replica
// acknowledgements the semi-sync source waits for. Used by the operator to
// self-heal semi-sync availability under "preferred" data durability.
func (c *Controller) SetSemiSyncWaitForReplicaCount(ctx context.Context, count int) error {
	return c.repl.SetSemiSyncWaitForReplicaCount(ctx, count)
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

// Shutdown stops mysqld via the supervisor. It sets innodb_fast_shutdown=2 for
// the fastest possible shutdown (just flush logs, crash recovery on next start)
// with a short timeout. Used as the fallback when a live demotion of a former
// primary fails.
func (c *Controller) Shutdown(ctx context.Context) error {
	if c.supervisor == nil {
		return errors.New("shutdown is not available: no supervisor configured")
	}
	logf.FromContext(ctx).WithName("instance-controller").Info("Shutting down mysqld", "instance", c.name)
	fastCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = setInnodbFastShutdown(fastCtx, c.conn, 2)
	return c.supervisor.Shutdown(ctx)
}

// setInnodbFastShutdown sets the innodb_fast_shutdown variable before mysqld
// termination. 0 = slow (full purge/merge), 1 = skip purge/merge (default),
// 2 = just flush logs (fastest, crash recovery on next start).
func setInnodbFastShutdown(ctx context.Context, conn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}, value int) error {
	_, err := conn.ExecContext(ctx, "SET GLOBAL innodb_fast_shutdown = ?", value)
	return err
}

// CreateUser creates a MySQL user and applies its grants.
func (c *Controller) CreateUser(ctx context.Context, req user.CreateUserRequest) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Creating user",
		"instance", c.name, "user", req.Name, "host", req.Host)
	return c.users.CreateUser(ctx, req)
}

// AlterUser mutates an existing MySQL user.
func (c *Controller) AlterUser(ctx context.Context, req user.AlterUserRequest) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Altering user",
		"instance", c.name, "user", req.Name, "host", req.Host)
	return c.users.AlterUser(ctx, req)
}

// DropUser removes a MySQL user.
func (c *Controller) DropUser(ctx context.Context, req user.DropUserRequest) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Dropping user",
		"instance", c.name, "user", req.Name, "host", req.Host)
	return c.users.DropUser(ctx, req)
}

// ListUsers reports the managed MySQL users and their attributes.
func (c *Controller) ListUsers(ctx context.Context) (*user.ListUsersResponse, error) {
	return c.users.ListUsers(ctx)
}

// CreateDatabase creates a MySQL schema.
func (c *Controller) CreateDatabase(ctx context.Context, req user.CreateDatabaseRequest) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Creating database",
		"instance", c.name, "database", req.Name)
	return c.users.CreateDatabase(ctx, req)
}

// DropDatabase drops a MySQL schema.
func (c *Controller) DropDatabase(ctx context.Context, req user.DropDatabaseRequest) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Dropping database",
		"instance", c.name, "database", req.Name)
	return c.users.DropDatabase(ctx, req)
}

// ListDatabases reports the user-managed MySQL schemas.
func (c *Controller) ListDatabases(ctx context.Context) (*user.ListDatabasesResponse, error) {
	return c.users.ListDatabases(ctx)
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
