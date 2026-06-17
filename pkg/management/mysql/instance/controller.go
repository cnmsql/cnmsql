/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
	"regexp"
	"sort"
	"strings"
	"time"

	mysqlconfig "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
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
	// fence, when set, lets the instance be fenced: mysqld is stopped while the
	// manager stays alive. nil disables fencing (no supervisor, e.g. in tests).
	fence *FenceGate
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

// SetFenceGate wires the fence gate shared with the run loop's mysqld
// supervisor, enabling Fence/Unfence.
func (c *Controller) SetFenceGate(gate *FenceGate) {
	c.fence = gate
}

// Fence stops mysqld while keeping the manager (PID 1) alive, so an operator can
// inspect or maintain the instance's data with the database down. It is
// idempotent: calling it while already fenced re-asserts the stopped state. The
// run loop's supervisor sees the intentional stop and does not treat it as a
// crash; Unfence restarts mysqld.
func (c *Controller) Fence(ctx context.Context) error {
	if c.fence == nil || c.supervisor == nil {
		return errors.New("fencing is not available in this context")
	}
	c.fence.Fence()
	// Shutdown is idempotent: a no-op once mysqld is already stopped.
	return c.supervisor.Shutdown(ctx)
}

// Unfence clears the fence so the supervisor restarts mysqld. It is idempotent
// and a no-op when the instance is not fenced.
func (c *Controller) Unfence(_ context.Context) error {
	if c.fence == nil {
		return nil
	}
	c.fence.Unfence()
	return nil
}

// Healthz reports liveness. liveness deliberately does
// not depend on mysqld being up: the manager answering this probe is itself the
// liveness signal, and the database may legitimately be stopped (for example
// while the instance is fenced). The only failure mode that warrants a kubelet
// restart is a primary that has lost contact with the Kubernetes API server,
// because a partitioned primary is a split-brain risk. A replica (or a
// non-cluster-managed instance) always passes; there is no point restarting an
// isolated replica.
func (c *Controller) Healthz(_ context.Context) error {
	if c.expected != webserver.RolePrimary {
		return nil
	}
	return c.isolation.Check()
}

// Startupz reports startup completion: mysqld is up and answering. Unlike
// readiness it does not gate on replication health, so a freshly cloned replica
// is considered "started" as soon as its server accepts connections, before it
// has caught up. This mirrors CloudNativePG's pg_isready-based startup probe.
func (c *Controller) Startupz(ctx context.Context) error {
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

// validVariableName matches a MySQL system-variable identifier. Variable names
// cannot be passed as a bound parameter to SET GLOBAL, so they are validated
// against this allowlist before being interpolated into the statement.
var validVariableName = regexp.MustCompile(`^[a-z0-9_]+$`)

// Reload re-applies dynamic configuration parameters to the running mysqld via
// SET GLOBAL, without restarting. Parameters that are operator-managed, are not
// valid identifiers, or are not settable at runtime (non-dynamic variables) are
// reported in the response's Skipped map rather than failing the whole request.
func (c *Controller) Reload(ctx context.Context, req webserver.ReloadRequest) (*webserver.ReloadResponse, error) {
	log := logf.FromContext(ctx).WithName("instance-controller")
	resp := &webserver.ReloadResponse{Skipped: map[string]string{}}

	// Apply in a deterministic order so logs and responses are stable.
	names := make([]string, 0, len(req.Parameters))
	for name := range req.Parameters {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		value := req.Parameters[name]
		variable := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(name)), "-", "_")
		if mysqlconfig.IsDeniedKey(variable) {
			resp.Skipped[name] = "operator-managed parameter, cannot be set at runtime"
			continue
		}
		if !validVariableName.MatchString(variable) {
			resp.Skipped[name] = "not a valid system-variable name"
			continue
		}
		if _, err := c.conn.ExecContext(ctx, "SET GLOBAL "+variable+" = ?", value); err != nil {
			// A non-dynamic variable (or a bad value) is reported per-parameter so
			// the operator can surface it; it does not fail the reload.
			resp.Skipped[name] = err.Error()
			continue
		}
		resp.Applied = append(resp.Applied, name)
	}

	log.Info("Reloaded dynamic parameters", "instance", c.name,
		"applied", len(resp.Applied), "skipped", len(resp.Skipped))
	return resp, nil
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
