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

// Package instance implements the InstanceController the control API serves: it
// builds the instance status and drives lifecycle actions by combining the
// connection pool and the replication manager.
package instance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	mysqlconfig "github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/config"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/executablehash"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/groupreplication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/pool"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/replication"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/user"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/version"
	"github.com/CloudNative-MySQL/cloudnative-mysql/pkg/management/mysql/webserver"
	"github.com/go-logr/logr"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Supervisor abstracts the mysqld process lifecycle so the controller can
// trigger a restart or shutdown without depending on the supervisor
// implementation.
type Supervisor interface {
	Restart(ctx context.Context) error
	Shutdown(ctx context.Context) error
	// Pid returns the supervised process PID (0 when nothing is running), so the
	// in-place upgrade path can hand it to the re-exec'd manager image to adopt.
	Pid() int
}

// Controller is the concrete webserver.InstanceController backed by a local
// mysqld connection.
type Controller struct {
	name       string
	conn       pool.Connection
	repl       *replication.Manager
	gr         *groupreplication.Manager
	users      *user.Manager
	version    version.Version
	versionStr string
	expected   webserver.Role
	supervisor Supervisor
	// groupReplication enables the Group Replication code paths (status GR block,
	// start/bootstrap). It stays false for async clusters so the async status path
	// is untouched and never queries the GR tables.
	groupReplication bool
	backup           *BackupConfig
	// archiving, when set, supplies the continuous archiver's current state so it
	// surfaces in the instance status.
	archiving func() *webserver.ArchivingStatus
	// isolation, when set, fails the liveness probe once the instance has lost
	// contact with the Kubernetes API server for too long. nil disables the check.
	isolation *IsolationDetector
	// fence, when set, lets the instance be fenced: mysqld is stopped while the
	// manager stays alive. nil disables fencing (no supervisor, e.g. in tests).
	fence *FenceGate
	// reExec performs the byte-identical in-place manager re-exec (restart-inplace).
	// It defaults to ReExecForUpgrade and is overridable in tests so the real
	// syscall.Exec (which would replace the test process) is not triggered.
	reExec func(mysqldPID int) error
	// writeManager streams and validates a new instance-manager binary, replacing
	// the on-disk binary. It defaults to WriteInstanceManager and is overridable in
	// tests so no real binary is written.
	writeManager func(r io.Reader, expectedHash string) error
	// reExecOnDisk re-execs the freshly written on-disk binary (the streamed
	// upgrade). It defaults to ReExecOnDiskForUpgrade and is overridable in tests.
	reExecOnDisk func(mysqldPID int) error
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
		name:         name,
		conn:         conn,
		repl:         replication.NewManager(conn, v),
		gr:           groupreplication.NewManager(conn, v),
		users:        user.NewManager(conn),
		version:      v,
		versionStr:   versionStr,
		expected:     expected,
		supervisor:   supervisor,
		reExec:       ReExecForUpgrade,
		writeManager: WriteInstanceManager,
		reExecOnDisk: ReExecOnDiskForUpgrade,
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
	// Under Group Replication readiness is bound to the member's group state: a
	// member serves consistent traffic only when ONLINE. This is the GR-health ⇄
	// readiness bridge — the kubelet turns each ONLINE/not-ONLINE transition into a
	// Pod Ready condition change the operator already watches via Owns(&Pod{}), so
	// member join/recover/expel becomes event-driven without operator polling.
	if c.groupReplication {
		return c.groupReplicationReady(ctx)
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

// groupReplicationReady reports readiness for a Group Replication member: the
// local member must appear in performance_schema.replication_group_members in the
// ONLINE state. A member that has not yet joined (no row for its server_uuid), is
// still RECOVERING, or is OFFLINE/ERROR/UNREACHABLE is not ready, so the kubelet
// keeps it out of the routing Services until the group accepts it.
func (c *Controller) groupReplicationReady(ctx context.Context) error {
	view, err := c.gr.ReadGroupView(ctx)
	if err != nil {
		return err
	}
	if !view.Configured {
		return errors.New("instance has not joined the group yet")
	}
	uuid, err := c.serverUUID(ctx)
	if err != nil {
		return err
	}
	for _, m := range view.Members {
		if m.MemberID != uuid {
			continue
		}
		if m.State == groupreplication.MemberStateOnline {
			return nil
		}
		return fmt.Errorf("group member is not ONLINE (state=%s)", m.State)
	}
	return errors.New("instance has not joined the group yet")
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
		InstanceName:     c.name,
		Version:          c.versionStr,
		Role:             c.role(replicaState),
		ReadOnly:         roState.ReadOnly,
		SuperReadOnly:    roState.SuperReadOnly,
		IsReady:          c.Readyz(ctx) == nil,
		InPlaceUpgrading: IsInPlaceUpgrading(),
	}

	if hash, err := executablehash.Get(); err == nil {
		status.ExecutableHash = hash
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

	// Under Group Replication, report this member's view of the group. It is the
	// signal the operator aggregates in observe() to set currentPrimary and the
	// status.groupReplication block. Left nil for async clusters and best-effort
	// (a transient read failure must not blank the rest of the status).
	if c.groupReplication {
		if gr := c.groupReplicationStatus(ctx); gr != nil {
			status.GroupReplication = gr
		}
	}

	return status, nil
}

// EnableGroupReplication turns on the Group Replication status and control paths.
// The run loop calls it for GR-mode clusters; async clusters leave it off.
func (c *Controller) EnableGroupReplication() {
	c.groupReplication = true
}

// groupReplicationStatus reads this member's view of the group from
// performance_schema.replication_group_members and shapes it into the webserver
// status. It returns nil when the member is not yet configured in a group (no
// rows), so a GR member that has not joined reports a nil block exactly like an
// async instance.
func (c *Controller) groupReplicationStatus(ctx context.Context) *webserver.GroupReplicationMemberStatus {
	view, err := c.gr.ReadGroupView(ctx)
	if err != nil || !view.Configured {
		return nil
	}

	gr := &webserver.GroupReplicationMemberStatus{
		Members: make([]webserver.GroupReplicationMember, 0, len(view.Members)),
	}
	if serverUUID, err := c.serverUUID(ctx); err == nil {
		gr.MemberID = serverUUID
	}
	for _, m := range view.Members {
		gr.Members = append(gr.Members, webserver.GroupReplicationMember{
			MemberID: m.MemberID,
			Host:     m.Host,
			Port:     m.Port,
			State:    m.State,
			Role:     m.Role,
		})
		if m.MemberID == gr.MemberID {
			gr.State = m.State
			gr.Role = m.Role
		}
		if m.Role == groupreplication.MemberRolePrimary {
			gr.PrimaryMemberID = m.MemberID
		}
	}
	if name, err := c.groupName(ctx); err == nil {
		gr.GroupName = name
	}
	return gr
}

// serverUUID reads this member's @@server_uuid, the key the group uses to
// identify it in replication_group_members.
func (c *Controller) serverUUID(ctx context.Context) (string, error) {
	var uuid string
	if err := c.conn.QueryRowContext(ctx, "SELECT @@global.server_uuid").Scan(&uuid); err != nil {
		return "", err
	}
	return uuid, nil
}

// groupName reads the active group_replication_group_name as this member sees it.
func (c *Controller) groupName(ctx context.Context) (string, error) {
	var name string
	if err := c.conn.QueryRowContext(ctx, "SELECT @@global.group_replication_group_name").Scan(&name); err != nil {
		return "", err
	}
	return name, nil
}

// GroupView reads the local member's view of the group. It backs the in-Pod GR
// role strategy's steady-state check.
func (c *Controller) GroupView(ctx context.Context) (groupreplication.GroupView, error) {
	return c.gr.ReadGroupView(ctx)
}

// PrepareGroupJoin readies a member to join the group via distributed recovery,
// before StartGroupReplication. It:
//
//   - Clears the GTIDs initdb authored locally (a fresh member only) so the group
//     does not see them as errant transactions, and forces a full clone so the
//     member is provisioned wholesale from a donor rather than by replaying the
//     donor's binlogs onto a server that already holds the cluster's accounts.
//   - Sets the distributed-recovery account on the group_replication_recovery
//     channel. The channel's TLS comes from the rendered
//     group_replication_recovery_ssl_* settings, so an X509 account needs no
//     password (empty password).
//
// A member that already holds group data (it cloned a donor and restarted, or is
// rejoining) is left to recover incrementally: its GTIDs are not self-authored,
// so neither the reset nor the forced clone runs.
func (c *Controller) PrepareGroupJoin(ctx context.Context, recoveryUser, password string) error {
	fresh, err := c.isFreshMember(ctx)
	if err != nil {
		return err
	}
	if fresh {
		if err := c.repl.ResetBinaryLogs(ctx); err != nil {
			return fmt.Errorf("clearing local GTIDs before group join: %w", err)
		}
		if err := c.gr.ForceClone(ctx); err != nil {
			return fmt.Errorf("forcing clone for group join: %w", err)
		}
	}
	return c.gr.ConfigureRecoveryChannel(ctx, recoveryUser, password)
}

// isFreshMember reports whether this member has never been part of the group and
// must therefore be provisioned wholesale (clear local initdb GTIDs + force a
// clone) rather than recovered incrementally.
//
// A member is fresh only when it holds GTIDs it authored itself (its own
// server_uuid, written by initdb) AND no group transactions. Group view-change
// events are logged under the group-name UUID, so any member that was ever part
// of the group — a rejoining replica or a restarted former primary — carries
// group-name GTIDs and must not be reset/cloned. This is essential for a former
// primary: it authored the group's data under its own server_uuid, so a
// self-authored check alone would misclassify it as fresh and wipe it into a
// clone loop. Members with group history are left to GR's distributed recovery,
// which decides incremental catch-up vs. clone vs. reject on its own.
func (c *Controller) isFreshMember(ctx context.Context) (bool, error) {
	uuid, err := c.serverUUID(ctx)
	if err != nil {
		return false, err
	}
	gtids, err := c.repl.GTIDExecuted(ctx)
	if err != nil {
		return false, err
	}
	if !strings.Contains(gtids, uuid) {
		// No self-authored GTIDs: empty (a brand-new server before any write) or
		// only a donor's GTIDs (already cloned). Either way, leave it to distributed
		// recovery rather than resetting and forcing another clone.
		return false, nil
	}
	groupName, err := c.groupName(ctx)
	if err != nil {
		return false, err
	}
	// Self-authored GTIDs but no group view-change history ⇒ a fresh, never-joined
	// member (initdb GTIDs only). With group history it is a returning member.
	return groupName == "" || !strings.Contains(gtids, groupName), nil
}

// StartGroupReplication joins an existing group via distributed recovery. It
// never bootstraps; the bootstrap member uses BootstrapGroup instead.
func (c *Controller) StartGroupReplication(ctx context.Context) error {
	return c.gr.Start(ctx)
}

// StopGroupReplication stops the member's Group Replication, making it leave
// the group while keeping mysqld alive (the GR fencing primitive). The member
// becomes super_read_only/OFFLINE but stays reachable for inspection. Unfence
// via StartGroupReplication rejoins via distributed recovery.
func (c *Controller) StopGroupReplication(ctx context.Context) error {
	return c.gr.Stop(ctx)
}

// BootstrapGroup runs the exactly-once group-creation sequence. The caller must
// gate it on being the designated bootstrap member with the group not yet
// bootstrapped (status.groupReplication.bootstrapped == false).
func (c *Controller) BootstrapGroup(ctx context.Context) error {
	return c.gr.Bootstrap(ctx)
}

// ForceGroupMembers executes group_replication_force_members with the given
// XCom addresses on this member, re-forming the group after a quorum loss.
func (c *Controller) ForceGroupMembers(ctx context.Context, addresses []string) error {
	return c.gr.ForceMembers(ctx, addresses)
}

// SetAsPrimary performs a planned primary change to the member with the given
// server_uuid via group_replication_set_as_primary on this member.
func (c *Controller) SetAsPrimary(ctx context.Context, memberUUID string) error {
	logf.FromContext(ctx).WithName("instance-controller").Info("Setting new primary",
		"instance", c.name, "targetUUID", memberUUID)
	return c.gr.SetAsPrimary(ctx, memberUUID)
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

// reExecDelay gives the HTTP response time to flush to the caller before the
// re-exec replaces the process image.
const reExecDelay = 250 * time.Millisecond

// RestartInPlace re-execs the instance manager in place, handing the running
// mysqld PID to the new image so it adopts the server instead of restarting it
// (the zero-restart operator-upgrade path). The re-exec is scheduled shortly after
// this returns so the caller receives the HTTP acknowledgement before
// syscall.Exec replaces the process; the caller then confirms the swap by polling
// status. A failed re-exec leaves the current manager supervising mysqld unharmed.
func (c *Controller) RestartInPlace(ctx context.Context) error {
	log := logf.FromContext(ctx).WithName("instance-controller")
	pid, err := c.adoptablePID("in-place restart")
	if err != nil {
		return err
	}
	c.scheduleReExec(log, pid, c.reExec)
	return nil
}

// UpgradeInstanceManager streams the new instance-manager binary, validates it
// against expectedHash, and writes it over the on-disk binary, then re-execs in
// place adopting the running mysqld. The write/validate is synchronous so a bad
// upload is rejected (the caller gets an error) before anything is swapped; the
// re-exec of the freshly written binary is scheduled after this returns so the
// HTTP acknowledgement flushes first.
func (c *Controller) UpgradeInstanceManager(ctx context.Context, r io.Reader, expectedHash string) error {
	log := logf.FromContext(ctx).WithName("instance-controller")
	pid, err := c.adoptablePID("in-place upgrade")
	if err != nil {
		return err
	}
	if err := c.writeManager(r, expectedHash); err != nil {
		log.Error(err, "Rejected in-place instance-manager upgrade", "instance", c.name)
		return err
	}
	log.Info("Wrote new instance-manager binary", "instance", c.name)
	c.scheduleReExec(log, pid, c.reExecOnDisk)
	return nil
}

// adoptablePID returns the running mysqld PID that the re-exec'd manager must
// adopt, or an error explaining why an in-place swap is unavailable.
func (c *Controller) adoptablePID(action string) (int, error) {
	if c.supervisor == nil {
		return 0, fmt.Errorf("%s is not available: no supervisor configured", action)
	}
	pid := c.supervisor.Pid()
	if pid <= 0 {
		return 0, fmt.Errorf("%s is not available: mysqld is not running", action)
	}
	return pid, nil
}

// scheduleReExec marks the upgrade in flight (so no concurrent shutdown path
// tears mysqld down mid-swap) and schedules reExec shortly after, giving the
// HTTP response time to flush before syscall.Exec replaces the process. A failed
// re-exec leaves the current manager supervising mysqld unharmed.
func (c *Controller) scheduleReExec(log logr.Logger, pid int, reExec func(int) error) {
	SetInPlaceUpgrading()
	log.Info("Scheduling in-place manager re-exec", "instance", c.name, "mysqldPid", pid)
	time.AfterFunc(reExecDelay, func() {
		if err := reExec(pid); err != nil {
			// Only reached if execve fails; mysqld keeps running under this manager.
			log.Error(err, "In-place manager re-exec failed; continuing with the current manager")
		}
	})
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
